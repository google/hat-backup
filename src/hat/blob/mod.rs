// Copyright 2014 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Combines data chunks into larger blobs to be stored externally.

use std::boxed::FnBox;
use std::collections::BTreeMap;

use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;

use std::sync::{mpsc, Arc, Mutex};
use std::thread;

use rustc_serialize::hex::ToHex;

use capnp;
use root_capnp;

use tags;
use process::{Process, MsgHandler};


mod index;
mod schema;

pub use self::index::{Index, IndexError, BlobDesc};


error_type! {
    #[derive(Debug)]
    pub enum MsgError {
        Recv(mpsc::RecvError) {
            cause;
        },
        Index(IndexError) {
            cause;
        }
    }
}


pub type StoreProcess = Process<Msg, Reply>;

pub trait StoreBackend {
    fn store(&mut self, name: &[u8], data: &[u8]) -> Result<(), String>;
    fn retrieve(&mut self, name: &[u8]) -> Result<Vec<u8>, String>;
    fn delete(&mut self, name: &[u8]) -> Result<(), String>;
}


#[derive(Clone)]
pub struct FileBackend {
    root: PathBuf,
    read_cache: Arc<Mutex<BTreeMap<Vec<u8>, Result<Vec<u8>, String>>>>,
    max_cache_size: usize,
}

impl FileBackend {
    pub fn new(root: PathBuf) -> Result<FileBackend, MsgError> {
        Ok(FileBackend {
            root: root,
            read_cache: Arc::new(Mutex::new(BTreeMap::new())),
            max_cache_size: 10,
        })
    }

    fn guarded_cache_get(&self, name: &[u8]) -> Option<Result<Vec<u8>, String>> {
        self.read_cache.lock().unwrap().get(name).cloned()
    }

    fn guarded_cache_delete(&self, name: &[u8]) {
        self.read_cache.lock().unwrap().remove(name);
    }

    fn guarded_cache_put(&mut self, name: Vec<u8>, result: Result<Vec<u8>, String>) {
        let mut cache = self.read_cache.lock().unwrap();
        if cache.len() >= self.max_cache_size {
            cache.clear();
        }
        cache.insert(name, result);
    }
}

impl StoreBackend for FileBackend {
    fn store(&mut self, name: &[u8], data: &[u8]) -> Result<(), String> {
        let mut path = self.root.clone();
        path.push(&name.to_hex());

        let mut file = match fs::File::create(&path) {
            Err(e) => return Err(e.to_string()),
            Ok(f) => f,
        };

        match file.write_all(data) {
            Err(e) => Err(e.to_string()),
            Ok(()) => Ok(()),
        }
    }

    fn retrieve(&mut self, name: &[u8]) -> Result<Vec<u8>, String> {
        // Check for key in cache:
        let value_opt = self.guarded_cache_get(name);
        if let Some(r) = value_opt {
            return r;
        }

        // Read key:
        let path = {
            let mut p = self.root.clone();
            p.push(&name.to_hex());
            p
        };

        let mut fd = fs::File::open(&path).unwrap();
        let mut buf = Vec::new();
        let res = match fd.read_to_end(&mut buf) {
            Ok(_) => Ok(buf),
            Err(e) => Err(e.to_string()),
        };

        // Update cache to contain key:
        self.guarded_cache_put(name.to_vec(), res.clone());

        res
    }

    fn delete(&mut self, name: &[u8]) -> Result<(), String> {
        let name = name.to_vec();
        self.guarded_cache_delete(&name);

        let path = {
            let mut p = self.root.clone();
            p.push(&name.to_hex());
            p
        };

        match fs::remove_file(&path) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Kind {
    TreeBranch = 1,
    TreeLeaf = 2,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ChunkRef {
    pub blob_id: Vec<u8>,
    pub offset: usize,
    pub length: usize,
    pub kind: Kind,
}

impl ChunkRef {
    pub fn from_bytes(bytes: &mut &[u8]) -> Result<ChunkRef, capnp::Error> {
        let reader = try!(capnp::serialize_packed::read_message(bytes,
                                                       capnp::message::ReaderOptions::new()));
        let root = try!(reader.get_root::<root_capnp::chunk_ref::Reader>());

        Ok(try!(ChunkRef::read_msg(&root)))
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut message = ::capnp::message::Builder::new_default();
        {
            let mut root = message.init_root::<root_capnp::chunk_ref::Builder>();
            self.populate_msg(root.borrow());
        }

        let mut out = Vec::new();
        capnp::serialize_packed::write_message(&mut out, &message).unwrap();

        out
    }

    pub fn populate_msg(&self, msg: root_capnp::chunk_ref::Builder) {
        let mut msg = msg;
        msg.set_blob_id(&self.blob_id[..]);
        msg.set_offset(self.offset as i64);
        msg.set_length(self.length as i64);
        match self.kind {
            Kind::TreeLeaf => msg.init_kind().set_tree_leaf(()),
            Kind::TreeBranch => msg.init_kind().set_tree_branch(()),
        }
    }

    pub fn read_msg(msg: &root_capnp::chunk_ref::Reader) -> Result<ChunkRef, capnp::Error> {
        Ok(ChunkRef {
            blob_id: try!(msg.get_blob_id()).to_owned(),
            offset: msg.get_offset() as usize,
            length: msg.get_length() as usize,
            kind: match try!(msg.get_kind().which()) {
                root_capnp::chunk_ref::kind::TreeBranch(()) => Kind::TreeBranch,
                root_capnp::chunk_ref::kind::TreeLeaf(()) => Kind::TreeLeaf,
            },
        })
    }
}


pub enum Msg {
    /// Store a new data chunk into the current blob. The callback is triggered after the blob
    /// containing the chunk has been committed to persistent storage (it is then safe to use the
    /// `ChunkRef` as persistent reference).
    Store(Vec<u8>, Kind, Box<FnBox(ChunkRef) + Send>),
    /// Retrieve the data chunk identified by `ChunkRef`.
    Retrieve(ChunkRef),
    /// Store a full named blob (used for writing root).
    StoreNamed(String, Vec<u8>),
    /// Retrieve full named blob.
    RetrieveNamed(String),
    /// Reinstall a blob recovered from external storage.
    Recover(ChunkRef),
    /// Tag helpers.
    Tag(ChunkRef, tags::Tag),
    TagAll(tags::Tag),
    DeleteByTag(tags::Tag),
    /// Flush the current blob, independent of its size.
    Flush,
}


#[derive(Eq, PartialEq, Debug)]
pub enum Reply {
    StoreOk(ChunkRef),
    StoreNamedOk(String),
    RetrieveOk(Vec<u8>),
    RecoverOk,
    FlushOk,
    Ok,
}


pub struct Store<B> {
    backend: B,

    blob_index: index::IndexProcess,
    blob_desc: BlobDesc,

    buffer_data: Vec<(ChunkRef, Vec<u8>, Box<FnBox(ChunkRef) + Send>)>,
    buffer_data_len: usize,

    max_blob_size: usize,
}


fn empty_blob_desc() -> BlobDesc {
    BlobDesc {
        name: vec![],
        id: 0,
    }
}


impl<B: StoreBackend> Store<B> {
    pub fn new(index: index::IndexProcess,
               backend: B,
               max_blob_size: usize)
               -> Result<Store<B>, MsgError> {
        let mut bs = Store {
            backend: backend,
            blob_index: index,
            blob_desc: empty_blob_desc(),
            buffer_data: Vec::new(),
            buffer_data_len: 0,
            max_blob_size: max_blob_size,
        };
        bs.reserve_new_blob();
        Ok(bs)
    }

    #[cfg(test)]
    pub fn new_for_testing(backend: B, max_blob_size: usize) -> Result<Store<B>, MsgError> {
        let bi_p = try!(Process::new(move || index::Index::new_for_testing()));
        let mut bs = Store {
            backend: backend,
            blob_index: bi_p,
            blob_desc: empty_blob_desc(),
            buffer_data: Vec::new(),
            buffer_data_len: 0,
            max_blob_size: max_blob_size,
        };
        bs.reserve_new_blob();
        Ok(bs)
    }

    fn reserve_new_blob(&mut self) -> BlobDesc {
        let old_blob_desc = self.blob_desc.clone();

        let res = self.blob_index.send_reply(index::Msg::Reserve);
        match res {
            index::Reply::Reserved(blob_desc) => {
                self.blob_desc = blob_desc;
            }
            _ => panic!("Could not reserve blob."),
        }

        old_blob_desc
    }

    fn backend_store(&mut self, name: &[u8], blob: &[u8]) {
        match self.backend.store(name, blob) {
            Ok(()) => (),
            Err(s) => panic!(s),
        }
    }

    fn backend_read(&mut self, name: &[u8]) -> Vec<u8> {
        match self.backend.retrieve(name) {
            Ok(data) => data,
            Err(s) => panic!(s),
        }
    }

    fn flush(&mut self) {
        if self.buffer_data_len == 0 {
            return;
        }

        // Replace blob id
        let old_blob_desc = self.reserve_new_blob();
        self.buffer_data_len = 0;

        // Prepare blob
        let mut ready_callback = Vec::new();
        let mut blob = Vec::new();

        self.buffer_data.reverse();
        while let Some((chunk_ref, chunk, cb)) = self.buffer_data.pop() {
            ready_callback.push((chunk_ref, cb));
            for c in chunk {
                blob.push(c);
            }
        }

        self.blob_index.send_reply(index::Msg::InAir(old_blob_desc.clone()));
        self.backend_store(&old_blob_desc.name[..], &blob[..]);
        self.blob_index.send_reply(index::Msg::CommitDone(old_blob_desc));

        // Go through callbacks
        for (blobid, callback) in ready_callback.into_iter() {
            callback(blobid);
        }
    }

    fn maybe_flush(&mut self) {
        if self.buffer_data_len >= self.max_blob_size {
            self.flush();
        }
    }
}

impl<B: StoreBackend> MsgHandler<Msg, Reply> for Store<B> {
    type Err = MsgError;

    fn handle(&mut self, msg: Msg, reply: Box<Fn(Reply)>) -> Result<(), MsgError> {
        match msg {
            Msg::Store(chunk, kind, callback) => {
                if chunk.is_empty() {
                    let id = ChunkRef {
                        blob_id: vec![0],
                        offset: 0,
                        length: 0,
                        kind: kind,
                    };
                    let cb_id = id.clone();
                    thread::spawn(move || callback(cb_id));
                    reply(Reply::StoreOk(id));
                    return Ok(());
                }

                let id = ChunkRef {
                    blob_id: self.blob_desc.name.clone(),
                    offset: self.buffer_data_len,
                    length: chunk.len(),
                    kind: kind,
                };

                let new_size = self.buffer_data_len + chunk.len();
                self.buffer_data_len = new_size;
                self.buffer_data.push((id.clone(), chunk, callback));

                // To avoid unnecessary blocking, we reply with the ID *before* possibly flushing.
                reply(Reply::StoreOk(id));

                // Flushing can be expensive, so try not block on it.
                self.maybe_flush();
            }
            Msg::Retrieve(id) => {
                if id.offset == 0 && id.length == 0 {
                    reply(Reply::RetrieveOk(vec![]));
                    return Ok(());
                }
                let blob = self.backend_read(&id.blob_id[..]);
                let chunk = &blob[id.offset..id.offset + id.length];
                reply(Reply::RetrieveOk(chunk.to_vec()));
            }
            Msg::StoreNamed(name, data) => {
                self.backend.store(name.as_bytes(), &data[..]).ok();
                reply(Reply::StoreNamedOk(name));
            }
            Msg::RetrieveNamed(name) => {
                reply(Reply::RetrieveOk(self.backend_read(name.as_bytes())));
            }
            Msg::Recover(chunk) => {
                if chunk.offset == 0 && chunk.length == 0 {
                    // This chunk is empty, so there is no blob to recover.
                    reply(Reply::RecoverOk);
                    return Ok(());
                }
                match self.blob_index.send_reply(index::Msg::Recover(chunk.blob_id)) {
                    index::Reply::RecoverOk(..) => reply(Reply::RecoverOk),
                    _ => panic!("Unexpected reply from blob index."),
                }
            }
            Msg::Tag(chunk, tag) => {
                match self.blob_index.send_reply(index::Msg::Tag(BlobDesc {
                                                                     id: 0,
                                                                     name: chunk.blob_id,
                                                                 },
                                                                 tag)) {
                    index::Reply::Ok => reply(Reply::Ok),
                    _ => panic!("Unexpected reply from blob index."),
                }
            }
            Msg::TagAll(tag) => {
                match self.blob_index.send_reply(index::Msg::TagAll(tag)) {
                    index::Reply::Ok => reply(Reply::Ok),
                    _ => panic!("Unexpected reply from blob index."),
                }
            }
            Msg::DeleteByTag(tag) => {
                let blobs = match self.blob_index
                                      .send_reply(index::Msg::ListByTag(tag.clone())) {
                    index::Reply::Listing(ch) => ch,
                    _ => panic!("Unexpected reply from blob index."),
                };
                for b in blobs.iter() {
                    match self.backend.delete(&b.name) {
                        Ok(_) => (),
                        Err(e) => println!("Could not delete {}: {}", b.name.to_hex(), e),
                    }
                }
                match self.blob_index.send_reply(index::Msg::DeleteByTag(tag)) {
                    index::Reply::Ok => (),
                    _ => panic!("Unexpected reply from blob index."),
                }
                reply(Reply::Ok);
            }
            Msg::Flush => {
                self.flush();
                match self.blob_index.send_reply(index::Msg::Flush) {
                    index::Reply::CommitOk => (),
                    _ => panic!("Unexpected reply from blob index."),
                }
                reply(Reply::FlushOk);
            }
        }

        Ok(())
    }
}


#[cfg(test)]
pub mod tests {
    use super::*;

    use process::Process;

    use std::sync::{Arc, Mutex};
    use std::collections::BTreeMap;
    use quickcheck;

    #[derive(Clone)]
    pub struct MemoryBackend {
        files: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    }

    impl MemoryBackend {
        pub fn new() -> MemoryBackend {
            MemoryBackend { files: Arc::new(Mutex::new(BTreeMap::new())) }
        }

        fn guarded_insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String> {
            let mut guarded_files = self.files.lock().unwrap();
            if guarded_files.contains_key(&key) {
                return Err(format!("Key already exists: '{:?}'", key));
            }
            guarded_files.insert(key, value);
            Ok(())
        }

        fn guarded_retrieve(&mut self, key: &[u8]) -> Result<Vec<u8>, String> {
            let value_opt = self.files.lock().unwrap().get(&key.to_vec()).map(|v| v.clone());
            value_opt.map(|v| Ok(v)).unwrap_or_else(|| Err(format!("Unknown key: '{:?}'", key)))
        }

        fn guarded_delete(&mut self, key: &[u8]) -> Result<(), String> {
            let mut guarded_files = self.files.lock().unwrap();
            guarded_files.remove(key);
            Ok(())
        }
    }

    impl StoreBackend for MemoryBackend {
        fn store(&mut self, name: &[u8], data: &[u8]) -> Result<(), String> {
            self.guarded_insert(name.to_vec(), data.to_vec())
        }

        fn retrieve(&mut self, name: &[u8]) -> Result<Vec<u8>, String> {
            self.guarded_retrieve(name)
        }

        fn delete(&mut self, name: &[u8]) -> Result<(), String> {
            self.guarded_delete(name)
        }
    }

    #[derive(Clone)]
    pub struct DevNullBackend;

    impl StoreBackend for DevNullBackend {
        fn store(&mut self, _name: &[u8], _data: &[u8]) -> Result<(), String> {
            Ok(())
        }
        fn retrieve(&mut self, name: &[u8]) -> Result<Vec<u8>, String> {
            Err(format!("Unknown key: '{:?}'", name))
        }
        fn delete(&mut self, _name: &[u8]) -> Result<(), String> {
            Ok(())
        }
    }


    #[test]
    fn identity() {
        fn prop(chunks: Vec<Vec<u8>>) -> bool {
            let mut backend = MemoryBackend::new();

            let local_backend = backend.clone();
            let bs_p: StoreProcess = Process::new(move || {
                                         Store::new_for_testing(local_backend, 1024)
                                     })
                                         .unwrap();

            let mut ids = Vec::new();
            for chunk in chunks.iter() {
                match bs_p.send_reply(Msg::Store(chunk.to_owned(),
                                                 Kind::TreeLeaf,
                                                 Box::new(move |_| {}))) {
                    Reply::StoreOk(id) => {
                        ids.push((id, chunk));
                    }
                    _ => panic!("Unexpected reply from blob store."),
                }
            }

            assert_eq!(bs_p.send_reply(Msg::Flush), Reply::FlushOk);

            // Non-empty chunks must be in the backend now:
            for &(ref id, chunk) in ids.iter() {
                if chunk.len() > 0 {
                    match backend.retrieve(&id.blob_id[..]) {
                        Ok(_) => (),
                        Err(e) => panic!(e),
                    }
                }
            }

            // All chunks must be available through the blob store:
            for &(ref id, chunk) in ids.iter() {
                match bs_p.send_reply(Msg::Retrieve(id.clone())) {
                    Reply::RetrieveOk(found_chunk) => assert_eq!(found_chunk, chunk.to_owned()),
                    _ => panic!("Unexpected reply from blob store."),
                }
            }

            return true;
        }
        quickcheck::quickcheck(prop as fn(Vec<Vec<u8>>) -> bool);
    }

    #[test]
    fn identity_with_excessive_flushing() {
        fn prop(chunks: Vec<Vec<u8>>) -> bool {
            let mut backend = MemoryBackend::new();

            let local_backend = backend.clone();
            let bs_p: StoreProcess = Process::new(move || {
                                         Store::new_for_testing(local_backend, 1024)
                                     })
                                         .unwrap();

            let mut ids = Vec::new();
            for chunk in chunks.iter() {
                match bs_p.send_reply(Msg::Store(chunk.to_owned(),
                                                 Kind::TreeLeaf,
                                                 Box::new(move |_| {}))) {
                    Reply::StoreOk(id) => {
                        ids.push((id, chunk));
                    }
                    _ => panic!("Unexpected reply from blob store."),
                }
                assert_eq!(bs_p.send_reply(Msg::Flush), Reply::FlushOk);
                let &(ref id, chunk) = ids.last().unwrap();
                assert_eq!(bs_p.send_reply(Msg::Retrieve(id.clone())),
                           Reply::RetrieveOk(chunk.clone()));
            }

            // Non-empty chunks must be in the backend now:
            for &(ref id, chunk) in ids.iter() {
                if chunk.len() > 0 {
                    match backend.retrieve(&id.blob_id[..]) {
                        Ok(_) => (),
                        Err(e) => panic!(e),
                    }
                }
            }

            // All chunks must be available through the blob store:
            for &(ref id, chunk) in ids.iter() {
                match bs_p.send_reply(Msg::Retrieve(id.clone())) {
                    Reply::RetrieveOk(found_chunk) => assert_eq!(found_chunk, chunk.to_owned()),
                    _ => panic!("Unexpected reply from blob store."),
                }
            }

            return true;
        }
        quickcheck::quickcheck(prop as fn(Vec<Vec<u8>>) -> bool);
    }

    #[test]
    fn blobid_identity() {
        fn prop(name: Vec<u8>, offset: usize, length: usize) -> bool {
            let blob_id = ChunkRef {
                blob_id: name.to_vec(),
                offset: offset,
                length: length,
                kind: Kind::TreeBranch,
            };
            let blob_id_bytes = blob_id.as_bytes();
            ChunkRef::from_bytes(&mut &blob_id_bytes[..]).unwrap() == blob_id
        }
        quickcheck::quickcheck(prop as fn(Vec<u8>, usize, usize) -> bool);
    }

}
