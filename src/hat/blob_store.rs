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
use std::sync::{Arc, Mutex};
use std::thread;

use rustc_serialize;
use rustc_serialize::hex::ToHex;

use std::collections::BTreeMap;

use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::str;

use process::{Process, MsgHandler};

use blob_index;
use blob_index::BlobIndexProcess;

#[cfg(test)]
use blob_index::BlobIndex;

use tags;


pub type BlobStoreProcess = Process<Msg, Reply>;

pub trait BlobStoreBackend {
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
    pub fn new(root: PathBuf) -> FileBackend {
        FileBackend {
            root: root,
            read_cache: Arc::new(Mutex::new(BTreeMap::new())),
            max_cache_size: 10,
        }
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

impl BlobStoreBackend for FileBackend {
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
        match value_opt {
            Some(result) => return result,
            None => (),
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

        return res;
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


#[derive(Debug, Clone, Eq, PartialEq, RustcEncodable, RustcDecodable)]
pub struct BlobID {
    name: Vec<u8>,
    begin: usize,
    end: usize,
}

impl BlobID {
    pub fn from_bytes(bytes: Vec<u8>) -> BlobID {
        return rustc_serialize::json::decode(str::from_utf8(&bytes[..]).unwrap()).unwrap();
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        return rustc_serialize::json::encode(&self).unwrap().as_bytes().to_vec();
    }
}


pub enum Msg {
    /// Store a new data chunk into the current blob. The callback is triggered after the blob
    /// containing the chunk has been committed to persistent storage (it is then safe to use the
    /// `BlobID` as persistent reference).
    Store(Vec<u8>, Box<FnBox(BlobID) + Send>),
    /// Retrieve the data chunk identified by `BlobID`.
    Retrieve(BlobID),
    /// Tag helpers.
    Tag(BlobID, tags::Tag),
    TagAll(tags::Tag),
    DeleteByTag(tags::Tag),
    /// Flush the current blob, independent of its size.
    Flush,
}


#[derive(Eq, PartialEq, Debug)]
pub enum Reply {
    StoreOk(BlobID),
    RetrieveOk(Vec<u8>),
    FlushOk,
    Ok,
}


pub struct BlobStore<B> {
    backend: B,

    blob_index: BlobIndexProcess,
    blob_desc: blob_index::BlobDesc,

    buffer_data: Vec<(BlobID, Vec<u8>, Box<FnBox(BlobID) + Send>)>,
    buffer_data_len: usize,

    max_blob_size: usize,
}


fn empty_blob_desc() -> blob_index::BlobDesc {
    blob_index::BlobDesc {
        name: vec![],
        id: 0,
    }
}


impl<B: BlobStoreBackend> BlobStore<B> {
    pub fn new(index: BlobIndexProcess, backend: B, max_blob_size: usize) -> BlobStore<B> {
        let mut bs = BlobStore {
            backend: backend,
            blob_index: index,
            blob_desc: empty_blob_desc(),
            buffer_data: Vec::new(),
            buffer_data_len: 0,
            max_blob_size: max_blob_size,
        };
        bs.reserve_new_blob();
        bs
    }

    #[cfg(test)]
    pub fn new_for_testing(backend: B, max_blob_size: usize) -> BlobStore<B> {
        let bi_p = Process::new(Box::new(move || BlobIndex::new_for_testing()));
        let mut bs = BlobStore {
            backend: backend,
            blob_index: bi_p,
            blob_desc: empty_blob_desc(),
            buffer_data: Vec::new(),
            buffer_data_len: 0,
            max_blob_size: max_blob_size,
        };
        bs.reserve_new_blob();
        bs
    }

    fn reserve_new_blob(&mut self) -> blob_index::BlobDesc {
        let old_blob_desc = self.blob_desc.clone();

        let res = self.blob_index.send_reply(blob_index::Msg::Reserve);
        match res {
            blob_index::Reply::Reserved(blob_desc) => {
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

        self.blob_index.send_reply(blob_index::Msg::InAir(old_blob_desc.clone()));
        self.backend_store(&old_blob_desc.name[..], &blob[..]);
        self.blob_index.send_reply(blob_index::Msg::CommitDone(old_blob_desc));

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

impl<B: BlobStoreBackend> MsgHandler<Msg, Reply> for BlobStore<B> {
    fn handle(&mut self, msg: Msg, reply: Box<Fn(Reply)>) {
        match msg {
            Msg::Store(blob, callback) => {
                if blob.is_empty() {
                    let id = BlobID {
                        name: vec![0],
                        begin: 0,
                        end: 0,
                    };
                    let cb_id = id.clone();
                    thread::spawn(move || callback(cb_id));
                    return reply(Reply::StoreOk(id));
                }

                let new_size = self.buffer_data_len + blob.len();
                let id = BlobID {
                    name: self.blob_desc.name.clone(),
                    begin: self.buffer_data_len,
                    end: new_size,
                };

                self.buffer_data_len = new_size;
                self.buffer_data.push((id.clone(), blob, callback));

                // To avoid unnecessary blocking, we reply with the ID *before* possibly flushing.
                reply(Reply::StoreOk(id));

                // Flushing can be expensive, so try not block on it.
                self.maybe_flush();
            }
            Msg::Retrieve(id) => {
                if id.begin == 0 && id.end == 0 {
                    return reply(Reply::RetrieveOk(vec![]));
                }
                let blob = self.backend_read(&id.name[..]);
                let chunk = &blob[id.begin..id.end];
                return reply(Reply::RetrieveOk(chunk.to_vec()));
            }
            Msg::Tag(blob, tag) => {
                match self.blob_index.send_reply(blob_index::Msg::Tag(blob_index::BlobDesc {
                                                                          id: 0,
                                                                          name: blob.name,
                                                                      },
                                                                      tag)) {
                    blob_index::Reply::Ok => reply(Reply::Ok),
                    _ => panic!("Unexpected reply from blob index."),
                }
            }
            Msg::TagAll(tag) => {
                match self.blob_index.send_reply(blob_index::Msg::TagAll(tag)) {
                    blob_index::Reply::Ok => reply(Reply::Ok),
                    _ => panic!("Unexpected reply from blob index."),
                }
            }
            Msg::DeleteByTag(tag) => {
                let blobs = match self.blob_index
                                      .send_reply(blob_index::Msg::ListByTag(tag.clone())) {
                    blob_index::Reply::Listing(ch) => ch,
                    _ => panic!("Unexpected reply from blob index."),
                };
                for b in blobs.iter() {
                    match self.backend.delete(&b.name) {
                        Ok(_) => (),
                        Err(e) => println!("Could not delete {}: {}", b.name.to_hex(), e),
                    }
                }
                match self.blob_index.send_reply(blob_index::Msg::DeleteByTag(tag)) {
                    blob_index::Reply::Ok => (),
                    _ => panic!("Unexpected reply from blob index."),
                }
                return reply(Reply::Ok);
            }
            Msg::Flush => {
                self.flush();
                match self.blob_index.send_reply(blob_index::Msg::Flush) {
                    blob_index::Reply::CommitOk => (),
                    _ => panic!("Unexpected reply from blob index."),
                }
                return reply(Reply::FlushOk);
            }
        }
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

    impl BlobStoreBackend for MemoryBackend {
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

    impl BlobStoreBackend for DevNullBackend {
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
            let bs_p: BlobStoreProcess = Process::new(Box::new(move || {
                BlobStore::new_for_testing(local_backend, 1024)
            }));

            let mut ids = Vec::new();
            for chunk in chunks.iter() {
                match bs_p.send_reply(Msg::Store(chunk.to_owned(), Box::new(move |_| {}))) {
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
                    match backend.retrieve(&id.name[..]) {
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
            let bs_p: BlobStoreProcess = Process::new(Box::new(move || {
                BlobStore::new_for_testing(local_backend, 1024)
            }));

            let mut ids = Vec::new();
            for chunk in chunks.iter() {
                match bs_p.send_reply(Msg::Store(chunk.to_owned(), Box::new(move |_| {}))) {
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
                    match backend.retrieve(&id.name[..]) {
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
        fn prop(name: Vec<u8>, begin: usize, end: usize) -> bool {
            let blob_id = BlobID {
                name: name.to_vec(),
                begin: begin,
                end: end,
            };
            BlobID::from_bytes(blob_id.as_bytes()) == blob_id
        }
        quickcheck::quickcheck(prop as fn(Vec<u8>, usize, usize) -> bool);
    }

}
