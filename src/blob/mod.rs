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

use std::mem;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;

use capnp;
use root_capnp;

use backend::StoreBackend;
use tags;
use util::FnBox;


mod index;
mod schema;
#[cfg(test)]
pub mod tests;

pub use self::index::{BlobDesc, BlobIndex};

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


pub struct BlobStore<B>(Arc<Mutex<StoreInner<B>>>);

pub struct StoreInner<B> {
    backend: Arc<B>,

    blob_index: Arc<BlobIndex>,
    blob_desc: BlobDesc,

    blob_data: Vec<u8>,
    blob_refs: Vec<(ChunkRef, Box<FnBox<ChunkRef, ()>>)>,

    max_blob_size: usize,
}

impl<B: StoreBackend> StoreInner<B> {
    fn new(index: Arc<BlobIndex>, backend: Arc<B>, max_blob_size: usize) -> StoreInner<B> {
        let mut bs = StoreInner {
            backend: backend,
            blob_index: index,
            blob_desc: Default::default(),
            blob_refs: Vec::new(),
            blob_data: Vec::with_capacity(max_blob_size),
            max_blob_size: max_blob_size,
        };
        bs.reserve_new_blob();
        bs
    }

    fn reserve_new_blob(&mut self) -> BlobDesc {
        mem::replace(&mut self.blob_desc, self.blob_index.reserve())
    }

    fn flush(&mut self) {
        if self.blob_data.len() == 0 {
            return;
        }

        // Replace blob id
        let old_blob_desc = self.reserve_new_blob();

        let old_blob = mem::replace(&mut self.blob_data, Vec::with_capacity(self.max_blob_size));

        self.blob_index.in_air(&old_blob_desc);
        self.backend.store(&old_blob_desc.name[..], &old_blob[..]).expect("Store operation failed");
        self.blob_index.commit_done(&old_blob_desc);

        // Go through callbacks
        while let Some((blobid, callback)) = self.blob_refs.pop() {
            callback.call(blobid);
        }
    }

    fn maybe_flush(&mut self) {
        if self.blob_data.len() >= self.max_blob_size {
            self.flush();
        }
    }

    fn store(&mut self,
             mut chunk: Vec<u8>,
             kind: Kind,
             callback: Box<FnBox<ChunkRef, ()>>)
             -> ChunkRef {
        if chunk.is_empty() {
            let id = ChunkRef {
                blob_id: vec![0],
                offset: 0,
                length: 0,
                kind: kind,
            };
            let cb_id = id.clone();
            thread::spawn(move || callback.call(cb_id));
            return id;
        }

        let id = ChunkRef {
            blob_id: self.blob_desc.name.clone(),
            offset: self.blob_data.len(),
            length: chunk.len(),
            kind: kind,
        };

        self.blob_refs.push((id.clone(), callback));
        self.blob_data.append(&mut chunk);
        drop(chunk);  // now empty.

        // To avoid unnecessary blocking, we reply with the ID *before* possibly flushing.
        id
    }

    fn retrieve(&mut self, id: &ChunkRef) -> Result<Option<Vec<u8>>, String> {
        if id.offset == 0 && id.length == 0 {
            return Ok(Some(Vec::new()));
        }
        match self.backend.retrieve(&id.blob_id[..]) {
            Ok(Some(blob)) => Ok(Some(blob[id.offset..id.offset + id.length].to_vec())),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn store_named(&mut self, name: &str, data: &[u8]) -> Result<(), String> {
        try!(self.backend.store(name.as_bytes(), data));
        Ok(())
    }

    fn retrieve_named(&mut self, name: &str) -> Result<Option<Vec<u8>>, String> {
        self.backend.retrieve(name.as_bytes())
    }

    fn recover(&mut self, chunk: ChunkRef) {
        if chunk.offset == 0 && chunk.length == 0 {
            // This chunk is empty, so there is no blob to recover.
            return;
        }
        self.blob_index.recover(chunk.blob_id);
    }

    fn tag(&mut self, chunk: ChunkRef, tag: tags::Tag) {
        self.blob_index.tag(&BlobDesc {
                                id: 0,
                                name: chunk.blob_id,
                            },
                            tag);
    }

    fn tag_all(&mut self, tag: tags::Tag) {
        self.blob_index.tag_all(tag);
    }

    fn delete_by_tag(&mut self, tag: tags::Tag) -> Result<(), String> {
        let blobs = self.blob_index.list_by_tag(tag);
        for b in blobs.iter() {
            try!(self.backend.delete(&b.name));
        }
        self.blob_index.delete_by_tag(tag);
        Ok(())
    }
}

impl<B: StoreBackend> BlobStore<B> {
    pub fn new(index: Arc<BlobIndex>, backend: Arc<B>, max_blob_size: usize) -> BlobStore<B> {
        BlobStore(Arc::new(Mutex::new(StoreInner::new(index, backend, max_blob_size))))
    }

    fn lock(&self) -> MutexGuard<StoreInner<B>> {
        self.0.lock().expect("Blob store was poisoned")
    }

    /// Store a new data chunk into the current blob. The callback is triggered after the blob
    /// containing the chunk has been committed to persistent storage (it is then safe to use the
    /// `ChunkRef` as persistent reference).
    pub fn store(&self,
                 chunk: Vec<u8>,
                 kind: Kind,
                 callback: Box<FnBox<ChunkRef, ()>>)
                 -> ChunkRef {
        let mut guard = self.lock();
        let res = guard.store(chunk, kind, callback);
        drop(guard);

        let inner: Arc<Mutex<StoreInner<B>>> = self.0.clone();
        thread::spawn(move || {
            let mut guard = inner.lock().unwrap();
            guard.maybe_flush();
        });

        res
    }

    /// Retrieve the data chunk identified by `ChunkRef`.
    pub fn retrieve(&self, id: &ChunkRef) -> Result<Option<Vec<u8>>, String> {
        self.lock().retrieve(id)
    }

    /// Store a full named blob (used for writing root).
    pub fn store_named(&self, name: &str, data: &[u8]) -> Result<(), String> {
        self.lock().store_named(name, data)
    }

    /// Retrieve full named blob.
    pub fn retrieve_named(&self, name: &str) -> Result<Option<Vec<u8>>, String> {
        self.lock().retrieve_named(name)
    }

    /// Reinstall a blob recovered from external storage.
    pub fn recover(&self, chunk: ChunkRef) {
        self.lock().recover(chunk)
    }

    pub fn tag(&self, chunk: ChunkRef, tag: tags::Tag) {
        self.lock().tag(chunk, tag)
    }

    pub fn tag_all(&self, tag: tags::Tag) {
        self.lock().tag_all(tag)
    }

    pub fn delete_by_tag(&self, tag: tags::Tag) -> Result<(), String> {
        self.lock().delete_by_tag(tag)
    }

    /// Flush the current blob, independent of its size.
    pub fn flush(&self) {
        let mut guard = self.lock();
        guard.flush();
        guard.blob_index.flush();
    }
}
