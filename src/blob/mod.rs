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

use std::borrow::Cow;
use std::mem;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;

use backend::StoreBackend;
use errors::LockError;
use tags;
use util::FnBox;


mod blob;
mod index;
mod schema;
#[cfg(test)]
pub mod tests;

pub use self::blob::{Blob, ChunkRef, Kind};
pub use self::index::{BlobDesc, BlobIndex};


error_type! {
    #[derive(Debug)]
    pub enum MsgError {
        Lock(LockError) {
            cause;
        },
        Message(Cow<'static, str>) {
            desc (e) &**e;
            from (s: &'static str) s.into();
            from (s: String) s.into();
        },
    }
}


pub struct BlobStore<B>(Arc<Mutex<(StoreInner<B>, Option<i64>)>>);

pub struct StoreInner<B> {
    backend: Arc<B>,

    blob_index: Arc<BlobIndex>,
    blob_desc: BlobDesc,
    blob_refs: Vec<(ChunkRef, Box<FnBox<ChunkRef, ()>>)>,
    blob: Blob,

    max_blob_size: usize,
}

impl<B: StoreBackend> StoreInner<B> {
    fn new(index: Arc<BlobIndex>, backend: Arc<B>, max_blob_size: usize) -> StoreInner<B> {
        let mut bs = StoreInner {
            backend: backend,
            blob_index: index,
            blob_desc: Default::default(),
            blob_refs: Vec::new(),
            blob: Blob::new(max_blob_size),
            max_blob_size: max_blob_size,
        };
        bs.reserve_new_blob();
        bs
    }

    fn reserve_new_blob(&mut self) -> BlobDesc {
        mem::replace(&mut self.blob_desc, self.blob_index.reserve())
    }

    fn flush(&mut self) {
        let length = self.blob.upperbound_len();
        if length == 0 {
            return;
        }

        // Replace blob id
        let old_blob_desc = self.reserve_new_blob();

        let mut data = Vec::with_capacity(length);
        self.blob.into_bytes(&mut data);

        self.blob_index.in_air(&old_blob_desc);
        self.backend.store(&old_blob_desc.name[..], &data[..]).expect("Store operation failed");
        self.blob_index.commit_done(&old_blob_desc);

        // Go through callbacks
        while let Some((blobid, callback)) = self.blob_refs.pop() {
            callback.call(blobid);
        }
    }

    fn reset(&mut self) {
        self.blob_index.reset();
        self.blob = Blob::new(self.max_blob_size);
        self.reserve_new_blob();
    }

    fn store(&mut self,
             chunk: Vec<u8>,
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

        let mut id = ChunkRef {
            blob_id: self.blob_desc.name.clone(),
            offset: self.blob.chunk_len(),
            length: chunk.len(),
            kind: kind,
        };

        if let Err(chunk) = self.blob.try_append(chunk, &id) {
            self.flush();

            id.blob_id = self.blob_desc.name.clone();
            id.offset = 0;
            self.blob.try_append(chunk, &id).unwrap();
        }
        self.blob_refs.push((id.clone(), callback));

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
        BlobStore::new_with_poison(index, backend, max_blob_size, None)
    }

    pub fn new_with_poison(index: Arc<BlobIndex>,
                           backend: Arc<B>,
                           max_blob_size: usize,
                           poison_after: Option<i64>)
                           -> BlobStore<B> {
        BlobStore(Arc::new(Mutex::new((StoreInner::new(index, backend, max_blob_size),
                                       poison_after))))
    }

    /// Reset in-memory state of a poisoned process, making it available again.
    pub fn reset(&self) -> Result<(), MsgError> {
        let mut guard = try!(self.lock_ignore_poison());
        if guard.1 != Some(0) {
            return Err(From::from("This process has not been poisoned".to_string()));
        }
        guard.1 = None;

        guard.0.reset();
        Ok(())
    }

    fn lock_ignore_poison(&self) -> Result<MutexGuard<(StoreInner<B>, Option<i64>)>, LockError> {
        match self.0.lock() {
            Err(_) => return Err(LockError::Poisoned),
            Ok(lock) => Ok(lock),
        }
    }

    fn lock(&self) -> Result<MutexGuard<(StoreInner<B>, Option<i64>)>, LockError> {
        let mut guard = try!(self.lock_ignore_poison());

        match &mut guard.1 {
            &mut None => (),
            &mut Some(0) => return Err(LockError::RequestLimitReached),
            &mut Some(ref mut n) => {
                *n -= 1;
            }
        }

        Ok(guard)
    }

    /// Store a new data chunk into the current blob. The callback is triggered after the blob
    /// containing the chunk has been committed to persistent storage (it is then safe to use the
    /// `ChunkRef` as persistent reference).
    pub fn store(&self,
                 chunk: Vec<u8>,
                 kind: Kind,
                 callback: Box<FnBox<ChunkRef, ()>>)
                 -> Result<ChunkRef, LockError> {
        let mut guard = try!(self.lock());
        let res = guard.0.store(chunk, kind, callback);
        Ok(res)
    }

    /// Retrieve the data chunk identified by `ChunkRef`.
    pub fn retrieve(&self, id: &ChunkRef) -> Result<Option<Vec<u8>>, MsgError> {
        let mut guard = try!(self.lock());
        let res = try!(guard.0.retrieve(id));
        Ok(res)
    }

    /// Store a full named blob (used for writing root).
    pub fn store_named(&self, name: &str, data: &[u8]) -> Result<(), MsgError> {
        let mut guard = try!(self.lock());
        try!(guard.0.store_named(name, data));
        Ok(())
    }

    /// Retrieve full named blob.
    pub fn retrieve_named(&self, name: &str) -> Result<Option<Vec<u8>>, MsgError> {
        let mut guard = try!(self.lock());
        let res = try!(guard.0.retrieve_named(name));
        Ok(res)
    }

    /// Reinstall a blob recovered from external storage.
    pub fn recover(&self, chunk: ChunkRef) -> Result<(), LockError> {
        let mut guard = try!(self.lock());
        guard.0.recover(chunk);
        Ok(())
    }

    pub fn tag(&self, chunk: ChunkRef, tag: tags::Tag) -> Result<(), LockError> {
        let mut guard = try!(self.lock());
        guard.0.tag(chunk, tag);
        Ok(())
    }

    pub fn tag_all(&self, tag: tags::Tag) -> Result<(), LockError> {
        let mut guard = try!(self.lock());
        guard.0.tag_all(tag);
        Ok(())
    }

    pub fn delete_by_tag(&self, tag: tags::Tag) -> Result<(), MsgError> {
        let mut guard = try!(self.lock());
        try!(guard.0.delete_by_tag(tag));
        Ok(())
    }

    /// Flush the current blob, independent of its size.
    pub fn flush(&self) -> Result<(), LockError> {
        let mut guard = try!(self.lock());
        guard.0.flush();
        guard.0.blob_index.flush();
        Ok(())
    }
}
