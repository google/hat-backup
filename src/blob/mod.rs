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


use backend::StoreBackend;
use capnp;
use errors;
use hash::Hash;
use hash::tree::HashRef;
use std::borrow::Cow;
use std::mem;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use tags;
use util::FnBox;


mod chunk;
mod blob;
mod index;
#[cfg(test)]
pub mod tests;

#[cfg(all(test, feature = "benchmarks"))]
mod benchmarks;


pub use self::blob::Blob;
pub use self::chunk::{ChunkRef, Key, Kind, Packing, node_from_height, node_height};
pub use self::index::{BlobDesc, BlobIndex};


error_type! {
    #[derive(Debug)]
    pub enum BlobError {
        Message(Cow<'static, str>) {
            desc (e) &**e;
            from (s: &'static str) s.into();
            from (s: String) s.into();
        },
        CryptoError(errors::CryptoError) {
            cause;
        },
        DataSerialization(capnp::Error) {
            cause;
        }
    }
}

pub struct BlobStore<B>(Arc<Mutex<StoreInner<B>>>);

pub struct StoreInner<B> {
    backend: Arc<B>,
    max_blob_size: usize,

    blob_index: Arc<BlobIndex>,
    blob_desc: BlobDesc,
    blob_refs: Vec<(HashRef, Box<FnBox<HashRef, ()>>)>,
    blob: Blob,
}

impl<B: StoreBackend> StoreInner<B> {
    fn new(index: Arc<BlobIndex>, backend: Arc<B>, max_blob_size: usize) -> StoreInner<B> {
        let mut bs = StoreInner {
            backend: backend,
            blob_index: index,
            blob_desc: Default::default(),
            blob_refs: Vec::new(),
            max_blob_size: max_blob_size,
            blob: Blob::new(max_blob_size),
        };
        bs.reserve_new_blob();
        bs
    }

    fn reserve_new_blob(&mut self) -> BlobDesc {
        mem::replace(&mut self.blob_desc, self.blob_index.reserve())
    }

    fn flush(&mut self) {
        let ct = match self.blob.to_ciphertext() {
            None => return,
            Some(ct) => ct,
        };

        // Replace blob id
        let old_blob_desc = self.reserve_new_blob();

        self.blob_index.in_air(&old_blob_desc);
        self.backend
            .store(&old_blob_desc.name[..], &ct)
            .expect("Store operation failed");
        self.blob_index.commit_done(&old_blob_desc);

        // Go through callbacks
        while let Some((href, callback)) = self.blob_refs.pop() {
            callback.call(href);
        }
    }

    fn store(&mut self,
             chunk: &[u8],
             hash: Hash,
             kind: Kind,
             callback: Box<FnBox<HashRef, ()>>)
             -> HashRef {
        if chunk.is_empty() {
            let href = HashRef {
                hash: hash,
                kind: kind,
                persistent_ref: ChunkRef {
                    blob_id: vec![0],
                    offset: 0,
                    length: 0,
                    packing: None,
                    key: None,
                },
            };
            let local_href = href.clone();
            thread::spawn(move || callback.call(local_href));
            return href;
        }

        let mut href = HashRef {
            hash: hash,
            kind: kind,
            persistent_ref: ChunkRef {
                blob_id: self.blob_desc.name.clone(),
                packing: None,
                // updated by try_append:
                offset: 0,
                length: 0,
                key: None,
            },
        };

        if let Err(()) = self.blob.try_append(chunk, &mut href) {
            self.flush();

            href.persistent_ref.blob_id = self.blob_desc.name.clone();
            self.blob.try_append(chunk, &mut href).unwrap();
        }
        self.blob_refs.push((href.clone(), callback));

        // To avoid unnecessary blocking, we reply with the ID *before* possibly flushing.
        href
    }

    fn retrieve(&mut self, hash: &Hash, cref: &ChunkRef) -> Result<Option<Vec<u8>>, BlobError> {
        if cref.offset == 0 && cref.length == 0 {
            return Ok(Some(Vec::new()));
        }
        match self.backend.retrieve(&cref.blob_id[..]) {
            Ok(Some(blob)) => Ok(Some(try!(Blob::read_chunk(&blob, hash, cref)))),
            Ok(None) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn store_named(&mut self, name: &str, data: &[u8]) -> Result<(), String> {
        assert!(data.len() < self.max_blob_size);
        let hash = Hash::new(&data[..]);
        let mut blob = Blob::new(self.max_blob_size);
        blob.try_append(data,
                        &mut HashRef {
                            hash: hash,
                            kind: Kind::TreeLeaf,
                            persistent_ref: ChunkRef {
                                blob_id: name.as_bytes().to_owned(),
                                offset: 0,
                                length: 0,
                                packing: None,
                                key: None,
                            },
                        })
            .unwrap();

        let ct = blob.to_ciphertext().unwrap();

        try!(self.backend.store(name.as_bytes(), &ct));
        Ok(())
    }

    fn retrieve_named(&mut self, name: &str) -> Result<Option<Vec<u8>>, BlobError> {
        match try!(self.backend.retrieve(name.as_bytes())) {
            None => Ok(None),
            Some(ct) => {
                let hrefs = try!(self.blob.refs_from_bytes(&ct));
                assert_eq!(hrefs.len(), 1);
                let href = &hrefs[0];
                assert_eq!(name.as_bytes(), &href.persistent_ref.blob_id[..]);
                Ok(Some(try!(Blob::read_chunk(&ct, &href.hash, &href.persistent_ref))))
            }
        }
    }

    fn recover(&mut self, chunk: HashRef) {
        if chunk.persistent_ref.offset == 0 && chunk.persistent_ref.length == 0 {
            // This chunk is empty, so there is no blob to recover.
            return;
        }
        self.blob_index.recover(chunk.persistent_ref.blob_id);
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
        for b in &blobs {
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
                 chunk: &[u8],
                 hash: Hash,
                 kind: Kind,
                 callback: Box<FnBox<HashRef, ()>>)
                 -> HashRef {
        let mut guard = self.lock();
        guard.store(chunk, hash, kind, callback)
    }

    /// Retrieve the data chunk identified by `ChunkRef`.
    pub fn retrieve(&self, hash: &Hash, cref: &ChunkRef) -> Result<Option<Vec<u8>>, BlobError> {
        self.lock().retrieve(hash, cref)
    }

    /// Store a full named blob (used for writing root).
    pub fn store_named(&self, name: &str, data: &[u8]) -> Result<(), String> {
        self.lock().store_named(name, data)
    }

    /// Retrieve full named blob.
    pub fn retrieve_named(&self, name: &str) -> Result<Option<Vec<u8>>, BlobError> {
        self.lock().retrieve_named(name)
    }

    /// Reinstall a blob recovered from external storage.
    pub fn recover(&self, chunk: HashRef) {
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
