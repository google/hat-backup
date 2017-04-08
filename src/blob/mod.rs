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
use key;


mod chunk;
mod blob;
mod index;
#[cfg(test)]
pub mod tests;

#[cfg(all(test, feature = "benchmarks"))]
mod benchmarks;


pub use self::blob::Blob;
pub use self::chunk::{ChunkRef, Key, NodeType, LeafType, Packing};
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
    blob_index: Arc<BlobIndex>,
    blob_desc: BlobDesc,
    blob_refs: Vec<(Box<FnBox<(), ()>>)>,
    blob: Blob,
}

impl<B> Drop for StoreInner<B> {
    fn drop(&mut self) {
        // Sanity check that we flushed this blob store before dropping it.
        assert_eq!(0, self.blob.upperbound_len());
    }
}

impl<B: StoreBackend> StoreInner<B> {
    fn new(index: Arc<BlobIndex>, backend: Arc<B>, max_blob_size: usize) -> StoreInner<B> {
        let mut bs = StoreInner {
            backend: backend,
            blob_index: index,
            blob_desc: Default::default(),
            blob_refs: Vec::new(),
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
        while let Some(callback) = self.blob_refs.pop() {
            callback.call(());
        }
    }

    fn store(&mut self,
             chunk: &[u8],
             hash: Hash,
             node: NodeType,
             leaf: LeafType,
             info: Option<&key::Info>,
             callback: Box<FnBox<(), ()>>)
             -> HashRef {
        let mut href = HashRef {
            hash: hash,
            node: node,
            leaf: leaf,
            info: info.cloned(),
            persistent_ref: ChunkRef {
                blob_id: Some(0),
                blob_name: vec![0],
                packing: None,
                // Updated by try_append.
                offset: 0,
                length: 0,
                key: None,
            },
        };

        if chunk.is_empty() {
            // We are not going to store an empty chunk, so commit it ASAP.
            thread::spawn(move || callback.call(()));
        } else {
            href.persistent_ref.blob_id = Some(self.blob_desc.id);
            href.persistent_ref.blob_name = self.blob_desc.name.clone();
            if let Err(()) = self.blob.try_append(chunk, &mut href) {
                self.flush();
                href.persistent_ref.blob_id = Some(self.blob_desc.id);
                href.persistent_ref.blob_name = self.blob_desc.name.clone();

                self.blob.try_append(chunk, &mut href).unwrap();
            }

            // Queue the callback; we will trigger it when the blob has been pushed.
            self.blob_refs.push(callback);
        }

        // Info is internal to the blob only.
        href.info = None;
        // To avoid unnecessary blocking, we reply with the ID *before* possibly flushing.
        href
    }

    fn retrieve(&mut self, hash: &Hash, cref: &ChunkRef) -> Result<Option<Vec<u8>>, BlobError> {
        if cref.offset == 0 && cref.length == 0 {
            return Ok(Some(Vec::new()));
        }
        match self.backend.retrieve(&cref.blob_name[..]) {
            Ok(Some(blob)) => Ok(Some(Blob::read_chunk(&blob, hash, cref)?)),
            Ok(None) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn retrieve_refs(&mut self, blob: BlobDesc) -> Result<Option<Vec<HashRef>>, BlobError> {
        match self.backend.retrieve(&blob.name[..])? {
            None => Ok(None),
            Some(ct) => {
                let hrefs = self.blob.refs_from_bytes(&ct)?;
                if hrefs.len() == 0 {
                    Ok(None)
                } else {
                    assert_eq!(&blob.name[..], &hrefs[0].persistent_ref.blob_name[..]);
                    Ok(Some(hrefs))
                }
            }
        }
    }

    fn recover(&mut self) -> Result<(), String> {
        self.backend.list()?.into_iter()
            .filter(|b| b.len() > 4)  // FIXME(jos): Remove when "root" is gone.
            .map(|b| self.blob_index.recover(b.into_vec())).last();
        Ok(())
    }

    fn tag(&mut self, chunk: ChunkRef, tag: tags::Tag) {
        self.blob_index.tag(&BlobDesc {
                                id: chunk.blob_id.unwrap_or(0),
                                name: chunk.blob_name,
                            },
                            tag);
    }

    fn tag_all(&mut self, tag: tags::Tag) {
        self.blob_index.tag_all(tag);
    }

    fn delete_by_tag(&mut self, tag: tags::Tag) -> Result<(), String> {
        let blobs = self.blob_index.list_by_tag(tag);
        for b in &blobs {
            self.backend.delete(&b.name)?;
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
                 node: NodeType,
                 leaf: LeafType,
                 info: Option<&key::Info>,
                 callback: Box<FnBox<(), ()>>)
                 -> HashRef {
        let mut guard = self.lock();
        guard.store(chunk, hash, node, leaf, info, callback)
    }

    /// Retrieve the data chunk identified by `ChunkRef`.
    pub fn retrieve(&self, hash: &Hash, cref: &ChunkRef) -> Result<Option<Vec<u8>>, BlobError> {
        self.lock().retrieve(hash, cref)
    }

    /// Fetch a blob and recover the HashRefs for its contents.
    pub fn retrieve_refs(&self, blob: BlobDesc) -> Result<Option<Vec<HashRef>>, BlobError> {
        self.lock().retrieve_refs(blob)
    }

    /// Reinstall a blob recovered from external storage.
    pub fn recover(&self) -> Result<(), String> {
        self.lock().recover()
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

    pub fn list_by_tag(&self, tag: tags::Tag) -> Vec<BlobDesc> {
        self.lock().blob_index.list_by_tag(tag)
    }

    pub fn find(&self, name: &[u8]) -> Option<BlobDesc> {
        self.lock().blob_index.find(name)
    }

    /// Flush the current blob, independent of its size.
    pub fn flush(&self) {
        let mut guard = self.lock();
        guard.flush();
        guard.blob_index.flush();
    }
}
