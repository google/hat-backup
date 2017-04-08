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


use backend::StoreBackend;
use blob;
use errors::RetryError;
use hash;
use hash::tree::HashTreeBackend;
use key::MsgError;
use key;
use std::sync::{Arc, Mutex};

pub struct HashStoreBackend<B> {
    hash_index: Arc<hash::HashIndex>,
    blob_store: Arc<blob::BlobStore<B>>,
}
impl<B> Clone for HashStoreBackend<B> {
    fn clone(&self) -> HashStoreBackend<B> {
        HashStoreBackend {
            hash_index: self.hash_index.clone(),
            blob_store: self.blob_store.clone(),
        }
    }
}

impl<B: StoreBackend> HashStoreBackend<B> {
    pub fn new(hash_index: Arc<hash::HashIndex>,
               blob_store: Arc<blob::BlobStore<B>>)
               -> HashStoreBackend<B> {
        HashStoreBackend {
            hash_index: hash_index,
            blob_store: blob_store,
        }
    }

    fn fetch_chunk_from_hash(&self, hash: &hash::Hash) -> Result<Option<Vec<u8>>, MsgError> {
        assert!(!hash.bytes.is_empty());
        match self.hash_index.fetch_persistent_ref(hash)? {
            None => Ok(None),
            Some(chunk_ref) => self.fetch_chunk_from_persistent_ref(hash, &chunk_ref),
        }
    }

    fn fetch_chunk_from_persistent_ref(&self,
                                       hash: &hash::Hash,
                                       cref: &blob::ChunkRef)
                                       -> Result<Option<Vec<u8>>, MsgError> {
        let res = self.blob_store.retrieve(&hash, &cref)?;
        Ok(res)
    }
}

impl<B: StoreBackend> HashTreeBackend for HashStoreBackend<B> {
    type Err = MsgError;

    fn fetch_chunk(&self,
                   hash: &hash::Hash,
                   persistent_ref: Option<&blob::ChunkRef>)
                   -> Result<Option<Vec<u8>>, MsgError> {
        assert!(!hash.bytes.is_empty());

        let data_opt = if let Some(r) = persistent_ref {
            self.fetch_chunk_from_persistent_ref(&hash, &r)?
        } else {
            self.fetch_chunk_from_hash(&hash)?
        };

        Ok(data_opt.and_then(|data| {
            let actual_hash = hash::Hash::new(&data[..]);
            if *hash == actual_hash {
                Some(data)
            } else {
                error!("Data hash does not match expectation: {:?} instead of {:?}",
                       actual_hash,
                       hash);
                None
            }
        }))
    }

    fn fetch_persistent_ref(&self, hash: &hash::Hash) -> Option<blob::ChunkRef> {
        assert!(!hash.bytes.is_empty());
        loop {
            match self.hash_index.fetch_persistent_ref(hash) {
                Ok(Some(r)) => return Some(r), // done
                Ok(None) => return None, // done
                Err(RetryError) => (),  // continue loop
            }
        }
    }

    fn fetch_childs(&self, hash: &hash::Hash) -> Option<Vec<i64>> {
        match self.hash_index.fetch_childs(hash) {
            Some(p) => p, // done
            None => None, // done
        }
    }

    fn insert_chunk(&self,
                    chunk: &[u8],
                    node: blob::NodeType,
                    leaf: blob::LeafType,
                    childs: Option<Vec<i64>>,
                    info: Option<&key::Info>)
                    -> Result<(i64, hash::tree::HashRef), MsgError> {
        let mut hash_entry = hash::Entry {
            hash: hash::Hash::new(chunk),
            node: node,
            leaf: leaf,
            childs: childs,
            persistent_ref: None,
        };

        match self.hash_index.reserve(&hash_entry) {
            hash::ReserveResult::HashKnown(id) => {
                // Someone came before us: piggyback on their result.
                let pref = self.fetch_persistent_ref(&hash_entry.hash)
                    .expect("Could not find persistent ref for known hash");
                Ok((id,
                    hash::tree::HashRef {
                        hash: hash_entry.hash,
                        node: node,
                        leaf: leaf,
                        info: None,
                        persistent_ref: pref,
                    }))
            }
            hash::ReserveResult::ReserveOk(id) => {
                // We came first: this data-chunk is ours to process.
                let local_hash_index = self.hash_index.clone();

                let m = Arc::new(Mutex::new(()));
                let guard = m.lock().unwrap();

                let local_m = m.clone();
                let callback = Box::new(move |()| {
                    // The callback has to run *after* we called update_reserved in the outer body.
                    let guard = local_m.lock().unwrap();
                    local_hash_index.commit(id, None);
                    drop(guard);
                });

                let href = self.blob_store
                    .store(chunk, hash_entry.hash.clone(), node, leaf, info, callback);

                // Update the hash entry now to enable reuse before the hash is fully committed.
                hash_entry.persistent_ref = Some(href.persistent_ref.clone());
                self.hash_index.update_reserved(id, hash_entry);

                // Allow callback to run, now that we have updated the entry it is going to commit.
                drop(guard);

                Ok((id, href))
            }
        }
    }
}
