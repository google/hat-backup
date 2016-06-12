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

use blob;
use errors::RetryError;
use hash;
use key::MsgError;
use hash::tree::HashTreeBackend;

#[derive(Clone)]
pub struct HashStoreBackend {
    hash_index: hash::HashIndex,
    blob_store: blob::StoreProcess,
}

impl HashStoreBackend {
    pub fn new(hash_index: hash::HashIndex, blob_store: blob::StoreProcess) -> HashStoreBackend {
        HashStoreBackend {
            hash_index: hash_index,
            blob_store: blob_store,
        }
    }

    fn fetch_chunk_from_hash(&mut self, hash: &hash::Hash) -> Result<Option<Vec<u8>>, MsgError> {
        assert!(!hash.bytes.is_empty());
        match try!(self.hash_index.fetch_persistent_ref(hash)) {
            Some(chunk_ref) => self.fetch_chunk_from_persistent_ref(chunk_ref),
            _ => Ok(None),  // TODO: Do we need to distinguish `missing` from `unknown ref`?
        }
    }

    fn fetch_chunk_from_persistent_ref(&mut self,
                                       chunk_ref: blob::ChunkRef)
                                       -> Result<Option<Vec<u8>>, MsgError> {
        match try!(self.blob_store.send_reply(blob::Msg::Retrieve(chunk_ref))) {
            blob::Reply::RetrieveOk(chunk) => Ok(Some(chunk)),
            _ => Ok(None),
        }
    }
}

impl HashTreeBackend for HashStoreBackend {
    type Err = MsgError;

    fn fetch_chunk(&mut self,
                   hash: &hash::Hash,
                   persistent_ref: Option<blob::ChunkRef>)
                   -> Result<Option<Vec<u8>>, MsgError> {
        assert!(!hash.bytes.is_empty());

        let data_opt = if let Some(r) = persistent_ref {
            try!(self.fetch_chunk_from_persistent_ref(r))
        } else {
            try!(self.fetch_chunk_from_hash(&hash))
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

    fn fetch_persistent_ref(&mut self,
                            hash: &hash::Hash)
                            -> Result<Option<blob::ChunkRef>, MsgError> {
        assert!(!hash.bytes.is_empty());
        loop {
            match self.hash_index.fetch_persistent_ref(hash) {
                Ok(Some(r)) => return Ok(Some(r)), // done
                Ok(None) => return Ok(None), // done
                Err(RetryError::Retry) => (),  // continue loop
                Err(err) => return Err(From::from(err)),
            }
        }
    }

    fn fetch_payload(&mut self, hash: &hash::Hash) -> Result<Option<Vec<u8>>, MsgError> {
        match try!(self.hash_index.fetch_payload(hash)) {
            Some(p) => Ok(p), // done
            None => Ok(None), // done
        }
    }

    fn insert_chunk(&mut self,
                    hash: &hash::Hash,
                    level: i64,
                    payload: Option<Vec<u8>>,
                    chunk: Vec<u8>)
                    -> Result<blob::ChunkRef, MsgError> {
        assert!(!hash.bytes.is_empty());

        let mut hash_entry = hash::Entry {
            hash: hash.clone(),
            level: level,
            payload: payload,
            persistent_ref: None,
        };

        match try!(self.hash_index.reserve(&hash_entry)) {
            hash::ReserveResult::HashKnown(..) => {
                // Someone came before us: piggyback on their result.
                Ok(try!(self.fetch_persistent_ref(hash))
                    .expect("Could not find persistent_ref for known chunk."))
            }
            hash::ReserveResult::ReserveOk(..) => {
                // We came first: this data-chunk is ours to process.
                let local_hash_index = self.hash_index.clone();

                let local_hash = hash.clone();
                let callback = Box::new(move |chunk_ref: blob::ChunkRef| {
                    local_hash_index.commit(&local_hash, chunk_ref).unwrap();
                });
                let kind = if level == 0 {
                    blob::Kind::TreeLeaf
                } else {
                    blob::Kind::TreeBranch
                };
                match try!(self.blob_store.send_reply(blob::Msg::Store(chunk, kind, callback))) {
                    blob::Reply::StoreOk(chunk_ref) => {
                        hash_entry.persistent_ref = Some(chunk_ref.clone());
                        try!(self.hash_index.update_reserved(hash_entry));
                        Ok(chunk_ref)
                    }
                    _ => Err(From::from("Unexpected reply for message Store from BlobStore.")),
                }
            }
        }
    }
}
