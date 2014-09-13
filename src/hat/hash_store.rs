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

//! The hash store de-duplicates data chunks via the hash index.
//!
//! The main purpose of the hash store is to split incoming data-chunks betweens those that are
//! already known, and those that needs to go to external storage via the blob store.

use blob_store::{BlobStoreProcess, Store, StoreOK, BlobID};
use blob_store;

use hash_index::{HashIndexProcess, HashEntry, Commit};
use hash_index;

use process::{Process, MsgHandler};


pub type HashStoreProcess<'db, B> = Process<Msg, Reply, HashStore<'db, B>>;

pub enum Msg {
  /// `Insert(Hash, level, opt_payload, data)`: Ensure that a data-chunk is present in the index.
  /// If the `Hash` is already present in the index, it is not reinserted.
  /// Returns `InsertOK` with the persistent reference.
  Insert(hash_index::Hash, i64, Option<Vec<u8>>, Vec<u8>),

  /// Check whether a `Hash` is present in the index.
  /// Returns `HashKnown` or `HashNotKnown`.
  HashExists(hash_index::Hash),

  /// Locate the `payload` of a `Hash` in the index.
  /// Returns `Payload` or `HashNotKnown`.
  FetchPayload(hash_index::Hash),

  /// Locate the `payload` of a `Hash` in the index.
  /// Returns `PersistentRef` or `HashNotKnown`.
  FetchPersistentRef(hash_index::Hash),

  /// Insert a callback to be called after `Hash` has been committed.
  /// Returns `CallbackRegistered` or `HashNotKnown`.
  CallAfterHashIsComitted(hash_index::Hash, proc():Send),

  /// Locate the data-chunk belonging to `Hash`.
  /// Returns `Chunk`.
  FetchChunkFromHash(hash_index::Hash),

  /// Locate the data-chunk belonging to a persistent reference.
  /// Returns `Chunk`.
  FetchChunkFromPersistentRef(Vec<u8>),

  /// Flush this hash store along with its dependencies.
  /// Returns `FlushOK`.
  Flush,
}

#[deriving(Eq, PartialEq, Show)]
pub enum Reply {
  InsertOK(Vec<u8>),
  Payload(Option<Vec<u8>>),
  PersistentRef(Vec<u8>),
  HashKnown,
  HashNotKnown,
  CallbackRegistered,

  Chunk(Vec<u8>),

  Retry,
  FlushOK,
}

pub struct HashStore<'db, B> {
  hash_index: Box<HashIndexProcess<'db>>,
  blob_store: Box<BlobStoreProcess<B>>,
}

impl <'db, B: Send + blob_store::BlobStoreBackend> HashStore<'db, B> {

  pub fn new(hash_index: Box<HashIndexProcess>, blob_store: Box<BlobStoreProcess<B>>)
             -> HashStore<B> {
    HashStore{hash_index: hash_index,
              blob_store: blob_store}
  }

  #[cfg(test)]
  pub fn newForTesting(backend: B) -> HashStore<B> {
    let hiP = Process::new(proc() { hash_index::HashIndex::newForTesting() });
    let bsP = Process::new(proc() { blob_store::BlobStore::newForTesting(backend, 1024) });
    HashStore{hash_index: hiP,
              blob_store: bsP}
  }

  pub fn flush(&mut self) {
    self.blob_store.sendReply(blob_store::Flush);
    self.hash_index.sendReply(hash_index::Flush);
  }

  fn fetchChunkFromHash(&mut self, hash: hash_index::Hash) -> Vec<u8> {
    assert!(hash.bytes.len() > 0);
    match self.hash_index.sendReply(hash_index::FetchPersistentRef(hash)) {
      hash_index::PersistentRef(chunk_ref_bytes) => {
        let chunk_ref = BlobID::from_bytes(chunk_ref_bytes);
        self.fetchChunkFromPersistentRef(chunk_ref)
      },
      _ => fail!("Could not find hash in hash index."),
    }
  }

  fn fetchChunkFromPersistentRef(&mut self, chunk_ref: BlobID) -> Vec<u8> {
    match self.blob_store.sendReply(blob_store::Retrieve(chunk_ref)) {
      blob_store::RetrieveOK(chunk) => chunk,
      _ => fail!("Could not retrieve chunk from blob store."),
    }
  }

}

impl <'db, B: Send + blob_store::BlobStoreBackend> MsgHandler<Msg, Reply> for HashStore<'db, B> {

  fn handle(&mut self, msg: Msg, reply: |Reply|) {
    match msg {

      HashExists(hash) => {
        return reply(match self.hash_index.sendReply(
          hash_index::HashExists(hash)) {
          hash_index::HashKnown    => HashKnown,
          hash_index::HashNotKnown => HashNotKnown,
          _ => fail!("Unexpected answer from hash index."),
        });
      },

      FetchPayload(hash) => {
        return reply(match self.hash_index.sendReply(hash_index::FetchPayload(hash)) {
          hash_index::Payload(p) => Payload(p),
          hash_index::HashNotKnown => HashNotKnown,
          _ => fail!("Unexpected answer from hash index."),
        });
      }

      FetchPersistentRef(hash) => {
        assert!(hash.bytes.len() > 0);
        return reply(match self.hash_index.sendReply(
          hash_index::FetchPersistentRef(hash)) {
          hash_index::PersistentRef(r) => PersistentRef(r),
          hash_index::HashNotKnown => HashNotKnown,
          hash_index::Retry => Retry,
          _ => fail!("Unexpected answer from hash index."),
        });
      },

      CallAfterHashIsComitted(hash, callback) => {
        self.hash_index.sendReply(
          hash_index::CallAfterHashIsComitted(hash, callback));
        return reply(CallbackRegistered);
      },

      Insert(hash, level, payload, persistent_bytes) => {
        assert!(hash.bytes.len() > 0);

        let mut hash_entry = HashEntry{hash:hash.clone(),
                                       level:level, payload:payload,
                                       persistent_ref: None};
        match self.hash_index.sendReply(hash_index::Reserve(hash_entry.clone())) {
          hash_index::HashKnown => {
            // Someone came before us; we'll piggyback on their result.
            loop {
              match self.hash_index.sendReply(hash_index::FetchPersistentRef(hash.clone())) {
                hash_index::PersistentRef(r) => return reply(InsertOK(r)),
                hash_index::Retry => (),  // The owner of this data-chunk is still processing.
                _ => fail!("Unexpected answer from hash index."),
              }
            }
          },
          hash_index::ReserveOK => {
            // This data-chunk is ours to process.
            let local_hash_index = self.hash_index.clone();
            match self.blob_store.sendReply(
              Store(persistent_bytes, proc(blobid) {
                local_hash_index.sendReply(Commit(hash, blobid.as_bytes().into_owned()));
              }))
            {
              StoreOK(blob_ref) => {
                hash_entry.persistent_ref = Some(blob_ref.as_bytes());
                self.hash_index.sendReply(hash_index::UpdateReserved(hash_entry));
                return reply(InsertOK(blob_ref.as_bytes()))
              },
              _ => fail!("Unexpected reply from BlobStore."),
            };
          },
          _ => fail!("Unexpected HashIndex reply."),
        };
      },

      FetchChunkFromHash(hash) => {
        assert!(hash.bytes.len() > 0);
        return reply(Chunk(self.fetchChunkFromHash(hash)));
      },

      FetchChunkFromPersistentRef(persistent_ref) => {
        let chunk_ref = BlobID::from_bytes(persistent_ref);
        return reply(Chunk(self.fetchChunkFromPersistentRef(chunk_ref)));
      },

      Flush => {
        self.flush();
        return reply(FlushOK);
      }

    };
  }
}



#[cfg(test)]
mod tests {
  use super::*;
  use std::rand::{task_rng};
  use quickcheck::{Config, Testable, gen};
  use quickcheck::{quickcheck_config};
  use process::{Process};

  use hash_index::{Hash};
  use blob_store::tests::{MemoryBackend};

  // QuickCheck configuration
  static SIZE: uint = 200;
  static CONFIG: Config = Config {
    tests: 100,
    max_tests: 1000,
  };

  // QuickCheck helpers:
  fn qcheck<A: Testable>(f: A) {
    quickcheck_config(CONFIG, &mut gen(task_rng(), SIZE), f)
  }

  #[test]
  fn hash_store_identity() {
    fn prop(chunks: Vec<Vec<u8>>) -> bool {
      let backend = MemoryBackend::new();
      let hsP = Process::new(proc() { HashStore::newForTesting(backend) });

      for chunk in chunks.iter() {
        match hsP.sendReply(
          Insert(Hash::new(chunk.as_slice()), 0, None, chunk.as_slice().into_owned()))
        {
          InsertOK(_chunk_ref) => (),
          _ => fail!("Insert failed."),
        }
      }

      assert_eq!(hsP.sendReply(Flush), FlushOK);

      for chunk in chunks.iter() {
        match hsP.sendReply(FetchChunkFromHash(Hash::new(chunk.as_slice()))) {
          Chunk(found_chunk) => assert_eq!(chunk.as_slice().into_owned(), found_chunk),
          _ => fail!("Could not find chunk."),
        }
      }

      true
    }
    qcheck(prop);
  }

}
