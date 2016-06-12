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

//! External API for creating and manipulating snapshots.

use std::borrow::Cow;
use std::sync::mpsc;

#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use backend::StoreBackend;

use blob;
use hash;
use hash::tree::{SimpleHashTreeWriter, SimpleHashTreeReader, ReaderResult};

use util::{FnBox, MsgHandler, Process};
use errors::{DieselError, LockError, RetryError};

mod schema;
mod index;
mod hash_store_backend;

#[cfg(test)]
mod tests;
#[cfg(all(test, feature = "benchmarks"))]
mod benchmarks;

pub use self::hash_store_backend::HashStoreBackend;
pub use self::index::{KeyIndex, Entry};


error_type! {
    #[derive(Debug)]
    pub enum MsgError {
        Recv(mpsc::RecvError) {
            cause;
        },
        Message(Cow<'static, str>) {
            desc (e) &**e;
            from (s: &'static str) s.into();
            from (s: String) s.into();
        },
        Blobs(blob::MsgError) {
            cause;
        },
        RetryError(RetryError) {
            cause;
        },
        LockError(LockError) {
            cause;
        },
        DieselError(DieselError) {
            cause;
        }
     }
}


pub type StoreProcess<IT> = Process<Msg<IT>, Reply, MsgError>;

pub type DirElem = (Entry, Option<blob::ChunkRef>, Option<HashTreeReaderInitializer>);

pub struct HashTreeReaderInitializer {
    hash: hash::Hash,
    persistent_ref: Option<blob::ChunkRef>,
    hash_index: hash::HashIndex,
    blob_store: blob::StoreProcess,
}

impl HashTreeReaderInitializer {
    pub fn init(self) -> Result<Option<ReaderResult<HashStoreBackend>>, MsgError> {
        let backend = HashStoreBackend::new(self.hash_index, self.blob_store);
        SimpleHashTreeReader::open(backend, &self.hash, self.persistent_ref)
    }
}

// Public structs
pub enum Msg<IT> {
    /// Insert a key into the index. If this key has associated data a "chunk-iterator creator"
    /// can be passed along with it. If the data turns out to be unreadable, this iterator proc
    /// can return `None`. Returns `Id` with the new entry ID.
    Insert(Entry, Option<Box<FnBox<(), Option<IT>>>>),

    /// List a "directory" (aka. a `level`) in the index.
    /// Returns `ListResult` with all the entries under the given parent.
    ListDir(Option<u64>),

    /// Flush this key store and its dependencies.
    /// Returns `FlushOk`.
    Flush,
}

pub enum Reply {
    Id(u64),
    ListResult(Vec<DirElem>),
    FlushOk,
}

#[derive(Clone)]
pub struct Store {
    index: index::KeyIndex,
    hash_index: hash::HashIndex,
    blob_store: blob::StoreProcess,
}

// Implementations
impl Store {
    pub fn new(index: index::KeyIndex,
               hash_index: hash::HashIndex,
               blob_store: blob::StoreProcess)
               -> Result<Store, MsgError> {
        Ok(Store {
            index: index,
            hash_index: hash_index,
            blob_store: blob_store,
        })
    }

    #[cfg(test)]
    pub fn new_for_testing<B: StoreBackend>(backend: Arc<B>) -> Result<Store, MsgError> {
        let ki_p = try!(index::KeyIndex::new_for_testing());
        let hi_p = try!(hash::HashIndex::new_for_testing(None));
        let blob_index = blob::BlobIndex::new_for_testing().unwrap();
        let bs_p = try!(Process::new(move || Ok(blob::Store::new(blob_index, backend, 1024))));
        Ok(Store {
            index: ki_p,
            hash_index: hi_p,
            blob_store: bs_p,
        })
    }

    pub fn flush(&mut self) -> Result<(), MsgError> {
        try!(self.blob_store.send_reply(blob::Msg::Flush));
        try!(self.hash_index.flush());
        try!(self.index.flush());

        Ok(())
    }

    pub fn hash_tree_writer(&mut self) -> SimpleHashTreeWriter<HashStoreBackend> {
        let backend = HashStoreBackend::new(self.hash_index.clone(), self.blob_store.clone());
        SimpleHashTreeWriter::new(8, backend)
    }
}

fn file_size_warning(name: &[u8], wanted: u64, got: u64) {
    if wanted < got {
        println!("Warning: File grew while reading it: {:?} (wanted {}, got {})",
                 name,
                 wanted,
                 got)
    } else if wanted > got {
        println!("Warning: Could not read whole file (or it shrank): {:?} (wanted {}, got {})",
                 name,
                 wanted,
                 got)
    }
}

impl<IT: Iterator<Item = Vec<u8>>> MsgHandler<Msg<IT>, Reply> for Store {
    type Err = MsgError;

    fn reset(&mut self) -> Result<(), MsgError> {
        try!(self.blob_store.reset());
        try!(self.hash_index.reset());
        Ok(())
    }

    fn handle<F: FnOnce(Result<Reply, MsgError>)>(&mut self,
                                                  msg: Msg<IT>,
                                                  reply: F)
                                                  -> Result<(), MsgError> {
        macro_rules! reply_ok(($x:expr) => {{
            reply(Ok($x));
            Ok(())
        }});

        macro_rules! reply_err(($x:expr) => {{
            reply(Err($x));
            Ok(())
        }});

        match msg {
            Msg::Flush => {
                try!(self.flush());
                reply_ok!(Reply::FlushOk)
            }

            Msg::ListDir(parent) => {
                match self.index.list_dir(parent) {
                    Ok(entries) => {
                        let mut my_entries: Vec<DirElem> = Vec::with_capacity(entries.len());
                        for (entry, persistent_ref) in entries.into_iter() {
                            let open_fn = entry.data_hash.as_ref().map(|bytes| {
                                HashTreeReaderInitializer {
                                    hash: hash::Hash { bytes: bytes.clone() },
                                    persistent_ref: persistent_ref.clone(),
                                    hash_index: self.hash_index.clone(),
                                    blob_store: self.blob_store.clone(),
                                }
                            });

                            my_entries.push((entry, persistent_ref, open_fn));
                        }
                        reply_ok!(Reply::ListResult(my_entries))
                    }
                    Err(e) => reply_err!(From::from(e)),
                }
            }

            Msg::Insert(org_entry, chunk_it_opt) => {
                let entry = match try!(self.index
                    .lookup(org_entry.parent_id, org_entry.name.clone())) {
                    Some(ref entry) if org_entry.accessed == entry.accessed &&
                                       org_entry.modified == entry.modified &&
                                       org_entry.created == entry.created => {
                        if chunk_it_opt.is_some() && entry.data_hash.is_some() {
                            let hash = hash::Hash { bytes: entry.data_hash.clone().unwrap() };
                            if try!(self.hash_index.hash_exists(&hash)) {
                                // Short-circuit: We have the data.
                                return reply_ok!(Reply::Id(entry.id.unwrap()));
                            }
                        } else if chunk_it_opt.is_none() && entry.data_hash.is_none() {
                            // Short-circuit: No data needed.
                            return reply_ok!(Reply::Id(entry.id.unwrap()));
                        }
                        // Our stored entry is incomplete.
                        Entry { id: entry.id, ..org_entry }
                    }
                    Some(entry) => Entry { id: entry.id, ..org_entry },
                    None => org_entry,
                };

                let entry = try!(self.index.insert(entry));

                // Send out the ID early to allow the client to continue its key discovery routine.
                // The bounded input-channel will prevent the client from overflowing us.
                assert!(entry.id.is_some());
                reply(Ok(Reply::Id(entry.id.unwrap())));


                // Setup hash tree structure
                let mut tree = self.hash_tree_writer();

                // Check if we have an data source:
                let it_opt = chunk_it_opt.and_then(|open| open.call(()));
                if it_opt.is_none() {
                    // No data is associated with this entry.
                    try!(self.index.update_data_hash(
                        entry.id.unwrap(),
                        entry.modified,
                        None,
                        None
                    ));
                    // Bail out before storing data that does not exist:
                    return Ok(());
                }

                // Read and insert all file chunks:
                // (see HashStoreBackend::insert_chunk above)
                let mut bytes_read = 0u64;
                for chunk in it_opt.unwrap() {
                    bytes_read += chunk.len() as u64;
                    try!(tree.append(chunk));
                }

                // Warn the user if we did not read the expected size:
                entry.data_length.map(|s| {
                    file_size_warning(&entry.name, s, bytes_read);
                });

                // Get top tree hash:
                let (hash, persistent_ref) = try!(tree.hash());

                // Update hash in key index.
                // It is OK that this has is not yet valid, as we check hashes at snapshot time.
                try!(self.index.update_data_hash(
                    entry.id.unwrap(),
                    entry.modified,
                    Some(hash),
                    Some(persistent_ref)
                ));

                Ok(())
            }
        }
    }
}
