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


use backend::StoreBackend;
use blob;
use crypto;
use errors::{DieselError, RetryError};
use hash;
use hash::tree::{LeafIterator, SimpleHashTreeWriter};
use std::borrow::Cow;
use std::io;
use std::sync::Arc;

use util::{FnBox, MsgHandler, Process};

mod schema;
mod index;
mod hash_store_backend;

#[cfg(test)]
mod tests;
#[cfg(all(test, feature = "benchmarks"))]
mod benchmarks;

pub use self::hash_store_backend::HashStoreBackend;
pub use self::index::{Data, Entry, Info, KeyIndex};


error_type! {
    #[derive(Debug)]
    pub enum MsgError {
        Message(Cow<'static, str>) {
            desc (e) &**e;
            from (s: &'static str) s.into();
            from (s: String) s.into();
        },
        RetryError(RetryError) {
            cause;
        },
        DieselError(DieselError) {
            cause;
        },
        Blob(blob::BlobError) {
            cause;
        }
     }
}


pub type StoreProcess<IT, B> = Process<Msg<IT>, Reply<B>, MsgError>;

pub type DirElem<B> = (Entry, Option<hash::tree::HashRef>, Option<HashTreeReaderInitializer<B>>);

pub struct HashTreeReaderInitializer<B> {
    hash_ref: hash::tree::HashRef,
    hash_index: Arc<hash::HashIndex>,
    blob_store: Arc<blob::BlobStore<B>>,
    keys: Arc<crypto::keys::Keeper>,
}

impl<B: StoreBackend> HashTreeReaderInitializer<B> {
    pub fn init(self) -> Result<Option<LeafIterator<HashStoreBackend<B>>>, MsgError> {
        let backend = HashStoreBackend::new(self.hash_index, self.blob_store, self.keys.clone());
        LeafIterator::new(backend, self.hash_ref.clone())
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

    /// Commit all reserved nodes and optionally execute recursive cleanup of part of the tree.
    /// Returns `Ok`.
    CommitReservedNodes(Option<Option<u64>>),

    /// Flush this key store and its dependencies.
    /// Returns `FlushOk`.
    Flush,
}

pub enum Reply<B> {
    Id(u64),
    ListResult(Vec<DirElem<B>>),
    Ok,
    FlushOk,
}

pub struct Store<B> {
    index: Arc<index::KeyIndex>,
    hash_index: Arc<hash::HashIndex>,
    blob_store: Arc<blob::BlobStore<B>>,
    keys: Arc<crypto::keys::Keeper>,
}
impl<B> Clone for Store<B> {
    fn clone(&self) -> Store<B> {
        Store {
            index: self.index.clone(),
            hash_index: self.hash_index.clone(),
            blob_store: self.blob_store.clone(),
            keys: self.keys.clone(),
        }
    }
}

// Implementations
impl<B: StoreBackend> Store<B> {
    pub fn new(
        index: Arc<index::KeyIndex>,
        hash_index: Arc<hash::HashIndex>,
        blob_store: Arc<blob::BlobStore<B>>,
        keys: Arc<crypto::keys::Keeper>,
    ) -> Store<B> {
        Store {
            index: index,
            hash_index: hash_index,
            blob_store: blob_store,
            keys: keys,
        }
    }

    #[cfg(test)]
    pub fn new_for_testing(backend: Arc<B>, max_blob_size: usize) -> Result<Store<B>, DieselError> {
        use crypto;
        use db;

        let keys = Arc::new(crypto::keys::Keeper::new_for_testing());
        let db_p = Arc::new(db::Index::new_for_testing());
        let ki_p = Arc::new(index::KeyIndex::new_for_testing()?);
        let hi_p = Arc::new(hash::HashIndex::new(db_p.clone())?);
        let blob_index = Arc::new(blob::BlobIndex::new(keys.clone(), db_p)?);
        let bs_p = Arc::new(blob::BlobStore::new(
            keys,
            blob_index,
            backend,
            max_blob_size,
        ));
        Ok(Store {
            index: ki_p,
            hash_index: hi_p,
            blob_store: bs_p,
            keys: Arc::new(crypto::keys::Keeper::new_for_testing()),
        })
    }

    pub fn flush(&mut self) -> Result<(), MsgError> {
        self.blob_store.flush();
        self.hash_index.flush();
        self.index.flush()?;

        Ok(())
    }

    pub fn hash_tree_writer(
        &mut self,
        leaf: blob::LeafType,
    ) -> SimpleHashTreeWriter<HashStoreBackend<B>> {
        let backend = HashStoreBackend::new(
            self.hash_index.clone(),
            self.blob_store.clone(),
            self.keys.clone(),
        );
        SimpleHashTreeWriter::new(leaf, 8, backend)
    }
}

fn file_size_warning(name: &[u8], wanted: u64, got: u64) {
    if wanted < got {
        println!(
            "Warning: File grew while reading it: {:?} (wanted {}, got {})",
            name,
            wanted,
            got
        )
    } else if wanted > got {
        println!(
            "Warning: Could not read whole file (or it shrank): {:?} (wanted {}, got {})",
            name,
            wanted,
            got
        )
    }
}

impl<IT: io::Read, B: StoreBackend> MsgHandler<Msg<IT>, Reply<B>> for Store<B> {
    type Err = MsgError;

    fn handle<F: FnOnce(Result<Reply<B>, MsgError>)>(
        &mut self,
        msg: Msg<IT>,
        reply: F,
    ) -> Result<(), MsgError> {
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
                self.flush()?;
                reply_ok!(Reply::FlushOk)
            }

            Msg::ListDir(parent) => {
                match self.index.list_dir(parent) {
                    Ok(entries) => {
                        let mut my_entries: Vec<DirElem<B>> = Vec::with_capacity(entries.len());
                        for (entry, hash_ref_opt) in entries {
                            let hash_ref = hash_ref_opt.or_else(|| match entry.data {
                                Data::FileHash(ref hash_bytes) => {
                                    let h = hash::Hash { bytes: hash_bytes.clone() };
                                    self.hash_index.fetch_hash_ref(&h).expect("Unknown hash")
                                }
                                _ => None,
                            });
                            let open_fn = hash_ref.as_ref().map(|r| {
                                HashTreeReaderInitializer {
                                    hash_ref: r.clone(),
                                    hash_index: self.hash_index.clone(),
                                    blob_store: self.blob_store.clone(),
                                    keys: self.keys.clone(),
                                }
                            });

                            my_entries.push((entry, hash_ref, open_fn));
                        }
                        reply_ok!(Reply::ListResult(my_entries))
                    }
                    Err(e) => reply_err!(From::from(e)),
                }
            }

            Msg::CommitReservedNodes(clean_parent_opt) => {
                self.index.commit_reserved_nodes()?;
                if let Some(parent) = clean_parent_opt {
                    self.index.cleanup_unused(parent)?;
                }
                return reply_ok!(Reply::Ok);
            }

            Msg::Insert(insert_entry, chunk_it_opt) => {
                let entry = match self.index.lookup(
                    insert_entry.parent_id,
                    insert_entry.info.name.clone(),
                )? {
                    Some(ref stored_entry) if insert_entry.data_looks_unchanged(stored_entry) => {
                        match &stored_entry.data {
                            &Data::FileHash(ref hash_bytes) if chunk_it_opt.is_some() => {
                                let hash = hash::Hash { bytes: hash_bytes.to_vec() };
                                if self.hash_index.hash_exists(&hash) {
                                    // Short-circuit: We have the data.
                                    debug!("Skip entry: {:?}", stored_entry.info.name);
                                    self.index.mark_reserved(&stored_entry)?;
                                    return reply_ok!(Reply::Id(stored_entry.node_id.unwrap()));
                                }
                            }
                            _ if chunk_it_opt.is_none() => {
                                // Short-circuit: No data needed.
                                debug!("Skip empty entry: {:?}", stored_entry.info.name);
                                self.index.mark_reserved(&stored_entry)?;
                                return reply_ok!(Reply::Id(stored_entry.node_id.unwrap()));
                            }
                            _ => (),
                        }
                        // Our stored entry is incomplete.
                        Entry {
                            node_id: stored_entry.node_id,
                            ..insert_entry
                        }
                    }
                    Some(entry) => {
                        Entry {
                            node_id: entry.node_id,
                            ..insert_entry
                        }
                    }
                    None => insert_entry,
                };

                // Check if we have an data source:
                let it_opt = chunk_it_opt.and_then(|open| open.call(()));
                if it_opt.is_none() {
                    // No data is associated with this entry.
                    debug!("Insert entry: {:?}", entry.info.name);
                    let entry = self.index.insert(entry, None)?;

                    // Bail out before storing data that does not exist:
                    return reply_ok!(Reply::Id(entry.node_id.unwrap()));
                }

                // Setup hash tree structure
                let mut tree = self.hash_tree_writer(blob::LeafType::FileChunk);

                // Read and insert all file chunks:
                // (see HashStoreBackend::insert_chunk above)
                let max_chunk_len = 128 * 1024;
                let mut chunk = vec![0; max_chunk_len];
                let mut reader = it_opt.unwrap();
                let mut file_len = 0u64;
                loop {
                    let mut chunk_len = 0;
                    while chunk_len < max_chunk_len {
                        chunk_len += match reader.read(&mut chunk[chunk_len..]) {
                            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                            Ok(0) | Err(_) => break,
                            Ok(size) => size,
                        }
                    }
                    if chunk_len == 0 {
                        break;
                    }
                    file_len += chunk_len as u64;
                    tree.append(&chunk[..chunk_len])?
                }

                // Warn the user if we did not read the expected size:
                entry.info.byte_length.map(|s| {
                    file_size_warning(&entry.info.name, s, file_len);
                });

                // Get top tree hash:
                let hash_ref = tree.hash(Some(&entry.info))?;

                // It is OK that this has is not yet valid, as we check hashes at snapshot time.
                debug!("Insert entry: {:?}", entry.info.name);
                let entry = self.index.insert(entry, Some(&hash_ref))?;

                return reply_ok!(Reply::Id(entry.node_id.unwrap()));
            }
        }
    }
}
