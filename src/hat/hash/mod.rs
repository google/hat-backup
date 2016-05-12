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

//! Local state for known hashes and their external location (blob reference).

use std::sync::{Arc, Mutex, MutexGuard};
use time::Duration;

use diesel;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use libsodium_sys;
use sodiumoxide::crypto::hash::sha512;

use blob;
use cumulative_counter::CumulativeCounter;
use periodic_timer::PeriodicTimer;
use tags;
use unique_priority_queue::UniquePriorityQueue;
use util;

mod schema;
pub mod tree;


error_type! {
    #[derive(Debug)]
    pub enum MsgError {
        SqlConnection(diesel::ConnectionError) {
            cause;
        },
        SqlMigration(diesel::migrations::MigrationError) {
            cause;
        },
        SqlRunMigration(diesel::migrations::RunMigrationsError) {
            cause;
        },
        SqlExecute(diesel::result::Error) {
            cause;
        },
    }
}


pub static HASHBYTES: usize = sha512::DIGESTBYTES;

#[derive(Clone)]
pub struct IndexProcess(Arc<Mutex<(Index, Option<i64>)>>);


/// A wrapper around Hash digests.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct Hash {
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcData {
    pub num: i64,
    pub bytes: Vec<u8>,
}
pub trait UpdateFn: FnOnce(GcData) -> Option<GcData> {}
impl<T> UpdateFn for T where T: FnOnce(GcData) -> Option<GcData> {}

impl Hash {
    /// Computes `hash(text)` and stores this digest as the `bytes` field in a new `Hash` structure.
    pub fn new(text: &[u8]) -> Hash {
        let digest_len = libsodium_sys::crypto_generichash_blake2b_BYTES_MAX;
        let mut digest = vec![0; digest_len];
        unsafe {
            libsodium_sys::crypto_generichash_blake2b(digest.as_mut_ptr(),
                                                      digest_len,
                                                      text.as_ptr(),
                                                      text.len() as u64,
                                                      vec![].as_ptr(),
                                                      0);
        }
        Hash { bytes: digest }
    }
}


/// An entry that can be inserted into the hash index.
#[derive(Clone)]
pub struct Entry {
    /// The hash of this entry (unique among all entries in the index).
    pub hash: Hash,

    /// The level in a hash tree that this entry is from. Level `0` represents `leaf`s, i.e. entries
    /// that represent user-data, where levels `1` and up represents `branches` of the tree,
    /// i.e. internal meta-data.
    pub level: i64,

    /// A local payload to store inside the index, along with this entry.
    pub payload: Option<Vec<u8>>,

    /// A reference to a location in the external persistent storage (a chunk reference) that
    /// contains the data for this entry (e.g. an object-name and a byte range).
    pub persistent_ref: Option<blob::ChunkRef>,
}

pub struct RetryError;
pub enum ReserveResult {
    HashKnown,
    ReserveOk,
}

#[derive(Clone)]
struct QueueEntry {
    id: i64,
    level: i64,
    payload: Option<Vec<u8>>,
    persistent_ref: Option<blob::ChunkRef>,
}

pub struct Index {
    conn: SqliteConnection,

    id_counter: CumulativeCounter,

    queue: UniquePriorityQueue<i64, Vec<u8>, QueueEntry>,

    flush_timer: PeriodicTimer,
    flush_periodically: bool,
}

impl Index {
    fn new(path: String) -> Result<Index, MsgError> {
        let conn = try!(SqliteConnection::establish(&path));

        let mut hi = Index {
            conn: conn,
            id_counter: CumulativeCounter::new(0),
            queue: UniquePriorityQueue::new(),
            flush_timer: PeriodicTimer::new(Duration::seconds(10)),
            flush_periodically: true,
        };

        let dir = try!(diesel::migrations::find_migrations_directory());
        try!(diesel::migrations::run_pending_migrations_in_directory(&hi.conn,
                                                                     &dir,
                                                                     &mut util::InfoWriter));

        try!(hi.conn.begin_transaction());

        hi.refresh_id_counter();
        Ok(hi)
    }

    fn new_for_testing() -> Result<Index, MsgError> {
        Index::new(":memory:".to_string())
    }

    fn index_locate(&mut self, hash_: &Hash) -> Option<QueueEntry> {
        assert!(!hash_.bytes.is_empty());
        use self::schema::hashes::dsl::*;

        let result_opt = hashes.filter(hash.eq(&hash_.bytes))
                               .first::<schema::Hash>(&self.conn)
                               .optional()
                               .expect("Error querying hashes");
        result_opt.map(|result| {
            let payload_ = result.payload.and_then(|b| {
                if b.is_empty() {
                    None
                } else {
                    Some(b)
                }
            });
            let persistent_ref = result.blob_ref.and_then(|b| {
                if b.is_empty() {
                    None
                } else {
                    Some(blob::ChunkRef::from_bytes(&mut &b[..]).unwrap())
                }
            });
            QueueEntry {
                id: result.id,
                level: result.height,
                payload: payload_,
                persistent_ref: persistent_ref,
            }
        })
    }

    fn locate(&mut self, hash: &Hash) -> Option<QueueEntry> {
        let result_opt = self.queue.find_value_of_key(&hash.bytes).map(|x| x.clone());
        result_opt.or_else(|| self.index_locate(hash))
    }

    fn locate_by_id(&mut self, id_: i64) -> Option<Entry> {
        use self::schema::hashes::dsl::*;

        let result_opt = hashes.find(id_)
                               .first::<schema::Hash>(&self.conn)
                               .optional()
                               .expect("Error querying hashes");

        result_opt.map(|result| {
            Entry {
                hash: Hash { bytes: result.hash },
                level: result.height,
                payload: result.payload.and_then(|p| {
                    if p.is_empty() {
                        None
                    } else {
                        Some(p)
                    }
                }),
                persistent_ref: result.blob_ref.and_then(|b| {
                    if b.is_empty() {
                        None
                    } else {
                        Some(blob::ChunkRef::from_bytes(&mut &b[..]).unwrap())
                    }
                }),
            }
        })
    }

    fn refresh_id_counter(&mut self) {
        use self::schema::hashes::dsl::*;
        use diesel::expression::max;

        let id_opt = hashes.select(max(id).nullable())
                           .first::<Option<i64>>(&self.conn)
                           .expect("Error selecting max hash id");

        self.id_counter = CumulativeCounter::new(id_opt.unwrap_or(0));
    }

    fn next_id(&mut self) -> i64 {
        self.id_counter.increment()
    }

    fn reserve(&mut self, hash_entry: Entry) -> i64 {
        self.maybe_flush();

        let Entry { hash, level, payload, persistent_ref } = hash_entry;
        assert!(!hash.bytes.is_empty());

        let my_id = self.next_id();

        assert!(self.queue.reserve_priority(my_id, hash.bytes.clone()).is_ok());
        self.queue.put_value(&hash.bytes,
                             QueueEntry {
                                 id: my_id,
                                 level: level,
                                 payload: payload,
                                 persistent_ref: persistent_ref,
                             });
        my_id
    }

    fn update_reserved(&mut self, hash_entry: Entry) {
        let Entry { hash, level, payload, persistent_ref } = hash_entry;
        assert!(!hash.bytes.is_empty());
        let old_entry = self.locate(&hash).expect("hash was reserved");

        // If we didn't already commit and pop() the hash, update it:
        let id_opt = self.queue.find_key(&hash.bytes).cloned();
        if id_opt.is_some() {
            assert_eq!(id_opt, Some(old_entry.id));
            self.queue.update_value(&hash.bytes, |qe| {
                QueueEntry {
                    level: level,
                    payload: payload,
                    persistent_ref: persistent_ref,
                    ..qe
                }
            });
        }
    }

    fn insert_completed_in_order(&mut self) {
        use self::schema::hashes::dsl::*;

        loop {
            match self.queue.pop_min_if_complete() {
                None => break,
                Some((id_, hash_bytes, queue_entry)) => {
                    assert_eq!(id_, queue_entry.id);

                    let persistent_ref_bytes = queue_entry.persistent_ref.map(|c| c.as_bytes());
                    let new = schema::NewHash {
                        id: id_,
                        hash: &hash_bytes,
                        tag: tags::Tag::Done as i64,
                        height: queue_entry.level,
                        payload: queue_entry.payload.as_ref().map(|v| &v[..]),
                        blob_ref: persistent_ref_bytes.as_ref().map(|v| &v[..]),
                    };

                    diesel::insert(&new)
                        .into(hashes)
                        .execute(&self.conn)
                        .expect("Error inserting new hash");
                }
            }
        }
    }

    fn set_tag(&mut self, id_opt: Option<i64>, tag_: tags::Tag) {
        use self::schema::hashes::dsl::*;

        match id_opt {
            None => {
                diesel::update(hashes)
                    .set(tag.eq(tag_ as i64))
                    .execute(&self.conn)
                    .expect("Error updating hash tags")
            }
            Some(id_) => {
                diesel::update(hashes.find(id_))
                    .set(tag.eq(tag_ as i64))
                    .execute(&self.conn)
                    .expect("Error updating specific hash tag")
            }
        };
    }

    fn get_tag(&mut self, id_: i64) -> Option<tags::Tag> {
        use self::schema::hashes::dsl::*;

        let tag_opt = hashes.find(id_)
                            .select(tag)
                            .first::<i64>(&self.conn)
                            .optional()
                            .expect("Error querying hash tag");

        tag_opt.and_then(tags::tag_from_num)
    }

    fn list_ids_by_tag(&mut self, tag_: i64) -> Vec<i64> {
        // We list hashes top-down.
        // This is required for safe deletion.
        // TODO(jos): consider moving this requirement closer to the code that needs it.
        use self::schema::hashes::dsl::*;

        hashes.filter(tag.eq(tag_))
              .order(height.desc())
              .select(id)
              .load::<i64>(&self.conn)
              .expect("Error listing hashes")
    }

    fn read_gc_data(&mut self, hash_id_: i64, family_id_: i64) -> GcData {
        use self::schema::gc_metadata::dsl::*;

        let result_opt = gc_metadata.filter(hash_id.eq(hash_id_))
                                    .filter(family_id.eq(family_id_))
                                    .first::<schema::GcMetadata>(&self.conn)
                                    .optional()
                                    .expect("Error querying GC metadata");
        match result_opt {
            None => {
                GcData {
                    num: 0,
                    bytes: vec![],
                }
            }
            Some(row) => {
                GcData {
                    num: row.gc_int,
                    bytes: row.gc_vec,
                }
            }
        }
    }

    fn set_gc_data(&mut self, hash_id_: i64, family_id_: i64, data: GcData) {
        use self::schema::gc_metadata::dsl::*;

        let count = diesel::update(gc_metadata.filter(hash_id.eq(hash_id_))
                                              .filter(family_id.eq(family_id_)))
                        .set((gc_int.eq(data.num), gc_vec.eq(&data.bytes)))
                        .execute(&self.conn)
                        .expect("Error updating GC metadata");
        assert!(count <= 1);

        if count == 0 {
            let new = schema::NewGcMetadata {
                hash_id: hash_id_,
                family_id: family_id_,
                gc_int: data.num,
                gc_vec: &data.bytes,
            };

            diesel::insert(&new)
                .into(gc_metadata)
                .execute(&self.conn)
                .expect("Error inserting GC metadata");
        }
    }

    fn update_gc_data<F: UpdateFn>(&mut self, hash_id: i64, family_id: i64, f: F) -> GcData {
        let data = self.read_gc_data(hash_id, family_id);
        match f(data.clone()) {
            None => {
                self.delete_gc_data(hash_id, family_id);
                data
            }
            Some(new) => {
                self.set_gc_data(hash_id, family_id, new.clone());
                new
            }
        }
    }

    fn update_family_gc_data<F: UpdateFn, I: Iterator<Item = F>>(&mut self,
                                                                 family_id_: i64,
                                                                 mut fns: I) {
        use self::schema::gc_metadata::dsl::*;

        let hash_ids_ = gc_metadata.filter(family_id.eq(family_id_))
                                   .select(hash_id)
                                   .load::<i64>(&self.conn)
                                   .expect("Error loading GC metadata");

        for hash_id_ in hash_ids_ {
            let f = fns.next().expect("Failed to recv update function");
            self.update_gc_data(hash_id_, family_id_, f);
        }
    }

    fn delete_gc_data(&mut self, hash_id_: i64, family_id_: i64) {
        use self::schema::gc_metadata::dsl::*;

        diesel::delete(gc_metadata.filter(hash_id.eq(hash_id_))
                                  .filter(family_id.eq(family_id_)))
            .execute(&self.conn)
            .expect("Error deleting GC metadata");
    }

    fn commit(&mut self, hash: &Hash, chunk_ref: blob::ChunkRef) {
        // Update persistent reference for ready hash
        let queue_entry = self.locate(hash).expect("hash was committed");
        self.queue.update_value(&hash.bytes,
                                |old_qe| QueueEntry { persistent_ref: Some(chunk_ref), ..old_qe });
        self.queue.set_ready(&queue_entry.id);

        self.insert_completed_in_order();

        self.maybe_flush();
    }

    fn list(&mut self) -> Vec<Entry> {
        use self::schema::hashes::dsl::*;
        hashes.load::<schema::Hash>(&self.conn)
              .expect("Error listing hashes")
              .into_iter()
              .map(|hash_| {
                  Entry {
                      hash: Hash { bytes: hash_.hash },
                      level: hash_.height,
                      payload: hash_.payload.and_then(|p| {
                          if p.is_empty() {
                              None
                          } else {
                              Some(p)
                          }
                      }),
                      persistent_ref: hash_.blob_ref.and_then(|b| {
                          if b.is_empty() {
                              None
                          } else {
                              Some(blob::ChunkRef::from_bytes(&mut &b[..]).unwrap())
                          }
                      }),
                  }
              })
              .collect()
    }

    fn delete(&mut self, id_: i64) {
        {
            use self::schema::hashes::dsl::*;
            let hash_count = diesel::delete(hashes.find(id_))
                                 .execute(&self.conn)
                                 .expect("Error deleting hash");
            assert!(hash_count <= 1);
        }

        {
            use self::schema::gc_metadata::dsl::*;
            diesel::delete(gc_metadata.filter(hash_id.eq(id_)))
                .execute(&self.conn)
                .expect("Error deleting GC metadata");
        }
    }

    fn maybe_flush(&mut self) {
        if self.flush_periodically && self.flush_timer.did_fire() {
            self.flush();
        }
    }

    fn flush(&mut self) {
        // Callbacks assume their data is safe, so commit before calling them
        self.conn.commit_transaction().unwrap();
        self.conn.begin_transaction().unwrap();
    }
}

impl IndexProcess {
    pub fn new(path: String) -> Result<IndexProcess, MsgError> {
        let index = try!(Index::new(path));
        Ok(IndexProcess(Arc::new(Mutex::new((index, None)))))
    }

    pub fn new_for_testing(shutdown: Option<i64>) -> Result<IndexProcess, MsgError> {
        let index = try!(Index::new_for_testing());
        Ok(IndexProcess(Arc::new(Mutex::new((index, shutdown)))))
    }

    fn lock(&self) -> MutexGuard<(Index, Option<i64>)> {
        let mut guard = self.0.lock().expect("index-process has failed");

        match &mut guard.1 {
            &mut None => (),
            &mut Some(0) => panic!("No more requests for this index process"),
            &mut Some(ref mut n) => {
                *n -= 1;
            }
        }

        guard
    }

    /// Locate the local ID of this hash.
    pub fn get_id(&self, hash: &Hash) -> Option<i64> {
        assert!(!hash.bytes.is_empty());
        self.lock().0.locate(&hash).map(|entry| entry.id)
    }

    /// Locate hash entry from its ID.
    pub fn get_hash(&self, id: i64) -> Option<Entry> {
        self.lock().0.locate_by_id(id)
    }

    /// Check whether this `Hash` already exists in the system.
    pub fn hash_exists(&self, hash: &Hash) -> bool {
        assert!(!hash.bytes.is_empty());
        self.lock().0.locate(hash).is_some()
    }

    /// Locate the local payload of the `Hash`.
    pub fn fetch_payload(&self, hash: &Hash) -> Option<Option<Vec<u8>>> {
        assert!(!hash.bytes.is_empty());
        self.lock().0.locate(hash).map(|queue_entry| queue_entry.payload)
    }

    /// Locate the persistent reference (external blob reference) for this `Hash`.
    pub fn fetch_persistent_ref(&self, hash: &Hash) -> Result<Option<blob::ChunkRef>, RetryError> {
        assert!(!hash.bytes.is_empty());
        match self.lock().0.locate(hash) {
            Some(ref queue_entry) if queue_entry.persistent_ref.is_none() => Err(RetryError),
            Some(queue_entry) => Ok(Some(queue_entry.persistent_ref.expect("persistent_ref"))),
            None => Ok(None),
        }
    }

    /// Reserve a `Hash` in the index, while sending its content to external storage.
    /// This is used to ensure that each `Hash` is stored only once.
    pub fn reserve(&self, hash_entry: Entry) -> ReserveResult {
        assert!(!hash_entry.hash.bytes.is_empty());
        // To avoid unused IO, we store entries in-memory until committed to persistent
        // storage. This allows us to continue after a crash without needing to scan
        // through and delete uncommitted entries.
        let mut guard = self.lock();
        match guard.0.locate(&hash_entry.hash) {
            Some(_) => ReserveResult::HashKnown,
            None => {
                guard.0.reserve(hash_entry);
                ReserveResult::ReserveOk
            }
        }
    }

    /// Update the info for a reserved `Hash`. The `Hash` remains reserved. This is used to update
    /// the persistent reference (external blob reference) as soon as it is available (to allow new
    /// references to the `Hash` to be created before it is committed).
    pub fn update_reserved(&self, hash_entry: Entry) {
        assert!(!hash_entry.hash.bytes.is_empty());
        self.lock().0.update_reserved(hash_entry);
    }

    /// A `Hash` is committed when it has been `finalized` in the external storage. `Commit`
    /// includes the persistent reference that the content is available at.
    pub fn commit(&self, hash: &Hash, persistent_ref: blob::ChunkRef) {
        assert!(!hash.bytes.is_empty());
        self.lock().0.commit(hash, persistent_ref);
    }

    /// List all hash entries.
    pub fn list(&self) -> Vec<Entry> {
        self.lock().0.list()
    }

    /// Permanently delete hash by its ID.
    pub fn delete(&self, id: i64) {
        self.lock().0.delete(id);
    }

    /// API related to tagging, which is useful to indicate state during operation stages.
    /// It operates directly on the underlying IDs.
    pub fn set_tag(&self, id: i64, tag: tags::Tag) {
        self.lock().0.set_tag(Some(id), tag);
    }

    /// API related to tagging, which is useful to indicate state during operation stages.
    /// It operates directly on the underlying IDs.
    pub fn set_all_tags(&self, tag: tags::Tag) {
        self.lock().0.set_tag(None, tag);
    }

    /// API related to tagging, which is useful to indicate state during operation stages.
    /// It operates directly on the underlying IDs.
    pub fn get_tag(&self, id: i64) -> Option<tags::Tag> {
        self.lock().0.get_tag(id)
    }

    /// API related to tagging, which is useful to indicate state during operation stages.
    /// It operates directly on the underlying IDs.
    pub fn get_ids_by_tag(&self, tag: i64) -> Vec<i64> {
        self.lock().0.list_ids_by_tag(tag)
    }

    /// API related to garbage collector metadata tied to (hash id, family id) pairs.
    pub fn read_gc_data(&self, hash_id: i64, family_id: i64) -> GcData {
        self.lock().0.read_gc_data(hash_id, family_id)
    }

    /// API related to garbage collector metadata tied to (hash id, family id) pairs.
    pub fn update_gc_data<F: UpdateFn>(&self,
                                       hash_id: i64,
                                       family_id: i64,
                                       update_fn: F)
                                       -> GcData {
        self.lock().0.update_gc_data(hash_id, family_id, update_fn)
    }

    /// API related to garbage collector metadata tied to (hash id, family id) pairs.
    pub fn update_family_gc_data<F: UpdateFn, I: Iterator<Item = F>>(&self,
                                                                     family_id: i64,
                                                                     update_fns: I) {
        self.lock().0.update_family_gc_data(family_id, update_fns)
    }

    /// Manual commit. This also disables automatic periodic commit.
    pub fn manual_commit(&self) {
        let mut guard = self.lock();
        guard.0.flush();
        guard.0.flush_periodically = false;
    }

    /// Flush the hash index to clear internal buffers and commit the underlying database.
    pub fn flush(&self) {
        self.lock().0.flush()
    }
}
