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

use std::borrow::Cow;
use std::boxed::FnBox;
use std::sync::mpsc;
use time::Duration;

use diesel;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use libsodium_sys;
use sodiumoxide::crypto::hash::sha512;

use blob;
use cumulative_counter::CumulativeCounter;
use periodic_timer::PeriodicTimer;
use process::{Process, MsgHandler};
use tags;
use unique_priority_queue::UniquePriorityQueue;
use util;

mod schema;
pub mod tree;


error_type! {
    #[derive(Debug)]
    pub enum MsgError {
        Recv(mpsc::RecvError) {
            cause;
        },
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
        Message(Cow<'static, str>) {
            desc (e) &**e;
            from (s: &'static str) s.into();
            from (s: String) s.into();
        }
    }
}


pub static HASHBYTES: usize = sha512::DIGESTBYTES;

pub type IndexProcess = Process<Msg, Reply>;


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

pub enum Msg {
    /// Check whether this `Hash` already exists in the system.
    /// Returns `HashKnown` or `HashNotKnown`.
    HashExists(Hash),

    /// Locate the local payload of the `Hash`.
    /// Returns `Payload` or `HashNotKnown`.
    FetchPayload(Hash),

    /// Locate the local ID of this hash.
    GetID(Hash),

    /// Locate hash entry from its ID.
    GetHash(i64),

    /// Locate the persistent reference (external blob reference) for this `Hash`.
    /// Returns `PersistentRef` or `HashNotKnown`.
    FetchPersistentRef(Hash),

    /// Reserve a `Hash` in the index, while sending its content to external storage.
    /// This is used to ensure that each `Hash` is stored only once.
    /// Returns `ReserveOk` or `HashKnown`.
    Reserve(Entry),

    /// Update the info for a reserved `Hash`. The `Hash` remains reserved. This is used to update
    /// the persistent reference (external blob reference) as soon as it is available (to allow new
    /// references to the `Hash` to be created before it is committed).
    /// Returns ReserveOk.
    UpdateReserved(Entry),

    /// A `Hash` is committed when it has been `finalized` in the external storage. `Commit`
    /// includes the persistent reference that the content is available at.
    /// Returns CommitOk.
    Commit(Hash, blob::ChunkRef),

    /// List all hash entries.
    List,

    /// APIs related to tagging, which is useful to indicate state during operation stages.
    /// These operate directly on the underlying IDs.
    SetTag(i64, tags::Tag),
    SetAllTags(tags::Tag),
    GetTag(i64),
    GetIDsByTag(i64),

    /// Permanently delete hash by its ID.
    Delete(i64),

    /// APIs related to garbage collector metadata tied to (hash id, family id) pairs.
    ReadGcData(i64, i64),
    UpdateGcData(i64, i64, Box<FnBox(GcData) -> Option<GcData> + Send>),
    UpdateFamilyGcData(i64, mpsc::Receiver<Box<FnBox(GcData) -> Option<GcData> + Send>>),

    /// Manual commit. This also disables automatic periodic commit.
    ManualCommit,

    /// Flush the hash index to clear internal buffers and commit the underlying database.
    Flush,
}

pub enum Reply {
    HashID(i64),
    HashKnown,
    HashNotKnown,
    Entry(Entry),

    Payload(Option<Vec<u8>>),
    PersistentRef(blob::ChunkRef),

    ReserveOk,
    CommitOk,
    CallbackRegistered,

    Listing(mpsc::Receiver<Entry>),

    HashTag(Option<tags::Tag>),
    HashIDs(Vec<i64>),

    CurrentGcData(GcData),

    Ok,
    Retry,
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
    pub fn new(path: String) -> Result<Index, MsgError> {
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

    #[cfg(test)]
    pub fn new_for_testing() -> Result<Index, MsgError> {
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
        let result_opt = self.queue.find_value_of_key(&hash.bytes);
        result_opt.map(|x| x).or_else(|| self.index_locate(hash))
    }

    fn locate_by_id(&mut self, id_: i64) -> Option<Entry> {
        use self::schema::hashes::dsl::*;

        let result_opt = hashes.find(id_)
                               .first::<schema::Hash>(&self.conn)
                               .optional()
                               .expect("Error querying hashes");
        return result_opt.map(|result| {
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
        });
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

        let Entry{hash, level, payload, persistent_ref} = hash_entry;
        assert!(!hash.bytes.is_empty());

        let my_id = self.next_id();

        assert!(self.queue.reserve_priority(my_id, hash.bytes.clone()).is_ok());
        self.queue.put_value(hash.bytes,
                             QueueEntry {
                                 id: my_id,
                                 level: level,
                                 payload: payload,
                                 persistent_ref: persistent_ref,
                             });
        my_id
    }

    fn update_reserved(&mut self, hash_entry: Entry) {
        let Entry{hash, level, payload, persistent_ref} = hash_entry;
        assert!(!hash.bytes.is_empty());
        let old_entry = self.locate(&hash).expect("hash was reserved");

        // If we didn't already commit and pop() the hash, update it:
        let id_opt = self.queue.find_key(&hash.bytes).cloned();
        if id_opt.is_some() {
            assert_eq!(id_opt, Some(old_entry.id));
            self.queue.update_value(&hash.bytes, |qe| {
                QueueEntry {
                    level: level,
                    payload: payload.clone(),
                    persistent_ref: persistent_ref.clone(),
                    ..qe.clone()
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

        return tag_opt.and_then(tags::tag_from_num);
    }

    fn list_ids_by_tag(&mut self, tag_: i64) -> Vec<i64> {
        // We list hashes top-down.
        // This is required for safe deletion.
        // TODO(jos): consider moving this requirement closer to the code that needs it.
        use self::schema::hashes::dsl::*;

        return hashes.filter(tag.eq(tag_))
                     .order(height.desc())
                     .select(id)
                     .load::<i64>(&self.conn)
                     .expect("Error listing hashes");

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

    fn update_gc_data(&mut self,
                      hash_id: i64,
                      family_id: i64,
                      f: Box<FnBox(GcData) -> Option<GcData> + Send>)
                      -> GcData {
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

    fn update_family_gc_data(&mut self,
                             family_id_: i64,
                             fs: mpsc::Receiver<Box<FnBox(GcData) -> Option<GcData> + Send>>) {
        use self::schema::gc_metadata::dsl::*;

        let hash_ids_ = gc_metadata.filter(family_id.eq(family_id_))
                                   .select(hash_id)
                                   .load::<i64>(&self.conn)
                                   .expect("Error loading GC metadata");

        for hash_id_ in hash_ids_ {
            let f = fs.recv().expect("Failed to recv update function");
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
        self.queue.update_value(&hash.bytes, |old_qe| {
            QueueEntry { persistent_ref: Some(chunk_ref.clone()), ..old_qe.clone() }
        });
        self.queue.set_ready(queue_entry.id);

        self.insert_completed_in_order();

        self.maybe_flush();
    }

    fn list(&mut self) -> mpsc::Receiver<Entry> {
        let (sender, receiver) = mpsc::channel();

        use self::schema::hashes::dsl::*;
        let hashes_ = hashes.load::<schema::Hash>(&self.conn)
                            .expect("Error listing hashes");
        for hash_ in hashes_ {
            if let Err(_) = sender.send(Entry {
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
            }) {
                break;
            }
        }
        return receiver;
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

impl MsgHandler<Msg, Reply> for Index {
    type Err = MsgError;

    fn handle(&mut self, msg: Msg, reply: Box<Fn(Reply)>) -> Result<(), MsgError> {
        match msg {

            Msg::GetID(hash) => {
                assert!(!hash.bytes.is_empty());
                reply(match self.locate(&hash) {
                    Some(entry) => Reply::HashID(entry.id),
                    None => Reply::HashNotKnown,
                });
            }

            Msg::GetHash(id) => {
                reply(match self.locate_by_id(id) {
                    Some(hash) => Reply::Entry(hash),
                    None => Reply::HashNotKnown,
                });
            }

            Msg::HashExists(hash) => {
                assert!(!hash.bytes.is_empty());
                reply(match self.locate(&hash) {
                    Some(_) => Reply::HashKnown,
                    None => Reply::HashNotKnown,
                });
            }

            Msg::FetchPayload(hash) => {
                assert!(!hash.bytes.is_empty());
                reply(match self.locate(&hash) {
                    Some(ref queue_entry) => Reply::Payload(queue_entry.payload.clone()),
                    None => Reply::HashNotKnown,
                });
            }

            Msg::FetchPersistentRef(hash) => {
                assert!(!hash.bytes.is_empty());
                reply(match self.locate(&hash) {
                    Some(ref queue_entry) if queue_entry.persistent_ref.is_none() => Reply::Retry,
                    Some(queue_entry) => {
                        Reply::PersistentRef(queue_entry.persistent_ref.expect("persistent_ref"))
                    }
                    None => Reply::HashNotKnown,
                });
            }

            Msg::Reserve(hash_entry) => {
                assert!(!hash_entry.hash.bytes.is_empty());
                // To avoid unused IO, we store entries in-memory until committed to persistent
                // storage. This allows us to continue after a crash without needing to scan
                // through and delete uncommitted entries.
                reply(match self.locate(&hash_entry.hash) {
                    Some(_) => Reply::HashKnown,
                    None => {
                        self.reserve(hash_entry);
                        Reply::ReserveOk
                    }
                });
            }

            Msg::UpdateReserved(hash_entry) => {
                assert!(!hash_entry.hash.bytes.is_empty());
                self.update_reserved(hash_entry);
                reply(Reply::ReserveOk);
            }

            Msg::Commit(hash, persistent_ref) => {
                assert!(!hash.bytes.is_empty());
                self.commit(&hash, persistent_ref);
                reply(Reply::CommitOk);
            }

            Msg::List => {
                reply(Reply::Listing(self.list()));
            }

            Msg::Delete(id) => {
                self.delete(id);
                reply(Reply::Ok);
            }

            Msg::SetTag(id, tag) => {
                self.set_tag(Some(id), tag);
                reply(Reply::Ok);
            }

            Msg::SetAllTags(tag) => {
                self.set_tag(None, tag);
                reply(Reply::Ok);
            }

            Msg::GetTag(id) => {
                reply(Reply::HashTag(self.get_tag(id)));
            }

            Msg::GetIDsByTag(tag) => {
                reply(Reply::HashIDs(self.list_ids_by_tag(tag)));
            }

            Msg::ReadGcData(hash_id, family_id) => {
                reply(Reply::CurrentGcData(self.read_gc_data(hash_id, family_id)));
            }

            Msg::UpdateGcData(hash_id, family_id, update_fn) => {
                reply(Reply::CurrentGcData(self.update_gc_data(hash_id, family_id, update_fn)));
            }

            Msg::UpdateFamilyGcData(family_id, update_fs) => {
                self.update_family_gc_data(family_id, update_fs);
                reply(Reply::Ok);
            }

            Msg::ManualCommit => {
                self.flush();
                self.flush_periodically = false;
                reply(Reply::CommitOk);
            }

            Msg::Flush => {
                self.flush();
                reply(Reply::CommitOk);
            }
        }
        return Ok(());
    }
}
