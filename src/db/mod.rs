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

//! Communication with SQLite.


use blob;

use capnp;

use diesel;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use errors::{DieselError, RetryError};

use hash;
use libsodium_sys;
use root_capnp;
use std::sync::{Mutex, MutexGuard};
use tags;
use time::Duration;
use util::{Counter, InfoWriter, PeriodicTimer, UniquePriorityQueue};

mod schema;


pub struct Index(Mutex<InternalIndex>);
pub type IndexGuard<'a> = MutexGuard<'a, InternalIndex>;

impl Index {
    pub fn new(path: &str) -> Result<Index, DieselError> {
        Ok(Index(Mutex::new(try!(InternalIndex::new(path)))))
    }
    pub fn lock(&self) -> MutexGuard<InternalIndex> {
        self.0.lock().expect("Database mutex is poisoned")
    }
    #[cfg(test)]
    pub fn new_for_testing() -> Index {
        Index(Mutex::new(InternalIndex::new(":memory:").unwrap()))
    }
}


fn encode_childs(childs: &[i64]) -> Vec<u8> {
    let mut message = capnp::message::Builder::new_default();
    {
        let root = message.init_root::<root_capnp::hash_ids::Builder>();
        let mut list = root.init_hash_ids(childs.len() as u32);
        for (i, id) in childs.iter().enumerate() {
            list.set(i as u32, *id as u64);
        }
    }
    let mut out = Vec::new();
    capnp::serialize_packed::write_message(&mut out, &message).unwrap();
    out
}

fn decode_childs(bytes: &[u8]) -> Result<Vec<i64>, capnp::Error> {
    let reader = capnp::serialize_packed::read_message(&mut &bytes[..],
                                                       capnp::message::ReaderOptions::new())
        .unwrap();
    let msg = reader.get_root::<root_capnp::hash_ids::Reader>().unwrap();

    let ids = try!(msg.get_hash_ids());
    let mut out = Vec::new();
    for i in 0..ids.len() {
        assert!(ids.get(i) as i64 > 0);
        out.push(ids.get(i) as i64);
    }
    Ok(out)
}


#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcData {
    pub num: i64,
    pub bytes: Vec<u8>,
}
pub trait UpdateFn: FnOnce(GcData) -> Option<GcData> {}
impl<T> UpdateFn for T where T: FnOnce(GcData) -> Option<GcData> {}


/// An entry that can be inserted into the hash index.
#[derive(Clone)]
pub struct Entry {
    /// The hash of this entry (unique among all entries in the index).
    pub hash: hash::Hash,

    /// The level in a hash tree that this entry is from. Level `0` represents `leaf`s, i.e. entries
    /// that represent user-data, where levels `1` and up represents `branches` of the tree,
    /// i.e. internal meta-data.
    pub level: i64,

    /// An optional list of child hash ids.
    pub childs: Option<Vec<i64>>,

    /// A reference to a location in the external persistent storage (a chunk reference) that
    /// contains the data for this entry (e.g. an object-name and a byte range).
    pub persistent_ref: Option<blob::ChunkRef>,
}

pub enum ReserveResult {
    HashKnown(i64),
    ReserveOk(i64),
}

#[derive(Clone)]
pub struct QueueEntry {
    pub id: i64,
    pub level: i64,
    pub childs: Option<Vec<i64>>,
    pub persistent_ref: Option<blob::ChunkRef>,
}

pub struct InternalIndex {
    conn: SqliteConnection,
    hash_id_counter: Counter,
    flush_timer: PeriodicTimer,
    flush_periodically: bool,
}

impl InternalIndex {
    fn new(path: &str) -> Result<InternalIndex, DieselError> {
        let conn = try!(SqliteConnection::establish(path));

        let mut idx = InternalIndex {
            conn: conn,
            hash_id_counter: Counter::new(0),
            flush_timer: PeriodicTimer::new(Duration::seconds(10)),
            flush_periodically: true,
        };

        let dir = try!(diesel::migrations::find_migrations_directory());
        try!(diesel::migrations::run_pending_migrations_in_directory(&idx.conn,
                                                                     &dir,
                                                                     &mut InfoWriter));

        try!(idx.conn.begin_transaction());

        idx.hash_refresh_id_counter();
        Ok(idx)
    }

    pub fn hash_locate(&mut self, hash_: &hash::Hash) -> Option<QueueEntry> {
        assert!(!hash_.bytes.is_empty());
        use self::schema::hashes::dsl::*;

        let result_opt = hashes.filter(hash.eq(&hash_.bytes))
            .first::<schema::Hash>(&self.conn)
            .optional()
            .expect("Error querying hashes");
        result_opt.map(|result| {
            let childs_ = result.childs.and_then(|b| {
                if b.is_empty() {
                    None
                } else {
                    Some(decode_childs(&b).unwrap())
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
                childs: childs_,
                persistent_ref: persistent_ref,
            }
        })
    }

    pub fn hash_locate_by_id(&mut self, id_: i64) -> Option<Entry> {
        use self::schema::hashes::dsl::*;

        let result_opt = hashes.find(id_)
            .first::<schema::Hash>(&self.conn)
            .optional()
            .expect("Error querying hashes");

        result_opt.map(|result| {
            Entry {
                hash: self::hash::Hash { bytes: result.hash },
                level: result.height,
                childs: result.childs.and_then(|p| {
                    if p.is_empty() {
                        None
                    } else {
                        Some(decode_childs(&p).unwrap())
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

    pub fn hash_refresh_id_counter(&mut self) {
        use self::schema::hashes::dsl::*;
        use diesel::expression::max;

        let id_opt = hashes.select(max(id).nullable())
            .first::<Option<i64>>(&self.conn)
            .expect("Error selecting max hash id");

        self.hash_id_counter = Counter::new(id_opt.unwrap_or(0));
    }

    pub fn hash_next_id(&mut self) -> i64 {
        self.hash_id_counter.next()
    }

    pub fn hash_insert_new(&mut self, id_: i64, hash_bytes: Vec<u8>, entry: QueueEntry) {
        use self::schema::hashes::dsl::*;

        let blob_ref_ = entry.persistent_ref.map(|c| c.as_bytes());
        let childs_ = entry.childs.as_ref().map(|v| encode_childs(&v[..]));

        let new = schema::NewHash {
            id: id_,
            hash: &hash_bytes,
            tag: tags::Tag::Done as i64,
            height: entry.level,
            childs: childs_.as_ref().map(|v| &v[..]),
            blob_ref: blob_ref_.as_ref().map(|v| &v[..]),
        };

        diesel::insert(&new)
            .into(hashes)
            .execute(&self.conn)
            .expect("Error inserting new hash");
    }

    pub fn hash_set_tag(&mut self, id_opt: Option<i64>, tag_: tags::Tag) {
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

    pub fn hash_get_tag(&mut self, id_: i64) -> Option<tags::Tag> {
        use self::schema::hashes::dsl::*;

        let tag_opt = hashes.find(id_)
            .select(tag)
            .first::<i64>(&self.conn)
            .optional()
            .expect("Error querying hash tag");

        tag_opt.and_then(tags::tag_from_num)
    }

    pub fn hash_list_ids_by_tag(&mut self, tag_: i64) -> Vec<i64> {
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

    pub fn hash_read_gc_data(&mut self, hash_id_: i64, family_id_: i64) -> GcData {
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

    pub fn hash_set_gc_data(&mut self, hash_id_: i64, family_id_: i64, data: GcData) {
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

    pub fn hash_update_gc_data<F: UpdateFn>(&mut self,
                                            hash_id: i64,
                                            family_id: i64,
                                            f: F)
                                            -> GcData {
        let data = self.hash_read_gc_data(hash_id, family_id);
        match f(data.clone()) {
            None => {
                self.hash_delete_gc_data(hash_id, family_id);
                data
            }
            Some(new) => {
                self.hash_set_gc_data(hash_id, family_id, new.clone());
                new
            }
        }
    }

    pub fn hash_update_family_gc_data<F: UpdateFn, I: Iterator<Item = F>>(&mut self,
                                                                          family_id_: i64,
                                                                          mut fns: I) {
        use self::schema::gc_metadata::dsl::*;

        let hash_ids_ = gc_metadata.filter(family_id.eq(family_id_))
            .select(hash_id)
            .load::<i64>(&self.conn)
            .expect("Error loading GC metadata");

        for hash_id_ in hash_ids_ {
            let f = fns.next().expect("Failed to recv update function");
            self.hash_update_gc_data(hash_id_, family_id_, f);
        }
    }

    pub fn hash_delete_gc_data(&mut self, hash_id_: i64, family_id_: i64) {
        use self::schema::gc_metadata::dsl::*;

        diesel::delete(gc_metadata.filter(hash_id.eq(hash_id_))
                .filter(family_id.eq(family_id_)))
            .execute(&self.conn)
            .expect("Error deleting GC metadata");
    }

    pub fn hash_list(&mut self) -> Vec<Entry> {
        use self::schema::hashes::dsl::*;
        hashes.load::<schema::Hash>(&self.conn)
            .expect("Error listing hashes")
            .into_iter()
            .map(|hash_| {
                Entry {
                    hash: self::hash::Hash { bytes: hash_.hash },
                    level: hash_.height,
                    childs: hash_.childs.as_ref().and_then(|p| {
                        if p.is_empty() {
                            None
                        } else {
                            Some(decode_childs(p).unwrap())
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

    pub fn hash_delete(&mut self, id_: i64) {
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

    pub fn maybe_flush(&mut self) {
        if self.flush_periodically && self.flush_timer.did_fire() {
            self.flush();
        }
    }

    pub fn set_auto_flush(&mut self, enabled: bool) {
        self.flush_periodically = enabled;
    }

    pub fn flush(&mut self) {
        // Callbacks assume their data is safe, so commit before calling them
        self.conn.commit_transaction().unwrap();
        self.conn.begin_transaction().unwrap();
    }

    pub fn blob_next_id(&mut self) -> i64 {
        // TODO(jos): use an id_counter.
        use diesel::expression::max;
        use self::schema::blobs::dsl::*;

        blobs.select(max(id).nullable())
            .first::<Option<i64>>(&self.conn)
            .optional()
            .expect("Error querying blobs")
            .and_then(|x| x)
            .unwrap_or(0)
    }

    pub fn blob_in_air(&mut self, blob: &blob::BlobDesc) {
        use self::schema::blobs::dsl::*;

        let new = schema::NewBlob {
            id: blob.id,
            name: &blob.name,
            tag: tags::Tag::InProgress as i32,
        };
        diesel::insert(&new)
            .into(blobs)
            .execute(&self.conn)
            .expect("Error inserting blob");

        self.flush();
    }

    pub fn blob_commit(&mut self, blob: &blob::BlobDesc) {
        use self::schema::blobs::dsl::*;

        diesel::update(blobs.find(blob.id))
            .set(tag.eq(tags::Tag::Done as i32))
            .execute(&self.conn)
            .expect("Error updating blob");
        self.flush();
    }

    pub fn blob_id_from_name(&self, name_: &[u8]) -> Option<i64> {
        use self::schema::blobs::dsl::*;
        blobs.filter(name.eq(name_))
            .select(id)
            .first::<i64>(&self.conn)
            .optional()
            .expect("Error reading blob")
    }

    pub fn blob_set_tag(&self, tag_: tags::Tag, target: Option<&blob::BlobDesc>) {
        use self::schema::blobs::dsl::*;
        match target {
            None => {
                diesel::update(blobs)
                    .set(tag.eq(tag_ as i32))
                    .execute(&self.conn)
                    .expect("Error updating blob tags")
            }
            Some(t) if t.id > 0 => {
                diesel::update(blobs.find(t.id))
                    .set(tag.eq(tag_ as i32))
                    .execute(&self.conn)
                    .expect("Error updating blob tags")
            }
            Some(t) if !t.name.is_empty() => {
                diesel::update(blobs.filter(name.eq(&t.name)))
                    .set(tag.eq(tag_ as i32))
                    .execute(&self.conn)
                    .expect("Error updating blob tags")
            }
            _ => unreachable!(),
        };
    }

    pub fn blob_delete_by_tag(&self, tag_: tags::Tag) {
        use self::schema::blobs::dsl::*;
        diesel::delete(blobs.filter(tag.eq(tag_ as i32)))
            .execute(&self.conn)
            .expect("Error deleting blobs");
    }

    pub fn blob_list_by_tag(&self, tag_: tags::Tag) -> Vec<blob::BlobDesc> {
        use self::schema::blobs::dsl::*;
        blobs.filter(tag.eq(tag_ as i32))
            .load::<schema::Blob>(&self.conn)
            .expect("Error listing blobs")
            .into_iter()
            .map(|blob_| {
                blob::BlobDesc {
                    id: blob_.id,
                    name: blob_.name,
                }
            })
            .collect()
    }
}
