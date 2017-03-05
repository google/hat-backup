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
use diesel::connection::TransactionManager;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use errors::DieselError;

use hash;
use root_capnp;
use std::sync::{Mutex, MutexGuard};
use tags;
use time::Duration;
use util::{Counter, InfoWriter, PeriodicTimer};

mod schema;


pub struct Index(Mutex<InternalIndex>);
pub type IndexGuard<'a> = MutexGuard<'a, InternalIndex>;

impl Index {
    pub fn new(path: &str) -> Result<Index, DieselError> {
        Ok(Index(Mutex::new(InternalIndex::new(path)?)))
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

    let ids = msg.get_hash_ids()?;
    let mut out = Vec::new();
    for i in 0..ids.len() {
        assert!(ids.get(i) as i64 > 0);
        out.push(ids.get(i) as i64);
    }
    Ok(out)
}

fn decode_chunk_ref(cref: Option<&Vec<u8>>,
                    blob: Option<self::schema::Blob>)
                    -> Option<blob::ChunkRef> {
    cref.and_then(|c| {
        let mut r = blob::ChunkRef::from_bytes(&mut &c[..]).expect("Failed to decode chunk");
        if r.length > 0 {
            blob.map(|b| {
                r.blob_name = b.name;
                r
            })
        } else {
            r.blob_name = vec![0];
            Some(r)
        }
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcData {
    pub num: i64,
    pub bytes: Vec<u8>,
}
pub trait UpdateFn: FnOnce(GcData) -> Option<GcData> {}
impl<T> UpdateFn for T where T: FnOnce(GcData) -> Option<GcData> {}

#[derive(Clone, Debug)]
pub struct SnapshotInfo {
    pub unique_id: i64,
    pub family_id: i64,
    pub snapshot_id: i64,
}

#[derive(Debug)]
pub enum SnapshotWorkStatus {
    CommitInProgress,
    CommitComplete,
    DeleteInProgress,
    DeleteComplete,
    RecoverInProgress,
}

#[derive(Debug)]
pub struct SnapshotStatus {
    pub family_name: String,
    pub info: SnapshotInfo,
    pub hash: Option<hash::Hash>,
    pub hash_ref: Option<Vec<u8>>,
    pub msg: Option<String>,
    pub status: SnapshotWorkStatus,
}

fn tag_to_work_status(tag: tags::Tag) -> SnapshotWorkStatus {
    match tag {
        tags::Tag::Reserved | tags::Tag::InProgress => SnapshotWorkStatus::CommitInProgress,
        tags::Tag::Complete | tags::Tag::Done => SnapshotWorkStatus::CommitComplete,
        tags::Tag::WillDelete => SnapshotWorkStatus::DeleteInProgress,
        tags::Tag::ReadyDelete |
        tags::Tag::DeleteComplete => SnapshotWorkStatus::DeleteComplete,
        tags::Tag::RecoverInProgress => SnapshotWorkStatus::RecoverInProgress,
    }
}

fn work_status_to_tag(status: SnapshotWorkStatus) -> tags::Tag {
    match status {
        SnapshotWorkStatus::CommitInProgress => tags::Tag::InProgress,
        SnapshotWorkStatus::CommitComplete => tags::Tag::Done,
        SnapshotWorkStatus::DeleteInProgress => tags::Tag::WillDelete,
        SnapshotWorkStatus::DeleteComplete => tags::Tag::DeleteComplete,
        SnapshotWorkStatus::RecoverInProgress => tags::Tag::RecoverInProgress,
    }
}

/// An entry that can be inserted into the hash index.
#[derive(Clone)]
pub struct Entry {
    /// The hash of this entry (unique among all entries in the index).
    pub hash: hash::Hash,

    /// Metadata on what sort of content we are referencing.
    pub node: blob::NodeType,
    pub leaf: blob::LeafType,

    /// An optional list of child hash ids.
    pub childs: Option<Vec<i64>>,

    /// A reference to a location in the external persistent storage (a chunk reference) that
    /// contains the data for this entry (e.g. an object-name and a byte range).
    pub persistent_ref: Option<blob::ChunkRef>,
}

#[derive(Clone)]
pub struct QueueEntry {
    pub id: i64,
    pub node: blob::NodeType,
    pub leaf: blob::LeafType,
    pub childs: Option<Vec<i64>>,
    pub persistent_ref: Option<blob::ChunkRef>,
    pub tag: Option<tags::Tag>,
}

pub struct InternalIndex {
    conn: SqliteConnection,
    hash_id_counter: Counter,
    flush_timer: PeriodicTimer,
    flush_periodically: bool,
}


impl InternalIndex {
    fn new(path: &str) -> Result<InternalIndex, DieselError> {
        let conn = SqliteConnection::establish(path)?;

        let mut idx = InternalIndex {
            conn: conn,
            hash_id_counter: Counter::new(0),
            flush_timer: PeriodicTimer::new(Duration::seconds(10)),
            flush_periodically: true,
        };

        let dir = diesel::migrations::find_migrations_directory()?;
        diesel::migrations::run_pending_migrations_in_directory(&idx.conn, &dir, &mut InfoWriter)?;

        {
            let tm = idx.conn.transaction_manager();
            tm.begin_transaction(&idx.conn)?;
        }

        idx.hash_refresh_id_counter();
        Ok(idx)
    }

    pub fn hash_locate(&mut self, hash_: &hash::Hash) -> Option<QueueEntry> {
        assert!(!hash_.bytes.is_empty());
        use self::schema::hashes::dsl::*;
        use self::schema::blobs::dsl::blobs;

        let result_opt = hashes.left_outer_join(blobs)
            .filter(hash.eq(&hash_.bytes))
            .first::<(self::schema::Hash, Option<self::schema::Blob>)>(&self.conn)
            .optional()
            .expect("Error querying hashes");
        result_opt.map(|(hash_, blob_)| {
            let childs_ = hash_.childs.and_then(|b| {
                if b.is_empty() {
                    None
                } else {
                    Some(decode_childs(&b).unwrap())
                }
            });
            let persistent_ref = decode_chunk_ref(hash_.blob_ref.as_ref(), blob_);
            QueueEntry {
                id: hash_.id,
                node: From::from(hash_.height),
                leaf: From::from(hash_.leaf_type),
                tag: tags::tag_from_num(hash_.tag),
                childs: childs_,
                persistent_ref: persistent_ref,
            }
        })
    }

    pub fn hash_locate_by_id(&mut self, id_: i64) -> Option<Entry> {
        use self::schema::hashes::dsl::*;
        use self::schema::blobs::dsl::blobs;

        let result_opt = hashes.left_outer_join(blobs)
            .filter(id.eq(id_))
            .first::<(self::schema::Hash, Option<self::schema::Blob>)>(&self.conn)
            .optional()
            .expect("Error querying hashes");

        result_opt.map(|(hash_, blob_)| {
            Entry {
                hash: self::hash::Hash { bytes: hash_.hash },
                node: From::from(hash_.height),
                leaf: From::from(hash_.leaf_type),
                childs: hash_.childs.and_then(|p| {
                    if p.is_empty() {
                        None
                    } else {
                        Some(decode_childs(&p).unwrap())
                    }
                }),
                persistent_ref: decode_chunk_ref(hash_.blob_ref.as_ref(), blob_),
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

        let blob_ref_ = entry.persistent_ref.as_ref().map(|c| c.as_bytes_no_name());
        let childs_ = entry.childs.as_ref().map(|v| encode_childs(&v[..]));

        let new = schema::NewHash {
            id: id_,
            hash: &hash_bytes,
            tag: entry.tag.unwrap_or(tags::Tag::Done) as i64,
            height: From::from(entry.node),
            leaf_type: From::from(entry.leaf),
            childs: childs_.as_ref().map(|v| &v[..]),
            blob_id: entry.persistent_ref.and_then(|r| r.blob_id).unwrap_or(0),
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
        use self::schema::blobs::dsl::blobs;

        hashes.left_outer_join(blobs)
            .load::<(self::schema::Hash, Option<self::schema::Blob>)>(&self.conn)
            .expect("Error listing hashes")
            .into_iter()
            .map(|(hash_, blob_)| {
                Entry {
                    hash: self::hash::Hash { bytes: hash_.hash },
                    node: From::from(hash_.height),
                    leaf: From::from(hash_.leaf_type),
                    childs: hash_.childs.as_ref().map(|p| decode_childs(p).unwrap()),
                    persistent_ref: decode_chunk_ref(hash_.blob_ref.as_ref(), blob_),
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
        let tm = self.conn.transaction_manager();
        tm.commit_transaction(&self.conn).unwrap();
        tm.begin_transaction(&self.conn).unwrap();
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
            Some(t) => {
                unreachable!("blob with neither id nor name: id={}, name={}",
                             t.id,
                             t.name.len())
            }
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
            .order(id.desc())
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

    pub fn last_insert_rowid(&self) -> i64 {
        diesel::select(diesel::expression::sql("last_insert_rowid()"))
            .first::<i64>(&self.conn)
            .unwrap()
    }

    pub fn family_id_from_name(&mut self, name_: &str) -> Option<i64> {
        use self::schema::family::dsl::*;

        family.filter(name.eq(name_))
            .select(id)
            .first::<i64>(&self.conn)
            .optional()
            .expect("Error reading family")
    }

    /// Delete snapshot.
    pub fn snapshot_delete(&self, info: SnapshotInfo) {
        use self::schema::snapshots::dsl::*;

        let count = diesel::delete(snapshots.find(info.unique_id)
                .filter(family_id.eq(info.family_id))
                .filter(snapshot_id.eq(info.snapshot_id)))
            .execute(&self.conn)
            .expect("Error deleting snapshots");
        assert!(count <= 1);
    }

    pub fn get_or_create_family_id(&mut self, name_: &str) -> i64 {
        let id_opt = self.family_id_from_name(name_);
        match id_opt {
            Some(id) => id,
            None => {
                use self::schema::family::dsl::*;

                let new = self::schema::NewFamily { name: name_ };

                diesel::insert(&new)
                    .into(family)
                    .execute(&self.conn)
                    .expect("Error inserting family");
                self.last_insert_rowid()
            }
        }
    }

    pub fn snapshot_latest_id(&mut self, family_id_: i64) -> Option<i64> {
        use self::schema::snapshots::dsl::*;
        use diesel::expression::max;

        snapshots.filter(family_id.eq(family_id_))
            .select(max(snapshot_id).nullable())
            .first::<Option<i64>>(&self.conn)
            .optional()
            .expect("Error reading latest snapshot id")
            .and_then(|x| x)
    }

    /// Lookup exact snapshot info from family and snapshot id.
    pub fn snapshot_lookup(&mut self,
                           family_name_: &str,
                           snapshot_id_: i64)
                           -> Option<(SnapshotInfo, hash::Hash, Option<hash::tree::HashRef>)> {
        use self::schema::snapshots::dsl::*;
        use self::schema::family::dsl::{family, name};

        let row_opt = snapshots.inner_join(family)
            .filter(name.eq(family_name_))
            .filter(snapshot_id.eq(snapshot_id_))
            .select((id, tag, family_id, snapshot_id, msg, hash, hash_ref))
            .first::<self::schema::Snapshot>(&self.conn)
            .optional()
            .expect("Error reading snapshot info");

        row_opt.map(|snap| {
            (SnapshotInfo {
                 unique_id: snap.id,
                 family_id: snap.family_id,
                 snapshot_id: snap.snapshot_id,
             },
             ::hash::Hash { bytes: snap.hash.unwrap().to_vec() },
             snap.hash_ref.and_then(|r| ::hash::tree::HashRef::from_bytes(&mut &r[..]).ok()))
        })
    }

    pub fn snapshot_reserve(&mut self, family_: String) -> SnapshotInfo {
        use self::schema::snapshots::dsl::*;

        let family_id_ = self.get_or_create_family_id(&family_);
        let snapshot_id_ = 1 + self.snapshot_latest_id(family_id_).unwrap_or(0);

        let new = self::schema::NewSnapshot {
            family_id: family_id_,
            snapshot_id: snapshot_id_,
            tag: tags::Tag::Reserved as i32,
            msg: None,
            hash: None,
            hash_ref: None,
        };

        diesel::insert(&new)
            .into(snapshots)
            .execute(&self.conn)
            .expect("Error inserting snapshot");

        let unique_id_ = self.last_insert_rowid();

        SnapshotInfo {
            unique_id: unique_id_,
            family_id: family_id_,
            snapshot_id: snapshot_id_,
        }
    }

    pub fn snapshot_update(&mut self,
                           snapshot_: &SnapshotInfo,
                           msg_: &str,
                           hash_: &hash::Hash,
                           hash_ref_: &hash::tree::HashRef) {
        use self::schema::snapshots::dsl::*;

        diesel::update(snapshots.find(snapshot_.unique_id))
            .set((msg.eq(Some(msg_)),
                  hash.eq(Some(&hash_.bytes)),
                  hash_ref.eq(Some(hash_ref_.as_bytes()))))
            .execute(&self.conn)
            .expect("Error updating snapshot");
    }

    pub fn snapshot_set_tag(&mut self, snapshot_: &SnapshotInfo, tag_: tags::Tag) {
        use self::schema::snapshots::dsl::*;

        diesel::update(snapshots.find(snapshot_.unique_id))
            .set(tag.eq(tag_ as i32))
            .execute(&self.conn)
            .expect("Error updating snapshot");
    }

    /// Extract latest snapshot data for family.
    pub fn snapshot_latest(&mut self,
                           family: &str)
                           -> Option<(SnapshotInfo, hash::Hash, Option<hash::tree::HashRef>)> {
        let family_id_opt = self.family_id_from_name(family);
        family_id_opt.and_then(|family_id_| {
            use self::schema::snapshots::dsl::*;

            let row_opt = snapshots.filter(family_id.eq(family_id_))
                .order(snapshot_id.desc())
                .first::<self::schema::Snapshot>(&self.conn)
                .optional()
                .expect("Error reading latest snapshot");

            row_opt.map(|snap| {
                (SnapshotInfo {
                     unique_id: snap.id,
                     family_id: snap.family_id,
                     snapshot_id: snap.snapshot_id,
                 },
                 ::hash::Hash { bytes: snap.hash.expect("Snapshot without top hash") },
                 snap.hash_ref.and_then(|r| ::hash::tree::HashRef::from_bytes(&mut &r[..]).ok()))
            })
        })
    }

    pub fn snapshot_list(&mut self, skip_tag: Option<tags::Tag>) -> Vec<SnapshotStatus> {
        use diesel::*;
        use self::schema::snapshots::dsl::*;
        use self::schema::family::dsl::family;
        let rows = match skip_tag {
                None => {
                    snapshots.inner_join(family)
                        .load::<(self::schema::Snapshot, self::schema::Family)>(&self.conn)
                }
                Some(skip) => {
                    snapshots.inner_join(family)
                        .filter(tag.ne(skip as i32))
                        .load::<(self::schema::Snapshot, self::schema::Family)>(&self.conn)
                }
            }
            .unwrap();

        rows.into_iter()
            .map(|(snap, fam)| {
                let status = tags::tag_from_num(snap.tag as i64)
                    .map_or(SnapshotWorkStatus::CommitComplete, tag_to_work_status);
                let hash_ = snap.hash.and_then(|bytes| {
                    if bytes.is_empty() {
                        None
                    } else {
                        Some(::hash::Hash { bytes: bytes })
                    }
                });
                SnapshotStatus {
                    family_name: fam.name,
                    msg: snap.msg,
                    hash: hash_,
                    hash_ref: snap.hash_ref,
                    status: status,
                    info: SnapshotInfo {
                        unique_id: snap.id,
                        snapshot_id: snap.snapshot_id,
                        family_id: fam.id,
                    },
                }
            })
            .collect()
    }

    /// Recover snapshot information.
    pub fn snapshot_recover(&mut self,
                            snapshot_id_: i64,
                            family: &str,
                            msg_: &str,
                            hash_ref_: &hash::tree::HashRef,
                            work_opt_: Option<SnapshotWorkStatus>) {
        let family_id_ = self.get_or_create_family_id(&family);
        let insert = match self.snapshot_lookup(family, snapshot_id_) {
            Some((_info, h, r)) => {
                if h.bytes != hash_ref_.hash.bytes || r.is_none() || &r.unwrap() != hash_ref_ {
                    panic!("Snapshot already exists, but with different hash");
                }
                false
            }
            None => true,
        };
        if insert {
            use self::schema::snapshots::dsl::*;

            let hash_ref_bytes = hash_ref_.as_bytes();
            let new = self::schema::NewSnapshot {
                family_id: family_id_,
                snapshot_id: snapshot_id_,
                msg: Some(msg_),
                hash: Some(&hash_ref_.hash.bytes[..]),
                hash_ref: Some(&hash_ref_bytes[..]),
                tag: work_opt_.map_or(tags::Tag::Done, work_status_to_tag) as i32,
            };

            diesel::insert(&new)
                .into(snapshots)
                .execute(&self.conn)
                .expect("Error inserting new snapshot");
        }
    }
}
