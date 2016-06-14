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

//! Local state for known snapshots.

use std::sync::{Mutex, MutexGuard};

use diesel;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use blob;
use errors::DieselError;
use hash;
use tags;
use util;

mod schema;


#[derive(Clone, Debug)]
pub struct Info {
    pub unique_id: i64,
    pub family_id: i64,
    pub snapshot_id: i64,
}

#[derive(Debug)]
pub enum WorkStatus {
    CommitInProgress,
    CommitComplete,
    DeleteInProgress,
    DeleteComplete,
    RecoverInProgress,
}

#[derive(Debug)]
pub struct Status {
    pub family_name: String,
    pub info: Info,
    pub hash: Option<hash::Hash>,
    pub msg: Option<String>,
    pub tree_ref: Option<Vec<u8>>,
    pub status: WorkStatus,
}


pub struct InternalSnapshotIndex {
    conn: SqliteConnection,
}

pub struct SnapshotIndex(Mutex<InternalSnapshotIndex>);


fn tag_to_work_status(tag: tags::Tag) -> WorkStatus {
    match tag {
        tags::Tag::Reserved | tags::Tag::InProgress => WorkStatus::CommitInProgress,
        tags::Tag::Complete | tags::Tag::Done => WorkStatus::CommitComplete,
        tags::Tag::WillDelete => WorkStatus::DeleteInProgress,
        tags::Tag::ReadyDelete |
        tags::Tag::DeleteComplete => WorkStatus::DeleteComplete,
        tags::Tag::RecoverInProgress => WorkStatus::RecoverInProgress,
    }
}

fn work_status_to_tag(status: WorkStatus) -> tags::Tag {
    match status {
        WorkStatus::CommitInProgress => tags::Tag::InProgress,
        WorkStatus::CommitComplete => tags::Tag::Done,
        WorkStatus::DeleteInProgress => tags::Tag::WillDelete,
        WorkStatus::DeleteComplete => tags::Tag::DeleteComplete,
        WorkStatus::RecoverInProgress => tags::Tag::RecoverInProgress,
    }
}

impl InternalSnapshotIndex {
    pub fn new(path: &str) -> Result<InternalSnapshotIndex, DieselError> {
        let conn = try!(SqliteConnection::establish(path));

        let si = InternalSnapshotIndex { conn: conn };

        let dir = try!(diesel::migrations::find_migrations_directory());
        try!(diesel::migrations::run_pending_migrations_in_directory(&si.conn,
                                                                     &dir,
                                                                     &mut util::InfoWriter));

        try!(si.conn.begin_transaction());
        Ok(si)
    }

    fn last_insert_rowid(&self) -> i64 {
        diesel::select(diesel::expression::sql("last_insert_rowid()"))
            .first::<i64>(&self.conn)
            .unwrap()
    }

    fn get_family_id(&mut self, name_: &str) -> Option<i64> {
        use self::schema::family::dsl::*;

        family.filter(name.eq(name_))
            .select(id)
            .first::<i64>(&self.conn)
            .optional()
            .expect("Error reading family")
    }

    fn delete_snapshot(&self, info: Info) {
        use self::schema::snapshots::dsl::*;

        let count = diesel::delete(snapshots.find(info.unique_id)
                .filter(family_id.eq(info.family_id))
                .filter(snapshot_id.eq(info.snapshot_id)))
            .execute(&self.conn)
            .expect("Error deleting snapshots");
        assert!(count <= 1);
    }

    fn get_or_create_family_id(&mut self, name_: &str) -> i64 {
        let id_opt = self.get_family_id(name_);
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

    fn get_latest_snapshot_id(&mut self, family_id_: i64) -> Option<i64> {
        use self::schema::snapshots::dsl::*;
        use diesel::expression::max;

        snapshots.filter(family_id.eq(family_id_))
            .select(max(snapshot_id).nullable())
            .first::<Option<i64>>(&self.conn)
            .optional()
            .expect("Error reading latest snapshot id")
            .and_then(|x| x)
    }

    fn get_snapshot_info(&mut self,
                         family_name_: &str,
                         snapshot_id_: i64)
                         -> Option<(Info, hash::Hash, Option<blob::ChunkRef>)> {
        use self::schema::snapshots::dsl::*;
        use self::schema::family::dsl::{family, name};

        let row_opt = snapshots.inner_join(family)
            .filter(name.eq(family_name_))
            .filter(snapshot_id.eq(snapshot_id_))
            .select((id, tag, family_id, snapshot_id, msg, hash, tree_ref))
            .first::<self::schema::Snapshot>(&self.conn)
            .optional()
            .expect("Error reading snapshot info");

        row_opt.map(|snap| {
            (Info {
                unique_id: snap.id,
                family_id: snap.family_id,
                snapshot_id: snap.snapshot_id,
            },
             ::hash::Hash { bytes: snap.hash.unwrap().to_vec() },
             ::blob::ChunkRef::from_bytes(&mut &snap.tree_ref.unwrap()[..]).ok())
        })
    }

    fn reserve_snapshot(&mut self, family_: String) -> Info {
        use self::schema::snapshots::dsl::*;

        let family_id_ = self.get_or_create_family_id(&family_);
        let snapshot_id_ = 1 + self.get_latest_snapshot_id(family_id_).unwrap_or(0);

        let new = self::schema::NewSnapshot {
            family_id: family_id_,
            snapshot_id: snapshot_id_,
            tag: tags::Tag::Reserved as i32,
            msg: None,
            hash: None,
            tree_ref: None,
        };

        diesel::insert(&new)
            .into(snapshots)
            .execute(&self.conn)
            .expect("Error inserting snapshot");

        let unique_id_ = self.last_insert_rowid();

        Info {
            unique_id: unique_id_,
            family_id: family_id_,
            snapshot_id: snapshot_id_,
        }
    }

    fn update(&mut self,
              snapshot_: &Info,
              msg_: &str,
              hash_: &hash::Hash,
              tree_ref_: &blob::ChunkRef) {
        use self::schema::snapshots::dsl::*;

        diesel::update(snapshots.find(snapshot_.unique_id))
            .set((msg.eq(Some(msg_)),
                  hash.eq(Some(&hash_.bytes)),
                  tree_ref.eq(Some(tree_ref_.as_bytes()))))
            .execute(&self.conn)
            .expect("Error updating snapshot");
    }

    fn set_tag(&mut self, snapshot_: &Info, tag_: tags::Tag) {
        use self::schema::snapshots::dsl::*;

        diesel::update(snapshots.find(snapshot_.unique_id))
            .set(tag.eq(tag_ as i32))
            .execute(&self.conn)
            .expect("Error updating snapshot");
    }

    fn latest_snapshot(&mut self,
                       family: &str)
                       -> Option<(Info, hash::Hash, Option<blob::ChunkRef>)> {
        let family_id_opt = self.get_family_id(&family);
        family_id_opt.and_then(|family_id_| {
            use self::schema::snapshots::dsl::*;

            let row_opt = snapshots.filter(family_id.eq(family_id_))
                .order(snapshot_id.desc())
                .first::<self::schema::Snapshot>(&self.conn)
                .optional()
                .expect("Error reading latest snapshot");

            row_opt.map(|snap| {
                (Info {
                    unique_id: snap.id,
                    family_id: snap.family_id,
                    snapshot_id: snap.snapshot_id,
                },
                 ::hash::Hash { bytes: snap.hash.unwrap().to_vec() },
                 ::blob::ChunkRef::from_bytes(&mut &snap.tree_ref.unwrap()[..]).ok())
            })
        })
    }

    fn list_all(&mut self, skip_tag: Option<tags::Tag>) -> Vec<Status> {
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
                    .map_or(WorkStatus::CommitComplete, tag_to_work_status);
                let hash_ = snap.hash.and_then(|bytes| {
                    if bytes.is_empty() {
                        None
                    } else {
                        Some(::hash::Hash { bytes: bytes })
                    }
                });
                Status {
                    family_name: fam.name,
                    msg: snap.msg,
                    hash: hash_,
                    tree_ref: snap.tree_ref,
                    status: status,
                    info: Info {
                        unique_id: snap.id,
                        snapshot_id: snap.snapshot_id,
                        family_id: fam.id,
                    },
                }
            })
            .collect()
    }

    fn recover(&mut self,
               snapshot_id_: i64,
               family: &str,
               msg_: &str,
               hash_: &[u8],
               tree_ref_: &blob::ChunkRef,
               work_opt_: Option<WorkStatus>) {
        let family_id_ = self.get_or_create_family_id(&family);
        let insert = match self.get_snapshot_info(family, snapshot_id_) {
            Some((_info, h, r)) => {
                if h.bytes != hash_ || r.is_none() || &r.unwrap() != tree_ref_ {
                    panic!("Snapshot already exists, but with different hash");
                }
                false
            }
            None => true,
        };
        if insert {
            use self::schema::snapshots::dsl::*;

            let tree_bytes = tree_ref_.as_bytes();
            let new = self::schema::NewSnapshot {
                family_id: family_id_,
                snapshot_id: snapshot_id_,
                msg: Some(msg_),
                hash: Some(hash_),
                tree_ref: Some(&tree_bytes[..]),
                tag: work_opt_.map_or(tags::Tag::Done, work_status_to_tag) as i32,
            };

            diesel::insert(&new)
                .into(snapshots)
                .execute(&self.conn)
                .expect("Error inserting new snapshot");
        }
    }

    fn flush(&mut self) {
        self.conn.commit_transaction().unwrap();
        self.conn.begin_transaction().unwrap();
    }
}

impl SnapshotIndex {
    pub fn new(path: &str) -> Result<SnapshotIndex, DieselError> {
        InternalSnapshotIndex::new(path).map(|index| SnapshotIndex(Mutex::new(index)))
    }

    #[cfg(test)]
    pub fn new_for_testing() -> Result<SnapshotIndex, DieselError> {
        SnapshotIndex::new(":memory:")
    }

    fn lock(&self) -> MutexGuard<InternalSnapshotIndex> {
        self.0.lock().expect("index-process has failed")
    }

    pub fn reserve(&self, family: String) -> Info {
        self.lock().reserve_snapshot(family)
    }

    /// Update existing snapshot.
    pub fn update(&self, snapshot: &Info, hash: &hash::Hash, tree_ref: &blob::ChunkRef) {
        self.lock().update(snapshot, "anonymous", hash, tree_ref);
    }

    /// ReadyCommit.
    pub fn ready_commit(&self, snapshot: &Info) {
        self.lock().set_tag(snapshot, tags::Tag::Complete)
    }

    /// Register a new snapshot by its family name, hash and persistent reference.
    pub fn commit(&self, snapshot: &Info) {
        self.lock().set_tag(snapshot, tags::Tag::Done)
    }

    /// Extract latest snapshot data for family.
    pub fn latest(&self, name: &str) -> Option<(Info, hash::Hash, Option<blob::ChunkRef>)> {
        self.lock().latest_snapshot(name)
    }

    /// Lookup exact snapshot info from family and snapshot id.
    pub fn lookup(&self,
                  name: &str,
                  id: i64)
                  -> Option<(Info, hash::Hash, Option<blob::ChunkRef>)> {
        self.lock().get_snapshot_info(name, id)
    }

    /// We are deleting this snapshot.
    pub fn will_delete(&self, snapshot: &Info) {
        self.lock().set_tag(snapshot, tags::Tag::WillDelete)
    }

    /// We are ready to delete of this snapshot.
    pub fn ready_delete(&self, snapshot: &Info) {
        self.lock().set_tag(snapshot, tags::Tag::ReadyDelete)
    }

    /// Delete snapshot.
    pub fn delete(&self, snapshot: Info) {
        self.lock().delete_snapshot(snapshot)
    }

    /// List incomplete snapshots (either committing or deleting).
    pub fn list_not_done(&self) -> Vec<Status> {
        self.lock().list_all(Some(tags::Tag::Done) /* not_tag */)
    }

    /// List all snapshots.
    pub fn list_all(&self) -> Vec<Status> {
        self.lock().list_all(None)
    }

    /// Flush the hash index to clear internal buffers and commit the underlying database.
    pub fn flush(&self) {
        self.lock().flush()
    }

    /// Recover snapshot information.
    pub fn recover(&self,
                   snapshot_id: i64,
                   family_name: &str,
                   msg: &str,
                   hash: &[u8],
                   tree_ref: &blob::ChunkRef,
                   work_opt: Option<WorkStatus>) {
        self.lock().recover(snapshot_id, family_name, msg, hash, tree_ref, work_opt)
    }
}
