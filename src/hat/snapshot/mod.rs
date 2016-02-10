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

use process;
use tags;

use diesel;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use blob;
use hash;

mod schema;


#[derive(Clone, Debug)]
pub struct SnapshotInfo {
    pub unique_id: i64,
    pub family_id: i64,
    pub snapshot_id: i64,
}

pub type SnapshotIndexProcess = process::Process<Msg, Reply>;

pub enum Msg {
    Reserve(String),

    /// Update existing snapshot.
    Update(SnapshotInfo, hash::Hash, blob::ChunkRef),

    /// ReadyCommit.
    ReadyCommit(SnapshotInfo),

    /// Register a new snapshot by its family name, hash and persistent reference.
    Commit(SnapshotInfo),

    /// Extract latest snapshot data for family.
    Latest(String),

    /// Lookup exact snapshot info from family and snapshot id.
    Lookup(String, i64),

    /// We are deleting this snapshot.
    WillDelete(SnapshotInfo),

    /// We are ready to delete of this snapshot.
    ReadyDelete(SnapshotInfo),

    /// Delete snapshot.
    Delete(SnapshotInfo),

    /// List incomplete snapshots (either committing or deleting).
    ListNotDone,

    /// List all snapshots.
    ListAll,

    /// Flush the hash index to clear internal buffers and commit the underlying database.
    Flush,

    /// Recover snapshot information.
    Recover(i64, String, String, Vec<u8>, blob::ChunkRef),
}

pub enum Reply {
    Reserved(SnapshotInfo),
    UpdateOk,
    CommitOk,
    Snapshot(Option<(SnapshotInfo, hash::Hash, Option<blob::ChunkRef>)>),
    NotDone(Vec<SnapshotStatus>),
    All(Vec<SnapshotStatus>),
    FlushOk,
    RecoverOk,
}

#[derive(Debug)]
pub enum WorkStatus {
    CommitInProgress,
    CommitComplete,
    DeleteInProgress,
    DeleteComplete,
}

#[derive(Debug)]
pub struct SnapshotStatus {
    pub family_name: String,
    pub info: SnapshotInfo,
    pub hash: Option<hash::Hash>,
    pub msg: Option<String>,
    pub tree_ref: Option<Vec<u8>>,
    pub status: WorkStatus,
}


pub struct SnapshotIndex {
    conn: SqliteConnection,
}

impl SnapshotIndex {
    pub fn new(path: String) -> SnapshotIndex {
        let conn = SqliteConnection::establish(&path).expect("Could not open SQLite database");

        let si = SnapshotIndex { conn: conn };

        diesel::migrations::run_pending_migrations(&si.conn).unwrap();
        si.conn.begin_transaction().unwrap();
        si
    }

    #[cfg(test)]
    pub fn new_for_testing() -> SnapshotIndex {
        SnapshotIndex::new(":memory:".to_string())
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

    fn delete_snapshot(&self, info: SnapshotInfo) {
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
                         family_name_: String,
                         snapshot_id_: i64)
                         -> Option<(SnapshotInfo, hash::Hash, Option<blob::ChunkRef>)> {
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
            (SnapshotInfo {
                unique_id: snap.id,
                family_id: snap.family_id,
                snapshot_id: snap.snapshot_id,
            },
             ::hash::Hash { bytes: snap.hash.unwrap().to_vec() },
             ::blob::ChunkRef::from_bytes(&mut &snap.tree_ref.unwrap()[..]).ok())
        })
    }

    fn reserve_snapshot(&mut self, family_: String) -> SnapshotInfo {
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
        return SnapshotInfo {
            unique_id: unique_id_,
            family_id: family_id_,
            snapshot_id: snapshot_id_,
        };
    }

    fn update(&mut self,
              snapshot_: SnapshotInfo,
              msg_: String,
              hash_: hash::Hash,
              tree_ref_: blob::ChunkRef) {
        use self::schema::snapshots::dsl::*;

        diesel::update(snapshots.find(snapshot_.unique_id))
            .set((msg.eq(Some(msg_)),
                  hash.eq(Some(hash_.bytes)),
                  tree_ref.eq(Some(tree_ref_.as_bytes()))))
            .execute(&self.conn)
            .expect("Error updating snapshot");
    }

    fn set_tag(&mut self, snapshot_: SnapshotInfo, tag_: tags::Tag) {
        use self::schema::snapshots::dsl::*;

        diesel::update(snapshots.find(snapshot_.unique_id))
            .set(tag.eq(tag_ as i32))
            .execute(&self.conn)
            .expect("Error updating snapshot");
    }

    fn latest_snapshot(&mut self,
                       family: String)
                       -> Option<(SnapshotInfo, hash::Hash, Option<blob::ChunkRef>)> {
        let family_id_opt = self.get_family_id(&family);
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
                 ::hash::Hash { bytes: snap.hash.unwrap().to_vec() },
                 ::blob::ChunkRef::from_bytes(&mut &snap.tree_ref.unwrap()[..]).ok())
            })
        })
    }

    fn list_all(&mut self, skip_tag: Option<tags::Tag>) -> Vec<SnapshotStatus> {
        use diesel::*;
        use self::schema::snapshots::dsl::*;
        use self::schema::family::dsl::family;
        let rows = match skip_tag {
                       None =>
                snapshots.inner_join(family)
                         .load::<(self::schema::Snapshot, self::schema::Family)>(&self.conn),
                       Some(skip) =>
                snapshots.inner_join(family)
                         .filter(tag.ne(skip as i32))
                         .load::<(self::schema::Snapshot, self::schema::Family)>(&self.conn),
                   }
                   .unwrap();

        rows.into_iter()
            .map(|(snap, fam)| {
                let status = match tags::tag_from_num(snap.tag as i64) {
                    Some(tags::Tag::Reserved) | Some(tags::Tag::InProgress) => {
                        WorkStatus::CommitInProgress
                    }
                    Some(tags::Tag::Complete) | Some(tags::Tag::Done) | None => {
                        WorkStatus::CommitComplete
                    }
                    Some(tags::Tag::WillDelete) => WorkStatus::DeleteInProgress,
                    Some(tags::Tag::ReadyDelete) | Some(tags::Tag::DeleteComplete) => {
                        WorkStatus::DeleteComplete
                    }
                };
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
                    tree_ref: snap.tree_ref,
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

    fn recover(&mut self,
               snapshot_id_: i64,
               family: String,
               msg_: String,
               hash_: Vec<u8>,
               tree_ref_: blob::ChunkRef) {
        let family_id_ = self.get_or_create_family_id(&family);
        let insert = match self.get_snapshot_info(family, snapshot_id_) {
            Some((_info, h, r)) => {
                if h.bytes != hash_ || r.is_none() || r.unwrap() != tree_ref_ {
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
                msg: Some(&msg_[..]),
                hash: Some(&hash_[..]),
                tree_ref: Some(&tree_bytes[..]),
                tag: tags::Tag::Done as i32,
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


impl process::MsgHandler<Msg, Reply> for SnapshotIndex {
    fn handle(&mut self, msg: Msg, reply: Box<Fn(Reply)>) {
        match msg {

            Msg::Reserve(family) => {
                reply(Reply::Reserved(self.reserve_snapshot(family)));
            }

            Msg::Update(snapshot, hash, tree_ref) => {
                self.update(snapshot, "anonymous".to_owned(), hash, tree_ref);
                return reply(Reply::UpdateOk);
            }

            Msg::ReadyCommit(snapshot) => {
                self.set_tag(snapshot, tags::Tag::Complete);
                return reply(Reply::UpdateOk);
            }

            Msg::Commit(snapshot) => {
                self.set_tag(snapshot, tags::Tag::Done);
                return reply(Reply::CommitOk);
            }

            Msg::Latest(name) => {
                let res_opt = self.latest_snapshot(name);
                return reply(Reply::Snapshot(res_opt));
            }

            Msg::Lookup(name, id) => {
                let res_opt = self.get_snapshot_info(name, id);
                return reply(Reply::Snapshot(res_opt));
            }

            Msg::WillDelete(snapshot) => {
                self.set_tag(snapshot, tags::Tag::WillDelete);
                return reply(Reply::UpdateOk);
            }

            Msg::ReadyDelete(snapshot) => {
                self.set_tag(snapshot, tags::Tag::ReadyDelete);
                return reply(Reply::UpdateOk);
            }

            Msg::Delete(snapshot) => {
                self.delete_snapshot(snapshot);
                return reply(Reply::UpdateOk);
            }

            Msg::ListNotDone => {
                return reply(Reply::NotDone(self.list_all(Some(tags::Tag::Done) /* not_tag */)));
            }

            Msg::ListAll => {
                return reply(Reply::All(self.list_all(None)));
            }

            Msg::Recover(snapshot_id, family_name, msg, hash, tree_ref) => {
                self.recover(snapshot_id, family_name, msg, hash, tree_ref);
                return reply(Reply::RecoverOk);
            }

            Msg::Flush => {
                self.flush();
                return reply(Reply::FlushOk);
            }
        }
    }
}
