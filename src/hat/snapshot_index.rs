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

use sqlite3::cursor::Cursor;
use sqlite3::database::Database;
use sqlite3::BindArg::{Blob, Integer64};
use sqlite3::types::ResultCode::{SQLITE_ERROR, SQLITE_ROW, SQLITE_DONE, SQLITE_OK};
use sqlite3::open;

use hash_index;

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
    Update(SnapshotInfo, hash_index::Hash, Vec<u8>),

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
    Recover(i64, String, String, Vec<u8>, Vec<u8>),
}

pub enum Reply {
    Reserved(SnapshotInfo),
    UpdateOk,
    CommitOk,
    Snapshot(Option<(SnapshotInfo, hash_index::Hash, Vec<u8>)>),
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
    pub hash: Option<hash_index::Hash>,
    pub msg: Option<String>,
    pub tree_ref: Option<Vec<u8>>,
    pub status: WorkStatus,
}


pub struct SnapshotIndex {
    dbh: Database,
}

impl SnapshotIndex {
    pub fn new(path: String) -> SnapshotIndex {
        let mut si = match open(&path) {
            Ok(dbh) => SnapshotIndex { dbh: dbh },
            Err(err) => panic!("{:?}", err),
        };
        si.exec_or_die("CREATE TABLE IF NOT EXISTS
                    family (id    INTEGER \
                        PRIMARY KEY,
                            name  BLOB)");
        si.exec_or_die("CREATE TABLE IF NOT EXISTS
                    snapshot_index (rowid        \
                        INTEGER PRIMARY KEY,
                                    tag          \
                        INTEGER,
                                    family_id    INTEGER,
                                    \
                        snapshot_id  INTEGER,
                                    msg          \
                        BLOB,
                                    hash         BLOB,
                                    \
                        tree_ref     BLOB)");
        si.exec_or_die("BEGIN");
        si
    }

    #[cfg(test)]
    pub fn new_for_testing() -> SnapshotIndex {
        SnapshotIndex::new(":memory:".to_string())
    }

    fn exec_or_die(&mut self, sql: &str) {
        match self.dbh.exec(sql) {
            Ok(true) => (),
            Ok(false) => panic!("exec: {:?}", self.dbh.get_errmsg()),
            Err(msg) => {
                panic!("exec: {:?}, {:?}\nIn sql: '{}'\n",
                       msg,
                       self.dbh.get_errmsg(),
                       sql)
            }
        }
    }

    fn prepare_or_die<'a>(&'a self, sql: &str) -> Cursor<'a> {
        match self.dbh.prepare(sql, &None) {
            Ok(s) => s,
            Err(x) => panic!("sqlite error: {} ({:?})", self.dbh.get_errmsg(), x),
        }
    }

    fn get_family_id(&mut self, family: &str) -> Option<i64> {
        let mut lookup_family = self.prepare_or_die("SELECT id FROM family WHERE name=?");
        assert_eq!(SQLITE_OK,
                   lookup_family.bind_param(1, &Blob(family.as_bytes().to_vec())));

        match lookup_family.step() {
            SQLITE_ERROR => panic!(self.dbh.get_errmsg()),
            SQLITE_ROW => Some(lookup_family.get_i64(0)),
            SQLITE_DONE | SQLITE_OK => None,
            _ => unreachable!(),  // SQLITE_INTERNAL etc
        }
    }

    fn delete_snapshot(&mut self, info: SnapshotInfo) {
        let mut stm = self.prepare_or_die("DELETE FROM snapshot_index WHERE rowid=? AND \
                                           family_id=? AND snapshot_id=? LIMIT 1");
        assert_eq!(SQLITE_OK, stm.bind_param(1, &Integer64(info.unique_id)));
        assert_eq!(SQLITE_OK, stm.bind_param(2, &Integer64(info.family_id)));
        assert_eq!(SQLITE_OK, stm.bind_param(3, &Integer64(info.snapshot_id)));

        assert_eq!(SQLITE_DONE, stm.step());
    }

    fn get_or_create_family_id(&mut self, family: &str) -> i64 {
        let id_opt = self.get_family_id(family);
        match id_opt {
            Some(id) => id,
            None => {
                let mut insert_stm = self.prepare_or_die("INSERT INTO family (name) VALUES (?)");
                assert_eq!(SQLITE_OK,
                           insert_stm.bind_param(1, &Blob(family.as_bytes().to_vec())));
                assert_eq!(SQLITE_DONE, insert_stm.step());
                self.dbh.get_last_insert_rowid()
            }
        }
    }

    fn get_latest_snapshot_id(&mut self, family_id: i64) -> i64 {
        let mut lookup_latest = self.prepare_or_die("SELECT MAX(snapshot_id) FROM snapshot_index \
                                                     WHERE family_id=?");
        assert_eq!(SQLITE_OK,
                   lookup_latest.bind_param(1, &Integer64(family_id)));
        assert_eq!(SQLITE_ROW, lookup_latest.step());
        let id = lookup_latest.get_i64(0);
        assert_eq!(SQLITE_DONE, lookup_latest.step());
        return id;
    }

    fn get_snapshot_info(&mut self,
                         family_name: String,
                         snapshot_id: i64)
                         -> Option<(SnapshotInfo, hash_index::Hash, Vec<u8>)> {
        let family_id = self.get_family_id(&family_name)
                            .expect(&format!("No such family: {}", family_name));
        let mut lookup = self.prepare_or_die("SELECT rowid, hash, tree_ref FROM snapshot_index \
                                              WHERE family_id=? AND snapshot_id=? LIMIT 1");
        assert_eq!(SQLITE_OK, lookup.bind_param(1, &Integer64(family_id)));
        assert_eq!(SQLITE_OK, lookup.bind_param(2, &Integer64(snapshot_id)));
        let status = lookup.step();
        if status == SQLITE_ROW {
            let id = lookup.get_i64(0);
            return Some((SnapshotInfo {
                unique_id: id,
                family_id: family_id,
                snapshot_id: snapshot_id,
            },
                         hash_index::Hash { bytes: lookup.get_blob(1).unwrap().to_vec() },
                         lookup.get_blob(2).unwrap().to_vec()));
        }
        assert_eq!(SQLITE_DONE, status);
        return None;
    }

    fn reserve_snapshot(&mut self, family: String) -> SnapshotInfo {
        let family_id = self.get_or_create_family_id(&family);
        let snapshot_id = 1 + self.get_latest_snapshot_id(family_id);

        let mut insert_stm = self.prepare_or_die("INSERT INTO snapshot_index (family_id, \
                                                  snapshot_id, tag)
       VALUES (?, ?, ?)");

        assert_eq!(SQLITE_OK, insert_stm.bind_param(1, &Integer64(family_id)));
        assert_eq!(SQLITE_OK, insert_stm.bind_param(2, &Integer64(snapshot_id)));
        assert_eq!(SQLITE_OK,
                   insert_stm.bind_param(3, &Integer64(tags::Tag::Reserved as i64)));

        assert_eq!(SQLITE_DONE, insert_stm.step());

        let unique_id = self.dbh.get_last_insert_rowid();
        return SnapshotInfo {
            unique_id: unique_id,
            family_id: family_id,
            snapshot_id: snapshot_id,
        };
    }

    fn update(&mut self,
              snapshot: SnapshotInfo,
              msg: String,
              hash: hash_index::Hash,
              tree_ref: Vec<u8>) {
        let mut update_stm = self.prepare_or_die("UPDATE snapshot_index SET msg=?, hash=?, \
                                                  tree_ref=? WHERE rowid=?");

        assert_eq!(SQLITE_OK,
                   update_stm.bind_param(1, &Blob(msg.as_bytes().to_vec())));
        assert_eq!(SQLITE_OK,
                   update_stm.bind_param(2, &Blob(hash.bytes.clone())));
        assert_eq!(SQLITE_OK, update_stm.bind_param(3, &Blob(tree_ref)));
        assert_eq!(SQLITE_OK,
                   update_stm.bind_param(4, &Integer64(snapshot.unique_id)));

        assert_eq!(SQLITE_DONE, update_stm.step());
    }

    fn set_tag(&mut self, snapshot: SnapshotInfo, tag: tags::Tag) {
        let mut update_stm = self.prepare_or_die("UPDATE snapshot_index SET tag=? WHERE rowid=?");

        assert_eq!(SQLITE_OK, update_stm.bind_param(1, &Integer64(tag as i64)));
        assert_eq!(SQLITE_OK,
                   update_stm.bind_param(2, &Integer64(snapshot.unique_id)));
        assert_eq!(SQLITE_DONE, update_stm.step());
    }

    fn latest_snapshot(&mut self,
                       family: String)
                       -> Option<(SnapshotInfo, hash_index::Hash, Vec<u8>)> {
        let family_id_opt = self.get_family_id(&family);
        family_id_opt.and_then(|family_id| {

            let mut lookup_stm = self.prepare_or_die("SELECT rowid, snapshot_id, hash, tree_ref \
                                                      FROM snapshot_index WHERE family_id=? \
                                                      ORDER BY snapshot_id DESC");

            assert_eq!(SQLITE_OK, lookup_stm.bind_param(1, &Integer64(family_id)));

            if lookup_stm.step() == SQLITE_ROW {
                return Some((SnapshotInfo {
                    unique_id: lookup_stm.get_i64(0),
                    family_id: family_id,
                    snapshot_id: lookup_stm.get_i64(1),
                },
                             hash_index::Hash { bytes: lookup_stm.get_blob(2).unwrap().to_vec() },
                             lookup_stm.get_blob(3).unwrap().to_vec()));
            }
            return None;
        })
    }

    fn list_all(&mut self, not_tag: Option<tags::Tag>) -> Vec<SnapshotStatus> {
        let sql = "SELECT rowid, family_id, snapshot_id, f.name, hash, tag, tree_ref, msg FROM \
                   snapshot_index JOIN family f ON (f.rowid == family_id)";

        let where_ = not_tag.map_or("".to_owned(), |t| format!(" WHERE tag!={:?}", t as i64));
        let mut lookup_stm = self.prepare_or_die(&format!("{} {}", sql, where_));

        let mut outs = vec![];
        while lookup_stm.step() == SQLITE_ROW {

            let info = SnapshotInfo {
                unique_id: lookup_stm.get_i64(0),
                family_id: lookup_stm.get_i64(1),
                snapshot_id: lookup_stm.get_i64(2),
            };

            let name = lookup_stm.get_text(3).unwrap().to_owned();

            let hash = lookup_stm.get_blob(4).and_then(|h| {
                if h.len() == 0 {
                    None
                } else {
                    Some(hash_index::Hash { bytes: h.to_vec() })
                }
            });

            let tag = tags::tag_from_num(lookup_stm.get_i64(5));
            let status = match tag {
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

            outs.push(SnapshotStatus {
                info: info,
                hash: hash,
                status: status,
                tree_ref: lookup_stm.get_blob(6).map(|s| s.to_owned()),
                msg: lookup_stm.get_text(7).map(|s| s.to_owned()),
                family_name: name,
            });
        }

        return outs;

    }

    fn recover(&mut self,
               snapshot_id: i64,
               family: String,
               msg: String,
               hash: Vec<u8>,
               tree_ref: Vec<u8>) {
        let family_id = self.get_or_create_family_id(&family);
        let insert = match self.get_snapshot_info(family, snapshot_id) {
            Some((_info, h, r)) => {
                if h.bytes != hash && r != tree_ref {
                    panic!("Snapshot already exists, but with different hash");
                }
                false
            }
            None => true,
        };
        if insert {
            let mut insert_stm = self.prepare_or_die("INSERT INTO snapshot_index (family_id, \
                                                      snapshot_id, msg, hash, tree_ref) VALUES \
                                                      (?, ?, ?, ?, ?)");
            assert_eq!(SQLITE_OK, insert_stm.bind_param(1, &Integer64(family_id)));
            assert_eq!(SQLITE_OK, insert_stm.bind_param(2, &Integer64(snapshot_id)));
            assert_eq!(SQLITE_OK,
                       insert_stm.bind_param(3, &Blob(msg.as_bytes().to_vec())));
            assert_eq!(SQLITE_OK, insert_stm.bind_param(4, &Blob(hash)));
            assert_eq!(SQLITE_OK, insert_stm.bind_param(5, &Blob(tree_ref)));
            assert_eq!(SQLITE_DONE, insert_stm.step());
        }
    }

    fn flush(&mut self) {
        self.exec_or_die("COMMIT; BEGIN");
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
