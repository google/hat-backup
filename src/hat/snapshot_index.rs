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

use sqlite3::cursor::{Cursor};
use sqlite3::database::{Database};
use sqlite3::BindArg::{Blob, Integer64};
use sqlite3::types::ResultCode::{SQLITE_ERROR, SQLITE_ROW, SQLITE_DONE, SQLITE_OK};
use sqlite3::{open};

use hash_index;


pub type SnapshotIndexProcess = process::Process<Msg, Reply>;

pub enum Msg {
  /// Register a new snapshot by its family name, hash and persistent reference.
  Add(String, hash_index::Hash, Vec<u8>),

  /// Extract latest snapshot data for family.
  Latest(String),

  /// Flush the hash index to clear internal buffers and commit the underlying database.
  Flush,
}

pub enum Reply {
  AddOK,
  Latest(Option<(hash_index::Hash, Vec<u8>)>),
  FlushOK,
}


pub struct SnapshotIndex {
  dbh: Database,
}

impl SnapshotIndex {

  pub fn new(path: String) -> SnapshotIndex {
    let mut si = match open(&path) {
      Ok(dbh) => { SnapshotIndex{dbh: dbh} },
      Err(err) => panic!("{:?}", err),
    };
    si.exec_or_die("CREATE TABLE IF NOT EXISTS
                    family (id    INTEGER PRIMARY KEY,
                            name  BLOB)");
    si.exec_or_die("CREATE TABLE IF NOT EXISTS
                    snapshot_index (family_id    INTEGER,
                                    snapshot_id  INTEGER,
                                    msg          BLOB,
                                    hash         BLOB,
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
      Err(msg) => panic!("exec: {:?}, {:?}\nIn sql: '{}'\n", msg, self.dbh.get_errmsg(), sql)
    }
  }

 fn prepare_or_die<'a>(&'a self, sql: &str) -> Cursor<'a> {
    match self.dbh.prepare(sql, &None) {
      Ok(s)  => s,
      Err(x) => panic!("sqlite error: {} ({:?})",
                       self.dbh.get_errmsg(), x),
    }
  }

  fn get_family_id(&mut self, family: &String) -> Option<i64> {
    let mut lookup_family = self.prepare_or_die("SELECT id FROM family WHERE name=?");
    assert_eq!(SQLITE_OK, lookup_family.bind_param(1, &Blob(family.as_bytes().to_vec())));

    match lookup_family.step() {
      SQLITE_ERROR => panic!(self.dbh.get_errmsg()),
      SQLITE_ROW => Some(lookup_family.get_i64(0)),
      SQLITE_DONE | SQLITE_OK => None,
      _ => unreachable!(),  // SQLITE_INTERNAL etc
    }
  }

  fn get_or_create_family_id(&mut self, family: &String) -> i64 {
    let id_opt = self.get_family_id(family);
    match id_opt {
      Some(id) => id,
      None => {
        let mut insert_stm = self.prepare_or_die("INSERT INTO family (name) VALUES (?)");
        assert_eq!(SQLITE_OK, insert_stm.bind_param(1, &Blob(family.as_bytes().to_vec())));
        assert_eq!(SQLITE_DONE, insert_stm.step());
        self.dbh.get_last_insert_rowid()
      }
    }
  }

  fn get_latest_snapshot_id(&mut self, family_id: i64) -> i64 {
    let mut lookup_latest = self.prepare_or_die(
      "SELECT MAX(snapshot_id) FROM snapshot_index WHERE family_id=?");
    assert_eq!(SQLITE_OK, lookup_latest.bind_param(1, &Integer64(family_id)));
    assert_eq!(SQLITE_ROW, lookup_latest.step());
    let id = lookup_latest.get_i64(0);
    assert_eq!(SQLITE_DONE, lookup_latest.step());
    return id;
  }

  fn add_snapshot(&mut self, family: String, msg: String, hash: hash_index::Hash, tree_ref: Vec<u8>)
  {
    let family_id = self.get_or_create_family_id(&family);
    let snapshot_id = 1 + self.get_latest_snapshot_id(family_id);

    let mut insert_stm = self.prepare_or_die(
      "INSERT INTO snapshot_index (family_id, snapshot_id, msg, hash, tree_ref)
       VALUES (?, ?, ?, ?, ?)");

    assert_eq!(SQLITE_OK, insert_stm.bind_param(1, &Integer64(family_id)));
    assert_eq!(SQLITE_OK, insert_stm.bind_param(2, &Integer64(snapshot_id)));
    assert_eq!(SQLITE_OK, insert_stm.bind_param(3, &Blob(msg.as_bytes().to_vec())));
    assert_eq!(SQLITE_OK, insert_stm.bind_param(4, &Blob(hash.bytes.clone())));
    assert_eq!(SQLITE_OK, insert_stm.bind_param(5, &Blob(tree_ref)));

    assert_eq!(SQLITE_DONE, insert_stm.step());
  }

  fn latest_snapshot(&mut self, family: String) -> Option<(hash_index::Hash, Vec<u8>)> {
    let family_id_opt = self.get_family_id(&family);
    family_id_opt.and_then(|family_id| {

    let mut lookup_stm = self.prepare_or_die(
      "SELECT hash, tree_ref FROM snapshot_index WHERE family_id=? ORDER BY snapshot_id DESC");

    assert_eq!(SQLITE_OK, lookup_stm.bind_param(1, &Integer64(family_id)));

    if lookup_stm.step() == SQLITE_ROW {
      return Some((hash_index::Hash{bytes: lookup_stm.get_blob(0).unwrap().to_vec()},
                   lookup_stm.get_blob(1).unwrap().to_vec()));
    }
    return None;
    })
  }

  fn flush(&mut self) {
    self.exec_or_die("COMMIT; BEGIN");
  }
}


impl process::MsgHandler<Msg, Reply> for SnapshotIndex {
  fn handle(&mut self, msg: Msg, reply: Box<Fn(Reply)>) {
    match msg {

      Msg::Add(family, hash, tree_ref) => {
        self.add_snapshot(family, "anonymous".to_string(), hash, tree_ref);
        return reply(Reply::AddOK);
      },

      Msg::Latest(name) => {
        let res_opt = self.latest_snapshot(name);
        return reply(Reply::Latest(res_opt));
      }

      Msg::Flush => {
        self.flush();
        return reply(Reply::FlushOK);
      }
    }
  }
}
