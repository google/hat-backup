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

use sqlite3::database::{Database};
use sqlite3::types::{SQLITE_DONE, SQLITE_OK, Blob};
use sqlite3::{open};

use hash_index;


pub type SnapshotIndexProcess = process::Process<Msg, Reply, SnapshotIndex>;

pub enum Msg {
  /// Register a new snapshot by its hash and persistent reference.
  Add(String, hash_index::Hash, Vec<u8>),

  /// Flush the hash index to clear internal buffers and commit the underlying database.
  Flush,
}

pub enum Reply {
  AddOK,
  FlushOK,
}


pub struct SnapshotIndex {
  dbh: Database,
}

impl SnapshotIndex {

  pub fn new(path: String) -> SnapshotIndex {
    let mut si = match open(path.as_slice()) {
      Ok(dbh) => { SnapshotIndex{dbh: dbh} },
      Err(err) => fail!(err.to_string()),
    };
    si.exec_or_die("CREATE TABLE IF NOT EXISTS
                    snapshot_index (id        INTEGER PRIMARY KEY,
                                    family    BLOB,
                                    hash      BLOB,
                                    tree_ref  BLOB)");
    si.exec_or_die("BEGIN");
    si
  }

  #[cfg(test)]
  pub fn new_for_testing() -> HashIndex {
    SnapshotIndex::new(":memory:".to_string())
  }

  fn exec_or_die(&mut self, sql: &str) {
    match self.dbh.exec(sql) {
      Ok(true) => (),
      Ok(false) => fail!("exec: {}", self.dbh.get_errmsg()),
      Err(msg) => fail!(format!("exec: {}, {}\nIn sql: '{}'\n",
                                msg.to_string(), self.dbh.get_errmsg(), sql))
    }
  }

  fn add_snapshot(&mut self, family: String, hash: hash_index::Hash, tree_ref: Vec<u8>) {
    let mut insert_stm = self.dbh.prepare(
      "INSERT INTO snapshot_index (family, hash, tree_ref) VALUES (?, ?, ?)", &None).unwrap();

    assert_eq!(SQLITE_OK, insert_stm.bind_param(1, &Blob(family.as_bytes().into_vec())));
    assert_eq!(SQLITE_OK, insert_stm.bind_param(2, &Blob(hash.bytes.clone())));
    assert_eq!(SQLITE_OK, insert_stm.bind_param(3, &Blob(tree_ref)));

    assert_eq!(SQLITE_DONE, insert_stm.step());
  }

  fn flush(&mut self) {
    // Callbacks assume their data is safe, so commit before calling them
    self.exec_or_die("COMMIT; BEGIN");
  }
}


impl process::MsgHandler<Msg, Reply> for SnapshotIndex {
  fn handle(&mut self, msg: Msg, reply: |Reply|) {
    match msg {

      Add(name, hash, tree_ref) => {
        self.add_snapshot(name, hash, tree_ref);
        return reply(AddOK);
      },

      Flush => {
        self.flush();
        return reply(FlushOK);
      }
    }
  }
}
