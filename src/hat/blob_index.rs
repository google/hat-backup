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

//! Local state for external blobs and their states.

use sodiumoxide::randombytes::{randombytes};

use collections::{HashMap};
use serialize::hex::{ToHex};

use process::{Process, MsgHandler};
use sqlite3::database::{Database};

use sqlite3::cursor::{Cursor};
use sqlite3::types::{SQLITE_ROW};
use sqlite3::{open};


pub type BlobIndexProcess = Process<Msg, Reply, BlobIndex>;

#[deriving(Clone, Show)]
pub struct BlobDesc {
  pub name: ~[u8],
  pub id: i64,
}

pub enum Msg {
  /// Reserve an internal `BlobDesc` for a new blob.
  Reserve,

  /// Report that this blob is in the process of being committed to persistent storage. If a
  /// blob is in this state when the system starts up, it may or may not exist in the persistent
  /// storage, but **should not** be referenced elsewhere, and is therefore safe to delete.
  InAir(BlobDesc),

  /// Report that this blob has been fully committed to persistent storage. We can now use its
  /// reference internally. Only committed blobs are considered "safe to use".
  CommitDone(BlobDesc),
}

pub enum Reply {
  Reserved(BlobDesc),
  CommitOK,
}

pub struct BlobIndex {
  dbh: Database,
  next_id: i64,
  reserved: HashMap<~[u8], BlobDesc>,
}


impl BlobIndex {

  pub fn new(path: ~str) -> BlobIndex {
    let mut hi = match open(path) {
      Ok(dbh) => BlobIndex{
        dbh: dbh,
        next_id: -1,
        reserved: HashMap::new(),
      },
      Err(err) => fail!(err.to_str()),
    };
    hi.initialize();
    hi
  }

  #[cfg(test)]
  pub fn newForTesting() -> BlobIndex {
    BlobIndex::new(":memory:".to_owned())
  }

  fn initialize(&mut self) {
    self.execOrDie("CREATE TABLE IF NOT EXISTS
                    blob_index (id        INTEGER PRIMARY KEY,
                                name      BLOB,
                                tag       INT)");
    self.execOrDie("CREATE UNIQUE INDEX IF NOT EXISTS
                    BlobIndex_UniqueName ON blob_index(name)");
    self.execOrDie("BEGIN");

    self.refresh_next_id();
  }

  fn newBlobDesc(&mut self) -> BlobDesc {
    BlobDesc{name: randombytes(24),
             id: self.next_id()}
  }

  fn execOrDie(&self, sql: &str) {
    match self.dbh.exec(sql) {
      Ok(true) => (),
      Ok(false) => fail!("exec: " + self.dbh.get_errmsg()),
      Err(msg) => fail!(format!("exec: {}, {}\nIn sql: '{}'\n",
                                msg.to_str(), self.dbh.get_errmsg(), sql))
    }
  }

  fn prepareOrDie<'a>(&'a self, sql: &str) -> Cursor<'a> {
    match self.dbh.prepare(sql, &None) {
      Ok(s)  => s,
      Err(x) => fail!(format!("sqlite error: {} ({:?})",
                              self.dbh.get_errmsg(), x)),
    }
  }

  fn select1<'a>(&'a self, sql: &str) -> Option<Cursor<'a>> {
    let cursor = self.prepareOrDie(sql);
    if cursor.step() == SQLITE_ROW {
      Some(cursor)
    } else { None }
  }

  fn refresh_next_id(&mut self) {
    let id = self.select1("SELECT MAX(id) FROM blob_index").unwrap().get_int(0);
    self.next_id = (id as i64) + 1;
  }

  fn next_id(&mut self) -> i64 {
    let id = self.next_id;
    self.next_id += 1;
    id
  }

  fn reserve(&mut self) -> BlobDesc {
    let blob = self.newBlobDesc();
    self.reserved.insert(blob.name.clone(), blob.clone());
    blob
  }

  fn in_air(&mut self, blob: &BlobDesc) {
    assert!(self.reserved.find(&blob.name).is_some(), "blob was not reserved!");
    self.execOrDie(format!(
      "INSERT INTO blob_index (id, name, tag) VALUES ({}, x'{}', {})",
      blob.id, blob.name.to_hex(), 1));
    self.new_transaction();
  }

  fn new_transaction(&mut self) {
    self.execOrDie("COMMIT; BEGIN");
  }

  fn commitBlob(&mut self, blob: &BlobDesc) {
    assert!(self.reserved.find(&blob.name).is_some(), "blob was not reserved!");
    self.execOrDie(format!("UPDATE blob_index SET tag=0 WHERE id={}", blob.id));
    self.new_transaction();
  }
}

impl Drop for BlobIndex {
  fn drop(&mut self) {
    self.execOrDie("COMMIT");
  }
}

impl MsgHandler<Msg, Reply> for BlobIndex {
  fn handle(&mut self, msg: Msg, reply: |Reply|) {
    match msg {
      Reserve => {
        return reply(Reserved(self.reserve()));
      },
      InAir(blob) => {
        self.in_air(&blob);
        return reply(CommitOK);
      },
      CommitDone(blob) => {
        self.commitBlob(&blob);
        return reply(CommitOK);
      }
    }
  }
}
