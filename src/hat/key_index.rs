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

//! Local state for keys in the snapshot in progress (the "index").

use std::time::duration::{Duration};

use periodic_timer::{PeriodicTimer};
use process::{Process, MsgHandler};
use sqlite3::database::{Database};

use sqlite3::cursor::{Cursor};
use sqlite3::types::ResultCode::{SQLITE_ROW, SQLITE_DONE};
use sqlite3::{open};

use rustc_serialize::hex::{ToHex};


pub trait KeyEntry<KE> {
  fn id(&self) -> Option<u64>;
  fn parent_id(&self) -> Option<u64>;

  fn name(&self) -> Vec<u8>;
  fn size(&self) -> Option<u64>;

  // TODO(jos): SQLite3 supports only i64 precisely. Do we propagate this type or convert to it?
  // Currently, values larger than 1<<63 gets converted to doubles silently and breaks.
  fn created(&self) -> Option<u64>;
  fn modified(&self) -> Option<u64>;
  fn accessed(&self) -> Option<u64>;

  fn permissions(&self) -> Option<u64>;
  fn user_id(&self) -> Option<u64>;
  fn group_id(&self) -> Option<u64>;

  fn with_id(self, Option<u64>) -> KE;
}

pub type KeyIndexProcess<KE> = Process<Msg<KE>, Reply>;

pub enum Msg<KeyEntryT> {

  /// Insert an entry in the key index.
  /// Returns `Id` with the new entry ID.
  Insert(KeyEntryT),

  /// Lookup an entry in the key index, to see if it exists.
  /// Returns either `Id` with the found entry ID or `Notfound`.
  LookupExact(KeyEntryT),

  /// Update the `payload` and `persistent_ref` of an entry.
  /// Returns `UpdateOK`.
  UpdateDataHash(KeyEntryT, Option<Vec<u8>>, Option<Vec<u8>>),

  /// List a directory (aka. `level`) in the index.
  /// Returns `ListResult` with all the entries under the given parent.
  ListDir(Option<u64>),

  /// Flush this key index.
  Flush,
}

pub enum Reply {
  Id(u64),
  NotFound,
  UpdateOK,
  ListResult(Vec<(u64, Vec<u8>, u64, u64, u64, Vec<u8>, Vec<u8>)>),
  FlushOK,
}


pub struct KeyIndex {
  path: String,
  dbh: Database,
  flush_timer: PeriodicTimer,
}


fn i64_to_u64_or_panic(x: i64) -> u64 {
  if x < 0 {
    panic!("expected u64, got {:?}", x);
  }
  return x as u64
}


impl KeyIndex {
  pub fn new(path: String) -> KeyIndex {
    let mut ki = match open(&path) {
      Ok(dbh) => {
        KeyIndex{path: path,
                 dbh: dbh,
                 flush_timer: PeriodicTimer::new(Duration::seconds(5))}
      },
      Err(err) => panic!("{:?}", err),
    };
    ki.exec_or_die("CREATE TABLE IF NOT EXISTS
                    key_index (rowid          INTEGER PRIMARY KEY,
                               parent         INTEGER,
                               name           BLOB,
                               created        UINT8,
                               modified       UINT8,
                               accessed       UINT8,
                               hash           BLOB,
                               persistent_ref BLOB
                            );");

    if cfg!(test) {
      ki.exec_or_die("CREATE UNIQUE INDEX IF NOT EXISTS
                      KeyIndex_UniqueParentName
                      ON key_index(parent, name)");
    }

    ki.exec_or_die("BEGIN");
    ki
  }

  #[cfg(test)]
  pub fn new_for_testing() -> KeyIndex {
    KeyIndex::new(":memory:".to_string())
  }

  fn exec_or_die(&mut self, sql: &str) {
    match self.dbh.exec(sql) {
      Ok(true) => (),
      Ok(false) => panic!("exec: {:?}, {}", self.dbh.get_errmsg(), sql),
      Err(msg) => panic!("exec: {:?}, {:?}, {}", msg, self.dbh.get_errmsg(), sql),
    }
  }

  fn prepare_or_die<'a>(&'a mut self, sql: &str) -> Cursor<'a> {
    match self.dbh.prepare(sql, &None) {
      Ok(s)  => s,
      Err(x) => panic!("sqlite error: {} ({:?})", self.dbh.get_errmsg(), x),
    }
  }

  pub fn maybe_flush(&mut self) {
    if self.flush_timer.did_fire() {
      self.flush();
    }
  }

  pub fn flush(&mut self) {
    self.exec_or_die("COMMIT; BEGIN");
  }
}

impl Clone for KeyIndex {
  fn clone(&self) -> KeyIndex {
    KeyIndex::new(self.path.clone())
  }
}

impl Drop for KeyIndex {
  fn drop(&mut self) {
    self.exec_or_die("COMMIT");
  }
}

impl <A: KeyEntry<A>> MsgHandler<Msg<A>, Reply> for KeyIndex {
  fn handle(&mut self, msg: Msg<A>, reply: Box<Fn(Reply)>) {
    match msg {

      Msg::Insert(entry) => {
        let parent = entry.parent_id().unwrap_or(0);

        self.exec_or_die(&format!(
          "INSERT OR REPLACE INTO key_index (parent, name, created, modified, accessed)
           VALUES ({:?}, x'{}', {}, {}, {})",
          parent, entry.name().to_hex(),
          entry.created().unwrap_or(0),
          entry.modified().unwrap_or(0),
          entry.accessed().unwrap_or(0)));

        let id = self.dbh.get_last_insert_rowid();
        return reply(Reply::Id(i64_to_u64_or_panic(id)));
      },

      Msg::LookupExact(entry) => {
        let parent = entry.parent_id().unwrap_or(0);
        let mut cursor = self.prepare_or_die(&format!(
          "SELECT rowid FROM key_index
           WHERE parent={:?} AND name=x'{}'
           AND created={} AND modified={} AND accessed={}
           LIMIT 1",
          parent, entry.name().to_hex(),
          entry.created().unwrap_or(0),
          entry.modified().unwrap_or(0),
          entry.accessed().unwrap_or(0)));
        if cursor.step() == SQLITE_ROW {
          let id = i64_to_u64_or_panic(cursor.get_i64(0));
          assert!(cursor.step() == SQLITE_DONE);
          return reply(Reply::Id(id));
        } else {
          return reply(Reply::NotFound);
        }
      },

      Msg::UpdateDataHash(entry, hash_opt, persistent_ref_opt) => {
        let parent = entry.parent_id().unwrap_or(0);

        assert!(hash_opt.is_some() == persistent_ref_opt.is_some());

        if hash_opt.is_some() && persistent_ref_opt.is_some() {
          match entry.modified() {
            Some(modified) => {
              self.exec_or_die(&format!(
                "UPDATE key_index SET hash=x'{}', persistent_ref=x'{}'
                                                  , modified={}
                  WHERE parent={:?} AND rowid={:?} AND IFNULL(modified,0)<={}",
                hash_opt.unwrap().to_hex(),
                persistent_ref_opt.unwrap().to_hex(),
                modified, parent, entry.id().expect("UpdateDataHash"),
                modified));
            },
            None => {
              self.exec_or_die(&format!(
                "UPDATE key_index SET hash=x'{}', persistent_ref=x'{}'
                 WHERE parent={:?} AND rowid={:?}",
                hash_opt.unwrap().to_hex(),
                persistent_ref_opt.unwrap().to_hex(),
                parent, entry.id().expect("UpdateDataHash, None")));
            }
          }
        } else {
          match entry.modified() {
            Some(modified) => {
              self.exec_or_die(&format!(
                "UPDATE key_index SET hash=NULL, persistent_ref=NULL
                                               , modified={}
                 WHERE parent={:?} AND rowid={:?} AND IFNULL(modified, 0)<={}",
                modified, parent, entry.id().expect("UpdateDataHash2"), modified));
            },
            None => {
              self.exec_or_die(&format!(
                "UPDATE key_index SET hash=NULL, persistent_ref=NULL
                 WHERE parent={:?} AND rowid={}",
                parent, entry.id().expect("UpdateDataHash2, None")));
            }
          }
        }

        self.maybe_flush();
        return reply(Reply::UpdateOK);
      },

      Msg::Flush => {
        self.flush();
        return reply(Reply::FlushOK);
      },

      Msg::ListDir(parent_opt) => {
        let mut listing = Vec::new();
        let parent = parent_opt.unwrap_or(0);

        let mut cursor = self.prepare_or_die(&format!(
           "SELECT rowid, name, created, modified, accessed, hash, persistent_ref
            FROM key_index
            WHERE parent={:?}", parent));

        while cursor.step() == SQLITE_ROW {
          let id = i64_to_u64_or_panic(cursor.get_i64(0));
          let name = cursor.get_blob(1).expect("name").iter().map(|&x| x).collect();
          let created = cursor.get_i64(2);
          let modified = cursor.get_i64(3);
          let accessed = cursor.get_i64(4);
          let hash = cursor.get_blob(5).unwrap_or(&[]).iter().map(|&x| x).collect();
          let persistent_ref = cursor.get_blob(6).unwrap_or(&[]).iter().map(|&x| x).collect();

          listing.push((id, name,
                        created as u64, modified as u64, accessed as u64,
                        hash, persistent_ref));
        }

        return reply(Reply::ListResult(listing));
      },
    }
  }
}


#[cfg(test)]
mod tests {
  use super::*;

  struct TestEntry {
    id: Option<u64>,
    parent: Option<u64>,
    name: Vec<u8>,
  }
  impl KeyEntry<TestEntry> for TestEntry {
    fn id(&self) -> Option<u64> {
      None
    }
    fn parent_id(&self) -> Option<u64>{
      self.parent.clone()
    }
    fn name(&self) -> Vec<u8> {
      self.name.clone()
    }

    fn size(&self) -> Option<u64> {
      None
    }

    fn created(&self) -> Option<u64> {
      None
    }
    fn modified(&self) -> Option<u64> {
      None
    }
    fn accessed(&self) -> Option<u64> {
      None
    }

    fn permissions(&self) -> Option<u64> {
      None
    }
    fn user_id(&self) -> Option<u64> {
      None
    }
    fn group_id(&self) -> Option<u64> {
      None
    }
    fn with_id(self, id: Option<u64>) -> TestEntry {
      let mut x = self;
      x.id = id;
      return x;
    }
  }

}
