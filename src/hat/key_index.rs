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
use std::vec;

use periodic_timer::{PeriodicTimer};
use sodiumoxide::randombytes::{randombytes};
use process::{Process, MsgHandler};
use sqlite3::database::{Database};

use sqlite3::cursor::{Cursor};
use sqlite3::types::ResultCode::{SQLITE_ROW, SQLITE_DONE};
use sqlite3::{open};

use rustc_serialize::hex::{ToHex};


pub trait KeyEntry<KE> {
  fn id(&self) -> Option<Vec<u8>>;
  fn parent_id(&self) -> Option<Vec<u8>>;

  fn name(&self) -> Vec<u8>;
  fn size(&self) -> Option<u64>;

  fn created(&self) -> Option<u64>;
  fn modified(&self) -> Option<u64>;
  fn accessed(&self) -> Option<u64>;

  fn permissions(&self) -> Option<u64>;
  fn user_id(&self) -> Option<u64>;
  fn group_id(&self) -> Option<u64>;

  fn with_id(&self, Vec<u8>) -> KE;
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
  ListDir(Option<Vec<u8>>),

  /// Flush this key index.
  Flush,
}

pub enum Reply {
  Id(Vec<u8>),
  NotFound,
  UpdateOK,
  ListResult(Vec<(Vec<u8>, Vec<u8>, u64, u64, u64, Vec<u8>, Vec<u8>)>),
  FlushOK,
}


pub struct KeyIndex {
  path: String,
  dbh: Database,
  flush_timer: PeriodicTimer,
}


impl KeyIndex {
  pub fn new(path: String) -> KeyIndex {
    let mut ki = match open(path.as_slice()) {
      Ok(dbh) => {
        KeyIndex{path: path,
                 dbh: dbh,
                 flush_timer: PeriodicTimer::new(Duration::seconds(5))}
      },
      Err(err) => panic!("{:?}", err),
    };
    ki.exec_or_die("CREATE TABLE IF NOT EXISTS
                  key_index (id     BLOB PRIMARY KEY,
                             parent BLOB,
                             name   BLOB,
                             created UINT8,
                             modified UINT8,
                             accessed UINT8,
                             hash BLOB,
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
      Ok(false) => panic!("exec: {:?}", self.dbh.get_errmsg()),
      Err(msg) => panic!("exec: {:?}, {:?}", msg, self.dbh.get_errmsg()),
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
        let parent = entry.parent_id().unwrap_or(vec![]);
        let id = entry.id().unwrap_or_else(|| randombytes(16));

        self.exec_or_die(format!(
          "INSERT OR REPLACE INTO key_index (id, parent, name, created, accessed)
           VALUES (x'{}', x'{}', x'{}', {}, {})",
          id.as_slice().to_hex(), parent.as_slice().to_hex(), entry.name().as_slice().to_hex(),
          entry.created().unwrap_or(0),
          entry.accessed().unwrap_or(0)).as_slice());

        return reply(Reply::Id(id));
      },

      Msg::LookupExact(entry) => {
        let parent = entry.parent_id().unwrap_or(vec![]);
        let mut cursor = match entry.id() {
          Some(id) => {
            self.prepare_or_die(format!(
              "SELECT id FROM key_index
                WHERE parent=x'{}' AND id=x'{}'
                AND created={} AND modified={} AND accessed={}
                LIMIT 1",
              parent.as_slice().to_hex(), id.as_slice().to_hex(),
              entry.created().unwrap_or(0),
              entry.modified().unwrap_or(0),
              entry.accessed().unwrap_or(0)).as_slice())
          },
          None => {
            self.prepare_or_die(format!(
              "SELECT id FROM key_index
                WHERE parent=x'{}' AND name=x'{}'
                AND created={} AND modified={} AND accessed={}
                LIMIT 1",
              parent.as_slice().to_hex(), entry.name().as_slice().to_hex(),
              entry.created().unwrap_or(0),
              entry.modified().unwrap_or(0),
              entry.accessed().unwrap_or(0)).as_slice())
          }
        };
        if cursor.step() == SQLITE_ROW {
          let res = Reply::Id(cursor.get_blob(0).expect("id").iter().map(|&x| x).collect());
          assert!(cursor.step() == SQLITE_DONE);
          return reply(res);
        } else {
          return reply(Reply::NotFound);
        }
      },

      Msg::UpdateDataHash(entry, hash_opt, persistent_ref_opt) => {
        let parent = entry.parent_id().unwrap_or(vec![]);

        assert!(hash_opt.is_some() == persistent_ref_opt.is_some());

        if hash_opt.is_some() && persistent_ref_opt.is_some() {
          match entry.modified() {
            Some(modified) => {
              self.exec_or_die(format!(
                "UPDATE key_index SET hash=x'{}', persistent_ref=x'{}'
                                                  , modified={}
                  WHERE parent=x'{}' AND id=x'{}' AND IFNULL(modified,0)<={}",
                hash_opt.unwrap().as_slice().to_hex(),
                persistent_ref_opt.unwrap().as_slice().to_hex(),
                modified, parent.as_slice().to_hex(), entry.id().unwrap().as_slice().to_hex(),
                modified).as_slice());
            },
            None => {
              self.exec_or_die(format!(
                "UPDATE key_index SET hash=x'{}', persistent_ref=x'{}'
                 WHERE parent=x'{}' AND id=x'{}'",
                hash_opt.unwrap().as_slice().to_hex(),
                persistent_ref_opt.unwrap().as_slice().to_hex(),
                parent.as_slice().to_hex(), entry.id().unwrap().as_slice().to_hex()).as_slice());
            }
          }
        } else {
          match entry.modified() {
            Some(modified) => {
              self.exec_or_die(format!(
                "UPDATE key_index SET hash=NULL, persistent_ref=NULL
                                               , modified={}
                  WHERE parent=x'{}' AND id=x'{}' AND IFNULL(modified, 0)<={}",
                modified, parent.as_slice().to_hex(), entry.id().unwrap().as_slice().to_hex(),
                modified).as_slice());
            },
            None => {
              self.exec_or_die(format!(
                "UPDATE key_index SET hash=NULL, persistent_ref=NULL
                 WHERE parent=x'{}' AND id=x'{}'",
                parent.as_slice().to_hex(), entry.id().unwrap().as_slice().to_hex()).as_slice());
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

      Msg::ListDir(parent) => {
        let mut listing = Vec::new();
        let parent = parent.unwrap_or(vec![]);

        let mut cursor = self.prepare_or_die(format!(
           "SELECT id, name, created, modified, accessed, hash, persistent_ref
            FROM key_index
            WHERE parent=x'{}'", parent.as_slice().to_hex()).as_slice());

        // TODO(jos): replace get_int with something that understands usize64
        while cursor.step() == SQLITE_ROW {
          let id = cursor.get_blob(0).expect("id").iter().map(|&x| x).collect();
          let name = cursor.get_blob(1).expect("name").iter().map(|&x| x).collect();
          let created = cursor.get_int(2);
          let modified = cursor.get_int(3);
          let accessed = cursor.get_int(4);
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
    id: Option<Vec<u8>>,
    parent: Option<Vec<u8>>,
    name: Vec<u8>,
  }
  impl KeyEntry<TestEntry> for TestEntry {
    fn id(&self) -> Option<Vec<u8>> {
      None
    }
    fn parent_id(&self) -> Option<Vec<u8>>{
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
    fn with_id(&self, id: Vec<u8>) -> TestEntry {
      TestEntry{id:Some(id),
                parent: self.parent_id(),
                name: self.name()}
    }
  }

}
