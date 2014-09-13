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

use periodic_timer::{PeriodicTimer};
use sodiumoxide::randombytes::{randombytes};
use process::{Process, MsgHandler};
use sqlite3::database::{Database};

use sqlite3::cursor::{Cursor};
use sqlite3::types::{SQLITE_ROW, SQLITE_DONE};
use sqlite3::{open};

use serialize::hex::{ToHex};


pub trait KeyEntry<KE> {
  fn id(&self) -> Option<Vec<u8>>;
  fn parentId(&self) -> Option<Vec<u8>>;

  fn name(&self) -> Vec<u8>;
  fn size(&self) -> Option<u64>;

  fn created(&self) -> Option<u64>;
  fn modified(&self) -> Option<u64>;
  fn accessed(&self) -> Option<u64>;

  fn permissions(&self) -> Option<u64>;
  fn userId(&self) -> Option<u64>;
  fn groupId(&self) -> Option<u64>;

  fn withId(&self, Vec<u8>) -> KE;
}

pub type KeyIndexProcess<KE> = Process<Msg<KE>, Reply, KeyIndex>;

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
  ListDirCallback(Option<Vec<u8>>,
                  fn(Option<(Vec<u8>, Vec<u8>, u64, u64, u64, Vec<u8>, Vec<u8>)>)),

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

pub struct KeyCursor<'a> {
  cursor: Cursor<'a>,
}

impl<'a> Iterator<(Vec<u8>, Vec<u8>, u64, u64, u64, Vec<u8>, Vec<u8>)> for KeyCursor<'a> {
  fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>, u64, u64, u64, Vec<u8>, Vec<u8>)> {
    match self.cursor.step() {
      SQLITE_ROW => {
        let id = self.cursor.get_blob(0);
        let name = self.cursor.get_blob(1);
        let created = self.cursor.get_int(2);
        let modified = self.cursor.get_int(3);
        let accessed = self.cursor.get_int(4);
        let hash = self.cursor.get_blob(5);
        let persistent_ref = self.cursor.get_blob(6);

        Some((id.as_slice().into_owned(),
              name.as_slice().into_owned(),
              created as u64, modified as u64, accessed as u64,
              hash.as_slice().into_owned(),
              persistent_ref.as_slice().into_owned()))
      },
      _ => None,
    }
  }
}

impl KeyIndex {
  pub fn new(path: String) -> KeyIndex {
    let mut ki = match open(path.as_slice()) {
      Ok(dbh) => {
        KeyIndex{path: path,
                 dbh: dbh,
                 flush_timer: PeriodicTimer::new(5 * 1000)}
      },
      Err(err) => fail!(err.to_str()),
    };
    ki.execOrDie("CREATE TABLE IF NOT EXISTS
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
      ki.execOrDie("CREATE UNIQUE INDEX IF NOT EXISTS
                    KeyIndex_UniqueParentName
                    ON key_index(parent, name)");
    }

    ki.execOrDie("BEGIN");
    ki
  }

  #[cfg(test)]
  pub fn newForTesting() -> KeyIndex {
    KeyIndex::new(":memory:".to_owned())
  }

  fn execOrDie(&mut self, sql: &str) {
    match self.dbh.exec(sql) {
      Ok(true) => (),
      Ok(false) => fail!("exec: {}", self.dbh.get_errmsg()),
      Err(msg) => fail!("exec: {}, {}", msg.to_str(), self.dbh.get_errmsg()),
    }
  }

  fn prepareOrDie<'a>(&'a mut self, sql: &str) -> Cursor<'a> {
    match self.dbh.prepare(sql, &None) {
      Ok(s)  => s,
      Err(x) => fail!("sqlite error: {} ({:?})", self.dbh.get_errmsg(), x),
    }
  }

  fn prepareOrDieCursor<'a>(&'a mut self, sql: &str) -> KeyCursor<'a> {
    KeyCursor{cursor: self.prepareOrDie(sql)}
  }

  pub fn maybeFlush(&mut self) {
    if self.flush_timer.did_fire() {
      self.flush();
    }
  }

  pub fn flush(&mut self) {
    self.execOrDie("COMMIT; BEGIN");
  }
}

impl Clone for KeyIndex {
  fn clone(&self) -> KeyIndex {
    KeyIndex::new(self.path.clone())
  }
}

impl Drop for KeyIndex {
  fn drop(&mut self) {
    self.execOrDie("COMMIT");
  }
}

impl <A: KeyEntry<A>> MsgHandler<Msg<A>, Reply> for KeyIndex {
  fn handle(&mut self, msg: Msg<A>, reply: |Reply|) {
    match msg {

      Insert(entry) => {
        let parent = entry.parentId().unwrap_or(b"".into_owned());
        let id = entry.id().unwrap_or_else(|| randombytes(16));

        self.execOrDie(format!(
          "INSERT OR REPLACE INTO key_index (id, parent, name, created, accessed)
           VALUES (x'{:s}', x'{:s}', x'{:s}', {:u}, {:u})",
          id.as_slice().to_hex(), parent.as_slice().to_hex(), entry.name().as_slice().to_hex(),
          entry.created().unwrap_or(0),
          entry.accessed().unwrap_or(0)).as_slice());

        return reply(Id(id));
      },

      LookupExact(entry) => {
        let parent = entry.parentId().unwrap_or(b"".into_owned());
        let cursor = match entry.id() {
          Some(id) => {
            self.prepareOrDie(format!(
              "SELECT id FROM key_index
                WHERE parent=x'{:s}' AND id=x'{:s}'
                AND created={:u} AND modified={:u} AND accessed={:u}
                LIMIT 1",
              parent.as_slice().to_hex(), id.as_slice().to_hex(),
              entry.created().unwrap_or(0),
              entry.modified().unwrap_or(0),
              entry.accessed().unwrap_or(0)).as_slice())
          },
          None => {
            self.prepareOrDie(format!(
              "SELECT id FROM key_index
                WHERE parent=x'{:s}' AND name=x'{:s}'
                AND created={:u} AND modified={:u} AND accessed={:u}
                LIMIT 1",
              parent.as_slice().to_hex(), entry.name().as_slice().to_hex(),
              entry.created().unwrap_or(0),
              entry.modified().unwrap_or(0),
              entry.accessed().unwrap_or(0)).as_slice())
          }
        };
        if cursor.step() == SQLITE_ROW {
          let res = Id(cursor.get_blob(0).as_slice().into_owned());
          assert!(cursor.step() == SQLITE_DONE);
          return reply(res);
            } else {
          return reply(NotFound);
        }
      },

      UpdateDataHash(entry, hash_opt, persistent_ref_opt) => {
        let parent = entry.parentId().unwrap_or(b"".into_owned());

        assert!(hash_opt.is_some() == persistent_ref_opt.is_some());

        if hash_opt.is_some() && persistent_ref_opt.is_some() {
          match entry.modified() {
            Some(modified) => {
              self.execOrDie(format!(
                "UPDATE key_index SET hash=x'{:s}', persistent_ref=x'{:s}'
                                                  , modified={:u}
                  WHERE parent=x'{:s}' AND id=x'{:s}' AND IFNULL(modified,0)<={:u}",
                hash_opt.unwrap().as_slice().to_hex(),
                persistent_ref_opt.unwrap().as_slice().to_hex(),
                modified, parent.as_slice().to_hex(), entry.id().unwrap().as_slice().to_hex(),
                modified).as_slice());
            },
            None => {
              self.execOrDie(format!(
                "UPDATE key_index SET hash=x'{:s}', persistent_ref=x'{:s}'
                 WHERE parent=x'{:s}' AND id=x'{:s}'",
                hash_opt.unwrap().as_slice().to_hex(),
                persistent_ref_opt.unwrap().as_slice().to_hex(),
                parent.as_slice().to_hex(), entry.id().unwrap().as_slice().to_hex()).as_slice());
            }
          }
        } else {
          match entry.modified() {
            Some(modified) => {
              self.execOrDie(format!(
                "UPDATE key_index SET hash=NULL, persistent_ref=NULL
                                               , modified={:u}
                  WHERE parent=x'{:s}' AND id=x'{:s}' AND IFNULL(modified, 0)<={:u}",
                modified, parent.as_slice().to_hex(), entry.id().unwrap().as_slice().to_hex(),
                modified).as_slice());
            },
            None => {
              self.execOrDie(format!(
                "UPDATE key_index SET hash=NULL, persistent_ref=NULL
                 WHERE parent=x'{:s}' AND id=x'{:s}'",
                parent.as_slice().to_hex(), entry.id().unwrap().as_slice().to_hex()).as_slice());
            }
          }
        }

        self.maybeFlush();
        return reply(UpdateOK);
      },

      Flush => {
        self.flush();
        return reply(FlushOK);
      },

      ListDir(parent) => {
        let mut listing = Vec::new();
        let parent = parent.unwrap_or(b"".into_owned());

        let cursor = self.prepareOrDie(format!(
           "SELECT id, name, created, modified, accessed, hash, persistent_ref
            FROM key_index
            WHERE parent=x'{:s}'", parent.as_slice().to_hex()).as_slice());

        // TODO(jos): replace get_int with something that understands uint64
        while cursor.step() == SQLITE_ROW {
          let id = cursor.get_blob(0);
          let name = cursor.get_blob(1);
          let created = cursor.get_int(2);
          let modified = cursor.get_int(3);
          let accessed = cursor.get_int(4);
          let hash = cursor.get_blob(5);
          let persistent_ref = cursor.get_blob(6);

          listing.push((id.as_slice().into_owned(),
                        name.as_slice().into_owned(),
                        created as u64, modified as u64, accessed as u64,
                        hash.as_slice().into_owned(),
                        persistent_ref.as_slice().into_owned()));
        }

        return reply(ListResult(listing));
      },

      ListDirCallback(parent, callback) => {
        let parent = parent.unwrap_or(b"".into_owned());

        let outer_ki = self.clone();

        spawn(proc() {
          let mut ki = outer_ki.clone();
          let mut cursor = ki.prepareOrDieCursor(format!(
            "SELECT id, name, created, modified, accessed, hash, persistent_ref
             FROM key_index
             WHERE parent=x'{:s}'", parent.as_slice().to_hex()).as_slice());

          // TODO(jos): replace get_int with something that understands uint64
          for item in cursor {
            callback(Some(item));
          }
          callback(None);
        });
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
    fn parentId(&self) -> Option<Vec<u8>>{
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
    fn userId(&self) -> Option<u64> {
      None
    }
    fn groupId(&self) -> Option<u64> {
      None
    }
    fn withId(&self, id: Vec<u8>) -> TestEntry {
      TestEntry{id:Some(id),
                parent: self.parentId(),
                name: self.name()}
    }
  }

}
