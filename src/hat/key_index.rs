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

use blob_store;
use hash_index;

use time::Duration;

use periodic_timer::PeriodicTimer;
use process::{Process, MsgHandler};
use sqlite3::database::Database;

use sqlite3::cursor::Cursor;
use sqlite3::types::ColumnType;
use sqlite3::types::ResultCode;
use sqlite3::open;

use rustc_serialize::hex::ToHex;


#[derive(Clone, Debug)]
pub struct KeyEntry {
    pub id: Option<u64>,
    pub parent_id: Option<u64>,

    pub name: Vec<u8>,

    // TODO(jos): SQLite3 supports only i64 precisely. Do we propagate this type or convert to it?
    // Currently, values larger than 1<<63 gets converted to doubles silently and breaks.
    pub created: Option<u64>,
    pub modified: Option<u64>,
    pub accessed: Option<u64>,

    pub permissions: Option<u64>,
    pub user_id: Option<u64>,
    pub group_id: Option<u64>,

    pub data_hash: Option<Vec<u8>>,
    pub data_length: Option<u64>,
}

pub type KeyIndexProcess = Process<Msg, Reply>;

pub enum Msg {
    /// Insert an entry in the key index.
    /// Returns `Id` with the new entry ID.
    Insert(KeyEntry),

    /// Lookup an entry in the key index, to see if it exists.
    /// Returns either `Id` with the found entry ID or `Notfound`.
    LookupExact(KeyEntry),

    /// Update the `payload` and `persistent_ref` of an entry.
    /// Returns `UpdateOk`.
    UpdateDataHash(KeyEntry, Option<hash_index::Hash>, Option<blob_store::ChunkRef>),

    /// List a directory (aka. `level`) in the index.
    /// Returns `ListResult` with all the entries under the given parent.
    ListDir(Option<u64>),

    /// Flush this key index.
    Flush,
}

pub enum Reply {
    Entry(KeyEntry),
    NotFound(KeyEntry),
    UpdateOk,
    ListResult(Vec<(KeyEntry, Option<blob_store::ChunkRef>)>),
    FlushOk,
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
    return x as u64;
}


impl KeyIndex {
    pub fn new(path: String) -> KeyIndex {
        let mut ki = match open(&path) {
            Ok(dbh) => {
                KeyIndex {
                    path: path,
                    dbh: dbh,
                    flush_timer: PeriodicTimer::new(Duration::seconds(5)),
                }
            }
            Err(err) => panic!("{:?}", err),
        };
        ki.exec_or_die("CREATE TABLE IF NOT EXISTS
                    key_index (rowid          \
                        INTEGER PRIMARY KEY,
                               parent         \
                        INTEGER,
                               name           BLOB,
                               \
                        created        UINT8,
                               modified       \
                        UINT8,
                               accessed       UINT8,
                               \
                        hash           BLOB,
                               persistent_ref BLOB
                            \
                        );");

        ki.exec_or_die("CREATE UNIQUE INDEX IF NOT EXISTS
                    \
                        KeyIndex_UniqueParentName
                    ON key_index(parent, name)");

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
            Ok(s) => s,
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

impl MsgHandler<Msg, Reply> for KeyIndex {
    fn handle(&mut self, msg: Msg, reply: Box<Fn(Reply)>) {
        let unwrap_u64_or_null = |x:Option<u64>| { x.map_or("NULL".to_string(), |v| v.to_string())};
        match msg {

            Msg::Insert(entry) => {
                let parent = entry.parent_id.unwrap_or(0);

                self.exec_or_die(&format!("INSERT OR REPLACE INTO key_index (parent, name,
                                           created, modified, accessed)
                                           VALUES
                                           ({:?}, x'{}', {}, {}, {})",
                                          parent,
                                          entry.name.to_hex(),
                                          unwrap_u64_or_null(entry.created),
                                          unwrap_u64_or_null(entry.modified),
                                          unwrap_u64_or_null(entry.accessed)));

                let mut entry = entry;
                entry.id = Some(i64_to_u64_or_panic(self.dbh.get_last_insert_rowid()));
                return reply(Reply::Entry(entry));
            }

            Msg::LookupExact(entry) => {
                let parent = entry.parent_id.unwrap_or(0);
                let mut cursor = self.prepare_or_die(&format!("SELECT rowid, hash FROM \
                                                               key_index
           WHERE \
                                                               parent={:?} AND name=x'{}'
           \
                                                               AND created={} AND modified={} \
                                                               AND accessed={}
           LIMIT \
                                                               1",
                                                              parent,
                                                              entry.name.to_hex(),
                                                              unwrap_u64_or_null(entry.created),
                                                              unwrap_u64_or_null(entry.modified),
                                                              unwrap_u64_or_null(entry.accessed)));
                if cursor.step() == ResultCode::SQLITE_ROW {
                    let id = i64_to_u64_or_panic(cursor.get_i64(0));
                    let hash_opt = cursor.get_blob(1).map(|s| s.to_vec());
                    assert!(cursor.step() == ResultCode::SQLITE_DONE);
                    let mut entry = entry;
                    entry.id = Some(id);
                    entry.data_hash = hash_opt;
                    return reply(Reply::Entry(entry));
                } else {
                    return reply(Reply::NotFound(entry));
                }
            }

            Msg::UpdateDataHash(entry, hash_opt, persistent_ref_opt) => {
                let parent = entry.parent_id.unwrap_or(0);

                assert!(hash_opt.is_some() == persistent_ref_opt.is_some());

                if hash_opt.is_some() && persistent_ref_opt.is_some() {
                    match entry.modified {
                        Some(modified) => {
                            self.exec_or_die(&format!("UPDATE key_index SET hash=x'{}', \
                                                       persistent_ref=x'{}'
                                                  \
                                                       , modified={}
                  WHERE \
                                                       parent={:?} AND rowid={:?} AND \
                                                       IFNULL(modified,0)<={}",
                                                      hash_opt.unwrap().bytes.to_hex(),
                                                      persistent_ref_opt.unwrap()
                                                                        .as_bytes()
                                                                        .to_hex(),
                                                      modified,
                                                      parent,
                                                      entry.id.expect("UpdateDataHash"),
                                                      modified));
                        }
                        None => {
                            self.exec_or_die(&format!("UPDATE key_index SET hash=x'{}', \
                                                       persistent_ref=x'{}'
                 \
                                                       WHERE parent={:?} AND rowid={:?}",
                                                      hash_opt.unwrap().bytes.to_hex(),
                                                      persistent_ref_opt.unwrap()
                                                                        .as_bytes()
                                                                        .to_hex(),
                                                      parent,
                                                      entry.id.expect("UpdateDataHash, None")));
                        }
                    }
                } else {
                    match entry.modified {
                        Some(modified) => {
                            self.exec_or_die(&format!("UPDATE key_index SET hash=NULL, \
                                                       persistent_ref=NULL
                                               \
                                                       , modified={}
                 WHERE \
                                                       parent={:?} AND rowid={:?} AND \
                                                       IFNULL(modified, 0)<={}",
                                                      modified,
                                                      parent,
                                                      entry.id.expect("UpdateDataHash2"),
                                                      modified));
                        }
                        None => {
                            self.exec_or_die(&format!("UPDATE key_index SET hash=NULL, \
                                                       persistent_ref=NULL
                 \
                                                       WHERE parent={:?} AND rowid={}",
                                                      parent,
                                                      entry.id.expect("UpdateDataHash2, None")));
                        }
                    }
                }

                self.maybe_flush();
                return reply(Reply::UpdateOk);
            }

            Msg::Flush => {
                self.flush();
                return reply(Reply::FlushOk);
            }

            Msg::ListDir(parent_opt) => {
                let mut listing = Vec::new();
                let parent = parent_opt.unwrap_or(0);

                let mut cursor = self.prepare_or_die(&format!("SELECT rowid, name, created, \
                                                               modified, accessed, hash, \
                                                               persistent_ref
            FROM \
                                                               key_index
            WHERE \
                                                               parent={:?}",
                                                              parent));

                let get_u64_opt = |c: &mut Cursor, i: isize| match c.get_column_type(i) {
                    ColumnType::SQLITE_NULL => None,
                    ColumnType::SQLITE_INTEGER => Some(c.get_i64(i) as u64),
                    _ => unreachable!(),
                };
                while cursor.step() == ResultCode::SQLITE_ROW {
                    let id = i64_to_u64_or_panic(cursor.get_i64(0));
                    let name = cursor.get_blob(1).expect("name").to_owned();
                    let created = get_u64_opt(&mut cursor, 2);
                    let modified = get_u64_opt(&mut cursor, 3);
                    let accessed = get_u64_opt(&mut cursor, 4);
                    let hash = cursor.get_blob(5).map(|s| s.to_vec());
                    let persistent_ref =
                        cursor.get_blob(6)
                              .and_then(|b| {
                                  if b.is_empty() {
                                      None
                                  } else {
                                      Some(blob_store::ChunkRef::from_bytes(&mut &b.to_owned()[..])
                                               .unwrap())
                                  }
                              });

                    listing.push((KeyEntry {
                        id: Some(id),
                        name: name,
                        created: created,
                        modified: modified,
                        accessed: accessed,
                        data_hash: hash,
                        data_length: None,
                        // TODO(jos): Implement support for remaining fields.
                        parent_id: None,
                        permissions: None,
                        group_id: None,
                        user_id: None,
                    },
                                  persistent_ref));
                }

                return reply(Reply::ListResult(listing));
            }
        }
    }
}
