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

use blob;
use hash;
use util;
use super::schema;

use diesel;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use time::Duration;

use periodic_timer::PeriodicTimer;
use process::{Process, MsgHandler};


#[derive(Clone, Debug)]
pub struct Entry {
    pub id: Option<u64>,
    pub parent_id: Option<u64>,

    pub name: Vec<u8>,

    pub created: Option<i64>,
    pub modified: Option<i64>,
    pub accessed: Option<i64>,

    pub permissions: Option<u64>,
    pub user_id: Option<u64>,
    pub group_id: Option<u64>,

    pub data_hash: Option<Vec<u8>>,
    pub data_length: Option<u64>,
}

pub type IndexProcess = Process<Msg, Reply>;

pub enum Msg {
    /// Insert an entry in the key index.
    /// Returns `Id` with the new entry ID.
    Insert(Entry),

    /// Lookup an entry in the key index by its parent id and name.
    /// Returns either `Entry` with the found entry or `NotFound`.
    Lookup(Option<u64>, Vec<u8>),

    /// Update the `payload` and `persistent_ref` of an entry.
    /// Returns `UpdateOk`.
    UpdateDataHash(Entry, Option<hash::Hash>, Option<blob::ChunkRef>),

    /// List a directory (aka. `level`) in the index.
    /// Returns `ListResult` with all the entries under the given parent.
    ListDir(Option<u64>),

    /// Flush this key index.
    Flush,
}

pub enum Reply {
    Entry(Entry),
    NotFound,
    UpdateOk,
    ListResult(Vec<(Entry, Option<blob::ChunkRef>)>),
    FlushOk,
}


pub struct Index {
    path: String,
    conn: SqliteConnection,
    flush_timer: PeriodicTimer,
}


fn i64_to_u64_or_panic(x: i64) -> u64 {
    if x < 0 {
        panic!("expected u64, got {:?}", x);
    }
    return x as u64;
}


impl Index {
    pub fn new(path: String) -> Index {
        let conn = SqliteConnection::establish(&path).expect("Could not open SQLite database");

        let ki = Index {
            path: path,
            conn: conn,
            flush_timer: PeriodicTimer::new(Duration::seconds(5)),
        };

        let dir = diesel::migrations::find_migrations_directory().unwrap();
        diesel::migrations::run_pending_migrations_in_directory(&ki.conn,
                                                                &dir,
                                                                &mut util::InfoWriter)
            .unwrap();
        ki.conn.begin_transaction().unwrap();

        ki
    }

    #[cfg(test)]
    pub fn new_for_testing() -> Index {
        Index::new(":memory:".to_string())
    }

    fn last_insert_rowid(&self) -> i64 {
        diesel::select(diesel::expression::sql("last_insert_rowid()"))
            .first::<i64>(&self.conn)
            .unwrap()
    }

    pub fn maybe_flush(&mut self) {
        if self.flush_timer.did_fire() {
            self.flush();
        }
    }

    pub fn flush(&mut self) {
        self.conn.commit_transaction().unwrap();
        self.conn.begin_transaction().unwrap();
    }
}

impl Clone for Index {
    fn clone(&self) -> Index {
        Index::new(self.path.clone())
    }
}

impl MsgHandler<Msg, Reply> for Index {
    fn handle(&mut self, msg: Msg, reply: Box<Fn(Reply)>) {
        match msg {

            Msg::Insert(entry) => {
                use super::schema::keys::dsl::*;

                let entry = match entry.id {
                    Some(id_) => {
                        // Replace existing entry.
                        diesel::update(keys.find(id_ as i64))
                            .set((parent.eq(entry.parent_id.map(|x| x as i64)),
                                  name.eq(&entry.name[..]),
                                  created.eq(entry.created),
                                  modified.eq(entry.modified),
                                  accessed.eq(entry.accessed)))
                            .execute(&self.conn)
                            .expect("Error updating key");
                        entry
                    }
                    None => {
                        // Insert new entry.
                        {
                            let new = schema::NewKey {
                                parent: entry.parent_id.map(|x| x as i64),
                                name: &entry.name[..],
                                created: entry.created,
                                modified: entry.modified,
                                accessed: entry.accessed,
                                permissions: entry.permissions.map(|x| x as i64),
                                group_id: entry.group_id.map(|x| x as i64),
                                user_id: entry.user_id.map(|x| x as i64),
                                hash: None,
                                persistent_ref: None,
                            };

                            diesel::insert(&new)
                                .into(keys)
                                .execute(&self.conn)
                                .expect("Error inserting key");
                        }
                        let mut entry = entry;
                        entry.id = Some(self.last_insert_rowid() as u64);
                        entry
                    }
                };

                return reply(Reply::Entry(entry));
            }

            Msg::Lookup(parent_, name_) => {
                use super::schema::keys::dsl::*;

                let row_opt = keys.filter(parent.eq(parent_.map(|x| x as i64)))
                                  .filter(name.eq(&name_[..]))
                                  .first::<schema::Key>(&self.conn)
                                  .optional()
                                  .expect("Error searching keys");

                if let Some(row) = row_opt {
                    return reply(Reply::Entry(Entry {
                        id: Some(row.id as u64),
                        parent_id: parent_,
                        name: name_,
                        created: row.created,
                        modified: row.modified,
                        accessed: row.accessed,
                        permissions: row.permissions.map(|x| x as u64),
                        user_id: row.user_id.map(|x| x as u64),
                        group_id: row.group_id.map(|x| x as u64),
                        data_hash: row.hash,
                        data_length: None,
                    }));
                } else {
                    return reply(Reply::NotFound);
                }
            }

            Msg::UpdateDataHash(entry, hash_opt, persistent_ref_opt) => {
                use super::schema::keys::dsl::*;

                let id_ = entry.id.expect("Tried to update without an id") as i64;
                assert!(hash_opt.is_some() == persistent_ref_opt.is_some());

                let hash_bytes = hash_opt.map(|h| h.bytes);
                let persistent_ref_bytes = persistent_ref_opt.map(|p| p.as_bytes());

                if entry.modified.is_some() {
                    diesel::update(keys.find(id_)
                                       .filter(modified.eq::<Option<i64>>(None)
                                                       .or(modified.le(entry.modified))))
                        .set((hash.eq(hash_bytes), persistent_ref.eq(persistent_ref_bytes)))
                        .execute(&self.conn)
                        .expect("Error updating key");
                } else {
                    diesel::update(keys.find(id_))
                        .set((hash.eq(hash_bytes), persistent_ref.eq(persistent_ref_bytes)))
                        .execute(&self.conn)
                        .expect("Error updating key");
                }

                self.maybe_flush();
                return reply(Reply::UpdateOk);
            }

            Msg::Flush => {
                self.flush();
                return reply(Reply::FlushOk);
            }

            Msg::ListDir(parent_opt) => {
                use super::schema::keys::dsl::*;

                let rows = keys.filter(parent.eq(parent_opt.map(|x| x as i64)))
                               .load::<schema::Key>(&self.conn)
                               .expect("Error listing keys");


                return reply(Reply::ListResult(rows.into_iter()
                                                   .map(|mut r| {
                                                       (Entry {
                                                           id: Some(r.id as u64),
                                                           parent_id: r.parent.map(|x| x as u64),
                                                           name: r.name,
                                                           created: r.created,
                                                           modified: r.modified,
                                                           accessed: r.accessed,
                                                           permissions: r.permissions
                                                                         .map(|x| x as u64),
                                                           user_id: r.user_id.map(|x| x as u64),
                                                           group_id: r.group_id.map(|x| x as u64),
                                                           data_hash: r.hash,
                                                           data_length: None,
                                                       },
                                                        r.persistent_ref.as_mut().map(|p| {
                                                           blob::ChunkRef::from_bytes(&mut &p[..])
                                                               .unwrap()
                                                       }))
                                                   })
                                                   .collect()));
            }
        }
    }
}
