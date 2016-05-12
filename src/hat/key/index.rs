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

use std::borrow::Cow;

use diesel;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use std::sync::{Arc, Mutex, MutexGuard};
use time::Duration;

use blob;
use hash;
use periodic_timer::PeriodicTimer;
use util;

use super::schema;

error_type! {
    #[derive(Debug)]
    pub enum IndexError {
        SqlConnection(diesel::ConnectionError) {
            cause;
        },
        SqlMigration(diesel::migrations::MigrationError) {
            cause;
        },
        SqlRunMigration(diesel::migrations::RunMigrationsError) {
            cause;
        },
        SqlExecute(diesel::result::Error) {
            cause;
        },
        Message(Cow<'static, str>) {
            desc (e) &**e;
            from (s: &'static str) s.into();
            from (s: String) s.into();
        }
     }
}


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

#[derive(Clone)]
pub struct IndexProcess(Arc<Mutex<(Index, Option<i64>)>>);

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


impl Index {
    pub fn new(path: String) -> Result<Index, IndexError> {
        let conn = try!(SqliteConnection::establish(&path));

        let ki = Index {
            path: path,
            conn: conn,
            flush_timer: PeriodicTimer::new(Duration::seconds(5)),
        };

        let dir = try!(diesel::migrations::find_migrations_directory());
        try!(diesel::migrations::run_pending_migrations_in_directory(&ki.conn,
                                                                     &dir,
                                                                     &mut util::InfoWriter));
        try!(ki.conn.begin_transaction());

        Ok(ki)
    }

    #[cfg(test)]
    pub fn new_for_testing() -> Result<Index, IndexError> {
        Index::new(":memory:".to_string())
    }

    fn last_insert_rowid(&self) -> Result<i64, IndexError> {
        let id = try!(diesel::select(diesel::expression::sql("last_insert_rowid()"))
                          .first::<i64>(&self.conn));
        Ok(id)
    }

    pub fn maybe_flush(&mut self) -> Result<(), IndexError> {
        if self.flush_timer.did_fire() {
            try!(self.flush());
        }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), IndexError> {
        try!(self.conn.commit_transaction());
        try!(self.conn.begin_transaction());

        Ok(())
    }
}

impl Clone for Index {
    fn clone(&self) -> Index {
        // We succeeded setup once, and so should succeed again unless the underlying database has
        // disappeared, in which case it is acceptable to terminate the program.
        Index::new(self.path.clone()).expect("Could not clone index")
    }
}

impl Index {
    fn handle(&mut self,
              msg: Msg)-> Result<Reply, IndexError> {
        match msg {
            Msg::Insert(entry) => {
                use super::schema::keys::dsl::*;

                let entry = match entry.id {
                    Some(id_) => {
                        // Replace existing entry.
                        try!(diesel::update(keys.find(id_ as i64))
                                 .set((parent.eq(entry.parent_id.map(|x| x as i64)),
                                       name.eq(&entry.name[..]),
                                       created.eq(entry.created),
                                       modified.eq(entry.modified),
                                       accessed.eq(entry.accessed)))
                                 .execute(&self.conn));
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

                            try!(diesel::insert(&new)
                                     .into(keys)
                                     .execute(&self.conn));
                        }
                        let mut entry = entry;
                        entry.id = Some(try!(self.last_insert_rowid()) as u64);
                        entry
                    }
                };

                Ok(Reply::Entry(entry))
            }
            Msg::Lookup(parent_, name_) => {
                use super::schema::keys::dsl::*;

                let row_opt = match parent_ {
                    Some(p) => {
                        try!(keys.filter(parent.eq(p as i64))
                                 .filter(name.eq(&name_[..]))
                                 .first::<schema::Key>(&self.conn)
                                 .optional())
                    }
                    None => {
                        try!(keys.filter(parent.is_null())
                                 .filter(name.eq(&name_[..]))
                                 .first::<schema::Key>(&self.conn)
                                 .optional())
                    }
                };

                if let Some(row) = row_opt {
                    Ok(Reply::Entry(Entry {
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
                    }))
                } else {
                    Ok(Reply::NotFound)
                }
            }
            Msg::UpdateDataHash(entry, hash_opt, persistent_ref_opt) => {
                use super::schema::keys::dsl::*;

                let id_ = match entry.id {
                    Some(i) => i as i64,
                    None => {
                        return Err(From::from("Tried to update data hash without an id"))
                    }
                };
                assert!(hash_opt.is_some() == persistent_ref_opt.is_some());

                let hash_bytes = hash_opt.map(|h| h.bytes);
                let persistent_ref_bytes = persistent_ref_opt.map(|p| p.as_bytes());

                if entry.modified.is_some() {
                    try!(diesel::update(keys.find(id_)
                                            .filter(modified.eq::<Option<i64>>(None)
                                                            .or(modified.le(entry.modified))))
                             .set((hash.eq(hash_bytes), persistent_ref.eq(persistent_ref_bytes)))
                             .execute(&self.conn));
                } else {
                    try!(diesel::update(keys.find(id_))
                             .set((hash.eq(hash_bytes), persistent_ref.eq(persistent_ref_bytes)))
                             .execute(&self.conn));
                }

                try!(self.maybe_flush());

                Ok(Reply::UpdateOk)
            }

            Msg::Flush => {
                try!(self.flush());

                Ok(Reply::FlushOk)
            }

            Msg::ListDir(parent_opt) => {
                use super::schema::keys::dsl::*;

                let rows = match parent_opt {
                    Some(p) => {
                        try!(keys.filter(parent.eq(p as i64))
                                 .load::<schema::Key>(&self.conn))
                    }
                    None => {
                        try!(keys.filter(parent.is_null())
                                 .load::<schema::Key>(&self.conn))
                    }
                };

                let res = rows.into_iter()
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
                                   r.persistent_ref
                                    .as_mut()
                                    .map(|p| blob::ChunkRef::from_bytes(&mut &p[..]).unwrap()))
                              });

                Ok(Reply::ListResult(res.collect()))
            }
        }
    }
}

impl IndexProcess {
    pub fn new(index: Index) -> IndexProcess {
        IndexProcess::new_with_shutdown(index, None)
    }

    pub fn new_with_shutdown(index: Index, shutdown: Option<i64>) -> IndexProcess {
        IndexProcess(Arc::new(Mutex::new((index, shutdown))))
    }

    fn lock(&self) -> MutexGuard<(Index, Option<i64>)> {
        let mut guard = self.0.lock().expect("index-process has failed");

        match &mut guard.1 {
            &mut None => (),
            &mut Some(0) => panic!("No more requests for this index process"),
            &mut Some(ref mut n) => {
                *n -= 1;
            }
        }

        guard
    }

    pub fn send_reply(&self, msg: Msg) -> Result<Reply, IndexError>  {
        self.lock().0.handle(msg)
    }
}
