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


use diesel;
use diesel::prelude::*;
use diesel::connection::TransactionManager;
use diesel::sqlite::SqliteConnection;
use errors::DieselError;
use hash;
use capnp;

use std::sync::{Mutex, MutexGuard};

use super::schema;
use time::Duration;
use util::{InfoWriter, PeriodicTimer};
use root_capnp;

#[derive(Clone, Debug)]
pub struct Entry {
    pub id: Option<u64>,
    pub parent_id: Option<u64>,

    pub data_hash: Option<Vec<u8>>,
    pub info: Info,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Info {
    pub name: Vec<u8>,

    pub created: Option<i64>,
    pub modified: Option<i64>,
    pub accessed: Option<i64>,

    pub permissions: Option<u64>,
    pub user_id: Option<u64>,
    pub group_id: Option<u64>,

    pub byte_length: Option<u64>,
    pub hat_snapshot_top: bool,
    pub hat_snapshot_ts: i64,
}

impl Info {
    pub fn read(msg: root_capnp::file_info::Reader) -> Result<Info, capnp::Error> {
        Ok(Info {
            name: msg.get_name()?.to_vec(),
            created: None,
            modified: None,
            accessed: None,
            permissions: None,
            user_id: None,
            group_id: None,
            byte_length: None,
            hat_snapshot_top: msg.get_hat_snapshot_top(),
            hat_snapshot_ts: msg.get_hat_snapshot_timestamp(),
        })
    }
    pub fn populate_msg(&self, mut msg: root_capnp::file_info::Builder) {
        msg.borrow().set_name(&self.name);

        match self.created {
            None => msg.borrow().init_created().set_unknown(()),
            Some(ts) => msg.borrow().init_created().set_timestamp(ts),
        }

        match self.modified {
            None => msg.borrow().init_modified().set_unknown(()),
            Some(ts) => msg.borrow().init_modified().set_timestamp(ts),
        }

        match self.accessed {
            None => msg.borrow().init_accessed().set_unknown(()),
            Some(ts) => msg.borrow().init_accessed().set_timestamp(ts),
        }

        msg.borrow().set_hat_snapshot_top(self.hat_snapshot_top);
        msg.borrow().set_hat_snapshot_timestamp(self.hat_snapshot_ts);
    }
}

pub struct KeyIndex(Mutex<InternalKeyIndex>);

pub struct InternalKeyIndex {
    conn: SqliteConnection,
    flush_timer: PeriodicTimer,
}


impl InternalKeyIndex {
    fn new(path: &str) -> Result<InternalKeyIndex, DieselError> {
        let conn = SqliteConnection::establish(path)?;

        let ki = InternalKeyIndex {
            conn: conn,
            flush_timer: PeriodicTimer::new(Duration::seconds(5)),
        };

        let dir = diesel::migrations::find_migrations_directory()?;
        diesel::migrations::run_pending_migrations_in_directory(&ki.conn, &dir, &mut InfoWriter)?;

        {
            let tm = ki.conn.transaction_manager();
            tm.begin_transaction(&ki.conn)?;
        }

        Ok(ki)
    }

    fn last_insert_rowid(&self) -> Result<i64, DieselError> {
        let id = try!(diesel::select(diesel::expression::sql("last_insert_rowid()"))
            .first::<i64>(&self.conn));
        Ok(id)
    }

    fn maybe_flush(&mut self) -> Result<(), DieselError> {
        if self.flush_timer.did_fire() {
            self.flush()?;
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<(), DieselError> {
        let tm = self.conn.transaction_manager();
        tm.commit_transaction(&self.conn)?;
        tm.begin_transaction(&self.conn)?;

        Ok(())
    }

    /// Insert an entry in the key index.
    /// Returns `Id` with the new entry ID.
    fn insert(&mut self, entry: Entry) -> Result<Entry, DieselError> {
        use super::schema::keys::dsl::*;

        let entry = match entry.id {
            Some(id_) => {
                // Replace existing entry.
                try!(diesel::update(keys.find(id_ as i64))
                    .set((parent.eq(entry.parent_id.map(|x| x as i64)),
                          name.eq(&entry.info.name[..]),
                          created.eq(entry.info.created),
                          modified.eq(entry.info.modified),
                          accessed.eq(entry.info.accessed)))
                    .execute(&self.conn));
                entry
            }
            None => {
                // Insert new entry.
                {
                    let new = schema::NewKey {
                        parent: entry.parent_id.map(|x| x as i64),
                        name: &entry.info.name[..],
                        created: entry.info.created,
                        modified: entry.info.modified,
                        accessed: entry.info.accessed,
                        permissions: entry.info.permissions.map(|x| x as i64),
                        group_id: entry.info.group_id.map(|x| x as i64),
                        user_id: entry.info.user_id.map(|x| x as i64),
                        hash: None,
                        hash_ref: None,
                    };

                    diesel::insert(&new).into(keys)
                        .execute(&self.conn)?;
                }
                let mut entry = entry;
                entry.id = Some(self.last_insert_rowid()? as u64);
                entry
            }
        };

        Ok(entry)
    }

    /// Lookup an entry in the key index by its parent id and name.
    /// Returns either `Entry` with the found entry or `NotFound`.
    fn lookup(&mut self,
              parent_: Option<u64>,
              name_: Vec<u8>)
              -> Result<Option<Entry>, DieselError> {
        use super::schema::keys::dsl::*;

        let row_opt = match parent_ {
            Some(p) => {
                keys.filter(parent.eq(p as i64))
                    .filter(name.eq(&name_[..]))
                    .first::<schema::Key>(&self.conn)
                    .optional()?
            }
            None => {
                keys.filter(parent.is_null())
                    .filter(name.eq(&name_[..]))
                    .first::<schema::Key>(&self.conn)
                    .optional()?
            }
        };

        if let Some(row) = row_opt {
            Ok(Some(Entry {
                id: Some(row.id as u64),
                parent_id: parent_,
                data_hash: row.hash,
                info: Info {
                    name: name_,
                    created: row.created,
                    modified: row.modified,
                    accessed: row.accessed,
                    permissions: row.permissions.map(|x| x as u64),
                    user_id: row.user_id.map(|x| x as u64),
                    group_id: row.group_id.map(|x| x as u64),
                    byte_length: None,
                    hat_snapshot_top: false,
                    hat_snapshot_ts: 0,
                },
            }))
        } else {
            Ok(None)
        }
    }


    /// Update the `payload` and `persistent_ref` of an entry.
    /// Returns `UpdateOk`.
    fn update_data_hash(&mut self,
                        id_: u64,
                        last_modified: Option<i64>,
                        hash_opt: Option<hash::Hash>,
                        hash_ref_opt: Option<hash::tree::HashRef>)
                        -> Result<(), DieselError> {
        use super::schema::keys::dsl::*;

        let id_ = id_ as i64;
        assert!(hash_opt.is_some() == hash_ref_opt.is_some());

        let hash_bytes = hash_opt.map(|h| h.bytes);
        let hash_ref_bytes = hash_ref_opt.map(|p| p.as_bytes());

        if last_modified.is_some() {
            try!(diesel::update(keys.find(id_)
                    .filter(modified.eq::<Option<i64>>(None)
                        .or(modified.le(last_modified))))
                .set((hash.eq(hash_bytes), hash_ref.eq(hash_ref_bytes)))
                .execute(&self.conn));
        } else {
            diesel::update(keys.find(id_)).set((hash.eq(hash_bytes), hash_ref.eq(hash_ref_bytes)))
                .execute(&self.conn)?;
        }

        self.maybe_flush()
    }

    /// List a directory (aka. `level`) in the index.
    /// Returns `ListResult` with all the entries under the given parent.
    fn list_dir(&mut self,
                parent_opt: Option<u64>)
                -> Result<Vec<(Entry, Option<hash::tree::HashRef>)>, DieselError> {
        use super::schema::keys::dsl::*;

        let rows = match parent_opt {
            Some(p) => {
                keys.filter(parent.eq(p as i64))
                    .load::<schema::Key>(&self.conn)?
            }
            None => {
                keys.filter(parent.is_null())
                    .load::<schema::Key>(&self.conn)?
            }
        };

        Ok(rows.into_iter()
            .map(|mut r| {
                (Entry {
                     id: Some(r.id as u64),
                     parent_id: r.parent.map(|x| x as u64),
                     data_hash: r.hash,
                     info: Info {
                         name: r.name,
                         created: r.created,
                         modified: r.modified,
                         accessed: r.accessed,
                         permissions: r.permissions
                             .map(|x| x as u64),
                         user_id: r.user_id.map(|x| x as u64),
                         group_id: r.group_id.map(|x| x as u64),
                         byte_length: None,
                         hat_snapshot_top: false,
                         hat_snapshot_ts: 0,
                     },
                 },
                 r.hash_ref
                     .as_mut()
                     .map(|p| ::hash::tree::HashRef::from_bytes(&mut &p[..]).unwrap()))
            })
            .collect())
    }
}

impl KeyIndex {
    pub fn new(path: &str) -> Result<KeyIndex, DieselError> {
        InternalKeyIndex::new(path).map(|index| KeyIndex(Mutex::new(index)))
    }

    #[cfg(test)]
    pub fn new_for_testing() -> Result<KeyIndex, DieselError> {
        KeyIndex::new(":memory:")
    }

    fn lock(&self) -> MutexGuard<InternalKeyIndex> {
        self.0.lock().expect("index-process has failed")
    }

    pub fn insert(&self, entry: Entry) -> Result<Entry, DieselError> {
        self.lock().insert(entry)
    }

    pub fn lookup(&self,
                  parent_: Option<u64>,
                  name_: Vec<u8>)
                  -> Result<Option<Entry>, DieselError> {
        self.lock().lookup(parent_, name_)
    }

    pub fn update_data_hash(&self,
                            id: u64,
                            last_modified: Option<i64>,
                            hash_opt: Option<hash::Hash>,
                            hash_ref_opt: Option<hash::tree::HashRef>)
                            -> Result<(), DieselError> {
        self.lock().update_data_hash(id, last_modified, hash_opt, hash_ref_opt)
    }

    pub fn list_dir(&self,
                    parent_opt: Option<u64>)
                    -> Result<Vec<(Entry, Option<hash::tree::HashRef>)>, DieselError> {
        self.lock().list_dir(parent_opt)
    }

    pub fn flush(&self) -> Result<(), DieselError> {
        self.lock().flush()
    }
}
