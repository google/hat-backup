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


use std::fs;
use std::os::unix::fs::PermissionsExt;

use chrono;
use diesel;
use diesel::prelude::*;
use diesel::connection::TransactionManager;
use diesel::sqlite::SqliteConnection;
use errors::DieselError;
use hash;
use capnp;
use filetime::FileTime;

use std::sync::{Mutex, MutexGuard};

use super::schema;
use time::Duration;
use std::path::Path;
use util::{InfoWriter, PeriodicTimer};
use tags::Tag;
use root_capnp;

#[derive(Clone, Debug)]
pub struct Entry {
    pub node_id: Option<u64>,
    pub parent_id: Option<u64>,

    pub data_hash: Option<Vec<u8>>,
    pub info: Info,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Info {
    pub name: Vec<u8>,

    pub created_ts_secs: Option<u64>,
    pub modified_ts_secs: Option<u64>,
    pub accessed_ts_secs: Option<u64>,

    pub permissions: Option<fs::Permissions>,
    pub user_id: Option<u64>,
    pub group_id: Option<u64>,

    pub byte_length: Option<u64>,
    pub hat_snapshot_ts: i64,
}

impl Entry {
    pub fn new(parent: Option<u64>, name: Vec<u8>, meta: Option<&fs::Metadata>) -> Entry {
        Entry {
            node_id: None,
            parent_id: parent,
            data_hash: None,
            info: Info::new(name, meta),
        }
    }

    pub fn data_looks_unchanged(&self, them: &Entry) -> bool {
        self.info.modified_ts_secs.is_some() &&
            ((self.parent_id, &self.info.name, self.info.modified_ts_secs) ==
                 (them.parent_id, &them.info.name, them.info.modified_ts_secs))
    }
}

impl Info {
    pub fn new(name: Vec<u8>, meta: Option<&fs::Metadata>) -> Info {
        use std::os::linux::fs::MetadataExt;

        let created = meta.and_then(|m| FileTime::from_creation_time(m)).map(
            |t| {
                t.seconds_relative_to_1970()
            },
        );
        let modified = meta.map(|m| {
            FileTime::from_last_modification_time(m).seconds_relative_to_1970()
        });
        let accessed = meta.map(|m| {
            FileTime::from_last_access_time(m).seconds_relative_to_1970()
        });

        Info {
            name: name,

            created_ts_secs: created,
            modified_ts_secs: modified,
            accessed_ts_secs: accessed,

            permissions: meta.map(|m| m.permissions()),

            user_id: meta.map(|m| m.st_uid() as u64),
            group_id: meta.map(|m| m.st_gid() as u64),

            byte_length: meta.map(|m| m.len()),
            hat_snapshot_ts: chrono::Utc::now().timestamp(),
        }
    }

    pub fn read(msg: root_capnp::file_info::Reader) -> Result<Info, capnp::Error> {
        fn none_if_zero(x: u64) -> Option<u64> {
            if x == 0 { None } else { Some(x) }
        }
        let owner = match msg.get_owner().which()? {
            root_capnp::file_info::owner::None(()) => None,
            root_capnp::file_info::owner::UserGroup(res) => {
                let ug = res?;
                Some((ug.get_user_id(), ug.get_group_id()))
            }
        };
        Ok(Info {
            name: msg.get_name()?.to_vec(),
            created_ts_secs: none_if_zero(msg.get_created_timestamp_secs()),
            modified_ts_secs: none_if_zero(msg.get_modified_timestamp_secs()),
            accessed_ts_secs: none_if_zero(msg.get_accessed_timestamp_secs()),
            permissions: match msg.get_permissions().which()? {
                root_capnp::file_info::permissions::None(()) => None,
                root_capnp::file_info::permissions::Mode(m) => Some(fs::Permissions::from_mode(m)),
            },

            user_id: owner.as_ref().map(|&(uid, _)| uid),
            group_id: owner.as_ref().map(|&(_, gid)| gid),

            byte_length: Some(msg.get_byte_length()),

            hat_snapshot_ts: msg.get_utc_timestamp(),
        })
    }
    pub fn populate_msg(&self, mut msg: root_capnp::file_info::Builder) {
        msg.borrow().set_name(&self.name);

        msg.borrow().set_created_timestamp_secs(
            self.created_ts_secs.unwrap_or(0),
        );
        msg.borrow().set_modified_timestamp_secs(
            self.modified_ts_secs.unwrap_or(0),
        );
        msg.borrow().set_accessed_timestamp_secs(
            self.accessed_ts_secs.unwrap_or(0),
        );
        msg.borrow().set_byte_length(self.byte_length.unwrap_or(0));

        match (self.user_id, self.group_id) {
            (Some(uid), Some(gid)) => {
                let mut ug = msg.borrow().get_owner().init_user_group();
                ug.set_user_id(uid);
                ug.set_group_id(gid);
            }
            _ => {
                msg.borrow().get_owner().set_none(());
            }
        }

        match self.permissions {
            Some(ref p) => msg.borrow().get_permissions().set_mode(p.mode()),
            None => msg.borrow().get_permissions().set_none(()),
        }

        msg.borrow().set_utc_timestamp(self.hat_snapshot_ts);
    }
}

pub struct KeyIndex(Mutex<InternalKeyIndex>);

pub struct InternalKeyIndex {
    conn: SqliteConnection,
    flush_timer: PeriodicTimer,
}


impl InternalKeyIndex {
    fn new(migrations_dir: &Path, path: &str) -> Result<InternalKeyIndex, DieselError> {
        let conn = SqliteConnection::establish(path)?;

        let ki = InternalKeyIndex {
            conn: conn,
            flush_timer: PeriodicTimer::new(Duration::seconds(5)),
        };

        {
            // Enable foreign key support.
            diesel::expression::sql::<diesel::types::Integer>("PRAGMA foreign_keys = ON;")
                .execute(&ki.conn)?;
        }

        diesel::migrations::run_pending_migrations_in_directory(
            &ki.conn,
            &migrations_dir,
            &mut InfoWriter,
        )?;

        {
            let tm = ki.conn.transaction_manager();
            tm.begin_transaction(&ki.conn)?;
        }

        {
            // Reset tags.
            use super::schema::key_data::dsl::*;
            diesel::update(key_data.filter(tag.ne(Tag::Done as i64)))
                .set(tag.eq(Tag::Done as i64))
                .execute(&ki.conn)?;
        }

        Ok(ki)
    }

    fn last_insert_rowid(&self) -> Result<i64, DieselError> {
        let id = diesel::select(diesel::expression::sql("last_insert_rowid()"))
            .first::<i64>(&self.conn)?;
        Ok(id)
    }

    fn maybe_flush(&mut self) -> Result<(), DieselError> {
        if self.flush_timer.did_fire() {
            self.flush()?;
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<(), DieselError> {
        debug!("SQL: key index commit");

        let tm = self.conn.transaction_manager();
        tm.commit_transaction(&self.conn)?;
        tm.begin_transaction(&self.conn)?;

        Ok(())
    }

    /// Insert an entry in the key index.
    /// Returns `Id` with the new entry ID.
    fn insert(
        &mut self,
        mut entry: Entry,
        hash_ref_opt: Option<&hash::tree::HashRef>,
    ) -> Result<Entry, DieselError> {
        if entry.node_id.is_none() {
            let new = schema::NewKeyNode {
                node_id: None, // new row id
                parent_id: entry.parent_id.map(|p| p as i64),
                name: &entry.info.name[..],
            };
            use super::schema::key_tree::dsl::*;
            diesel::insert(&new).into(key_tree).execute(&self.conn)?;
            entry.node_id = Some(self.last_insert_rowid()? as u64);
        }

        {
            let hash_ref_bytes = hash_ref_opt.map(|r| r.as_bytes());
            let new = schema::NewKeyData {
                node_id: entry.node_id.map(|i| i as i64),
                committed: false,
                tag: Tag::Reserved as i64,
                created: entry.info.created_ts_secs.map(|u| u as i64),
                modified: entry.info.modified_ts_secs.map(|u| u as i64),
                accessed: entry.info.accessed_ts_secs.map(|u| u as i64),
                permissions: entry.info.permissions.as_ref().map(|p| p.mode() as i64),
                group_id: entry.info.group_id.map(|u| u as i64),
                user_id: entry.info.user_id.map(|u| u as i64),
                hash: hash_ref_opt.map(|h| &h.hash.bytes[..]),
                hash_ref: hash_ref_bytes.as_ref().map(|v| &v[..]),
            };

            // Insert replaces when (node_id, committed) already exists.
            use super::schema::key_data::dsl::*;
            diesel::insert(&new).into(key_data).execute(&self.conn)?;
            self.maybe_flush()?;
        }

        Ok(entry)
    }

    /// Lookup an entry in the key index by its parent id and name.
    /// Returns either `Entry` with the found entry or `NotFound`.
    fn lookup(
        &mut self,
        parent_: Option<u64>,
        name_: Vec<u8>,
    ) -> Result<Option<Entry>, DieselError> {
        use super::schema::key_tree::dsl::{name, parent_id, key_tree};
        use super::schema::key_data::dsl::*;

        let row_opt = match parent_ {
            Some(p) => {
                key_tree
                    .inner_join(key_data)
                    .filter(parent_id.eq(p as i64))
                    .filter(name.eq(&name_[..]))
                    .order(committed)
                    .first::<(schema::KeyNode, schema::KeyData)>(&self.conn)
                    .optional()?
            }
            None => {
                key_tree
                    .inner_join(key_data)
                    .filter(parent_id.is_null())
                    .filter(name.eq(&name_[..]))
                    .order(committed)
                    .first::<(schema::KeyNode, schema::KeyData)>(&self.conn)
                    .optional()?
            }
        };

        if let Some((node, data)) = row_opt {
            Ok(Some(Entry {
                node_id: node.node_id.map(|n| n as u64),
                parent_id: node.parent_id.map(|p| p as u64),
                data_hash: data.hash,
                info: Info {
                    name: name_,
                    created_ts_secs: data.created.map(|i| i as u64),
                    modified_ts_secs: data.modified.map(|i| i as u64),
                    accessed_ts_secs: data.accessed.map(|i| i as u64),
                    permissions: data.permissions.map(
                        |m| fs::Permissions::from_mode(m as u32),
                    ),
                    user_id: data.user_id.map(|x| x as u64),
                    group_id: data.group_id.map(|x| x as u64),
                    byte_length: None,
                    hat_snapshot_ts: 0,
                },
            }))
        } else {
            Ok(None)
        }
    }

    /// List a directory (aka. `level`) in the index.
    /// Returns `ListResult` with all the entries under the given parent.
    fn list_dir(
        &mut self,
        parent_opt: Option<u64>,
    ) -> Result<Vec<(Entry, Option<hash::tree::HashRef>)>, DieselError> {
        use diesel::prelude::*;
        use super::schema::key_tree::dsl::*;
        use super::schema::key_data::dsl::{committed, key_data};

        let rows = match parent_opt {
            Some(p) => {
                key_tree
                    .inner_join(key_data)
                    .filter(parent_id.eq(p as i64))
                    .filter(committed.eq(true))
                    .load::<(schema::KeyNode, schema::KeyData)>(&self.conn)?
            }
            None => {
                key_tree
                    .inner_join(key_data)
                    .filter(parent_id.is_null())
                    .filter(committed.eq(true))
                    .load::<(schema::KeyNode, schema::KeyData)>(&self.conn)?
            }
        };

        Ok(
            rows.into_iter()
                .map(|(node, mut data)| {
                    (
                        Entry {
                            node_id: node.node_id.map(|n| n as u64),
                            parent_id: node.parent_id.map(|i| i as u64),
                            data_hash: data.hash,
                            info: Info {
                                name: node.name,
                                created_ts_secs: data.created.map(|i| i as u64),
                                modified_ts_secs: data.modified.map(|i| i as u64),
                                accessed_ts_secs: data.accessed.map(|i| i as u64),
                                permissions: data.permissions.map(|m| {
                                    fs::Permissions::from_mode(m as u32)
                                }),
                                user_id: data.user_id.map(|x| x as u64),
                                group_id: data.group_id.map(|x| x as u64),
                                byte_length: None,
                                hat_snapshot_ts: 0,
                            },
                        },
                        data.hash_ref.as_mut().map(|p| {
                            ::hash::tree::HashRef::from_bytes(&mut &p[..]).unwrap()
                        }),
                    )
                })
                .collect(),
        )
    }

    fn mark_reserved(&mut self, entry: &Entry) -> Result<(), DieselError> {
        use super::schema::key_data::dsl::*;
        diesel::update(key_data.filter(node_id.eq(entry.node_id.expect(
            "Need ID to reserve entry",
        ) as i64))).set((tag.eq(Tag::Reserved as i64)))
            .execute(&self.conn)?;
        Ok(())
    }

    /// Commit individual nodes marked reserved.
    fn commit_reserved_nodes(&mut self) -> Result<(), DieselError> {
        // Promote all needed keys to 'ready' in one statement to preserve referential integrity.
        // If the (node_id, ready) combination already exist, the conflict is resolved by replace.
        use super::schema::key_data::dsl::*;
        diesel::update(key_data.filter(tag.eq(Tag::Reserved as i64)).filter(
            committed.eq(
                false,
            ),
        )).set(committed.eq(true))
            .execute(&self.conn)?;

        Ok(())
    }

    /// Delete children not marked reserved.
    /// This function applies recursively to child directories.
    fn cleanup_unused(&mut self, parent_opt: Option<u64>) -> Result<(), DieselError> {
        use super::schema::key_tree::dsl::*;
        use super::schema::key_data::dsl::{tag, key_data};

        let children = match parent_opt {
            Some(p) => {
                key_tree
                    .inner_join(key_data)
                    .filter(parent_id.eq(p as i64))
                    .select((node_id, tag))
                    .load::<(Option<i64>, i64)>(&self.conn)?
            }
            None => {
                key_tree
                    .inner_join(key_data)
                    .filter(parent_id.is_null())
                    .select((node_id, tag))
                    .load::<(Option<i64>, i64)>(&self.conn)?
            }
        };

        for (node_id_, tag_) in children {
            let id = node_id_.unwrap();
            if tag_ == Tag::Reserved as i64 {
                self.cleanup_unused(Some(id as u64))?;
            } else {
                diesel::delete(key_tree.filter(node_id.eq(id))).execute(
                    &self.conn,
                )?;
            }
        }

        self.flush()?;

        Ok(())
    }
}

impl KeyIndex {
    pub fn new(migration_dir: &Path, name: &str) -> Result<KeyIndex, DieselError> {
        InternalKeyIndex::new(migration_dir, name).map(|index| KeyIndex(Mutex::new(index)))
    }

    #[cfg(test)]
    pub fn new_for_testing() -> Result<KeyIndex, DieselError> {
        KeyIndex::new(Path::new("migrations"), ":memory:")
    }

    fn lock(&self) -> MutexGuard<InternalKeyIndex> {
        self.0.lock().expect("index-process has failed")
    }

    pub fn insert(
        &self,
        entry: Entry,
        hash_ref_opt: Option<&hash::tree::HashRef>,
    ) -> Result<Entry, DieselError> {
        self.lock().insert(entry, hash_ref_opt)
    }

    pub fn lookup(
        &self,
        parent_: Option<u64>,
        name_: Vec<u8>,
    ) -> Result<Option<Entry>, DieselError> {
        self.lock().lookup(parent_, name_)
    }

    pub fn list_dir(
        &self,
        parent_opt: Option<u64>,
    ) -> Result<Vec<(Entry, Option<hash::tree::HashRef>)>, DieselError> {
        self.lock().list_dir(parent_opt)
    }

    pub fn mark_reserved(&self, entry: &Entry) -> Result<(), DieselError> {
        self.lock().mark_reserved(entry)
    }

    pub fn commit_reserved_nodes(&self) -> Result<(), DieselError> {
        self.lock().commit_reserved_nodes()
    }

    pub fn cleanup_unused(&self, parent_opt: Option<u64>) -> Result<(), DieselError> {
        self.lock().cleanup_unused(parent_opt)
    }

    pub fn flush(&self) -> Result<(), DieselError> {
        self.lock().flush()
    }
}
