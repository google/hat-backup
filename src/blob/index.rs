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

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use diesel;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use sodiumoxide::randombytes::randombytes;

use tags;
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
    }
}


#[derive(Clone, Debug)]
pub struct BlobDesc {
    pub name: Vec<u8>,
    pub id: i64,
}

pub struct InternalBlobIndex {
    conn: SqliteConnection,
    next_id: i64,
    reserved: HashMap<Vec<u8>, BlobDesc>,
}

#[derive(Clone)]
pub struct BlobIndex(Arc<Mutex<InternalBlobIndex>>);


impl InternalBlobIndex {
    pub fn new(path: &str) -> Result<InternalBlobIndex, IndexError> {
        let conn = try!(SqliteConnection::establish(path));

        let mut bi = InternalBlobIndex {
            conn: conn,
            next_id: -1,
            reserved: HashMap::new(),
        };

        let dir = try!(diesel::migrations::find_migrations_directory());
        try!(diesel::migrations::run_pending_migrations_in_directory(&bi.conn,
                                                                     &dir,
                                                                     &mut util::InfoWriter));
        try!(bi.conn.begin_transaction());
        bi.refresh_next_id();
        Ok(bi)
    }

    fn new_blob_desc(&mut self) -> BlobDesc {
        BlobDesc {
            name: randombytes(24),
            id: self.next_id(),
        }
    }

    fn refresh_next_id(&mut self) {
        use diesel::expression::max;
        use super::schema::blobs::dsl::*;

        let id_opt = blobs.select(max(id).nullable())
            .first::<Option<i64>>(&self.conn)
            .optional()
            .expect("Error querying blobs")
            .and_then(|x| x);

        self.next_id = 1 + id_opt.unwrap_or(0);
    }

    fn next_id(&mut self) -> i64 {
        let id = self.next_id;
        self.next_id += 1;

        id
    }

    fn recover(&mut self, name: Vec<u8>) -> BlobDesc {
        if let Some(id) = self.find_id(&name[..]) {
            // Blob exists.
            return BlobDesc {
                name: name,
                id: id,
            };
        }

        let blob = BlobDesc {
            name: name,
            id: self.next_id(),
        };
        self.reserved.insert(blob.name.clone(), blob.clone());
        self.in_air(&blob);
        self.commit_blob(&blob);

        blob
    }

    fn reserve(&mut self) -> BlobDesc {
        let blob = self.new_blob_desc();
        self.reserved.insert(blob.name.clone(), blob.clone());

        blob
    }

    fn in_air(&mut self, blob: &BlobDesc) {
        assert!(self.reserved.get(&blob.name).is_some(),
                "blob was not reserved!");
        use super::schema::blobs::dsl::*;

        let new = schema::NewBlob {
            id: blob.id,
            name: &blob.name,
            tag: tags::Tag::InProgress as i32,
        };
        diesel::insert(&new)
            .into(blobs)
            .execute(&self.conn)
            .expect("Error inserting blob");

        self.new_transaction();
    }

    fn new_transaction(&mut self) {
        self.conn.commit_transaction().unwrap();
        self.conn.begin_transaction().unwrap();
    }

    fn commit_blob(&mut self, blob: &BlobDesc) {
        assert!(self.reserved.get(&blob.name).is_some(),
                "blob was not reserved!");
        use super::schema::blobs::dsl::*;

        diesel::update(blobs.find(blob.id))
            .set(tag.eq(tags::Tag::Done as i32))
            .execute(&self.conn)
            .expect("Error updating blob");
        self.new_transaction();
    }

    fn find_id(&mut self, name_: &[u8]) -> Option<i64> {
        use super::schema::blobs::dsl::*;
        blobs.filter(name.eq(name_))
            .select(id)
            .first::<i64>(&self.conn)
            .optional()
            .expect("Error reading blob")
    }

    fn tag(&mut self, tag_: tags::Tag, target: Option<&BlobDesc>) {
        use super::schema::blobs::dsl::*;
        match target {
            None => {
                diesel::update(blobs)
                    .set(tag.eq(tag_ as i32))
                    .execute(&self.conn)
                    .expect("Error updating blob tags")
            }
            Some(ref t) if t.id > 0 => {
                diesel::update(blobs.find(t.id))
                    .set(tag.eq(tag_ as i32))
                    .execute(&self.conn)
                    .expect("Error updating blob tags")
            }
            Some(ref t) if !t.name.is_empty() => {
                diesel::update(blobs.filter(name.eq(&t.name)))
                    .set(tag.eq(tag_ as i32))
                    .execute(&self.conn)
                    .expect("Error updating blob tags")
            }
            _ => unreachable!(),
        };
    }

    fn delete_by_tag(&mut self, tag_: tags::Tag) {
        use super::schema::blobs::dsl::*;
        diesel::delete(blobs.filter(tag.eq(tag_ as i32)))
            .execute(&self.conn)
            .expect("Error deleting blobs");
    }

    fn list_by_tag(&mut self, tag_: tags::Tag) -> Vec<BlobDesc> {
        use super::schema::blobs::dsl::*;
        blobs.filter(tag.eq(tag_ as i32))
            .load::<schema::Blob>(&self.conn)
            .expect("Error listing blobs")
            .into_iter()
            .map(|blob_| {
                BlobDesc {
                    id: blob_.id,
                    name: blob_.name,
                }
            })
            .collect()
    }
}

impl BlobIndex {
    pub fn new(path: &str) -> Result<BlobIndex, IndexError> {
        let index = try!(InternalBlobIndex::new(path));
        Ok(BlobIndex(Arc::new(Mutex::new(index))))
    }

    #[cfg(test)]
    pub fn new_for_testing() -> Result<BlobIndex, IndexError> {
        BlobIndex::new(":memory:")
    }

    fn lock(&self) -> MutexGuard<InternalBlobIndex> {
        self.0.lock().expect("index-process has failed")
    }

    /// Reserve an internal `BlobDesc` for a new blob.
    pub fn reserve(&self) -> BlobDesc {
        self.lock().reserve()
    }

    /// Report that this blob is in the process of being committed to persistent storage. If a
    /// blob is in this state when the system starts up, it may or may not exist in the persistent
    /// storage, but **should not** be referenced elsewhere, and is therefore safe to delete.
    pub fn in_air(&self, blob: &BlobDesc) {
        self.lock().in_air(&blob)
    }

    /// Report that this blob has been fully committed to persistent storage. We can now use its
    /// reference internally. Only committed blobs are considered "safe to use".
    pub fn commit_done(&self, blob: &BlobDesc) {
        self.lock().commit_blob(blob)
    }

    /// Reinstall blob recovered by from external storage.
    /// Creates a new blob by a known external name.
    pub fn recover(&self, name: Vec<u8>) -> BlobDesc {
        self.lock().recover(name)
    }

    pub fn tag(&self, blob: &BlobDesc, tag: tags::Tag) {
        self.lock().tag(tag, Some(blob))
    }

    pub fn tag_all(&self, tag: tags::Tag) {
        self.lock().tag(tag, None)
    }

    pub fn list_by_tag(&self, tag: tags::Tag) -> Vec<BlobDesc> {
        self.lock().list_by_tag(tag)
    }

    pub fn delete_by_tag(&self, tag: tags::Tag) {
        self.lock().delete_by_tag(tag)
    }

    pub fn flush(&self) {
        self.lock().new_transaction()
    }
}
