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


use crypto;
use db;

use errors::DieselError;

use std::sync::{Arc, Mutex};

use tags;


#[derive(Clone, Debug, Default)]
pub struct BlobDesc {
    pub name: Vec<u8>,
    pub id: i64,
}

pub struct InternalBlobIndex {
    index: Arc<db::Index>,
    next_id: Arc<Mutex<i64>>,
    keys: Arc<crypto::keys::Keeper>,
}

pub struct BlobIndex(InternalBlobIndex);


impl InternalBlobIndex {
    pub fn new(
        keys: Arc<crypto::keys::Keeper>,
        index: Arc<db::Index>,
    ) -> Result<InternalBlobIndex, DieselError> {
        let bi = InternalBlobIndex {
            index: index,
            next_id: Arc::new(Mutex::new(0)),
            keys: keys,
        };
        bi.refresh_next_id();
        Ok(bi)
    }

    fn name_of_id(&self, id: i64) -> Vec<u8> {
        return crypto::FixedKey::new(&self.keys)
            .seal_blob_name(crypto::PlainText::from_i64(id).as_ref())
            .to_vec();
    }

    fn id_of_name(&self, name: &[u8]) -> Result<i64, String> {
        return Ok(
            crypto::FixedKey::new(&self.keys)
                .unseal_blob_name(crypto::CipherTextRef::new(name))
                .as_ref()
                .read_i64()
                .unwrap(),
        );
    }

    fn new_blob_desc(&self) -> BlobDesc {
        let id = self.next_id();
        BlobDesc {
            name: self.name_of_id(id),
            id: id,
        }
    }

    pub fn refresh_next_id(&self) {
        let id = {
            self.index.lock().blob_next_id()
        };
        let mut next_id = self.next_id.lock().unwrap();
        *next_id = 1 + id;
    }

    fn next_id(&self) -> i64 {
        let mut id = self.next_id.lock().unwrap();
        *id += 1;
        *id
    }

    fn recover(&self, name: Vec<u8>) -> BlobDesc {
        let wanted_id = self.id_of_name(&name).unwrap();
        if let Some(id) = {
            self.index.lock().blob_id_from_name(&name[..])
        }
        {
            assert_eq!(id, wanted_id);

            // Blob exists.
            return BlobDesc { name: name, id: id };
        }

        let blob = BlobDesc {
            name: name,
            id: wanted_id,
        };
        self.index.lock().blob_in_air(&blob);
        self.index.lock().blob_commit(&blob);

        blob
    }

    fn reserve(&self) -> BlobDesc {
        self.new_blob_desc()
    }
}

impl BlobIndex {
    pub fn new(
        keys: Arc<crypto::keys::Keeper>,
        index: Arc<db::Index>,
    ) -> Result<BlobIndex, DieselError> {
        InternalBlobIndex::new(keys, index).map(|bi| BlobIndex(bi))
    }

    /// Reserve an internal `BlobDesc` for a new blob.
    pub fn reserve(&self) -> BlobDesc {
        self.0.reserve()
    }

    /// Report that this blob is in the process of being committed to persistent storage. If a
    /// blob is in this state when the system starts up, it may or may not exist in the persistent
    /// storage, but **should not** be referenced elsewhere, and is therefore safe to delete.
    pub fn in_air(&self, blob: &BlobDesc) {
        self.0.index.lock().blob_in_air(blob)
    }

    /// Report that this blob has been fully committed to persistent storage. We can now use its
    /// reference internally. Only committed blobs are considered "safe to use".
    pub fn commit_done(&self, blob: &BlobDesc) {
        self.0.index.lock().blob_commit(blob)
    }

    /// Reinstall blob recovered by from external storage.
    /// Creates a new blob by a known external name.
    pub fn recover(&self, name: Vec<u8>) -> BlobDesc {
        self.0.recover(name)
    }

    pub fn find(&self, name: &[u8]) -> Option<BlobDesc> {
        if let Some(id) = self.0.index.lock().blob_id_from_name(&name) {
            Some(BlobDesc {
                name: name.to_vec(),
                id: id,
            })
        } else {
            None
        }
    }

    pub fn tag(&self, blob: &BlobDesc, tag: tags::Tag) {
        self.0.index.lock().blob_set_tag(tag, Some(blob))
    }

    pub fn tag_all(&self, tag: tags::Tag) {
        self.0.index.lock().blob_set_tag(tag, None)
    }

    pub fn list_by_tag(&self, tag: tags::Tag) -> Vec<BlobDesc> {
        self.0.index.lock().blob_list_by_tag(tag)
    }

    pub fn delete_by_tag(&self, tag: tags::Tag) {
        self.0.index.lock().blob_delete_by_tag(tag)
    }

    pub fn flush(&self) {
        self.0.index.lock().flush()
    }
}
