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

//! Local state for known hashes and their external location (blob reference).


use blob;
use crypto;
use db;

use errors::{DieselError, RetryError};

use std::sync::{Arc, Mutex, MutexGuard};
use tags;
use util::UniquePriorityQueue;

pub mod tree;

#[cfg(test)]
mod tests;
#[cfg(all(test, feature = "benchmarks"))]
mod benchmarks;


pub struct HashIndex(InternalHashIndex);


/// A wrapper around Hash digests.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct Hash {
    pub bytes: Vec<u8>,
}


impl Hash {
    /// Computes `hash(text)` and stores this digest as the `bytes` field in a new `Hash` structure.
    pub fn new(text: &[u8]) -> Hash {
        Hash { bytes: crypto::authed::hash::new(text) }
    }
}


/// An entry that can be inserted into the hash index.
#[derive(Clone)]
pub struct Entry {
    /// The hash of this entry (unique among all entries in the index).
    pub hash: Hash,

    /// The level in a hash tree that this entry is from. Level `0` represents `leaf`s, i.e. entries
    /// that represent user-data, where levels `1` and up represents `branches` of the tree,
    /// i.e. internal meta-data.
    pub node: blob::NodeType,
    pub leaf: blob::LeafType,

    /// An optional list of child hash ids.
    pub childs: Option<Vec<i64>>,

    /// A reference to a location in the external persistent storage (a chunk reference) that
    /// contains the data for this entry (e.g. an object-name and a byte range).
    pub persistent_ref: Option<blob::ChunkRef>,
}

pub enum ReserveResult {
    HashKnown(i64),
    ReserveOk(i64),
}

type Queue = UniquePriorityQueue<i64, Vec<u8>, db::QueueEntry>;

pub struct InternalHashIndex {
    index: Arc<db::Index>,
    queue: Mutex<Queue>,
}

impl Drop for InternalHashIndex {
    fn drop(&mut self) {
        // Sanity check that we flushed this hash index fully before dropping it.
        // Blob store accumulates chunks for the next blob and needs flushing.
        assert_eq!(0, self.queue.lock().unwrap().len());
    }
}

impl InternalHashIndex {
    fn new(index: Arc<db::Index>) -> Result<InternalHashIndex, DieselError> {
        Ok(InternalHashIndex {
            index: index,
            queue: Mutex::new(UniquePriorityQueue::new()),
        })
    }

    pub fn queue_lock(&self) -> MutexGuard<Queue> {
        self.queue.lock().expect("Hash queue mutex poisoned")
    }

    pub fn lock(&self) -> (MutexGuard<Queue>, db::IndexGuard) {
        let queue = self.queue_lock();
        let index = self.index.lock();
        (queue, index)
    }

    fn locate(&self,
              hash: &Hash,
              queue: &MutexGuard<Queue>,
              index: &mut db::IndexGuard)
              -> Option<db::QueueEntry> {
        let result_opt = queue.find_value_of_key(&hash.bytes).cloned();
        result_opt.or_else(|| index.hash_locate(hash))
    }

    fn reserve(&self,
               hash_entry: &Entry,
               mut queue: &mut MutexGuard<Queue>,
               mut index: &mut db::IndexGuard)
               -> i64 {
        index.maybe_flush();

        let Entry { ref hash, node, leaf, ref childs, ref persistent_ref } = *hash_entry;
        assert!(!hash.bytes.is_empty());

        let my_id = index.hash_next_id();
        let qe = db::QueueEntry {
            id: my_id,
            node: node,
            leaf: leaf,
            childs: childs.clone(),
            tag: None,
            persistent_ref: persistent_ref.clone(),
        };
        index.hash_insert_new(my_id, hash.bytes.clone(), qe.clone());
        assert!(queue.put_value(my_id, hash.bytes.clone(), qe).is_ok());

        my_id
    }

    fn reserved_id(&self, hash: &Hash, queue: &MutexGuard<Queue>) -> Option<i64> {
        queue.find_key(&hash.bytes).cloned()
    }

    fn update_reserved(&self,
                       id: i64,
                       hash_entry: Entry,
                       mut queue: &mut MutexGuard<Queue>,
                       mut index: &mut db::IndexGuard) {
        let Entry { hash, node, leaf, childs, persistent_ref } = hash_entry;
        assert!(!hash.bytes.is_empty());

        let old_id = queue.find_key(&hash.bytes).cloned();
        match old_id {
            None => {
                // FIXME(jos): avoid having to set the blob ref with update_reserved.
                return;
            }
            Some(old) => {
                assert_eq!(old, id);
            }
        }
        assert_eq!(old_id, Some(id));

        // If we didn't already commit and pop() the hash, update it:
        queue.update_value(&hash.bytes, |qe| {
            qe.node = node;
            qe.leaf = leaf;
            qe.childs = childs;
            qe.persistent_ref = persistent_ref;
        });
    }

    fn insert_completed_in_order(&self,
                                 mut queue: &mut MutexGuard<Queue>,
                                 mut index: &mut db::IndexGuard) {
        while let Some((id_, _, queue_entry)) = queue.pop_min_if_complete() {
            assert_eq!(id_, queue_entry.id);
            index.hash_set_ready(id_, &queue_entry);
        }
        index.maybe_flush();
    }

    fn commit(&self,
              hash: &Hash,
              chunk_ref: blob::ChunkRef,
              mut queue: &mut MutexGuard<Queue>,
              mut index: &mut db::IndexGuard) {
        let queue_entry = self.locate(hash, queue, index).expect("hash was committed");
        queue.update_value(&hash.bytes, |old_qe| {
            old_qe.persistent_ref = Some(chunk_ref);
        });
        queue.set_ready(&queue_entry.id);
        self.insert_completed_in_order(&mut queue, &mut index);
    }
}


impl HashIndex {
    pub fn new(index: Arc<db::Index>) -> Result<HashIndex, DieselError> {
        index.lock().hash_delete_not_ready();
        Ok(HashIndex(InternalHashIndex::new(index)?))
    }

    /// Locate the local ID of this hash.
    pub fn get_id(&self, hash: &Hash) -> Option<i64> {
        assert!(!hash.bytes.is_empty());
        let (queue, mut index) = self.0.lock();
        self.0.locate(hash, &queue, &mut index).map(|entry| entry.id)
    }

    /// Locate hash entry from its ID.
    pub fn get_hash(&self, id: i64) -> Option<db::Entry> {
        self.0.index.lock().hash_locate_by_id(id)
    }

    /// Check whether this `Hash` already exists in the system.
    pub fn hash_exists(&self, hash: &Hash) -> bool {
        assert!(!hash.bytes.is_empty());
        let (queue, mut index) = self.0.lock();
        self.0.locate(hash, &queue, &mut index).is_some()
    }

    /// Locate the local childs of the `Hash`.
    pub fn fetch_childs(&self, hash: &Hash) -> Option<Option<Vec<i64>>> {
        assert!(!hash.bytes.is_empty());
        let (queue, mut index) = self.0.lock();
        self.0.locate(hash, &queue, &mut index).map(|queue_entry| queue_entry.childs)
    }

    /// Locate the persistent reference (external blob reference) for this `Hash`.
    pub fn fetch_persistent_ref(&self, hash: &Hash) -> Result<Option<blob::ChunkRef>, RetryError> {
        assert!(!hash.bytes.is_empty());
        let (queue, mut index) = self.0.lock();
        match self.0.locate(hash, &queue, &mut index) {
            Some(ref queue_entry) if queue_entry.persistent_ref.is_none() => Err(RetryError),
            Some(queue_entry) => Ok(Some(queue_entry.persistent_ref.expect("persistent_ref"))),
            None => Ok(None),
        }
    }

    /// Locate the hash reference (including persistent blob reference) for this `Hash~.
    pub fn fetch_hash_ref(&self, hash: &Hash) -> Result<Option<tree::HashRef>, RetryError> {
        assert!(!hash.bytes.is_empty());
        let (queue, mut index) = self.0.lock();
        match self.0.locate(hash, &queue, &mut index) {
            Some(ref queue_entry) if queue_entry.persistent_ref.is_none() => Err(RetryError),
            Some(queue_entry) => {
                Ok(Some(tree::HashRef {
                    hash: hash.clone(),
                    node: queue_entry.node,
                    leaf: queue_entry.leaf,
                    info: None,
                    persistent_ref: queue_entry.persistent_ref.expect("persistent_ref"),
                }))
            }
            None => Ok(None),
        }
    }

    /// Reserve a `Hash` in the index, while sending its content to external storage.
    /// This is used to ensure that each `Hash` is stored only once.
    pub fn reserve(&self, hash_entry: &Entry) -> ReserveResult {
        assert!(!hash_entry.hash.bytes.is_empty());
        // To avoid unused IO, we store entries in-memory until committed to persistent
        // storage. This allows us to continue after a crash without needing to scan
        // through and delete uncommitted entries.
        let (mut queue, mut index) = self.0.lock();
        match self.0.locate(&hash_entry.hash, &queue, &mut index) {
            Some(entry) => ReserveResult::HashKnown(entry.id),
            None => {
                let id = self.0.reserve(hash_entry, &mut queue, &mut index);
                ReserveResult::ReserveOk(id)
            }
        }
    }

    /// Check whether an entry was previously reserved.
    pub fn reserved_id(&self, hash: &Hash) -> Option<i64> {
        let queue = self.0.queue_lock();
        self.0.reserved_id(hash, &queue)
    }

    /// Update the info for a reserved `Hash`. The `Hash` remains reserved. This is used to update
    /// the persistent reference (external blob reference) as soon as it is available (to allow new
    /// references to the `Hash` to be created before it is committed).
    pub fn update_reserved(&self, id: i64, hash_entry: Entry) {
        assert!(!hash_entry.hash.bytes.is_empty());
        let (mut queue, mut index) = self.0.lock();
        self.0.update_reserved(id, hash_entry, &mut queue, &mut index);
    }

    /// A `Hash` is committed when it has been `finalized` in the external storage. `Commit`
    /// includes the persistent reference that the content is available at.
    pub fn commit(&self, hash: &Hash, persistent_ref: blob::ChunkRef) {
        assert!(!hash.bytes.is_empty());
        let (mut queue, mut index) = self.0.lock();
        self.0.commit(hash, persistent_ref, &mut queue, &mut index);
    }

    /// List all hash entries.
    pub fn list(&self) -> Vec<db::Entry> {
        self.0.index.lock().hash_list()
    }

    /// Permanently delete hash by its ID.
    pub fn delete(&self, id: i64) {
        self.0.index.lock().hash_delete(id)
    }

    /// API related to tagging, which is useful to indicate state during operation stages.
    /// It operates directly on the underlying IDs.
    pub fn set_tag(&self, id: i64, tag: tags::Tag) {
        self.0.index.lock().hash_set_tag(Some(id), tag);
    }

    /// API related to tagging, which is useful to indicate state during operation stages.
    /// It operates directly on the underlying IDs.
    pub fn set_all_tags(&self, tag: tags::Tag) {
        self.0.index.lock().hash_set_tag(None, tag)
    }

    /// API related to tagging, which is useful to indicate state during operation stages.
    /// It operates directly on the underlying IDs.
    pub fn get_tag(&self, id: i64) -> Option<tags::Tag> {
        {
            if let Some(ref q) = self.0.queue_lock().find_mut_value_of_priority(&id) {
                return q.tag;
            }
        }
        self.0.index.lock().hash_get_tag(id)
    }

    /// API related to tagging, which is useful to indicate state during operation stages.
    /// It operates directly on the underlying IDs.
    pub fn get_ids_by_tag(&self, tag: i64) -> Vec<i64> {
        self.0.index.lock().hash_list_ids_by_tag(tag)
    }

    /// API related to garbage collector metadata tied to (hash id, family id) pairs.
    pub fn read_gc_data(&self, hash_id: i64, family_id: i64) -> db::GcData {
        self.0.index.lock().hash_read_gc_data(hash_id, family_id)
    }

    /// API related to garbage collector metadata tied to (hash id, family id) pairs.
    pub fn update_gc_data<F: db::UpdateFn>(&self,
                                           hash_id: i64,
                                           family_id: i64,
                                           update_fn: F)
                                           -> db::GcData {
        self.0.index.lock().hash_update_gc_data(hash_id, family_id, update_fn)
    }

    /// API related to garbage collector metadata tied to (hash id, family id) pairs.
    pub fn update_family_gc_data<F: db::UpdateFn, I: Iterator<Item = F>>(&self,
                                                                         family_id: i64,
                                                                         update_fns: I) {
        self.0.index.lock().hash_update_family_gc_data(family_id, update_fns)
    }

    /// Manual commit. This also disables automatic periodic commit.
    pub fn manual_commit(&self) {
        let mut guard = self.0.index.lock();
        guard.flush();
        guard.set_auto_flush(false);
    }

    /// Flush the hash index to clear internal buffers and commit the underlying database.
    pub fn flush(&self) {
        self.0.index.lock().flush()
    }
}
