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

//! Local state for known snapshots.

use chrono;
use db;
use hash;
use std::sync::Arc;
use tags;

pub struct SnapshotIndex {
    index: Arc<db::Index>,
}

impl SnapshotIndex {
    pub fn new(idx: Arc<db::Index>) -> SnapshotIndex {
        SnapshotIndex { index: idx }
    }

    /// Delete snapshot.
    pub fn delete(&self, info: db::SnapshotInfo) {
        self.index.lock().snapshot_delete(info);
    }

    /// Lookup exact snapshot info from family and snapshot id.
    pub fn lookup(
        &mut self,
        family_name: &str,
        snapshot_id: u64,
    ) -> Option<(db::SnapshotInfo, hash::Hash, Option<hash::tree::HashRef>)> {
        self.index.lock().snapshot_lookup(family_name, snapshot_id)
    }

    pub fn reserve(&mut self, family: String) -> db::SnapshotInfo {
        self.index.lock().snapshot_reserve(family)
    }

    /// Update existing snapshot.
    pub fn update(
        &mut self,
        snapshot: &db::SnapshotInfo,
        hash: &hash::Hash,
        hash_ref: &hash::tree::HashRef,
    ) {
        self.index
            .lock()
            .snapshot_update(snapshot, "anonymous", hash, hash_ref);
    }

    /// ReadyCommit.
    pub fn ready_commit(&mut self, snapshot: &db::SnapshotInfo) {
        self.index
            .lock()
            .snapshot_set_tag(snapshot, tags::Tag::Complete)
    }

    /// Register a new snapshot by its family name, hash and persistent reference.
    pub fn commit(&mut self, snapshot: &db::SnapshotInfo) {
        self.index
            .lock()
            .snapshot_set_tag(snapshot, tags::Tag::Done)
    }

    /// We are deleting this snapshot.
    pub fn will_delete(&mut self, snapshot: &db::SnapshotInfo) {
        self.index
            .lock()
            .snapshot_set_tag(snapshot, tags::Tag::WillDelete)
    }

    /// We are ready to delete of this snapshot.
    pub fn ready_delete(&mut self, snapshot: &db::SnapshotInfo) {
        self.index
            .lock()
            .snapshot_set_tag(snapshot, tags::Tag::ReadyDelete)
    }

    /// Extract latest snapshot data for family.
    pub fn latest(
        &mut self,
        family: &str,
    ) -> Option<(db::SnapshotInfo, hash::Hash, Option<hash::tree::HashRef>)> {
        self.index.lock().snapshot_latest(family)
    }

    fn list(&mut self, skip_tag: Option<tags::Tag>) -> Vec<db::SnapshotStatus> {
        self.index.lock().snapshot_list(skip_tag)
    }

    /// List incomplete snapshots (either committing or deleting).
    pub fn list_not_done(&mut self) -> Vec<db::SnapshotStatus> {
        self.list(Some(tags::Tag::Done) /* not_tag */)
    }

    /// List all snapshots.
    pub fn list_all(&mut self) -> Vec<db::SnapshotStatus> {
        self.list(None)
    }

    /// Recover snapshot information.
    pub fn recover(
        &mut self,
        snapshot_id: u64,
        family: &str,
        created: chrono::DateTime<chrono::Utc>,
        msg: &str,
        hash_ref: &hash::tree::HashRef,
        work_opt: Option<db::SnapshotWorkStatus>,
    ) {
        self.index
            .lock()
            .snapshot_recover(snapshot_id, family, created, msg, hash_ref, work_opt)
    }

    /// Flush the hash index to clear internal buffers and commit the underlying database.
    pub fn flush(&mut self) {
        self.index.lock().flush()
    }
}
