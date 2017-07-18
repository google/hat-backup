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


use db::{GcData, SnapshotInfo};
use gc;
use std::sync::mpsc;
use tags;


// This GC does not store per-family data.
// Instead this constant family ID is always used.
const DATA_FAMILY: u64 = 0;


pub struct GcRc<B> {
    backend: B,
}

impl<B: gc::GcBackend> gc::Gc<B> for GcRc<B> {
    type Err = B::Err;

    fn new(backend: B) -> GcRc<B>
    where
        B: gc::GcBackend,
    {
        GcRc { backend: backend }
    }

    fn is_exact() -> bool {
        true
    }

    fn register_final(
        &mut self,
        _snapshot: &SnapshotInfo,
        ref_final: gc::Id,
    ) -> Result<(), Self::Err> {
        // Start off with a commit to disable automatic commit and run register as one transaction.
        self.backend.manual_commit()?;

        // Add final reference to the set of hashes to update.
        self.backend.set_tag(ref_final, tags::Tag::Reserved)?;

        for r in self.backend.list_ids_by_tag(tags::Tag::Reserved)? {
            self.backend.update_data(
                r,
                DATA_FAMILY,
                move |GcData { num, bytes }| {
                    Some(GcData {
                        num: num + 1,
                        bytes: bytes,
                    })
                },
            )?;
        }

        self.backend.set_tag(ref_final, tags::Tag::InProgress)?;

        Ok(())
    }

    fn register_cleanup(
        &mut self,
        _snapshot: &SnapshotInfo,
        _ref_final: gc::Id,
    ) -> Result<(), Self::Err> {
        // Clear all tags including final reference.
        self.backend.set_all_tags(tags::Tag::Done)?;

        Ok(())
    }

    fn deregister<F>(
        &mut self,
        _snapshot: &SnapshotInfo,
        ref_final: gc::Id,
        refs: F,
    ) -> Result<(), Self::Err>
    where
        F: FnOnce() -> mpsc::Receiver<gc::Id>,
    {
        self.backend.set_all_tags(tags::Tag::Done)?;
        // Tag hashes whose counters will be decremented.
        for r in refs().iter() {
            self.backend.set_tag(r, tags::Tag::Reserved)?;
        }

        // Start off with a commit to disable automatic commit.
        // This causes deregister to run as one transaction.
        self.backend.manual_commit()?;

        for r in self.backend.list_ids_by_tag(tags::Tag::Reserved)? {
            self.backend.update_data(
                r,
                DATA_FAMILY,
                move |GcData { num, bytes }| {
                    Some(GcData {
                        num: num - 1,
                        bytes: bytes,
                    })
                },
            )?;
        }
        self.backend.set_tag(ref_final, tags::Tag::ReadyDelete)?;

        Ok(())
    }


    fn list_unused_ids(&mut self, refs: mpsc::Sender<gc::Id>) -> Result<(), Self::Err> {
        self.backend.set_all_tags(tags::Tag::Done)?;
        for r in self.backend.list_ids_by_tag(tags::Tag::Done)? {
            let data = self.backend.get_data(r, DATA_FAMILY)?;
            assert!(data.num >= 0);
            if data.num > 0 {
                gc::mark_tree(&mut self.backend, r, tags::Tag::Reserved)?;
            }
        }
        // Everything that is still 'Done' is unused.
        // Everything that is 'Reserved' is used.
        for r in self.backend.list_ids_by_tag(tags::Tag::Done)?.iter() {
            if refs.send(r).is_err() {
                break;
            }
        }

        Ok(())
    }

    fn status(&mut self, final_ref: gc::Id) -> Result<Option<gc::Status>, Self::Err> {
        Ok(match self.backend.get_tag(final_ref)? {
            Some(tags::Tag::Complete) |
            Some(tags::Tag::ReadyDelete) => Some(gc::Status::Complete),
            Some(tags::Tag::InProgress) => Some(gc::Status::InProgress),
            _ => None,
        })
    }
}

#[test]
fn gc_rc_test() {
    gc::gc_test::<GcRc<_>>(vec![vec![1], vec![2], vec![1, 2, 3], vec![4, 5, 6]]);
}

#[test]
fn gc_rc_resume_register_test() {
    gc::resume_register_test::<GcRc<_>>();
}

#[test]
fn gc_rc_resume_deregister_test() {
    gc::resume_deregister_test::<GcRc<_>>();
}
