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


use hash_index::{GcData};
use std::sync::mpsc;
use snapshot_index::{SnapshotInfo};
use gc;
use tags;


// This GC does not store per-family data.
// Instead this constant family ID is always used.
const DATA_FAMILY: i64 = 0;


pub struct GcRc<B> {
  backend: Box<B>,
}

impl <B: gc::GcBackend> GcRc<B> {
  pub fn new(backend: Box<B>) -> GcRc<B> where B: gc::GcBackend {
    GcRc{backend: backend}
  }

  fn mark_tree(&mut self, root: gc::Id, tag: tags::Tag) {
    self.backend.set_tag(root, tag.clone());
    for r in self.backend.reverse_refs(root) {
      if let Some(current) = self.backend.get_tag(r.clone()) {
        if current == tag {
          continue;
        }
      }
      self.backend.set_tag(r.clone(), tag.clone());
      self.mark_tree(r, tag.clone());
    }
  }
}


impl <B: gc::GcBackend> gc::Gc for GcRc<B> {

  fn register(&mut self, _snapshot: SnapshotInfo, refs: mpsc::Receiver<gc::Id>) {
    // Increment counters.
    for r in refs.iter() {
      self.backend.update_data(r, DATA_FAMILY,
                               Box::new(move|GcData{num, bytes}| Some(GcData{num:num + 1, bytes:bytes})));
    }
  }

  fn register_final(&mut self, _snapshot: SnapshotInfo, ref_final: gc::Id) {
    // Increment final counter and tag it as ready.
    self.backend.update_data(ref_final, DATA_FAMILY,
                             Box::new(move|GcData{num, bytes}| Some(GcData{num:num + 1, bytes:bytes})));
    self.backend.set_tag(ref_final, tags::Tag::InProgress);
  }

  fn register_cleanup(&mut self, _snapshot: SnapshotInfo, ref_final: gc::Id) {
    // Clear tag of final reference.
    self.backend.set_tag(ref_final, tags::Tag::Done);
  }

  fn deregister(&mut self, snapshot: SnapshotInfo) {
    for r in self.backend.list_snapshot_refs(snapshot).iter() {
      self.backend.update_data(r, DATA_FAMILY,
                               Box::new(move|GcData{num, bytes}| Some(GcData{num:num - 1, bytes:bytes})));
    }
  }


  fn list_unused_ids(&mut self, refs: mpsc::Sender<gc::Id>) {
    self.backend.set_all_tags(tags::Tag::Done);
    for r in self.backend.list_ids_by_tag(tags::Tag::Done) {
      let data = self.backend.get_data(r, DATA_FAMILY);
      if data.num > 0 {
        self.mark_tree(r, tags::Tag::Reserved);
      }
    }
    // Everything that is still 'Done' is unused.
    // Everything that is 'Reserved' is used.
    for r in self.backend.list_ids_by_tag(tags::Tag::Done).iter() {
      if let Err(_) = refs.send(r) {
        return;
      }
    }
  }

}

#[test]
fn gc_rc_test() {
  gc::gc_test(vec![vec![1], vec![2], vec![1,2,3], vec![4, 5, 6]],
              Box::new(move|backend| Box::new(GcRc::new(Box::new(backend)))),
              gc::GcType::Exact);
}
