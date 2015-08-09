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


use std::sync::mpsc;
use snapshot_index::{SnapshotInfo};
use gc;


pub struct GcNoop<B> {
  backend: Box<B>,
}

impl <B: gc::GcBackend> GcNoop<B> {
  pub fn new(backend: Box<B>) -> GcNoop<B> where B: gc::GcBackend {
    GcNoop{backend: backend}
  }
}


impl <B: gc::GcBackend> gc::Gc for GcNoop<B> {

  fn register(&self, _snapshot: SnapshotInfo, refs: mpsc::Receiver<gc::Id>) {
    // It is an error to ignore the provided refereces, so we consume them here.
    refs.iter().last();
  }

  fn register_final(&self, _snapshot: SnapshotInfo, _ref_final: gc::Id) {
  }

  fn register_cleanup(&self, _snapshot: SnapshotInfo, _ref_final: gc::Id) {
  }

  fn deregister(&self, _snapshot: SnapshotInfo) {}

  fn list_unused_ids(&self, _refs: mpsc::Sender<gc::Id>) {}

}

#[test]
fn gc_noop_test() {
  gc::gc_test(vec![vec![1], vec![2], vec![1,2,3]],
              Box::new(move|backend| Box::new(GcNoop::new(Box::new(backend)))),
              gc::GcType::InExact);
}
