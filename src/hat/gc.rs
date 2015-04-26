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
use std::thunk::{Thunk};

use hash_index::{GcData};


pub type Id = i64;
pub type UpdateFn = Thunk<'static, (GcData,), Option<GcData>>;

pub struct SnapshotInfo{
  unique_id: i64,
  family_id: i64,
  snapshot_id: i64
}


pub trait GcBackend {
  fn get_data(&self, hash_id: Id, family_id: Id)
              -> GcData;

  fn update_data(&self, hash_id: Id, family_id: Id, f: UpdateFn)
                 -> GcData;
  fn update_all_data_by_family(&self, family_id: Id, fs: mpsc::Receiver<UpdateFn>);

  fn set_tag(&self, hash_id: Id, tag: Option<i64>);
  fn get_tag(&self, hash_id: Id) -> Option<i64>;

  fn set_all_tags(&self, tag: Option<i64>);
  fn reverse_refs(&self, hash_id: Id) -> Vec<Id>;

  fn list_ids_by_tag(&self, tag: i64) -> mpsc::Receiver<Id>;
  fn list_snapshot_refs(&self, snapshot: SnapshotInfo) -> mpsc::Receiver<Id>;
}


pub trait Gc {
  fn register(&self, snapshot: SnapshotInfo, refs: mpsc::Receiver<Id>);
  fn deregister(&self, snapshot: SnapshotInfo);

  fn list_unused_ids(&self, refs: mpsc::Sender<Id>);
}
