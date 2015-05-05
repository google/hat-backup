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
use snapshot_index::{SnapshotInfo};
use tags;

pub type Id = i64;
pub type UpdateFn = Thunk<'static, (GcData,), Option<GcData>>;


pub trait GcBackend {
  fn get_data(&self, hash_id: Id, family_id: Id)
              -> GcData;

  fn update_data(&self, hash_id: Id, family_id: Id, f: UpdateFn)
                 -> GcData;
  fn update_all_data_by_family(&self, family_id: Id, fs: mpsc::Receiver<UpdateFn>);

  fn set_tag(&self, hash_id: Id, tag: tags::Tag);
  fn get_tag(&self, hash_id: Id) -> Option<tags::Tag>;

  fn set_all_tags(&self, tag: tags::Tag);
  fn reverse_refs(&self, hash_id: Id) -> Vec<Id>;

  fn list_ids_by_tag(&self, tag: tags::Tag) -> mpsc::Receiver<Id>;
  fn list_snapshot_refs(&self, snapshot: SnapshotInfo) -> mpsc::Receiver<Id>;
}


pub trait Gc {
  fn register(&self, snapshot: SnapshotInfo, refs: mpsc::Receiver<Id>);
  fn register_final(&self, snapshot: SnapshotInfo, final_ref: Id);
  fn register_cleanup(&self, snapshot: SnapshotInfo, final_ref: Id);

  fn deregister(&self, snapshot: SnapshotInfo);

  fn list_unused_ids(&self, refs: mpsc::Sender<Id>);
}
