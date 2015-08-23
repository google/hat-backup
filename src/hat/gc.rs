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


use std::boxed::FnBox;
use std::collections::{HashMap};
use std::sync::{mpsc, Arc, Mutex};

use hash_index::{GcData};
use snapshot_index::{SnapshotInfo};
use tags;


pub type Id = i64;
pub type UpdateFn = Box<FnBox(GcData) -> Option<GcData> + Send>;


pub trait GcBackend {
  fn get_data(&self, hash_id: Id, family_id: Id)
              -> GcData;

  fn update_data(&mut self, hash_id: Id, family_id: Id, f: UpdateFn)
                 -> GcData;
  fn update_all_data_by_family(&mut self, family_id: Id, fs: mpsc::Receiver<UpdateFn>);

  fn set_tag(&mut self, hash_id: Id, tag: tags::Tag);
  fn get_tag(&self, hash_id: Id) -> Option<tags::Tag>;

  fn set_all_tags(&mut self, tag: tags::Tag);
  fn reverse_refs(&self, hash_id: Id) -> Vec<Id>;

  fn list_ids_by_tag(&self, tag: tags::Tag) -> mpsc::Receiver<Id>;
  fn list_snapshot_refs(&self, snapshot: SnapshotInfo) -> mpsc::Receiver<Id>;
}


pub trait Gc {
  fn register(&mut self, snapshot: SnapshotInfo, refs: mpsc::Receiver<Id>);
  fn register_final(&mut self, snapshot: SnapshotInfo, final_ref: Id);
  fn register_cleanup(&mut self, snapshot: SnapshotInfo, final_ref: Id);

  fn deregister(&mut self, snapshot: SnapshotInfo);

  fn list_unused_ids(&self, refs: mpsc::Sender<Id>);
}


pub struct MemoryBackend {
  gc_data: HashMap<(Id, Id), GcData>,
  tags: HashMap<Id, tags::Tag>,
  parents: HashMap<Id, Vec<Id>>,
  snapshot_refs: HashMap<Id, Vec<Id>>,
}

impl MemoryBackend {
  fn new() -> MemoryBackend {
    MemoryBackend{
      gc_data:       HashMap::new(),
      tags:          HashMap::new(),
      parents:       HashMap::new(),
      snapshot_refs: HashMap::new(),
    }
  }
}


#[derive(Clone)]
pub struct SafeMemoryBackend {
  backend: Arc<Mutex<MemoryBackend>>
}

impl SafeMemoryBackend {
  fn new() -> SafeMemoryBackend {
    SafeMemoryBackend{backend: Arc::new(Mutex::new(MemoryBackend::new()))}
  }

  fn insert_parent(&mut self, hash_id: Id, childs: Vec<Id>) {
    self.backend.lock().unwrap().parents.insert(hash_id, childs);
  }

  fn insert_snapshot(&mut self, info: &SnapshotInfo, refs: Vec<Id>) {
    self.backend.lock().unwrap().snapshot_refs.insert(info.unique_id, refs);
  }
}

impl GcBackend for SafeMemoryBackend {
  fn get_data(&self, hash_id: Id, family_id: Id) -> GcData {
    self.backend.lock().unwrap()
        .gc_data.get(&(hash_id, family_id)).unwrap_or(&GcData{num: 0, bytes: vec![]}).clone()
  }

  fn update_data(&mut self, hash_id: Id, family_id: Id, f: UpdateFn) -> GcData {
    let new = match f(self.get_data(hash_id, family_id)) {
      Some(d) => d,
      None    => GcData{num: 0, bytes: vec![]},
    };
    self.backend.lock().unwrap().gc_data.insert((hash_id, family_id), new.clone());
    return new;
  }

  fn update_all_data_by_family(&mut self, family_id: Id, fs: mpsc::Receiver<UpdateFn>) {
    for (k, v) in self.backend.lock().unwrap().gc_data.iter_mut() {
      if k.1 == family_id {
        let f = fs.recv().unwrap();
        *v = f(v.clone()).unwrap_or(GcData{num: 0, bytes: vec![]});
      }
    }
  }

  fn set_tag(&mut self, hash_id: Id, tag: tags::Tag) {
    self.backend.lock().unwrap().tags.insert(hash_id, tag);
  }

  fn get_tag(&self, hash_id: Id) -> Option<tags::Tag> {
    self.backend.lock().unwrap().tags.get(&hash_id).map(|t| t.clone())
  }

  fn set_all_tags(&mut self, tag: tags::Tag) {
    for (_k, v) in self.backend.lock().unwrap().tags.iter_mut() {
      *v = tag.clone();
    }
  }

  fn reverse_refs(&self, hash_id: Id) -> Vec<Id> {
    self.backend.lock().unwrap().parents.get(&hash_id).unwrap_or(&vec![]).clone()
  }

  fn list_ids_by_tag(&self, tag: tags::Tag) -> mpsc::Receiver<Id> {
    let mut ids = vec![];
    for (id, id_tag) in self.backend.lock().unwrap().tags.iter() {
      if *id_tag == tag {
        ids.push(*id);
      }
    }

    let (sender, receiver) = mpsc::channel();
    ids.iter().map(|id| sender.send(*id)).last();
    return receiver;
  }

  fn list_snapshot_refs(&self, info: SnapshotInfo) -> mpsc::Receiver<Id> {
    let (sender, receiver) = mpsc::channel();
    let refs = self.backend.lock().unwrap()
                   .snapshot_refs.get(&info.unique_id).unwrap_or(&vec![]).clone();
    refs.iter().map(|id| sender.send(*id)).last();
    return receiver;
  }
}

pub enum GcType {
  Exact,
  InExact,  // Includes probalistic gc.
}

pub fn gc_test<GC>(snapshots: Vec<Vec<u8>>,
                   mk_gc: Box<FnBox(SafeMemoryBackend) -> Box<GC>>,
                   gc_type: GcType) where GC: Gc {
  let mut backend = SafeMemoryBackend::new();
  let mut gc = mk_gc(backend.clone());

  let mut infos = vec![];
  for (i, refs) in snapshots.iter().enumerate() {
    let info = SnapshotInfo{unique_id: i as i64, family_id: 1, snapshot_id: i as i64};
    backend.insert_snapshot(&info, refs.iter().map(|i| *i as Id).collect());
    infos.push(info);
  }

  for (i, refs) in snapshots.iter().enumerate() {
    let (sender, receiver) = mpsc::channel();
    refs[..refs.len() - 1].iter().map(|id| sender.send(*id as Id)).last();
    drop(sender);

    gc.register(infos[i].clone(), receiver);
    let last_ref = *refs.iter().last().expect("len() >= 0") as i64;
    gc.register_final(infos[i].clone(), last_ref);
    gc.register_cleanup(infos[i].clone(), last_ref);
  }

  for (i, refs) in snapshots.iter().enumerate() {
    // Check that snapshot is still valid.
    let (sender, receiver) = mpsc::channel();
    gc.list_unused_ids(sender);
    receiver.iter().filter(|i:&i64| refs.contains(&(*i as u8))).map(|i| {
        panic!("ID prematurely deleted by GC: {}", i) }).last();
    // Deregister snapshot.
    gc.deregister(infos[i].clone());
  }

  let mut all_refs: Vec<u8> = snapshots.iter().flat_map(|v| v.clone().into_iter()).collect();
  all_refs.dedup();

  match gc_type {
    GcType::Exact => {
      // Check that all IDs were eventually marked unused.
      let (sender, receiver) = mpsc::channel();
      gc.list_unused_ids(sender);
      assert_eq!(receiver.iter().count(), all_refs.len());
    },
    GcType::InExact => {},
  }
}
