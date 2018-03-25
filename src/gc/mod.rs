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

use db::{GcData, SnapshotInfo, UpdateFn};
#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use std::fmt;
#[cfg(test)]
use std::mem;
#[cfg(test)]
use std::sync::{Arc, Mutex};

use std::sync::mpsc;
use tags;

mod noop;
mod rc;
pub use self::noop::GcNoop;
pub use self::rc::GcRc;

pub type Id = u64;

#[derive(PartialEq, Debug)]
pub enum Status {
    InProgress,
    Complete,
}

pub trait GcBackend {
    type Err;

    fn get_data(&self, hash_id: Id, family_id: Id) -> Result<GcData, Self::Err>;

    fn update_data<F: UpdateFn>(
        &mut self,
        hash_id: Id,
        family_id: Id,
        f: F,
    ) -> Result<GcData, Self::Err>;
    fn update_all_data_by_family<F: UpdateFn, I: Iterator<Item = F>>(
        &mut self,
        family_id: Id,
        fns: I,
    ) -> Result<(), Self::Err>;

    fn set_tag(&mut self, hash_id: Id, tag: tags::Tag) -> Result<(), Self::Err>;
    fn get_tag(&self, hash_id: Id) -> Result<Option<tags::Tag>, Self::Err>;

    fn set_all_tags(&mut self, tag: tags::Tag) -> Result<(), Self::Err>;
    fn reverse_refs(&self, hash_id: Id) -> Result<Vec<Id>, Self::Err>;

    fn list_ids_by_tag(&self, tag: tags::Tag) -> Result<mpsc::Receiver<Id>, Self::Err>;

    fn manual_commit(&mut self) -> Result<(), Self::Err>;
}

pub fn mark_tree<B>(backend: &mut B, root: Id, tag: tags::Tag) -> Result<(), B::Err>
where
    B: GcBackend,
{
    backend.set_tag(root, tag)?;
    for r in backend.reverse_refs(root)? {
        if let Some(current) = backend.get_tag(r)? {
            if current == tag {
                continue;
            }
        }
        backend.set_tag(r, tag)?;
        mark_tree(backend, r, tag)?;
    }

    Ok(())
}

pub trait Gc<B> {
    type Err;

    fn new(B) -> Self;

    fn is_exact() -> bool;

    fn register_final(&mut self, snapshot: &SnapshotInfo, final_ref: Id) -> Result<(), Self::Err>;
    fn register_cleanup(&mut self, snapshot: &SnapshotInfo, final_ref: Id)
        -> Result<(), Self::Err>;

    fn deregister<F>(
        &mut self,
        snapshot: &SnapshotInfo,
        final_ref: Id,
        refs: F,
    ) -> Result<(), Self::Err>
    where
        F: FnOnce() -> mpsc::Receiver<Id>;

    fn list_unused_ids(&mut self, refs: mpsc::Sender<Id>) -> Result<(), Self::Err>;

    fn status(&mut self, final_ref: Id) -> Result<Option<Status>, Self::Err>;
}

#[cfg(test)]
#[derive(Clone)]
pub struct MemoryBackend {
    gc_data: HashMap<(Id, Id), GcData>,
    tags: HashMap<Id, tags::Tag>,
    parents: HashMap<Id, Vec<Id>>,
    snapshot_refs: HashMap<Id, Vec<Id>>,
    commit: Option<Box<MemoryBackend>>,
}

#[cfg(test)]
impl MemoryBackend {
    fn new() -> MemoryBackend {
        MemoryBackend {
            gc_data: HashMap::new(),
            tags: HashMap::new(),
            parents: HashMap::new(),
            snapshot_refs: HashMap::new(),
            commit: None,
        }
    }
}

#[cfg(test)]
#[derive(Clone)]
pub struct SafeMemoryBackend {
    backend: Arc<Mutex<MemoryBackend>>,
}

#[cfg(test)]
impl SafeMemoryBackend {
    fn new() -> SafeMemoryBackend {
        SafeMemoryBackend {
            backend: Arc::new(Mutex::new(MemoryBackend::new())),
        }
    }

    fn insert_snapshot(&mut self, info: &SnapshotInfo, refs: Vec<Id>) {
        self.backend
            .lock()
            .unwrap()
            .snapshot_refs
            .insert(info.unique_id, refs);
    }

    fn list_snapshot_refs(&self, info: SnapshotInfo) -> mpsc::Receiver<Id> {
        let (sender, receiver) = mpsc::channel();
        let refs = self.backend
            .lock()
            .unwrap()
            .snapshot_refs
            .get(&info.unique_id)
            .unwrap_or(&vec![])
            .clone();
        refs.iter().map(|id| sender.send(*id)).last();

        receiver
    }

    fn commit(&mut self) {
        let mut backend = self.backend.lock().unwrap();
        backend.commit = Some(Box::new(backend.clone()));
    }

    fn rollback(&mut self) {
        let mut backend = self.backend.lock().unwrap();
        let commit = &mut None;
        mem::swap(commit, &mut backend.commit);
        match *commit {
            None => return,
            Some(ref mut commit) => {
                mem::swap(&mut backend.gc_data, &mut commit.gc_data);
                mem::swap(&mut backend.tags, &mut commit.tags);
                mem::swap(&mut backend.parents, &mut commit.parents);
                mem::swap(&mut backend.snapshot_refs, &mut commit.snapshot_refs);
            }
        }
    }
}

#[cfg(test)]
impl GcBackend for SafeMemoryBackend {
    type Err = String;

    fn get_data(&self, hash_id: Id, family_id: Id) -> Result<GcData, Self::Err> {
        Ok(self.backend
            .lock()
            .unwrap()
            .gc_data
            .get(&(hash_id, family_id))
            .unwrap_or(&GcData {
                num: 0,
                bytes: vec![],
            })
            .clone())
    }

    fn update_data<F: UpdateFn>(
        &mut self,
        hash_id: Id,
        family_id: Id,
        f: F,
    ) -> Result<GcData, Self::Err> {
        let new = match f(self.get_data(hash_id, family_id)?) {
            Some(d) => d,
            None => GcData {
                num: 0,
                bytes: vec![],
            },
        };
        self.backend
            .lock()
            .unwrap()
            .gc_data
            .insert((hash_id, family_id), new.clone());

        Ok(new)
    }

    fn update_all_data_by_family<F: UpdateFn, I: Iterator<Item = F>>(
        &mut self,
        family_id: Id,
        mut fns: I,
    ) -> Result<(), Self::Err> {
        for (k, v) in &mut self.backend.lock().unwrap().gc_data {
            if k.1 == family_id {
                let f = fns.next().unwrap();
                *v = f(v.clone()).unwrap_or(GcData {
                    num: 0,
                    bytes: vec![],
                });
            }
        }

        Ok(())
    }

    fn set_tag(&mut self, hash_id: Id, tag: tags::Tag) -> Result<(), Self::Err> {
        self.backend.lock().unwrap().tags.insert(hash_id, tag);
        Ok(())
    }

    fn get_tag(&self, hash_id: Id) -> Result<Option<tags::Tag>, Self::Err> {
        Ok(self.backend.lock().unwrap().tags.get(&hash_id).cloned())
    }

    fn set_all_tags(&mut self, tag: tags::Tag) -> Result<(), Self::Err> {
        let mut backend = self.backend.lock().unwrap();
        let vals: Vec<Vec<Id>> = backend.snapshot_refs.values().cloned().collect();
        for refs in vals {
            for r in refs {
                backend.tags.insert(r, tag);
            }
        }
        Ok(())
    }

    fn reverse_refs(&self, hash_id: Id) -> Result<Vec<Id>, Self::Err> {
        Ok(self.backend
            .lock()
            .unwrap()
            .parents
            .get(&hash_id)
            .unwrap_or(&vec![])
            .clone())
    }

    fn list_ids_by_tag(&self, tag: tags::Tag) -> Result<mpsc::Receiver<Id>, Self::Err> {
        let mut ids = vec![];
        for (id, id_tag) in &self.backend.lock().unwrap().tags {
            if *id_tag == tag {
                ids.push(*id);
            }
        }

        let (sender, receiver) = mpsc::channel();
        ids.iter().map(|id| sender.send(*id)).last();

        Ok(receiver)
    }

    fn manual_commit(&mut self) -> Result<(), Self::Err> {
        self.commit();
        Ok(())
    }
}

#[cfg(test)]
pub fn gc_test<GC>(snapshots: Vec<Vec<u8>>)
where
    GC: Gc<SafeMemoryBackend>,
    GC::Err: fmt::Debug,
{
    let mut backend = SafeMemoryBackend::new();
    let mut gc = GC::new(backend.clone());

    let mut infos = vec![];
    for (i, refs) in snapshots.iter().enumerate() {
        let info = SnapshotInfo {
            unique_id: i as u64,
            family_id: 1,
            snapshot_id: i as u64,
        };
        backend.insert_snapshot(&info, refs.iter().map(|i| *i as Id).collect());
        infos.push(info);
    }

    for (i, refs) in snapshots.iter().enumerate() {
        backend.set_all_tags(tags::Tag::Done).unwrap();
        refs[..refs.len() - 1]
            .iter()
            .map(|id| backend.set_tag(*id as Id, tags::Tag::Reserved))
            .last();

        let last_ref = *refs.iter().last().expect("len() >= 0") as u64;
        gc.register_final(&infos[i], last_ref).unwrap();
        gc.register_cleanup(&infos[i], last_ref).unwrap();
    }

    for (i, refs) in snapshots.iter().enumerate() {
        // Check that snapshot is still valid.
        let (sender, receiver) = mpsc::channel();
        gc.list_unused_ids(sender).unwrap();
        receiver
            .iter()
            .filter(|i: &u64| refs.contains(&(*i as u8)))
            .map(|i| panic!("ID prematurely deleted by GC: {}", i))
            .last();
        // Deregister snapshot.
        let last = backend
            .list_snapshot_refs(infos[i].clone())
            .iter()
            .last()
            .unwrap();
        let refs = backend.list_snapshot_refs(infos[i].clone());
        gc.deregister(&infos[i], last, || refs).unwrap();
    }

    let mut all_refs: Vec<u8> = snapshots
        .iter()
        .flat_map(|v| v.clone().into_iter())
        .collect();
    all_refs.sort();
    all_refs.dedup();

    if GC::is_exact() {
        // Check that all IDs were eventually marked unused.
        let (sender, receiver) = mpsc::channel();
        gc.list_unused_ids(sender).unwrap();
        let unused: Vec<Id> = receiver.iter().collect();
        if unused.len() != all_refs.len() {
            panic!(
                "Did not mark all IDs as unused. Wanted {:?}, got {:?}.",
                all_refs, unused
            );
        }
    }
}

#[cfg(test)]
pub fn resume_register_test<GC>()
where
    GC: Gc<SafeMemoryBackend>,
    GC::Err: fmt::Debug,
{
    let mut backend = SafeMemoryBackend::new();
    let mut gc = GC::new(backend.clone());

    let refs = vec![1, 2, 3, 4, 5];
    let info = SnapshotInfo {
        unique_id: 1,
        family_id: 1,
        snapshot_id: 1,
    };
    backend.insert_snapshot(&info, refs.iter().map(|i| *i as Id).collect());

    backend.set_all_tags(tags::Tag::Done).unwrap();
    refs[..refs.len() - 1]
        .iter()
        .map(|id| backend.set_tag(*id as Id, tags::Tag::Reserved))
        .last();

    let final_ref = *refs.last().expect("nonempty");
    gc.register_final(&info, final_ref).unwrap();
    assert_eq!(gc.status(final_ref).ok(), Some(Some(Status::InProgress)));
    gc.register_cleanup(&info, final_ref).unwrap();
    assert_eq!(gc.status(final_ref).ok(), Some(None));

    let receiver = |n| {
        let (sender, receiver) = mpsc::channel();
        refs[..n].iter().map(|id| sender.send(*id as Id)).last();
        drop(sender);

        receiver
    };
    let last = receiver(refs.len()).iter().last().unwrap();
    let receive = receiver(refs.len());
    gc.deregister(&info, last, move || receive).unwrap();

    let (sender, receiver) = mpsc::channel();
    gc.list_unused_ids(sender).unwrap();

    let mut unused: Vec<_> = receiver.iter().collect();
    unused.sort();

    if GC::is_exact() {
        assert_eq!(unused, vec![1, 2, 3, 4, 5]);
    }
}

#[cfg(test)]
pub fn resume_deregister_test<GC>()
where
    GC: Gc<SafeMemoryBackend>,
    GC::Err: fmt::Debug,
{
    let mut backend = SafeMemoryBackend::new();
    let mut gc = GC::new(backend.clone());

    let refs = vec![1, 2, 3, 4, 5];
    let info = SnapshotInfo {
        unique_id: 1,
        family_id: 1,
        snapshot_id: 1,
    };
    backend.insert_snapshot(&info, refs.iter().map(|i| *i as Id).collect());

    backend.set_all_tags(tags::Tag::Done).unwrap();
    refs[..refs.len() - 1]
        .iter()
        .map(|id| backend.set_tag(*id as Id, tags::Tag::Reserved))
        .last();

    let final_ref = *refs.last().expect("nonempty");
    gc.register_final(&info, final_ref).unwrap();
    gc.register_cleanup(&info, final_ref).unwrap();

    let receiver = |n| {
        let (sender, receiver) = mpsc::channel();
        refs[..n].iter().map(|id| sender.send(*id as Id)).last();
        drop(sender);

        receiver
    };

    backend.manual_commit().unwrap();
    for _ in 1..10 {
        backend.rollback();

        let receive = receiver(refs.len());

        gc.deregister(&info, final_ref, move || receive).unwrap();
        assert_eq!(gc.status(final_ref).ok(), Some(Some(Status::Complete)));
    }

    gc.register_cleanup(&info, final_ref).unwrap();
    assert_eq!(gc.status(final_ref).ok(), Some(None));

    let (sender, receiver) = mpsc::channel();
    gc.list_unused_ids(sender).unwrap();

    let mut unused: Vec<_> = receiver.iter().collect();
    unused.sort();

    if GC::is_exact() {
        assert_eq!(unused, vec![1, 2, 3, 4, 5]);
    }
}
