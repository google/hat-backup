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

use std::fs;
use std::path::PathBuf;
use std::str;
use std::sync::{Arc, mpsc};
use std::thread;
use capnp;
use void::Void;

use backend::StoreBackend;
use blob;
use errors::HatError;
use gc::{self, Gc, GcRc};
use hash;
use key;
use root_capnp;
use snapshot;
use tags;
use util::Process;

mod family;
mod insert_path_handler;
use self::family::Family;

#[cfg(test)]
mod tests;
#[cfg(all(test, feature = "benchmarks"))]
mod benchmarks;



pub struct GcBackend {
    hash_index: Arc<hash::HashIndex>,
}

impl gc::GcBackend for GcBackend {
    type Err = Void;

    fn get_data(&self, hash_id: gc::Id, family_id: gc::Id) -> Result<hash::GcData, Self::Err> {
        Ok(self.hash_index.read_gc_data(hash_id, family_id))
    }
    fn update_data<F: hash::UpdateFn>(&mut self,
                                      hash_id: gc::Id,
                                      family_id: gc::Id,
                                      f: F)
                                      -> Result<hash::GcData, Self::Err> {
        Ok(self.hash_index.update_gc_data(hash_id, family_id, f))
    }
    fn update_all_data_by_family<F: hash::UpdateFn, I: Iterator<Item = F>>
        (&mut self,
         family_id: gc::Id,
         fns: I)
         -> Result<(), Self::Err> {
        self.hash_index.update_family_gc_data(family_id, fns);
        Ok(())
    }

    fn get_tag(&self, hash_id: gc::Id) -> Result<Option<tags::Tag>, Self::Err> {
        Ok(self.hash_index.get_tag(hash_id))
    }

    fn set_tag(&mut self, hash_id: gc::Id, tag: tags::Tag) -> Result<(), Self::Err> {
        self.hash_index.set_tag(hash_id, tag);
        Ok(())
    }

    fn set_all_tags(&mut self, tag: tags::Tag) -> Result<(), Self::Err> {
        self.hash_index.set_all_tags(tag);
        Ok(())
    }

    fn reverse_refs(&self, hash_id: gc::Id) -> Result<Vec<gc::Id>, Self::Err> {
        let entry = match self.hash_index.get_hash(hash_id) {
            Some(entry) => entry,
            None => panic!("HashNotKnown in hash index."),
        };
        if entry.childs.is_none() {
            return Ok(Vec::new());
        }
        Ok(entry.childs.unwrap())
    }

    fn list_ids_by_tag(&self, tag: tags::Tag) -> Result<mpsc::Receiver<i64>, Self::Err> {
        let (sender, receiver) = mpsc::channel();
        self.hash_index.get_ids_by_tag(tag as i64).iter().map(|i| sender.send(*i)).last();

        Ok(receiver)
    }

    fn manual_commit(&mut self) -> Result<(), Self::Err> {
        self.hash_index.manual_commit();
        Ok(())
    }
}


pub struct Hat<B: StoreBackend, G: gc::Gc<GcBackend>> {
    repository_root: Option<PathBuf>,
    snapshot_index: snapshot::SnapshotIndex,
    hash_index: Arc<hash::HashIndex>,
    backend: Arc<B>,
    blob_index: Arc<blob::BlobIndex>,
    blob_store: Arc<blob::BlobStore<B>>,
    blob_max_size: usize,
    gc: G,
}

pub type HatRc<B> = Hat<B, GcRc<GcBackend>>;

fn concat_filename(mut a: PathBuf, b: &str) -> String {
    a.push(b);
    a.into_os_string().into_string().unwrap()
}

fn snapshot_index_name(root: PathBuf) -> String {
    concat_filename(root, "snapshot_index.sqlite3")
}

fn blob_index_name(root: PathBuf) -> String {
    concat_filename(root, "blob_index.sqlite3")
}

fn hash_index_name(root: PathBuf) -> String {
    concat_filename(root, "hash_index.sqlite3")
}

struct SnapshotLister<'a, B: StoreBackend> {
    backend: &'a key::HashStoreBackend<B>,
    family: &'a Family<B>,
    // Invariant: Only save the chunkref if it is a directory
    queue: Vec<(hash::Hash, Option<blob::ChunkRef>)>,
}

impl<'a, B: StoreBackend> SnapshotLister<'a, B> {
    fn fetch(&mut self, hash: &hash::Hash, chunk: blob::ChunkRef) -> Result<(), HatError> {
        let res = try!(self.family.fetch_dir_data(hash, chunk, self.backend.clone()));
        for (entry, hash, pref) in res.into_iter().rev() {
            if entry.data_hash.is_some() {
                self.queue.push((hash, None));
            } else {
                self.queue.push((hash, Some(pref)));
            }
        }
        Ok(())
    }
}

impl<'a, B: StoreBackend> Iterator for SnapshotLister<'a, B> {
    type Item = Result<hash::Hash, HatError>;
    fn next(&mut self) -> Option<Result<hash::Hash, HatError>> {
        let (hash, chunk) = match self.queue.pop() {
            None => return None,
            Some((hash, chunk)) => (hash, chunk),
        };

        if let Some(chunk) = chunk {
            if let Err(e) = self.fetch(&hash, chunk) {
                // We yield the error now, but save the hash so
                // we can output it next time
                self.queue.push((hash, None));
                return Some(Err(e));
            }
        }

        Some(Ok(hash))
    }
}

fn list_snapshot<'a, B: StoreBackend>(backend: &'a key::HashStoreBackend<B>,
                                      family: &'a Family<B>,
                                      dir_hash: hash::Hash,
                                      dir_ref: blob::ChunkRef)
                                      -> SnapshotLister<'a, B> {
    SnapshotLister {
        backend: backend,
        family: family,
        queue: vec![(dir_hash, Some(dir_ref))],
    }
}

impl<B: StoreBackend> HatRc<B> {
    pub fn open_repository(repository_root: PathBuf,
                           backend: Arc<B>,
                           max_blob_size: usize)
                           -> Result<HatRc<B>, HatError> {
        let snapshot_index_path = snapshot_index_name(repository_root.clone());
        let blob_index_path = blob_index_name(repository_root.clone());
        let hash_index_path = hash_index_name(repository_root.clone());
        let si_p = try!(snapshot::SnapshotIndex::new(&snapshot_index_path));
        let bi_p = Arc::new(try!(blob::BlobIndex::new(&blob_index_path)));
        let hi_p = Arc::new(try!(hash::HashIndex::new(&hash_index_path)));

        let bs_p = Arc::new(blob::BlobStore::new(bi_p.clone(), backend.clone(), max_blob_size));

        let gc_backend = GcBackend { hash_index: hi_p.clone() };
        let gc = gc::Gc::new(gc_backend);

        let mut hat = Hat {
            repository_root: Some(repository_root),
            snapshot_index: si_p,
            hash_index: hi_p.clone(),
            backend: backend,
            blob_index: bi_p,
            blob_store: bs_p,
            blob_max_size: max_blob_size,
            gc: gc,
        };

        // Resume any unfinished commands.
        try!(hat.resume());

        Ok(hat)
    }

    #[cfg(test)]
    pub fn new_for_testing(backend: Arc<B>, max_blob_size: usize) -> Result<HatRc<B>, HatError> {
        let si_p = snapshot::SnapshotIndex::new_for_testing().unwrap();
        let bi_p = Arc::new(blob::BlobIndex::new_for_testing().unwrap());
        let hi_p = Arc::new(hash::HashIndex::new_for_testing().unwrap());

        let bs_p = Arc::new(blob::BlobStore::new(bi_p, backend, max_blob_size));

        let gc_backend = GcBackend { hash_index: hi_p.clone() };
        let gc = gc::Gc::new(gc_backend);

        let mut hat = Hat {
            repository_root: None,
            snapshot_index: si_p,
            hash_index: hi_p.clone(),
            blob_store: bs_p.clone(),
            gc: gc,
        };

        // Resume any unfinished commands.
        try!(hat.resume());

        Ok(hat)
    }

    pub fn hash_tree_writer(&self) -> hash::tree::SimpleHashTreeWriter<key::HashStoreBackend<B>> {
        hash::tree::SimpleHashTreeWriter::new(8, self.hash_backend())
    }

    pub fn open_family(&self, name: String) -> Result<Family<B>, HatError> {
        // We setup a standard pipeline of processes:
        // key::Store -> key::Index
        //            -> hash::Index
        //            -> blob::Store -> blob::Index

        let key_index_path = match self.repository_root {
            Some(ref root) => concat_filename(root.clone(), &name),
            None => ":memory:".to_string(),
        };

        let ki_p = Arc::new(try!(key::KeyIndex::new(&key_index_path)));

        let ks = key::Store::new(ki_p.clone(),
                                 self.hash_index.clone(),
                                 self.blob_store.clone());

        let mut kss = vec![];
        for _ in 0..5 {
            // To allow parallel processing, each key store gets its own dedicated blob store.
            let bs = Arc::new(blob::BlobStore::new(self.blob_index.clone(),
                                                   self.backend.clone(),
                                                   self.blob_max_size));
            kss.push(Process::new(key::Store::new(ki_p.clone(), self.hash_index.clone(), bs)));
        }
        Ok(Family {
            name: name,
            key_store: ks,
            key_store_process: kss,
        })
    }

    pub fn meta_commit(&mut self) -> Result<(), HatError> {
        let all_snapshots = self.snapshot_index.list_all();

        // TODO(jos): use a hash tree for this listing.
        let mut message = capnp::message::Builder::new_default();

        {
            let root = message.init_root::<root_capnp::snapshot_list::Builder>();
            let mut snapshots = root.init_snapshots(all_snapshots.len() as u32);

            for (i, snapshot) in all_snapshots.into_iter().enumerate() {
                let mut s = snapshots.borrow().get(i as u32);
                s.set_id(snapshot.info.snapshot_id);
                s.set_family_name(&snapshot.family_name);
                s.set_msg(&snapshot.msg.unwrap_or("".to_owned()));
                s.set_hash(&snapshot.hash.unwrap().bytes);
                s.set_tree_reference(&snapshot.tree_ref.unwrap());
            }
        }
        let mut listing = Vec::new();
        capnp::serialize_packed::write_message(&mut listing, &message).unwrap();

        // TODO(jos): make sure this operation is atomic or resumable.
        try!(self.blob_store.store_named("root", listing.as_slice()));
        Ok(())
    }

    pub fn recover(&mut self) -> Result<(), HatError> {
        let root = match try!(self.blob_store.retrieve_named("root")) {
            Some(r) => r,
            _ => return Err(From::from("Could not read root file")),
        };
        let message_reader =
            capnp::serialize_packed::read_message(&mut &root[..],
                                                  capnp::message::ReaderOptions::new())
                .unwrap();
        let snapshot_list = message_reader.get_root::<root_capnp::snapshot_list::Reader>().unwrap();

        for s in snapshot_list.get_snapshots().unwrap().iter() {
            let tree_ref = blob::ChunkRef::from_bytes(&mut s.get_tree_reference().unwrap())
                .unwrap();
            self.snapshot_index
                .recover(s.get_id(),
                         s.get_family_name()
                             .unwrap(),
                         s.get_msg().unwrap(),
                         s.get_hash().unwrap(),
                         &tree_ref,
                         Some(snapshot::WorkStatus::RecoverInProgress));
        }
        self.flush_snapshot_index();
        try!(self.resume());

        Ok(())
    }

    fn recover_snapshot(&mut self,
                        family_name: String,
                        info: snapshot::Info,
                        hash: &hash::Hash,
                        dir_ref: blob::ChunkRef)
                        -> Result<(), HatError> {
        fn recover_entry<B: StoreBackend>(hashes: &hash::HashIndex,
                                          blobs: &blob::BlobStore<B>,
                                          childs_opt: &Option<Vec<hash::Hash>>,
                                          mut entry: hash::Entry)
                                          -> Result<i64, HatError> {
            let pref = entry.persistent_ref.clone().unwrap();

            // Make sure we have the blob described.
            blobs.recover(pref.clone());

            entry.childs = match childs_opt {
                &None => None,
                &Some(ref hs) => {
                    let mut ids = vec![];
                    for h in hs {
                        ids.push(hashes.get_id(h).expect("Child hash not yet recovered"));
                    }
                    Some(ids)
                }
            };

            // Now insert the hash information if needed.
            let id = match hashes.reserve(&entry) {
                hash::ReserveResult::HashKnown(id) => id,
                hash::ReserveResult::ReserveOk(id) => {
                    // Commit hash.
                    hashes.commit(&entry.hash, pref);
                    id
                }
            };

            Ok(id)
        }

        let family = self.open_family(family_name.clone())
            .expect(&format!("Could not open family '{}'", family_name));
        let mut registered = Vec::new();
        let mut recovered = Vec::new();
        let (final_childs, final_level) = try!(self.recover_dir_ref(&family,
                                                                    hash,
                                                                    dir_ref.clone(),
                                                                    &mut registered,
                                                                    &mut recovered));
        // Recover hashes for tree child chunks.
        for (childs_opt, entry) in recovered {
            try!(recover_entry(&self.hash_index, &self.blob_store, &childs_opt, entry));
        }

        // Recover hashes for tree-tops. These are also registered with the GC.
        let (id_sender, id_receiver) = mpsc::channel();
        let local_hash_index = self.hash_index.clone();
        let local_blob_store = self.blob_store.clone();
        thread::spawn(move || {
            for (childs_opt, entry) in registered {
                let id = recover_entry(&local_hash_index, &local_blob_store, &childs_opt, entry)
                    .unwrap();
                id_sender.send(id).unwrap();
            }
        });

        try!(self.gc.register(&info, id_receiver));
        self.flush_blob_store();

        // Recover final root hash for the snapshot.
        try!(recover_entry(&self.hash_index,
                           &self.blob_store,
                           &final_childs,
                           hash::Entry {
                               hash: hash.clone(),
                               persistent_ref: Some(dir_ref),
                               level: final_level,
                               childs: None,
                           }));

        let final_id = self.hash_index.get_id(hash).unwrap();
        try!(self.gc.register_final(&info, final_id));

        try!(self.commit_finalize(&family, info, hash));

        Ok(())
    }

    fn recover_dir_ref(&mut self,
                       family: &Family<B>,
                       dir_hash: &hash::Hash,
                       dir_ref: blob::ChunkRef,
                       register_out: &mut Vec<(Option<Vec<hash::Hash>>, hash::Entry)>,
                       recover_out: &mut Vec<(Option<Vec<hash::Hash>>, hash::Entry)>)
                       -> Result<(Option<Vec<hash::Hash>>, i64), HatError> {
        fn recover_tree<B: hash::tree::HashTreeBackend<Err = key::MsgError>>
            (backend: B,
             hash: &hash::Hash,
             pref: blob::ChunkRef,
             out: &mut Vec<(Option<Vec<hash::Hash>>, hash::Entry)>)
             -> Result<(Option<Vec<hash::Hash>>, i64), HatError> {
            match try!(hash::tree::SimpleHashTreeReader::open(backend, &hash, Some(pref)))
                .unwrap() {
                hash::tree::ReaderResult::Empty |
                hash::tree::ReaderResult::SingleBlock(..) => Ok((None, 0)),
                hash::tree::ReaderResult::Tree(mut reader) => {
                    let (childs, level) = try!(reader.list_entries(out));
                    Ok((Some(childs), level))
                }
            }
        }
        for (file, hash, pref) in
            try!(family.fetch_dir_data(dir_hash, dir_ref.clone(), self.hash_backend())) {
            let (childs, level) = match file.data_hash {
                Some(..) => {
                    // Entry is a data leaf. Read the hash tree.
                    try!(recover_tree(self.hash_backend(), &hash, pref.clone(), recover_out))
                }
                None => {
                    try!(self.recover_dir_ref(&family,
                                              &hash,
                                              pref.clone(),
                                              register_out,
                                              recover_out))
                }
            };
            // We register the top hash with the GC (tree nodes are inferred).
            let r = (childs,
                     hash::Entry {
                hash: hash,
                persistent_ref: Some(pref),
                level: level,
                childs: None,
            });
            register_out.push(r);
        }

        recover_tree(self.hash_backend(), dir_hash, dir_ref, recover_out)
    }

    pub fn resume(&mut self) -> Result<(), HatError> {
        let need_work = self.snapshot_index.list_not_done();

        for snapshot in need_work.into_iter() {
            match snapshot.status {
                snapshot::WorkStatus::CommitInProgress |
                snapshot::WorkStatus::RecoverInProgress => {
                    let done_hash_opt = match &snapshot.hash {
                        &None => None,
                        &Some(ref h) => {
                            let status_res = self.hash_index
                                .get_id(h)
                                .map(|id| self.gc.status(id));
                            let status_opt = match status_res {
                                None => None,
                                Some(res) => try!(res),
                            };
                            match status_opt {
                                None => None,  // We did not fully commit.
                                Some(gc::Status::InProgress) |
                                Some(gc::Status::Complete) => Some(h),
                            }
                        }
                    };
                    match (done_hash_opt, snapshot.status) {
                        (Some(hash), snapshot::WorkStatus::CommitInProgress) => {
                            try!(self.commit_finalize_by_name(snapshot.family_name,
                                                              snapshot.info,
                                                              hash))
                        }
                        (None, snapshot::WorkStatus::CommitInProgress) => {
                            println!("Resuming commit of: {}", snapshot.family_name);
                            try!(self.commit_by_name(snapshot.family_name, Some(snapshot.info)))
                        }
                        (None, snapshot::WorkStatus::RecoverInProgress) => {
                            println!("Resuming recovery of: {}", snapshot.family_name);
                            let hash = try!(snapshot.hash
                                .as_ref()
                                .ok_or("Recovered snapshot without hash"));
                            let tree_ref = try!(snapshot.tree_ref
                                .ok_or("Recovered hash tree has no root hash"));
                            let tree = try!(blob::ChunkRef::from_bytes(&mut &tree_ref[..]));
                            try!(self.recover_snapshot(snapshot.family_name,
                                                       snapshot.info,
                                                       hash,
                                                       tree))
                        }
                        (hash, status) => {
                            return Err(From::from(format!("unexpected state: ({:?}, {:?})",
                                                          hash,
                                                          status)))
                        }
                    }
                }
                snapshot::WorkStatus::CommitComplete => {
                    match &snapshot.hash {
                        &Some(ref h) => {
                            try!(self.commit_finalize_by_name(snapshot.family_name,
                                                              snapshot.info,
                                                              h))
                        }
                        &None => {
                            // This should not happen.
                            return Err(From::from(format!("Snapshot {:?} is fully registered \
                                                           in GC, but has no hash",
                                                          snapshot)));
                        }
                    }
                }
                snapshot::WorkStatus::DeleteInProgress => {
                    let hash = snapshot.hash.expect("Snapshot has no hash");
                    let hash_id = self.hash_index
                        .get_id(&hash)
                        .expect("Snapshot hash not recognized");
                    let status = try!(self.gc.status(hash_id));
                    match status {
                        None |
                        Some(gc::Status::InProgress) => {
                            println!("Resuming delete of: {} #{:?}",
                                     snapshot.family_name,
                                     snapshot.info.snapshot_id);
                            try!(self.deregister_by_name(snapshot.family_name,
                                                         snapshot.info.snapshot_id))
                        }
                        Some(gc::Status::Complete) => {
                            try!(self.deregister_finalize_by_name(snapshot.family_name,
                                                                  snapshot.info,
                                                                  hash_id))
                        }
                    }
                }
                snapshot::WorkStatus::DeleteComplete => {
                    let hash = snapshot.hash.expect("Snapshot has no hash");
                    let hash_id = self.hash_index
                        .get_id(&hash)
                        .expect("Snapshot hash not recognized");
                    try!(self.deregister_finalize_by_name(snapshot.family_name,
                                                          snapshot.info,
                                                          hash_id))
                }
            }
        }

        // We should have finished everything there was to finish.
        let need_work = self.snapshot_index.list_not_done();
        assert_eq!(need_work.len(), 0);

        Ok(())
    }

    pub fn commit_by_name(&mut self,
                          family_name: String,
                          resume_info: Option<snapshot::Info>)
                          -> Result<(), HatError> {
        let family = try!(self.open_family(family_name));
        try!(self.commit(&family, resume_info));

        Ok(())
    }

    pub fn commit(&mut self,
                  family: &Family<B>,
                  resume_info: Option<snapshot::Info>)
                  -> Result<(), HatError> {
        //  Tag 1:
        //  Reserve the snapshot and commit the reservation.
        //  Register all but the last hashes.
        //  (the last hash is a special-case, as the GC use it to save meta-data for resuming)
        let snap_info = match resume_info {
            Some(info) => info,  // Resume already started commit.
            None => {
                // Create new commit.
                self.snapshot_index.reserve(family.name.clone())
            }
        };
        self.flush_snapshot_index();

        // Prepare.
        let (hash_sender, hash_receiver) = mpsc::channel();
        let (hash_id_sender, hash_id_receiver) = mpsc::channel();

        let local_hash_index = self.hash_index.clone();
        thread::spawn(move || {
            for hash in hash_receiver.iter() {
                hash_id_sender.send(local_hash_index.get_id(&hash)
                        .expect("Hash not found"))
                    .expect("Channel failed");
            }
        });

        // Commit metadata while registering needed data-hashes (files and dirs).
        let (hash, top_ref) = {
            let mut local_family: Family<B> = (*family).clone();
            let (s, r) = mpsc::channel();

            thread::spawn(move || s.send(local_family.commit(&hash_sender)));
            try!(self.gc.register(&snap_info, hash_id_receiver));

            try!(try!(r.recv()))
        };

        // Push any remaining data to external storage.
        // This also flushes our hashes from the memory index, so we can tag them.
        self.flush_blob_store();

        // Tag 2:
        // We update the snapshot entry with the tree hash, which we then register.
        // When the GC has seen the final hash, we flush everything so far.
        self.snapshot_index.update(&snap_info, &hash, &top_ref);
        self.flush_snapshot_index();

        // Register the final hash.
        // At this point, the GC should still be able to either resume or rollback safely.
        // After a successful flush, all GC work is done.
        // The GC must be able to tell if it has completed or not.
        let hash_id = self.hash_index.get_id(&hash).expect("Hash does not exist");
        try!(self.gc.register_final(&snap_info, hash_id));
        try!(family.flush());
        try!(self.commit_finalize(family, snap_info, &hash));

        Ok(())
    }

    fn commit_finalize_by_name(&mut self,
                               family_name: String,
                               snap_info: snapshot::Info,
                               hash: &hash::Hash)
                               -> Result<(), HatError> {
        let family = try!(self.open_family(family_name));
        try!(self.commit_finalize(&family, snap_info, &hash));

        Ok(())
    }

    fn commit_finalize(&mut self,
                       family: &Family<B>,
                       snap_info: snapshot::Info,
                       hash: &hash::Hash)
                       -> Result<(), HatError> {
        // Commit locally. Let the GC perform any needed cleanup.
        self.snapshot_index.ready_commit(&snap_info);
        self.flush_snapshot_index();

        let hash_id = self.hash_index.get_id(hash).expect("Hash does not exist");
        try!(self.gc.register_cleanup(&snap_info, hash_id));
        try!(family.flush());

        // Tag 0: All is done.
        self.snapshot_index.commit(&snap_info);
        self.flush_snapshot_index();

        Ok(())
    }

    pub fn flush_snapshot_index(&mut self) {
        self.snapshot_index.flush();
    }

    pub fn flush_blob_store(&self) {
        self.blob_store.flush();
    }

    pub fn checkout_in_dir(&mut self,
                           family_name: String,
                           output_dir: PathBuf)
                           -> Result<(), HatError> {
        // Extract latest snapshot info:
        let (_info, dir_hash, dir_ref) = match self.snapshot_index.latest(&family_name) {
            Some((i, h, Some(r))) => (i, h, r),
            _ => {
                panic!("Tried to checkout family '{}' before first completed commit",
                       family_name)
            }
        };

        let family = self.open_family(family_name.clone())
            .expect(&format!("Could not open family '{}'", family_name));

        let mut output_dir = output_dir;
        self.checkout_dir_ref(&family, &mut output_dir, &dir_hash, dir_ref)
    }

    fn checkout_dir_ref(&self,
                        family: &Family<B>,
                        output: &mut PathBuf,
                        dir_hash: &hash::Hash,
                        dir_ref: blob::ChunkRef)
                        -> Result<(), HatError> {
        fs::create_dir_all(&output).unwrap();
        for (entry, hash, pref) in
            try!(family.fetch_dir_data(dir_hash, dir_ref, self.hash_backend())) {
            assert!(entry.name.len() > 0);

            output.push(str::from_utf8(&entry.name[..]).unwrap());
            println!("{}", output.display());

            if entry.data_hash.is_some() {
                let mut fd = fs::File::create(&output).unwrap();
                let tree_opt = try!(hash::tree::SimpleHashTreeReader::open(self.hash_backend(),
                                                                           &hash,
                                                                           Some(pref)));
                if let Some(tree) = tree_opt {
                    family.write_file_chunks(&mut fd, tree);
                }
            } else {
                try!(self.checkout_dir_ref(family, output, &hash, pref));
            }
            output.pop();
        }
        Ok(())
    }

    pub fn deregister_by_name(&mut self,
                              family_name: String,
                              snapshot_id: i64)
                              -> Result<(), HatError> {
        let family = try!(self.open_family(family_name));
        try!(self.deregister(&family, snapshot_id));

        Ok(())
    }

    pub fn deregister(&mut self, family: &Family<B>, snapshot_id: i64) -> Result<(), HatError> {
        let (info, dir_hash, dir_ref) = match self.snapshot_index
            .lookup(&family.name, snapshot_id) {
            Some((i, h, Some(r))) => (i, h, r),
            _ => {
                return Err(From::from(format!("No complete snapshot found for family {} with \
                                               id {:?}",
                                              family.name,
                                              snapshot_id)));
            }
        };

        // Make the snapshot to enable resuming.
        self.snapshot_index.will_delete(&info);
        self.flush_snapshot_index();

        let final_ref = self.hash_index
            .get_id(&dir_hash)
            .expect("Snapshot hash does not exist");

        {
            let hash_backend = self.hash_backend();
            let &mut Hat { ref hash_index, ref mut gc, .. } = self;

            let listing = || {
                let (id_sender, id_receiver) = mpsc::channel();
                for hash in list_snapshot(&hash_backend, &family, dir_hash, dir_ref) {
                    let hash = hash.unwrap();
                    match hash_index.get_id(&hash) {
                        Some(id) => id_sender.send(id).unwrap(),
                        None => panic!("Unexpected reply from hash index."),
                    }
                }
                id_receiver
            };

            try!(gc.deregister(&info, final_ref, listing));
        }
        try!(family.flush());

        self.deregister_finalize(family, info, final_ref)
    }

    fn deregister_finalize_by_name(&mut self,
                                   family_name: String,
                                   snap_info: snapshot::Info,
                                   hash_id: gc::Id)
                                   -> Result<(), HatError> {
        let family = try!(self.open_family(family_name));
        try!(self.deregister_finalize(&family, snap_info, hash_id));

        Ok(())
    }

    fn deregister_finalize(&mut self,
                           family: &Family<B>,
                           snap_info: snapshot::Info,
                           final_ref: gc::Id)
                           -> Result<(), HatError> {
        // Mark the snapshot to enable resuming.
        self.snapshot_index.ready_delete(&snap_info);
        self.flush_snapshot_index();

        // Clear GC state.
        try!(self.gc.register_cleanup(&snap_info, final_ref));
        try!(family.flush());

        // Delete snapshot metadata.
        self.snapshot_index.delete(snap_info);
        self.flush_snapshot_index();

        self.snapshot_index.flush();

        Ok(self.flush_snapshot_index())
    }

    pub fn gc(&mut self) -> Result<(i64, i64), HatError> {
        // Remove unused hashes.
        let mut deleted_hashes = 0;
        let (sender, receiver) = mpsc::channel();
        try!(self.gc.list_unused_ids(sender));
        for id in receiver.iter() {
            deleted_hashes += 1;
            self.hash_index.delete(id);
        }
        self.hash_index.flush();
        // Mark used blobs.
        let entries = self.hash_index.list();
        self.blob_store.tag_all(tags::Tag::InProgress);

        let mut live_blobs = 0;
        for entry in entries.into_iter() {
            if let Some(pref) = entry.persistent_ref {
                live_blobs += 1;
                self.blob_store.tag(pref, tags::Tag::Reserved);
            }
        }
        // Anything still marked "in progress" is not referenced by any hash.
        try!(self.blob_store.delete_by_tag(tags::Tag::InProgress));
        self.blob_store.tag_all(tags::Tag::Done);
        self.blob_store.flush();

        Ok((deleted_hashes, live_blobs))
    }

    fn hash_backend(&self) -> key::HashStoreBackend<B> {
        key::HashStoreBackend::new(self.hash_index.clone(), self.blob_store.clone())
    }
}
