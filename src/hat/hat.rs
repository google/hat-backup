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

//! High level Hat API


use std::cmp;
use std::borrow::Cow;
use std::error::Error;
use std::fs;
use std::io;
use std::io::{Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::str;
use std::sync;
use std::sync::atomic;
use std::sync::mpsc;
use std::thread;

use capnp;
use root_capnp;
use time;

use blob;
use gc;
use gc::Gc;
use gc_rc;
use hash;
use key;
use listdir;
use process::Process;
use snapshot;
use tags;


error_type! {
    #[derive(Debug)]
    pub enum HatError {
        Recv(mpsc::RecvError) {
            cause;
        },
        Keys(key::MsgError) {
            cause;
        },
        KeyDb(key::IndexError) {
            cause;
        },
        Snapshots(snapshot::IndexError) {
            cause;
        },
        Blobs(blob::MsgError) {
            cause;
        },
        BlobDb(blob::IndexError) {
            cause;
        },
        Hashes(hash::MsgError) {
            cause;
        },
        DataSerialization(capnp::Error) {
            cause;
        },
        IO(io::Error) {
            cause;
        },
        Message(Cow<'static, str>) {
            desc (e) &**e;
            from (s: &'static str) s.into();
            from (s: String) s.into();
        }
    }
}


pub struct GcBackend {
    hash_index: hash::HashIndex,
}

impl gc::GcBackend for GcBackend {
    type Err = HatError;

    fn get_data(&self, hash_id: gc::Id, family_id: gc::Id) -> Result<hash::GcData, Self::Err> {
        Ok(try!(self.hash_index.read_gc_data(hash_id, family_id)))
    }
    fn update_data<F: hash::UpdateFn>(&mut self,
                                      hash_id: gc::Id,
                                      family_id: gc::Id,
                                      f: F)
                                      -> Result<hash::GcData, Self::Err> {
        Ok(try!(self.hash_index.update_gc_data(hash_id, family_id, f)))
    }
    fn update_all_data_by_family<F: hash::UpdateFn, I: Iterator<Item = F>>
        (&mut self,
         family_id: gc::Id,
         fns: I)
         -> Result<(), Self::Err> {
        try!(self.hash_index.update_family_gc_data(family_id, fns));
        Ok(())
    }

    fn get_tag(&self, hash_id: gc::Id) -> Result<Option<tags::Tag>, Self::Err> {
        Ok(try!(self.hash_index.get_tag(hash_id)))
    }

    fn set_tag(&mut self, hash_id: gc::Id, tag: tags::Tag) -> Result<(), Self::Err> {
        try!(self.hash_index.set_tag(hash_id, tag));
        Ok(())
    }

    fn set_all_tags(&mut self, tag: tags::Tag) -> Result<(), Self::Err> {
        try!(self.hash_index.set_all_tags(tag));
        Ok(())
    }

    fn reverse_refs(&self, hash_id: gc::Id) -> Result<Vec<gc::Id>, Self::Err> {
        let entry = match try!(self.hash_index.get_hash(hash_id)) {
            Some(entry) => entry,
            None => panic!("HashNotKnown in hash index."),
        };
        if entry.payload.is_none() {
            return Ok(Vec::new());
        }

        let mut out = vec![];
        for hash_bytes in hash::tree::decode_metadata_refs(&entry.payload.unwrap()) {
            match try!(self.hash_index.get_id(&hash::Hash { bytes: hash_bytes })) {
                Some(id) => out.push(id),
                None => panic!("HashNotKnown in hash index."),
            }
        }
        Ok(out)
    }

    fn list_ids_by_tag(&self, tag: tags::Tag) -> Result<mpsc::Receiver<i64>, Self::Err> {
        let (sender, receiver) = mpsc::channel();
        try!(self.hash_index.get_ids_by_tag(tag as i64)).iter().map(|i| sender.send(*i)).last();

        Ok(receiver)
    }

    fn manual_commit(&mut self) -> Result<(), Self::Err> {
        try!(self.hash_index.manual_commit());
        Ok(())
    }
}


pub struct Hat<B, G: gc::Gc> {
    repository_root: Option<PathBuf>,
    snapshot_index: snapshot::SnapshotIndex,
    blob_store: blob::StoreProcess,
    hash_index: hash::HashIndex,
    blob_backend: B,
    hash_backend: key::HashStoreBackend,
    gc: G,
    max_blob_size: usize,
}

pub type HatRc<B> = Hat<B, gc_rc::GcRc<GcBackend>>;

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

fn list_snapshot(backend: &key::HashStoreBackend,
                 out: &mpsc::Sender<hash::Hash>,
                 family: &Family,
                 dir_hash: &hash::Hash,
                 dir_ref: blob::ChunkRef)
                 -> Result<(), HatError> {
    for (entry, hash, pref) in try!(family.fetch_dir_data(dir_hash, dir_ref, backend.clone())) {
        if entry.data_hash.is_some() {
            // File.
            out.send(hash).unwrap();
        } else {
            // Directory.
            out.send(hash.clone()).unwrap();
            try!(list_snapshot(backend, out, family, &hash, pref));
        }
    }
    Ok(())
}

impl<B: 'static + blob::StoreBackend + Clone + Send> HatRc<B> {
    pub fn open_repository(repository_root: PathBuf,
                           backend: B,
                           max_blob_size: usize)
                           -> Result<HatRc<B>, HatError> {
        let snapshot_index_path = snapshot_index_name(repository_root.clone());
        let blob_index_path = blob_index_name(repository_root.clone());
        let hash_index_path = hash_index_name(repository_root.clone());
        let si_p = try!(snapshot::SnapshotIndex::new(&snapshot_index_path));
        let bi_p = try!(blob::BlobIndex::new(&blob_index_path));
        let hi_p = try!(hash::HashIndex::new(&hash_index_path));

        let local_blob_index = bi_p.clone();
        let local_backend = backend.clone();
        let bs_p = try!(Process::new(move || {
            blob::Store::new(local_blob_index, local_backend, max_blob_size)
        }));

        let gc_backend = GcBackend { hash_index: hi_p.clone() };
        let gc = gc::Gc::new(gc_backend);

        let mut hat = Hat {
            repository_root: Some(repository_root),
            snapshot_index: si_p,
            hash_index: hi_p.clone(),
            blob_store: bs_p.clone(),
            blob_backend: backend.clone(),
            hash_backend: key::HashStoreBackend::new(hi_p, bs_p),
            gc: gc,
            max_blob_size: max_blob_size,
        };

        // Resume any unfinished commands.
        try!(hat.resume());

        Ok(hat)
    }

    #[cfg(test)]
    pub fn new_for_testing(backend: B,
                           max_blob_size: usize,
                           poison_after: &[i64])
                           -> Result<HatRc<B>, HatError> {
        // If provided, we cycle the possible poison values to give every process one.
        let mut poison = poison_after.iter().cycle();

        let si_p = snapshot::SnapshotIndex::new_for_testing().unwrap();
        let bi_p = blob::BlobIndex::new_for_testing().unwrap();
        let hi_p = hash::HashIndex::new_for_testing(poison.next().cloned()).unwrap();

        let local_blob_index = bi_p.clone();
        let local_backend = backend.clone();
        let bs_p = Process::new_with_poison(move || {
                                                blob::Store::new(local_blob_index,
                                                                 local_backend,
                                                                 max_blob_size)
                                            },
                                            poison.next().cloned())
            .unwrap();

        let gc_backend = GcBackend { hash_index: hi_p.clone() };
        let gc = gc::Gc::new(gc_backend);

        let mut hat = Hat {
            repository_root: None,
            snapshot_index: si_p,
            hash_index: hi_p.clone(),
            blob_store: bs_p.clone(),
            blob_backend: backend.clone(),
            hash_backend: key::HashStoreBackend::new(hi_p, bs_p),
            gc: gc,
            max_blob_size: max_blob_size,
        };

        // Resume any unfinished commands.
        try!(hat.resume());

        Ok(hat)
    }

    pub fn hash_tree_writer(&mut self) -> hash::tree::SimpleHashTreeWriter<key::HashStoreBackend> {
        let backend = key::HashStoreBackend::new(self.hash_index.clone(), self.blob_store.clone());
        hash::tree::SimpleHashTreeWriter::new(8, backend)
    }

    pub fn open_family(&self, name: String) -> Result<Family, HatError> {
        self.open_family_with_poison(name, None)
    }

    pub fn open_family_with_poison(&self,
                                   name: String,
                                   poison_after: Option<i64>)
                                   -> Result<Family, HatError> {
        // We setup a standard pipeline of processes:
        // key::Store -> key::Index
        //            -> hash::Index
        //            -> blob::Store -> blob::Index

        let key_index_path = match self.repository_root {
            Some(ref root) => concat_filename(root.clone(), &name),
            None => ":memory:".to_string(),
        };

        let ki_p = try!(key::KeyIndex::new(&key_index_path));

        let local_ks = key::Store::new(ki_p.clone(),
                                       self.hash_index.clone(),
                                       self.blob_store.clone());
        let ks_p = try!(Process::new_with_poison(move || local_ks, poison_after));

        let ks = try!(key::Store::new(ki_p, self.hash_index.clone(), self.blob_store.clone()));

        Ok(Family {
            name: name,
            key_store: ks,
            key_store_process: ks_p,
        })
    }

    pub fn meta_commit(&mut self) -> Result<(), HatError> {
        let all_snapshots = self.snapshot_index.list_all();

        // TODO(jos): use a hash tree for this listing.
        let mut message = ::capnp::message::Builder::new_default();

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
        match try!(self.blob_store.send_reply(blob::Msg::StoreNamed("root".to_owned(), listing))) {
            blob::Reply::StoreNamedOk(_) => Ok(()),
            _ => panic!("Invalid reply from blob store"),
        }
    }

    pub fn recover(&mut self) -> Result<(), HatError> {
        let root = match try!(self.blob_store
            .send_reply(blob::Msg::RetrieveNamed("root".to_owned()))) {
            blob::Reply::RetrieveOk(r) => r,
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
        fn recover_entry(hashes: &hash::HashIndex,
                         blobs: &blob::StoreProcess,
                         entry: hash::Entry)
                         -> Result<i64, HatError> {
            let hash = entry.hash.clone();
            let pref = entry.persistent_ref.clone().unwrap();

            // Make sure we have the blob described.
            match try!(blobs.send_reply(blob::Msg::Recover(pref.clone()))) {
                blob::Reply::RecoverOk => (),
                _ => return Err(From::from("Failed to add recovered blob")),
            }
            // Now insert the hash information if needed.
            match try!(hashes.reserve(entry)) {
                hash::ReserveResult::HashKnown => return Ok(try!(hashes.get_id(&hash)).unwrap()),
                hash::ReserveResult::ReserveOk => (),
            }
            // Commit hash.
            try!(hashes.commit(&hash, pref));

            Ok(try!(hashes.get_id(&hash)).unwrap())
        }

        let family = self.open_family(family_name.clone())
            .expect(&format!("Could not open family '{}'", family_name));
        let (register_sender, register_receiver) = mpsc::channel();
        let (recover_sender, recover_receiver) = mpsc::channel();
        let (final_payload, final_level) = try!(self.recover_dir_ref(&family,
                                                                     hash,
                                                                     dir_ref.clone(),
                                                                     register_sender,
                                                                     recover_sender));
        // Recover hashes for tree child chunks.
        for entry in recover_receiver {
            try!(recover_entry(&self.hash_index, &self.blob_store, entry));
        }

        // Recover hashes for tree-tops. These are also registered with the GC.
        let (id_sender, id_receiver) = mpsc::channel();
        let local_hash_index = self.hash_index.clone();
        let local_blob_store = self.blob_store.clone();
        thread::spawn(move || {
            for entry in register_receiver {
                let id = recover_entry(&local_hash_index, &local_blob_store, entry).unwrap();
                id_sender.send(id).unwrap();
            }
        });

        try!(self.gc.register(info.clone(), id_receiver));
        try!(self.flush_blob_store());

        // Recover final root hash for the snapshot.
        try!(recover_entry(&self.hash_index,
                           &self.blob_store,
                           hash::Entry {
                               hash: hash.clone(),
                               persistent_ref: Some(dir_ref),
                               level: final_level,
                               payload: final_payload,
                           }));

        let final_id = try!(self.hash_index.get_id(hash)).unwrap();
        try!(self.gc.register_final(info.clone(), final_id));

        try!(self.commit_finalize(&family, info, hash));

        Ok(())
    }

    fn recover_dir_ref(&mut self,
                       family: &Family,
                       dir_hash: &hash::Hash,
                       dir_ref: blob::ChunkRef,
                       register_out: mpsc::Sender<hash::Entry>,
                       recover_out: mpsc::Sender<hash::Entry>)
                       -> Result<(Option<Vec<u8>>, i64), HatError> {
        fn recover_tree<B: hash::tree::HashTreeBackend<Err = key::MsgError> + Clone>
            (backend: B,
             hash: &hash::Hash,
             pref: blob::ChunkRef,
             out: mpsc::Sender<hash::Entry>)
             -> Result<(Option<Vec<u8>>, i64), HatError> {
            match try!(hash::tree::SimpleHashTreeReader::open(backend, &hash, Some(pref)))
                .unwrap() {
                hash::tree::ReaderResult::Empty |
                hash::tree::ReaderResult::SingleBlock(..) => Ok((None, 0)),
                hash::tree::ReaderResult::Tree(mut reader) => {
                    let (payload, level) = try!(reader.list_entries(out));
                    Ok((Some(payload), level))
                }
            }
        }
        for (file, hash, pref) in
            try!(family.fetch_dir_data(dir_hash, dir_ref.clone(), self.hash_backend.clone())) {
            let (payload, level) = match file.data_hash {
                Some(..) => {
                    // Entry is a data leaf. Read the hash tree.
                    try!(recover_tree(self.hash_backend.clone(),
                                      &hash,
                                      pref.clone(),
                                      recover_out.clone()))
                }
                None => {
                    try!(self.recover_dir_ref(&family,
                                              &hash,
                                              pref.clone(),
                                              register_out.clone(),
                                              recover_out.clone()))
                }
            };
            // We register the top hash with the GC (tree nodes are inferred).
            let r = hash::Entry {
                hash: hash,
                persistent_ref: Some(pref),
                level: level,
                payload: payload,
            };
            register_out.send(r).unwrap();
        }

        recover_tree(self.hash_backend.clone(), dir_hash, dir_ref, recover_out)
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
                            let status_res = try!(self.hash_index
                                    .get_id(h))
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
                            let hash = match &snapshot.hash {
                                &Some(ref h) => h,
                                &None => {
                                    return Err(From::from("recovered snapshot must have a hash"))
                                }
                            };
                            let tree = match snapshot.tree_ref {
                                Some(tr) => try!(blob::ChunkRef::from_bytes(&mut &tr[..])),
                                None => {
                                    return Err(From::from("Recovered hash tree has no root hash"))
                                }
                            };
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
                    let hash_id = try!(self.hash_index
                            .get_id(&hash))
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
                    let hash_id = try!(self.hash_index
                            .get_id(&hash))
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
        let family = try!(self.open_family(family_name.clone()));
        try!(self.commit(&family, resume_info));

        Ok(())
    }

    pub fn commit(&mut self,
                  family: &Family,
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
                        .expect("Hash index failed")
                        .expect("Hash not found"))
                    .expect("Channel failed");
            }
        });

        // Commit metadata while registering needed data-hashes (files and dirs).
        let (hash, top_ref) = {
            let mut local_family = family.clone();
            let (s, r) = mpsc::channel();

            thread::spawn(move || s.send(local_family.commit(hash_sender)));
            try!(self.gc.register(snap_info.clone(), hash_id_receiver));

            try!(try!(r.recv()))
        };

        // Push any remaining data to external storage.
        // This also flushes our hashes from the memory index, so we can tag them.
        try!(self.flush_blob_store());

        // Tag 2:
        // We update the snapshot entry with the tree hash, which we then register.
        // When the GC has seen the final hash, we flush everything so far.
        self.snapshot_index.update(&snap_info, &hash, &top_ref);
        self.flush_snapshot_index();

        // Register the final hash.
        // At this point, the GC should still be able to either resume or rollback safely.
        // After a successful flush, all GC work is done.
        // The GC must be able to tell if it has completed or not.
        let hash_id = try!(self.hash_index.get_id(&hash)).expect("Hash does not exist");
        try!(self.gc.register_final(snap_info.clone(), hash_id));
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
                       family: &Family,
                       snap_info: snapshot::Info,
                       hash: &hash::Hash)
                       -> Result<(), HatError> {
        // Commit locally. Let the GC perform any needed cleanup.
        self.snapshot_index.ready_commit(&snap_info);
        self.flush_snapshot_index();

        let hash_id = try!(self.hash_index.get_id(hash)).expect("Hash does not exist");
        try!(self.gc.register_cleanup(snap_info.clone(), hash_id));
        try!(family.flush());

        // Tag 0: All is done.
        self.snapshot_index.commit(&snap_info);
        self.flush_snapshot_index();

        Ok(())
    }

    pub fn flush_snapshot_index(&self) {
        self.snapshot_index.flush();
    }

    pub fn flush_blob_store(&self) -> Result<(), HatError> {
        match try!(self.blob_store.send_reply(blob::Msg::Flush)) {
            blob::Reply::FlushOk => Ok(()),
            _ => panic!("Invalid reply from blob store"),
        }
    }

    pub fn checkout_in_dir(&self,
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
                        family: &Family,
                        output: &mut PathBuf,
                        dir_hash: &hash::Hash,
                        dir_ref: blob::ChunkRef)
                        -> Result<(), HatError> {
        fs::create_dir_all(&output).unwrap();
        for (entry, hash, pref) in
            try!(family.fetch_dir_data(dir_hash, dir_ref, self.hash_backend.clone())) {
            assert!(entry.name.len() > 0);

            output.push(str::from_utf8(&entry.name[..]).unwrap());
            println!("{}", output.display());

            if entry.data_hash.is_some() {
                let mut fd = fs::File::create(&output).unwrap();
                let tree_opt = try!(hash::tree::SimpleHashTreeReader::open(self.hash_backend
                                                                               .clone(),
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
        let family = try!(self.open_family(family_name.clone()));
        try!(self.deregister(&family, snapshot_id));

        Ok(())
    }

    pub fn deregister(&mut self, family: &Family, snapshot_id: i64) -> Result<(), HatError> {
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

        let local_family = family.clone();
        let local_hash_index = self.hash_index.clone();
        let local_hash_backend = self.hash_backend.clone();
        let local_dir_hash = dir_hash.clone();
        let listing = move || {
            let (sender, receiver) = mpsc::channel();
            list_snapshot(&local_hash_backend,
                          &sender,
                          &local_family,
                          &local_dir_hash,
                          dir_ref)
                .unwrap();
            drop(sender);

            let (id_sender, id_receiver) = mpsc::channel();
            for hash in receiver.iter() {
                match local_hash_index.get_id(&hash).expect("Hash index failed") {
                    Some(id) => id_sender.send(id).unwrap(),
                    None => panic!("Unexpected reply from hash index."),
                }
            }
            match local_hash_index.get_id(&local_dir_hash).expect("Hash index failed") {
                Some(id) => id_sender.send(id).unwrap(),
                None => panic!("Unexpected reply from hash index."),
            }
            id_receiver
        };

        let final_ref = try!(self.hash_index
                .get_id(&dir_hash))
            .expect("Snapshot hash does not exist");
        try!(self.gc.deregister(info.clone(), final_ref, listing));
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
                           family: &Family,
                           snap_info: snapshot::Info,
                           final_ref: gc::Id)
                           -> Result<(), HatError> {
        // Mark the snapshot to enable resuming.
        self.snapshot_index.ready_delete(&snap_info);
        self.flush_snapshot_index();

        // Clear GC state.
        try!(self.gc.register_cleanup(snap_info.clone(), final_ref));
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
            try!(self.hash_index.delete(id));
        }
        try!(self.hash_index.flush());
        // Mark used blobs.
        let entries = try!(self.hash_index.list());
        match try!(self.blob_store.send_reply(blob::Msg::TagAll(tags::Tag::InProgress))) {
            blob::Reply::Ok => (),
            _ => panic!("Unexpected reply from blob store."),
        }
        let mut live_blobs = 0;
        for entry in entries.into_iter() {
            if let Some(pref) = entry.persistent_ref {
                live_blobs += 1;
                match try!(self.blob_store.send_reply(blob::Msg::Tag(pref, tags::Tag::Reserved))) {
                    blob::Reply::Ok => (),
                    _ => panic!("Unexpected reply from blob store."),
                }
            }
        }
        // Anything still marked "in progress" is not referenced by any hash.
        match try!(self.blob_store.send_reply(blob::Msg::DeleteByTag(tags::Tag::InProgress))) {
            blob::Reply::Ok => (),
            _ => panic!("Unexpected reply from blob store."),
        }
        match try!(self.blob_store.send_reply(blob::Msg::TagAll(tags::Tag::Done))) {
            blob::Reply::Ok => (),
            _ => panic!("Unexpected reply from blob store."),
        }
        match try!(self.blob_store.send_reply(blob::Msg::Flush)) {
            blob::Reply::FlushOk => (),
            _ => panic!("Unexpected reply from blob store."),
        }

        Ok((deleted_hashes, live_blobs))
    }
}

struct FileEntry {
    key_entry: key::Entry,
    metadata: fs::Metadata,
    full_path: PathBuf,
    link_path: Option<PathBuf>,
}

impl FileEntry {
    fn new(full_path: PathBuf, parent: Option<u64>) -> Result<FileEntry, Box<Error>> {
        debug!("FileEntry::new({:?})", full_path);

        let filename_opt = full_path.file_name().and_then(|n| n.to_str());

        if filename_opt.is_some() {
            let md = try!(fs::symlink_metadata(&full_path));
            let link_path = fs::read_link(&full_path).ok();
            Ok(FileEntry {
                key_entry: key::Entry {
                    name: filename_opt.unwrap().bytes().collect(),
                    created: Some(md.ctime_nsec()),
                    modified: Some(md.mtime_nsec()),
                    accessed: Some(md.atime_nsec()),
                    parent_id: parent,
                    data_length: Some(md.len()),
                    data_hash: None,
                    id: None,
                    permissions: None,
                    user_id: None,
                    group_id: None,
                },
                metadata: md,
                full_path: full_path.clone(),
                link_path: link_path,
            })
        } else {
            Err(From::from("Could not parse filename."[..].to_owned()))
        }
    }

    fn file_iterator(&self) -> io::Result<FileIterator> {
        FileIterator::new(&self.full_path)
    }

    fn is_directory(&self) -> bool {
        self.metadata.is_dir()
    }
    fn is_symlink(&self) -> bool {
        self.link_path.is_some()
    }
    fn is_file(&self) -> bool {
        self.metadata.is_file()
    }
}

impl Clone for FileEntry {
    fn clone(&self) -> FileEntry {
        FileEntry {
            metadata: fs::metadata(&self.full_path).unwrap(),
            full_path: self.full_path.clone(),
            link_path: self.link_path.clone(),
            key_entry: self.key_entry.clone(),
        }
    }
}

pub enum FileIterator {
    File(fs::File),
    Buf(Vec<u8>, usize),
    Iter(Box<Iterator<Item = Vec<u8>> + Send>),
}

impl FileIterator {
    fn new(path: &PathBuf) -> io::Result<FileIterator> {
        match fs::File::open(path) {
            Ok(f) => Ok(FileIterator::File(f)),
            Err(e) => Err(e),
        }
    }
    fn from_bytes(contents: Vec<u8>) -> FileIterator {
        FileIterator::Buf(contents, 0)
    }
    fn from_iter<I>(i: Box<I>) -> FileIterator
        where I: Iterator<Item = Vec<u8>> + Send + 'static
    {
        FileIterator::Iter(i)
    }
}

impl Iterator for FileIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Vec<u8>> {
        let chunk_size = 128 * 1024;
        match self {
            &mut FileIterator::File(ref mut f) => {
                let mut buf = vec![0u8; chunk_size];
                match f.read(&mut buf[..]) {
                    Err(_) => None,
                    Ok(size) if size == 0 => None,
                    Ok(size) => Some(buf[..size].to_vec()),
                }
            }
            &mut FileIterator::Buf(ref vec, ref mut pos) => {
                if *pos >= vec.len() {
                    None
                } else {
                    let next = &vec[*pos..cmp::min(*pos + chunk_size, vec.len())];
                    *pos += chunk_size;
                    Some(next.to_owned())
                }
            }
            &mut FileIterator::Iter(ref mut iter) => iter.next(),
        }
    }
}


struct InsertPathHandler {
    count: sync::atomic::AtomicIsize,
    last_print: sync::Mutex<time::Timespec>,
    key_store: sync::Mutex<key::StoreProcess<FileIterator>>,
}

impl InsertPathHandler {
    pub fn new(key_store: key::StoreProcess<FileIterator>) -> InsertPathHandler {
        InsertPathHandler {
            count: sync::atomic::AtomicIsize::new(0),
            last_print: sync::Mutex::new(time::now().to_timespec()),
            key_store: sync::Mutex::new(key_store),
        }
    }
}

impl listdir::PathHandler<Option<u64>> for InsertPathHandler {
    type DirItem = fs::DirEntry;
    type DirIter = fs::ReadDir;

    fn read_dir(&self, path: &PathBuf) -> io::Result<Self::DirIter> {
        fs::read_dir(path)
    }

    fn handle_path(&self, parent: &Option<u64>, path: PathBuf) -> Option<Option<u64>> {
        let count = self.count.fetch_add(1, atomic::Ordering::SeqCst) + 1;

        if count % 16 == 0 {
            // don't hammer the mutex
            let mut guarded_last_print = self.last_print.lock().unwrap();
            let now = time::now().to_timespec();
            if guarded_last_print.sec <= now.sec - 1 {
                println!("#{}: {}", count, path.display());
                *guarded_last_print = now;
            }
        }

        match FileEntry::new(path.clone(), parent.clone()) {
            Err(e) => {
                println!("Skipping '{}': {}", path.display(), e);
            }
            Ok(file_entry) => {
                if file_entry.is_symlink() {
                    return None;
                }
                let is_directory = file_entry.is_directory();
                let local_root = path;
                let local_file_entry = file_entry.clone();
                let chunk_it =
                    if is_directory {
                        None
                    } else {
                        match local_file_entry.file_iterator() {
                            Err(e) => {
                                println!("Skipping '{}': {}",
                                         local_root.display(),
                                         e.to_string());
                                Some(None)
                            }
                            Ok(it) => Some(Some(it))
                        }
                    };
                match self.key_store
                    .lock()
                    .unwrap()
                    .send_reply(key::Msg::Insert(file_entry.key_entry, chunk_it)) {
                    Ok(key::Reply::Id(id)) => {
                        if is_directory {
                            return Some(Some(id));
                        }
                    }
                    _ => panic!("Unexpected reply from key store."),
                }
            }
        }

        None
    }
}


fn try_a_few_times_then_panic<F>(f: F, msg: &str)
    where F: FnMut() -> bool
{
    let mut f = f;
    for _ in 1 as i32..5 {
        if f() {
            return;
        }
    }
    panic!(msg.to_owned());
}


#[derive(Clone)]
pub struct Family {
    name: String,
    key_store: key::Store,
    key_store_process: key::StoreProcess<FileIterator>,
}

impl Family {
    pub fn snapshot_dir(&self, dir: PathBuf) {
        let handler = InsertPathHandler::new(self.key_store_process.clone());
        listdir::iterate_recursively((PathBuf::from(&dir), None), &sync::Arc::new(handler));
    }

    pub fn snapshot_direct(&self,
                           file: key::Entry,
                           is_directory: bool,
                           contents: Option<FileIterator>)
                           -> Result<(), HatError> {
        let chunk_it = if is_directory {
            None
        } else {
            Some(contents)
        };
        match try!(self.key_store_process.send_reply(key::Msg::Insert(file, chunk_it))) {
            key::Reply::Id(..) => return Ok(()),
            _ => Err(From::from("Unexpected reply from key store")),
        }
    }

    pub fn flush(&self) -> Result<(), HatError> {
        if let key::Reply::FlushOk = try!(self.key_store_process.send_reply(key::Msg::Flush)) {
            return Ok(());
        }
        Err(From::from("Unexpected reply from key store"))
    }

  fn write_file_chunks<HTB: hash::tree::HashTreeBackend<Err=key::MsgError> + Clone>(
    &self, fd: &mut fs::File, tree: hash::tree::ReaderResult<HTB>)
  {
        for chunk in tree {
            try_a_few_times_then_panic(|| fd.write_all(&chunk[..]).is_ok(),
                                       "Could not write chunk.");
        }
        try_a_few_times_then_panic(|| fd.flush().is_ok(), "Could not flush file.");
    }

    pub fn checkout_in_dir(&self,
                           output_dir: PathBuf,
                           dir_id: Option<u64>)
                           -> Result<(), HatError> {
        let mut path = output_dir;
        for (entry, _ref, read_fn_opt) in try!(self.list_from_key_store(dir_id)).into_iter() {
            // Extend directory with filename:
            path.push(str::from_utf8(&entry.name[..]).unwrap());

            match read_fn_opt {
                None => {
                    // This is a directory, recurse!
                    fs::create_dir_all(&path).unwrap();
                    try!(self.checkout_in_dir(path.clone(), entry.id));
                }
                Some(read_fn) => {
                    // This is a file, write it
                    let mut fd = fs::File::create(&path).unwrap();
                    if let Some(tree) = try!(read_fn.init()) {
                        self.write_file_chunks(&mut fd, tree);
                    }
                }
            }

            // Prepare for next filename:
            path.pop();
        }

        Ok(())
    }

    pub fn list_from_key_store(&self, dir_id: Option<u64>) -> Result<Vec<key::DirElem>, HatError> {
        match try!(self.key_store_process.send_reply(key::Msg::ListDir(dir_id))) {
            key::Reply::ListResult(ls) => Ok(ls),
            _ => Err(From::from("Unexpected result from key store")),
        }
    }

    pub fn fetch_dir_data<HTB: hash::tree::HashTreeBackend<Err = key::MsgError> + Clone>
        (&self,
         dir_hash: &hash::Hash,
         dir_ref: blob::ChunkRef,
         backend: HTB)
         -> Result<Vec<(key::Entry, hash::Hash, blob::ChunkRef)>, HatError> {
        let mut out = Vec::new();
        let it = try!(hash::tree::SimpleHashTreeReader::open(backend, dir_hash, Some(dir_ref)))
            .expect("unable to open dir");

        for chunk in it {
            if chunk.is_empty() {
                continue;
            }
            let reader =
                capnp::serialize_packed::read_message(&mut &chunk[..],
                                                      capnp::message::ReaderOptions::new())
                    .unwrap();

            let list = reader.get_root::<root_capnp::file_list::Reader>().unwrap();
            for f in list.get_files().unwrap().iter() {
                if f.get_name().unwrap().len() == 0 {
                    // Empty entry at end.
                    // TODO(jos): Can we get rid of these?
                    break;
                }
                let entry = key::Entry {
                    id: Some(f.get_id()),
                    name: f.get_name().unwrap().to_owned(),
                    created: match f.get_created().which().unwrap() {
                        root_capnp::file::created::Unknown(()) => None,
                        root_capnp::file::created::Timestamp(ts) => Some(ts),
                    },
                    modified: match f.get_modified().which().unwrap() {
                        root_capnp::file::modified::Unknown(()) => None,
                        root_capnp::file::modified::Timestamp(ts) => Some(ts),
                    },
                    accessed: match f.get_accessed().which().unwrap() {
                        root_capnp::file::accessed::Unknown(()) => None,
                        root_capnp::file::accessed::Timestamp(ts) => Some(ts),
                    },
                    data_hash: match f.get_content().which().unwrap() {
                        root_capnp::file::content::Data(r) => {
                            Some(r.unwrap().get_hash().unwrap().to_owned())
                        }
                        root_capnp::file::content::Directory(_) => None,
                    },
                    // TODO(jos): Implement support for these remaining fields.
                    user_id: None,
                    group_id: None,
                    permissions: None,
                    data_length: None,
                    parent_id: None,
                };
                let hash = match f.get_content().which().unwrap() {
                    root_capnp::file::content::Data(r) => r.unwrap().get_hash().unwrap().to_owned(),
                    root_capnp::file::content::Directory(d) => {
                        d.unwrap().get_hash().unwrap().to_owned()
                    }
                };
                let pref = match f.get_content().which().unwrap() {
                    root_capnp::file::content::Data(r) => {
                        blob::ChunkRef::read_msg(&r.unwrap().get_chunk_ref().unwrap()).unwrap()
                    }
                    root_capnp::file::content::Directory(d) => {
                        blob::ChunkRef::read_msg(&d.unwrap().get_chunk_ref().unwrap()).unwrap()
                    }
                };

                out.push((entry, hash::Hash { bytes: hash }, pref));
            }
        }

        Ok(out)
    }

    pub fn commit(&mut self,
                  hash_ch: mpsc::Sender<hash::Hash>)
                  -> Result<(hash::Hash, blob::ChunkRef), HatError> {
        let mut top_tree = self.key_store.hash_tree_writer();
        try!(self.commit_to_tree(&mut top_tree, None, hash_ch));

        Ok(try!(top_tree.hash()))
    }

    pub fn commit_to_tree(&mut self,
                          tree: &mut hash::tree::SimpleHashTreeWriter<key::HashStoreBackend>,
                          dir_id: Option<u64>,
                          hash_ch: mpsc::Sender<hash::Hash>)
                          -> Result<(), HatError> {

        let files_at_a_time = 1024;
        let mut it = try!(self.list_from_key_store(dir_id)).into_iter();

        loop {
            let mut current_msg_is_empty = true;
            let mut file_block_msg = capnp::message::Builder::new_default();

            {
                let files_root = file_block_msg.init_root::<root_capnp::file_list::Builder>();
                let mut files = files_root.init_files(files_at_a_time as u32);

                for (idx, (entry, data_ref, _data_res_open)) in it.by_ref()
                    .take(files_at_a_time)
                    .enumerate() {
                    assert!(idx < files_at_a_time);

                    current_msg_is_empty = false;
                    let mut file_msg = files.borrow().get(idx as u32);

                    file_msg.set_id(entry.id.unwrap_or(0));
                    file_msg.set_name(&entry.name);

                    match entry.created {
                        None => file_msg.borrow().init_created().set_unknown(()),
                        Some(ts) => file_msg.borrow().init_created().set_timestamp(ts),
                    }

                    match entry.modified {
                        None => file_msg.borrow().init_modified().set_unknown(()),
                        Some(ts) => file_msg.borrow().init_modified().set_timestamp(ts),
                    }

                    match entry.accessed {
                        None => file_msg.borrow().init_accessed().set_unknown(()),
                        Some(ts) => file_msg.borrow().init_accessed().set_timestamp(ts),
                    }

                    if let Some(hash_bytes) = entry.data_hash {
                        // This is a file, store its data hash:
                        let mut hash_ref_msg = capnp::message::Builder::new_default();
                        let mut hash_ref_root =
                            hash_ref_msg.init_root::<root_capnp::hash_ref::Builder>();

                        // Populate data hash and ChunkRef.
                        hash_ref_root.set_hash(&hash_bytes);
                        data_ref.expect("has data")
                            .populate_msg(hash_ref_root.borrow().init_chunk_ref());
                        // Set as file content.
                        try!(file_msg.borrow()
                            .init_content()
                            .set_data(hash_ref_root.as_reader()));
                        hash_ch.send(hash::Hash { bytes: hash_bytes }).unwrap();
                    } else {
                        drop(data_ref);  // May not use data reference without hash.

                        // This is a directory, recurse!
                        let mut inner_tree = self.key_store.hash_tree_writer();
                        try!(self.commit_to_tree(&mut inner_tree, entry.id, hash_ch.clone()));
                        // Store a reference for the sub-tree in our tree:
                        let (dir_hash, dir_ref) = try!(inner_tree.hash());

                        let mut hash_ref_msg = capnp::message::Builder::new_default();
                        let mut hash_ref_root =
                            hash_ref_msg.init_root::<root_capnp::hash_ref::Builder>();

                        // Populate directory hash and ChunkRef.
                        hash_ref_root.set_hash(&dir_hash.bytes);
                        dir_ref.populate_msg(hash_ref_root.borrow().init_chunk_ref());
                        // Set as directory content.
                        try!(file_msg.borrow()
                            .init_content()
                            .set_directory(hash_ref_root.as_reader()));

                        hash_ch.send(dir_hash).unwrap();
                    }
                }
            }

            // Flush to our own tree when we have a decent amount.
            // The tree prevents large directories from clogging ram.
            if current_msg_is_empty {
                break;
            } else {
                let mut buf = vec![];
                try!(capnp::serialize_packed::write_message(&mut buf, &file_block_msg));
                try!(tree.append(buf));
            }
        }

        Ok(())
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    use quickcheck;

    use blob::StoreBackend;
    use blob::tests::MemoryBackend;
    use key;

    pub fn setup_hat<B: Clone + Send + StoreBackend + 'static>(backend: B,
                                                               poison_after: &[i64])
                                                               -> HatRc<B> {
        let max_blob_size = 1024 * 1024;
        Hat::new_for_testing(backend, max_blob_size, poison_after).unwrap()
    }

    fn setup_family(poison_after: Option<Vec<i64>>) -> (MemoryBackend, HatRc<MemoryBackend>, Family) {
        let poison = poison_after.unwrap_or(vec![]);

        let backend = MemoryBackend::new();
        let hat = setup_hat(backend.clone(), &poison[..]);

        let family = "familyname".to_string();
        let fam = hat.open_family_with_poison(family.clone(), poison.last().cloned()).unwrap();

        (backend, hat, fam)
    }

    pub fn entry(name: Vec<u8>) -> key::Entry {
        key::Entry {
            name: name,
            id: None,
            parent_id: None,
            created: None,
            modified: None,
            accessed: None,
            permissions: None,
            user_id: None,
            group_id: None,
            data_hash: None,
            data_length: None,
        }
    }

    fn snapshot_files(family: &Family, files: Vec<(&str, Vec<u8>)>) -> Result<(), HatError> {
        for (name, contents) in files {
            try!(family.snapshot_direct(entry(name.bytes().collect()),
                                        false,
                                        Some(FileIterator::from_bytes(contents))));
        }
        Ok(())
    }

    #[test]
    fn snapshot_commit() {
        let (_, mut hat, fam) = setup_family(None);

        snapshot_files(&fam,
                       vec![("name1", vec![0; 1000000]),
                            ("name2", vec![1; 1000000]),
                            ("name3", vec![2; 1000000])]).unwrap();

        fam.flush().unwrap();
        hat.commit(&fam, None).unwrap();
        hat.meta_commit().unwrap();

        let (deleted, live) = hat.gc().unwrap();
        assert_eq!(deleted, 0);
        assert!(live > 0);
    }

    #[test]
    fn snapshot_commit_many_empty_files() {
        let (_, mut hat, fam) = setup_family(None);

        let names: Vec<String> = (0..3000).map(|i| format!("name-{}", i)).collect();
        snapshot_files(&fam, names.iter().map(|n| (n.as_str(), vec![])).collect()).unwrap();

        fam.flush().unwrap();
        hat.commit(&fam, None).unwrap();
        hat.meta_commit().unwrap();

        let (deleted, live) = hat.gc().unwrap();
        assert_eq!(deleted, 0);
        assert!(live > 0);

        hat.deregister(&fam, 1).unwrap();
        let (deleted, live) = hat.gc().unwrap();
        assert!(deleted > 0);
        assert_eq!(live, 0);
    }

    #[test]
    fn snapshot_commit_many_empty_directories() {
        let (_, mut hat, fam) = setup_family(None);

        for i in 0..3000 {
            fam.snapshot_direct(entry(format!("name-{}", i).bytes().collect()), true, None)
                .unwrap();
        }

        fam.flush().unwrap();
        hat.commit(&fam, None).unwrap();
        hat.meta_commit().unwrap();

        let (deleted, live) = hat.gc().unwrap();
        assert_eq!(deleted, 0);
        assert!(live > 0);

        hat.deregister(&fam, 1).unwrap();
        let (deleted, live) = hat.gc().unwrap();
        assert!(deleted > 0);
        assert_eq!(live, 0);
    }

    #[test]
    fn snapshot_reuse_index() {
        let (_, mut hat, fam) = setup_family(None);

        let files = vec![("file1", "block1".bytes().collect()),
                         ("file2", "block2".bytes().collect()),
                         ("file3", "block3".bytes().collect()),
                         ("file4", "block1".bytes().collect()),
                         ("file5", "block2".bytes().collect())];

        // Insert hashes.
        snapshot_files(&fam, files.clone()).unwrap();
        fam.flush().unwrap();

        // Reuse hashes.
        snapshot_files(&fam, files.clone()).unwrap();
        snapshot_files(&fam, files.clone()).unwrap();
        fam.flush().unwrap();

        // No commit, so GC removes all the new hashes.
        let (deleted, live) = hat.gc().unwrap();
        assert!(deleted > 0);
        assert_eq!(live, 0);

        // Update index and reinsert hashes.
        snapshot_files(&fam, files.clone()).unwrap();
        fam.flush().unwrap();

        // Commit.
        hat.commit(&fam, None).unwrap();
        let (deleted, live) = hat.gc().unwrap();
        assert_eq!(deleted, 0);
        assert!(live > 0);

        // Inserting again does not increase number of hashes.
        snapshot_files(&fam, files.clone()).unwrap();
        fam.flush().unwrap();
        let (deleted2, live2) = hat.gc().unwrap();
        assert_eq!(live2, live);
        assert_eq!(deleted2, 0);

        // Cleanup: only 1 snapshot was committed.
        hat.deregister(&fam, 1).unwrap();
        let (deleted, live) = hat.gc().unwrap();
        assert!(deleted > 0);
        assert_eq!(live, 0);
    }

    #[test]
    fn snapshot_gc() {
        let (_, mut hat, fam) = setup_family(None);

        snapshot_files(&fam,
                       vec![("name1", vec![0; 1000000]),
                            ("name2", vec![1; 1000000]),
                            ("name3", vec![2; 1000000])]).unwrap();

        fam.flush().unwrap();

        // No commit so everything is deleted.
        let (deleted, live) = hat.gc().unwrap();
        assert!(deleted > 0);
        assert_eq!(live, 0);
    }

    #[test]
    fn recover() {
        // Prepare a snapshot.
        let (backend, mut hat, fam) = setup_family(None);

        snapshot_files(&fam,
                       vec![("name1", vec![0; 1000000]),
                            ("name2", vec![1; 1000000]),
                            ("name3", vec![2; 1000000])]).unwrap();
        fam.flush().unwrap();
        hat.commit(&fam, None).unwrap();
        hat.meta_commit().unwrap();

        let (deleted, live1) = hat.gc().unwrap();
        assert_eq!(deleted, 0);
        assert!(live1 > 0);

        // Create a new hat to wipe the index states.
        let poison = vec![];
        let mut hat2 = setup_hat(backend.clone(), &poison[..]);

        // Recover index states.
        hat2.recover().unwrap();

        // Check that we now reference all the blobs.
        let (deleted, live2) = hat2.gc().unwrap();
        assert_eq!(deleted, 0);
        assert_eq!(live1, live2);

        // Check that we can delete the snapshot.
        hat2.deregister(&fam, 1).unwrap();

        let (deleted, live3) = hat2.gc().unwrap();
        assert!(deleted > 0);
        assert_eq!(live3, 0);
    }

    #[test]
    fn poisoned_process_does_not_panic() {
        // TODO: Upgrade to proper error handling so we can enable commit.

        fn prop(poison_after: u8) -> bool {
            let poison = vec![poison_after as i64];
            let (_, _, fam) = setup_family(Some(poison));

            let run_until_error = || {
                try!(snapshot_files(&fam,
                                    vec![("name1", vec![0; 1000000]),
                                         ("name2", vec![1; 1000000]),
                                         ("name3", vec![2; 1000000])]));

                try!(fam.flush());
                // try!(hat.commit(&fam, None));
                // try!(hat.meta_commit());

                Ok(())
            };

            let _: Result<(), HatError> = run_until_error();
            true  // no panics.
        };

        quickcheck::quickcheck(prop as fn(u8) -> bool);
    }
}

#[cfg(all(test, feature = "benchmarks"))]
mod bench {
    use super::*;
    use super::tests::*;

    use std::sync::{Arc, Mutex};
    use test::Bencher;

    use blob;

    fn setup_family() -> (HatRc<blob::tests::DevNullBackend>, Family) {
        let empty = vec![];
        let backend = blob::tests::DevNullBackend {};
        let hat = setup_hat(backend.clone(), &empty[..]);

        let family = "familyname".to_string();
        let fam = hat.open_family(family.clone()).unwrap();

        (hat, fam)
    }

    #[derive(Clone)]
    struct UniqueBlockFiller(Arc<Mutex<u32>>);

    impl UniqueBlockFiller {
        fn new(id: u32) -> UniqueBlockFiller {
            UniqueBlockFiller(Arc::new(Mutex::new(id)))
        }

        /// Fill the buffer with 1KB unique blocks of data.
        /// Each block is unique for the given id and among the other blocks.
        fn fill_bytes(&mut self, buf: &mut [u8]) {
            for block in buf.chunks_mut(1024) {
                if block.len() < 4 {
                    // Last block is too short to make unique.
                    break;
                }
                let mut n = self.0.lock().unwrap();

                block[0] = *n as u8;
                block[1] = (*n >> 8) as u8;
                block[2] = (*n >> 16) as u8;
                block[3] = (*n >> 24) as u8;
                *n += 1;
            }
        }
    }

    #[derive(Clone)]
    struct UniqueBlockIter {
        filler: UniqueBlockFiller,
        block: Vec<u8>,
        filesize: i32,
    }

    impl UniqueBlockIter {
        fn new(filler: UniqueBlockFiller, blocksize: usize, filesize: i32) -> UniqueBlockIter {
            UniqueBlockIter {
                filler: filler,
                block: vec![0; blocksize],
                filesize: filesize,
            }
        }
        fn reset_filesize(&mut self, filesize: i32) {
            self.filesize = filesize;
        }
        fn reset_filler(&mut self, id: u32) {
            self.filler = UniqueBlockFiller::new(id);
        }
    }

    impl Iterator for UniqueBlockIter {
        type Item = Vec<u8>;

        fn next(&mut self) -> Option<Vec<u8>> {
            if self.filesize <= 0 {
                None
            } else {
                self.filesize -= self.block.len() as i32;
                self.filler.fill_bytes(&mut self.block);
                Some(self.block.clone())
            }
        }
    }

    fn insert_files(bench: &mut Bencher, filesize: i32, unique: bool) {
        let (_, family) = setup_family();

        let mut filler = UniqueBlockFiller::new(0);
        let mut name = vec![0; 8];

        let mut file_iter = UniqueBlockIter::new(filler.clone(), 128 * 1024, filesize);

        bench.iter(|| {
            filler.fill_bytes(&mut name);

            file_iter.reset_filesize(filesize);
            if !unique {
                // Reset data filler.
                file_iter.reset_filler(0);
            }

            let file = FileIterator::from_iter(Box::new(file_iter.clone()));
            family.snapshot_direct(entry(name.clone()), false, Some(file)).unwrap();
        });

        bench.bytes = filesize as u64;
    }

    #[bench]
    fn insert_small_unique_files(mut bench: &mut Bencher) {
        insert_files(&mut bench, 8, true);
    }

    #[bench]
    fn insert_small_identical_files(mut bench: &mut Bencher) {
        insert_files(&mut bench, 8, false);
    }

    #[bench]
    fn insert_medium_unique_files(mut bench: &mut Bencher) {
        insert_files(&mut bench, 1024 * 1024, true);
    }

    #[bench]
    fn insert_medium_identical_files(mut bench: &mut Bencher) {
        insert_files(&mut bench, 1024 * 1024, false);
    }

    #[bench]
    fn insert_large_unique_files(mut bench: &mut Bencher) {
        insert_files(&mut bench, 8 * 1024 * 1024, true);
    }

    #[bench]
    fn insert_large_identical_files(mut bench: &mut Bencher) {
        insert_files(&mut bench, 8 * 1024 * 1024, false);
    }
}
