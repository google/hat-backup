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


use capnp;
use std::str;
use std::error::Error;

use process::Process;
use tags;

use blob;
use hash;
use key;

use snapshot;

use listdir;
use gc;
use gc_rc;

use std::path::PathBuf;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::io;
use std::io::{Read, Write};

use std::sync;
use std::sync::mpsc;
use std::sync::atomic;
use std::thread;

use time;
use root_capnp;


struct GcBackend {
    hash_index: hash::IndexProcess,
}

impl gc::GcBackend for GcBackend {
    fn get_data(&self, hash_id: i64, family_id: i64) -> hash::GcData {
        match self.hash_index.send_reply(hash::Msg::ReadGcData(hash_id, family_id)) {
            hash::Reply::CurrentGcData(data) => data,
            _ => panic!("Unexpected reply from hash index."),
        }
    }
    fn update_data(&mut self, hash_id: i64, family_id: i64, f: gc::UpdateFn) -> hash::GcData {
        match self.hash_index.send_reply(hash::Msg::UpdateGcData(hash_id, family_id, f)) {
            hash::Reply::CurrentGcData(data) => data,
            _ => panic!("Unexpected reply from hash index."),
        }
    }
    fn update_all_data_by_family(&mut self, family_id: i64, fs: mpsc::Receiver<gc::UpdateFn>) {
        match self.hash_index.send_reply(hash::Msg::UpdateFamilyGcData(family_id, fs)) {
            hash::Reply::Ok => (),
            _ => panic!("Unexpected reply from hash index."),
        }
    }

    fn get_tag(&self, hash_id: i64) -> Option<tags::Tag> {
        match self.hash_index.send_reply(hash::Msg::GetTag(hash_id)) {
            hash::Reply::HashTag(tag) => tag,
            _ => panic!("Unexpected reply from hash index."),
        }
    }

    fn set_tag(&mut self, hash_id: i64, tag: tags::Tag) {
        match self.hash_index.send_reply(hash::Msg::SetTag(hash_id, tag)) {
            hash::Reply::Ok => (),
            _ => panic!("Unexpected reply from hash index."),
        }
    }

    fn set_all_tags(&mut self, tag: tags::Tag) {
        match self.hash_index.send_reply(hash::Msg::SetAllTags(tag)) {
            hash::Reply::Ok => (),
            _ => panic!("Unexpected reply from hash index."),
        }
    }

    fn reverse_refs(&self, hash_id: i64) -> Vec<i64> {
        let entry = match self.hash_index.send_reply(hash::Msg::GetHash(hash_id)) {
            hash::Reply::Entry(entry) => entry,
            hash::Reply::HashNotKnown => panic!("HashNotKnown in hash index."),
            _ => panic!("Unexpected reply from hash index."),
        };
        if entry.payload.is_none() {
            return Vec::new();
        }

        hash::tree::decode_metadata_refs(&entry.payload.unwrap())
            .into_iter()
            .map(|bytes| {
                match self.hash_index.send_reply(hash::Msg::GetID(hash::Hash { bytes: bytes })) {
                    hash::Reply::HashID(id) => id,
                    hash::Reply::HashNotKnown => panic!("HashNotKnown in hash index."),
                    _ => panic!("Unexpected result from hash index."),
                }
            })
            .collect()
    }

    fn list_ids_by_tag(&self, tag: tags::Tag) -> mpsc::Receiver<i64> {
        let (sender, receiver) = mpsc::channel();
        match self.hash_index.send_reply(hash::Msg::GetIDsByTag(tag as i64)) {
            hash::Reply::HashIDs(ids) => {
                ids.iter().map(|i| sender.send(*i)).last();
            }
            _ => panic!("Unexpected reply from hash index."),
        }

        receiver
    }

    fn manual_commit(&mut self) {
        match self.hash_index.send_reply(hash::Msg::ManualCommit) {
            hash::Reply::CommitOk => return,
            _ => panic!("Unexpected reply from hash index."),
        }
    }
}


pub struct Hat<B> {
    repository_root: Option<PathBuf>,
    snapshot_index: snapshot::IndexProcess,
    blob_store: blob::StoreProcess,
    hash_index: hash::IndexProcess,
    blob_backend: B,
    hash_backend: key::HashStoreBackend,
    gc: Box<gc::Gc>,
    max_blob_size: usize,
}

fn concat_filename(a: &PathBuf, b: String) -> String {
    let mut result = a.clone();
    result.push(&b);
    result.into_os_string().into_string().unwrap()
}

fn snapshot_index_name(root: &PathBuf) -> String {
    concat_filename(root, "snapshot_index.sqlite3".to_owned())
}

fn blob_index_name(root: &PathBuf) -> String {
    concat_filename(root, "blob_index.sqlite3".to_owned())
}

fn hash_index_name(root: &PathBuf) -> String {
    concat_filename(root, "hash_index.sqlite3".to_owned())
}

fn list_snapshot(backend: &key::HashStoreBackend,
                 out: &mpsc::Sender<hash::Hash>,
                 family: &Family,
                 dir_hash: hash::Hash,
                 dir_ref: blob::ChunkRef) {
    for (entry, hash, pref) in family.fetch_dir_data(dir_hash, dir_ref, backend.clone()) {
        if entry.data_hash.is_some() {
            // File.
            out.send(hash).unwrap();
        } else {
            // Directory.
            out.send(hash.clone()).unwrap();
            list_snapshot(backend, out, family, hash, pref);
        }
    }
}

fn get_hash_id(index: &hash::IndexProcess, hash: hash::Hash) -> Result<i64, String> {
    match index.send_reply(hash::Msg::GetID(hash)) {
        hash::Reply::HashID(id) => Ok(id),
        _ => Err("Tried to register an unknown hash".to_owned()),
    }
}

impl<B: 'static + blob::StoreBackend + Clone + Send> Hat<B> {
    pub fn open_repository(repository_root: &PathBuf, backend: B, max_blob_size: usize) -> Hat<B> {
        let snapshot_index_path = snapshot_index_name(repository_root);
        let blob_index_path = blob_index_name(repository_root);
        let hash_index_path = hash_index_name(repository_root);
        let si_p = Process::new(Box::new(move || snapshot::Index::new(snapshot_index_path)));
        let bi_p = Process::new(Box::new(move || blob::Index::new(blob_index_path)));
        let hi_p = Process::new(Box::new(move || hash::Index::new(hash_index_path)));

        let local_blob_index = bi_p.clone();
        let local_backend = backend.clone();
        let bs_p = Process::new(Box::new(move || {
            blob::Store::new(local_blob_index, local_backend, max_blob_size)
        }));

        let gc_backend = GcBackend { hash_index: hi_p.clone() };
        let gc = gc_rc::GcRc::new(Box::new(gc_backend));

        let mut hat = Hat {
            repository_root: Some(repository_root.clone()),
            snapshot_index: si_p,
            hash_index: hi_p.clone(),
            blob_store: bs_p.clone(),
            blob_backend: backend.clone(),
            hash_backend: key::HashStoreBackend::new(hi_p, bs_p),
            gc: Box::new(gc),
            max_blob_size: max_blob_size,
        };

        // Resume any unfinished commands.
        hat.resume();

        hat
    }

    #[cfg(test)]
    pub fn new_for_testing(backend: B, max_blob_size: usize) -> Hat<B> {
        let si_p = Process::new(Box::new(move || snapshot::Index::new_for_testing()));
        let bi_p = Process::new(Box::new(move || blob::Index::new_for_testing()));
        let hi_p = Process::new(Box::new(move || hash::Index::new_for_testing()));

        let local_blob_index = bi_p.clone();
        let local_backend = backend.clone();
        let bs_p = Process::new(Box::new(move || {
            blob::Store::new(local_blob_index, local_backend, max_blob_size)
        }));

        let gc_backend = GcBackend { hash_index: hi_p.clone() };
        let gc = gc_rc::GcRc::new(Box::new(gc_backend));

        let mut hat = Hat {
            repository_root: None,
            snapshot_index: si_p,
            hash_index: hi_p.clone(),
            blob_store: bs_p.clone(),
            blob_backend: backend.clone(),
            hash_backend: key::HashStoreBackend::new(hi_p, bs_p),
            gc: Box::new(gc),
            max_blob_size: max_blob_size,
        };

        // Resume any unfinished commands.
        hat.resume();

        hat
    }

    pub fn hash_tree_writer(&mut self) -> hash::tree::SimpleHashTreeWriter<key::HashStoreBackend> {
        let backend = key::HashStoreBackend::new(self.hash_index.clone(), self.blob_store.clone());
        return hash::tree::SimpleHashTreeWriter::new(8, backend);
    }

    pub fn open_family(&self, name: String) -> Option<Family> {
        // We setup a standard pipeline of processes:
        // key::Store -> key::Index
        //            -> hash::Index
        //            -> blob::Store -> blob::Index

        let ki_p = match self.repository_root {
            Some(ref root) => {
                let key_index_path = concat_filename(root, name.clone());
                Process::new(Box::new(move || key::Index::new(key_index_path)))
            }
            None => {
                // Index is clean and in-memory only.
                Process::new(Box::new(move || key::Index::new(From::from(":memory:".to_string()))))
            }
        };

        let local_ks = key::Store::new(ki_p.clone(),
                                       self.hash_index.clone(),
                                       self.blob_store.clone());
        let ks_p = Process::new(Box::new(move || local_ks));

        let ks = key::Store::new(ki_p, self.hash_index.clone(), self.blob_store.clone());

        Some(Family {
            name: name,
            key_store: ks,
            key_store_process: ks_p,
        })
    }

    pub fn meta_commit(&mut self) {
        let all_snapshots = match self.snapshot_index.send_reply(snapshot::Msg::ListAll) {
            snapshot::Reply::All(lst) => lst,
            _ => panic!("Invalid reply from snapshot index"),
        };

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
        match self.blob_store.send_reply(blob::Msg::StoreNamed("root".to_owned(), listing)) {
            blob::Reply::StoreNamedOk(_) => (),
            _ => panic!("Invalid reply from blob store"),
        };
    }

    pub fn recover(&mut self) {
        let root = match self.blob_store
                             .send_reply(blob::Msg::RetrieveNamed("root".to_owned())) {
            blob::Reply::RetrieveOk(r) => r,
            _ => panic!("Could not read root file"),
        };
        let message_reader =
            capnp::serialize_packed::read_message(&mut &root[..],
                                                  capnp::message::ReaderOptions::new())
                .unwrap();
        let snapshot_list = message_reader.get_root::<root_capnp::snapshot_list::Reader>().unwrap();

        for s in snapshot_list.get_snapshots().unwrap().iter() {
            let tree_ref = blob::ChunkRef::from_bytes(&mut s.get_tree_reference().unwrap())
                               .unwrap();
            match self.snapshot_index
                      .send_reply(
                          snapshot::Msg::Recover(s.get_id(),
                                                 s.get_family_name()
                                                  .unwrap()
                                                  .to_owned(),
                                                 s.get_msg().unwrap().to_owned(),
                                                 s.get_hash().unwrap().to_vec(),
                                                 tree_ref,
                                                 Some(snapshot::WorkStatus::RecoverInProgress))) {
                snapshot::Reply::RecoverOk => (),
                _ => panic!("Invalid reply from snapshot index"),
            }
        }
        self.flush_snapshot_index();
        self.resume();
    }

    fn recover_snapshot(&mut self,
                        family_name: String,
                        info: snapshot::Info,
                        hash: hash::Hash,
                        dir_ref: blob::ChunkRef) {
        fn recover_entry(hashes: &hash::IndexProcess,
                         blobs: &blob::StoreProcess,
                         entry: hash::Entry)
                         -> i64 {
            let hash = entry.hash.clone();
            let pref = entry.persistent_ref.clone().unwrap();

            // Make sure we have the blob described.
            match blobs.send_reply(blob::Msg::Recover(pref.clone())) {
                blob::Reply::RecoverOk => (),
                _ => panic!("Failed to add recovered blob"),
            }
            // Now insert the hash information if needed.
            match hashes.send_reply(hash::Msg::Reserve(entry)) {
                hash::Reply::HashKnown => return get_hash_id(hashes, hash).unwrap(),
                hash::Reply::ReserveOk => (),
                _ => panic!("Unexpected reply from hash index"),
            }
            // Commit hash.
            match hashes.send_reply(hash::Msg::Commit(hash.clone(), pref.clone())) {
                hash::Reply::CommitOk => (),
                _ => panic!("Unexpected reply from hash index."),
            }
            return get_hash_id(hashes, hash).unwrap();
        }

        let family = self.open_family(family_name.clone())
                         .expect(&format!("Could not open family '{}'", family_name));
        let (register_sender, register_receiver) = mpsc::channel();
        let (recover_sender, recover_receiver) = mpsc::channel();
        let (final_payload, final_level) = self.recover_dir_ref(&family,
                                                                hash.clone(),
                                                                dir_ref.clone(),
                                                                register_sender,
                                                                recover_sender);
        // Recover hashes for tree child chunks.
        for entry in recover_receiver {
            recover_entry(&self.hash_index, &self.blob_store, entry);
        }

        // Recover hashes for tree-tops. These are also registered with the GC.
        let (id_sender, id_receiver) = mpsc::channel();
        let local_hash_index = self.hash_index.clone();
        let local_blob_store = self.blob_store.clone();
        thread::spawn(move || {
            for entry in register_receiver {
                let id = recover_entry(&local_hash_index, &local_blob_store, entry);
                id_sender.send(id).unwrap();
            }
        });

        self.gc.register(info.clone(), id_receiver);
        self.flush_blob_store();

        // Recover final root hash for the snapshot.
        recover_entry(&self.hash_index,
                      &self.blob_store,
                      hash::Entry {
                          hash: hash.clone(),
                          persistent_ref: Some(dir_ref),
                          level: final_level,
                          payload: final_payload,
                      });

        let final_id = get_hash_id(&self.hash_index, hash.clone()).unwrap();
        self.gc.register_final(info.clone(), final_id);
        self.commit_finalize(&family, info, hash);
    }

    fn recover_dir_ref(&mut self,
                       family: &Family,
                       dir_hash: hash::Hash,
                       dir_ref: blob::ChunkRef,
                       register_out: mpsc::Sender<hash::Entry>,
                       recover_out: mpsc::Sender<hash::Entry>)
                       -> (Option<Vec<u8>>, i64) {
        fn recover_tree<B: hash::tree::HashTreeBackend + Clone>(backend: B,
                                                                hash: hash::Hash,
                                                                pref: blob::ChunkRef,
                                                                out: mpsc::Sender<hash::Entry>)
                                                                -> (Option<Vec<u8>>, i64) {
            match hash::tree::SimpleHashTreeReader::open(backend, hash, Some(pref)).unwrap() {
                hash::tree::ReaderResult::Empty => (None, 0),
                hash::tree::ReaderResult::SingleBlock(..) => (None, 0),
                hash::tree::ReaderResult::Tree(mut reader) => {
                    let (payload, level) = reader.list_entries(out);
                    (Some(payload), level)
                }
            }
        }
        for (file, hash, pref) in family.fetch_dir_data(dir_hash.clone(),
                                                        dir_ref.clone(),
                                                        self.hash_backend.clone()) {
            let (payload, level) = match file.data_hash {
                Some(..) => {
                    // Entry is a data leaf. Read the hash tree.
                    recover_tree(self.hash_backend.clone(),
                                 hash.clone(),
                                 pref.clone(),
                                 recover_out.clone())
                }
                None => {
                    self.recover_dir_ref(&family,
                                         hash.clone(),
                                         pref.clone(),
                                         register_out.clone(),
                                         recover_out.clone())
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
        return recover_tree(self.hash_backend.clone(), dir_hash, dir_ref, recover_out);
    }

    pub fn resume(&mut self) {
        let need_work = match self.snapshot_index.send_reply(snapshot::Msg::ListNotDone) {
            snapshot::Reply::NotDone(lst) => lst,
            _ => panic!("Invalid reply from snapshot index"),
        };

        for snapshot in need_work.into_iter() {
            match snapshot.status {
                snapshot::WorkStatus::CommitInProgress |
                snapshot::WorkStatus::RecoverInProgress => {
                    let done_hash_opt = match snapshot.hash.clone() {
                        None => None,
                        Some(h) => {
                            let status = get_hash_id(&self.hash_index, h.clone())
                                             .ok()
                                             .and_then(|id| self.gc.status(id));
                            match status {
                                None => None,  // We did not fully commit.
                                Some(gc::Status::InProgress) | Some(gc::Status::Complete) => {
                                    Some(h)
                                }
                            }
                        }
                    };
                    match (done_hash_opt, snapshot.status) {
                        (Some(hash), snapshot::WorkStatus::CommitInProgress) => {
                            self.commit_finalize_by_name(snapshot.family_name, snapshot.info, hash)
                        }
                        (None, snapshot::WorkStatus::CommitInProgress) => {
                            println!("Resuming commit of: {}", snapshot.family_name);
                            self.commit_by_name(snapshot.family_name, Some(snapshot.info));
                        }
                        (None, snapshot::WorkStatus::RecoverInProgress) => {
                            println!("Resuming recovery of: {}", snapshot.family_name);
                            let hash = snapshot.hash.expect("recovered snapshot must have a hash");
                            let tree = blob::ChunkRef::from_bytes(
                                &mut &snapshot.tree_ref.expect(
                                    "Snapshot has no tree ref")[..]).unwrap();
                            self.recover_snapshot(snapshot.family_name, snapshot.info, hash, tree);
                        }
                        (hash, status) => panic!("unexpected state: ({:?}, {:?})", hash, status),
                    }
                }
                snapshot::WorkStatus::CommitComplete => {
                    match snapshot.hash.clone() {
                        Some(h) => {
                            self.commit_finalize_by_name(snapshot.family_name, snapshot.info, h)
                        }
                        None => {
                            // This should not happen.
                            panic!("Snapshot {:?} is fully registered in GC, but has not hash in \
                                    the index",
                                   snapshot);
                        }
                    }
                }
                snapshot::WorkStatus::DeleteInProgress => {
                    let hash = snapshot.hash.expect("Snapshot has no hash");
                    let hash_id = get_hash_id(&self.hash_index, hash.clone())
                                      .expect("Snapshot hash not recognized");
                    let status = self.gc.status(hash_id);
                    match status {
                        None | Some(gc::Status::InProgress) => {
                            println!("Resuming delete of: {} #{:?}",
                                     snapshot.family_name,
                                     snapshot.info.snapshot_id);
                            self.deregister_by_name(snapshot.family_name,
                                                    snapshot.info.snapshot_id);
                        }
                        Some(gc::Status::Complete) => {
                            self.deregister_finalize_by_name(snapshot.family_name,
                                                             snapshot.info,
                                                             hash_id)
                        }
                    }
                }
                snapshot::WorkStatus::DeleteComplete => {
                    let hash = snapshot.hash.expect("Snapshot has no hash");
                    let hash_id = get_hash_id(&self.hash_index, hash.clone())
                                      .expect("Snapshot hash not recognized");
                    self.deregister_finalize_by_name(snapshot.family_name, snapshot.info, hash_id);
                }
            }
        }

        // We should have finished everything there was to finish.
        let need_work = match self.snapshot_index.send_reply(snapshot::Msg::ListNotDone) {
            snapshot::Reply::NotDone(lst) => lst,
            _ => panic!("Invalid reply from snapshot index"),
        };
        assert_eq!(need_work.len(), 0);
    }

    pub fn commit_by_name(&mut self, family_name: String, resume_info: Option<snapshot::Info>) {
        let family = self.open_family(family_name.clone())
                         .expect(&format!("Could not open family '{}'", family_name));
        self.commit(&family, resume_info);
    }

    pub fn commit(&mut self, family: &Family, resume_info: Option<snapshot::Info>) {
        //  Tag 1:
        //  Reserve the snapshot and commit the reservation.
        //  Register all but the last hashes.
        //  (the last hash is a special-case, as the GC use it to save meta-data for resuming)
        let snap_info = match resume_info {
            Some(info) => info,  // Resume already started commit.
            None => {
                // Create new commit.
                match self.snapshot_index
                          .send_reply(snapshot::Msg::Reserve(family.name.clone())) {
                    snapshot::Reply::Reserved(info) => info,
                    _ => panic!("Invalid reply from snapshot index"),
                }
            }
        };
        self.flush_snapshot_index();

        // Prepare.
        let (hash_sender, hash_receiver) = mpsc::channel();
        let (hash_id_sender, hash_id_receiver) = mpsc::channel();

        let local_hash_index = self.hash_index.clone();
        thread::spawn(move || {
            for hash in hash_receiver.iter() {
                hash_id_sender.send(get_hash_id(&local_hash_index, hash).unwrap()).unwrap();
            }
        });

        // Commit metadata while registering needed data-hashes (files and dirs).
        let (hash, top_ref) = {
            let mut local_family = family.clone();
            let (s, r) = mpsc::channel();

            thread::spawn(move || s.send(local_family.commit(hash_sender)));
            self.gc.register(snap_info.clone(), hash_id_receiver);

            r.recv().unwrap()
        };

        // Push any remaining data to external storage.
        // This also flushes our hashes from the memory index, so we can tag them.
        self.flush_blob_store();

        // Tag 2:
        // We update the snapshot entry with the tree hash, which we then register.
        // When the GC has seen the final hash, we flush everything so far.
        match self.snapshot_index
                  .send_reply(snapshot::Msg::Update(snap_info.clone(), hash.clone(), top_ref)) {
            snapshot::Reply::UpdateOk => (),
            _ => panic!("Snapshot index update failed"),
        };
        self.flush_snapshot_index();

        // Register the final hash.
        // At this point, the GC should still be able to either resume or rollback safely.
        // After a successful flush, all GC work is done.
        // The GC must be able to tell if it has completed or not.
        let hash_id = get_hash_id(&self.hash_index, hash.clone()).unwrap();
        self.gc.register_final(snap_info.clone(), hash_id);
        family.flush();
        self.commit_finalize(family, snap_info, hash);
    }

    fn commit_finalize_by_name(&mut self,
                               family_name: String,
                               snap_info: snapshot::Info,
                               hash: hash::Hash) {
        let family = self.open_family(family_name).expect("Unknown family name");
        self.commit_finalize(&family, snap_info, hash);
    }

    fn commit_finalize(&mut self, family: &Family, snap_info: snapshot::Info, hash: hash::Hash) {
        // Commit locally. Let the GC perform any needed cleanup.
        match self.snapshot_index
                  .send_reply(snapshot::Msg::ReadyCommit(snap_info.clone())) {
            snapshot::Reply::UpdateOk => (),
            _ => panic!("Invalid reply from snapshot index"),
        };
        self.flush_snapshot_index();

        let hash_id = get_hash_id(&self.hash_index, hash).unwrap();
        self.gc.register_cleanup(snap_info.clone(), hash_id);
        family.flush();

        // Tag 0: All is done.
        match self.snapshot_index.send_reply(snapshot::Msg::Commit(snap_info)) {
            snapshot::Reply::CommitOk => (),
            _ => panic!("Invalid reply from snapshot index"),
        };
        self.flush_snapshot_index();
    }

    pub fn flush_snapshot_index(&self) {
        match self.snapshot_index.send_reply(snapshot::Msg::Flush) {
            snapshot::Reply::FlushOk => (),
            _ => panic!("Invalid reply from snapshot index"),
        };
    }

    pub fn flush_blob_store(&self) {
        match self.blob_store.send_reply(blob::Msg::Flush) {
            blob::Reply::FlushOk => (),
            _ => panic!("Invalid reply from blob store"),
        }
    }

    pub fn checkout_in_dir(&self, family_name: String, output_dir: PathBuf) {
        // Extract latest snapshot info:
        let (_info, dir_hash, dir_ref) =
            match self.snapshot_index
                      .send_reply(snapshot::Msg::Latest(family_name.clone())) {
                snapshot::Reply::Snapshot(Some((i, h, Some(r)))) => (i, h, r),
                snapshot::Reply::Snapshot(_) => {
                    panic!("Tried to checkout family '{}' before first completed commit",
                           family_name)
                }
                _ => panic!("Unexpected result from snapshot index"),
            };

        let family = self.open_family(family_name.clone())
                         .expect(&format!("Could not open family '{}'", family_name));

        let mut output_dir = output_dir;
        self.checkout_dir_ref(&family, &mut output_dir, dir_hash, dir_ref);
    }

    fn checkout_dir_ref(&self,
                        family: &Family,
                        output: &mut PathBuf,
                        dir_hash: hash::Hash,
                        dir_ref: blob::ChunkRef) {
        fs::create_dir_all(&output).unwrap();
        for (entry, hash, pref) in family.fetch_dir_data(dir_hash,
                                                         dir_ref,
                                                         self.hash_backend.clone()) {
            assert!(entry.name.len() > 0);

            output.push(str::from_utf8(&entry.name[..]).unwrap());
            println!("{}", output.display());

            if entry.data_hash.is_some() {
                let mut fd = fs::File::create(&output).unwrap();
                let tree_opt = hash::tree::SimpleHashTreeReader::open(self.hash_backend.clone(),
                                                                      hash,
                                                                      Some(pref));
                if let Some(tree) = tree_opt {
                    family.write_file_chunks(&mut fd, tree);
                }
            } else {
                self.checkout_dir_ref(family, output, hash, pref);
            }
            output.pop();
        }
    }

    pub fn deregister_by_name(&mut self, family_name: String, snapshot_id: i64) {
        let family = self.open_family(family_name.clone())
                         .expect(&format!("Could not open family '{}'", family_name));
        self.deregister(&family, snapshot_id);
    }

    pub fn deregister(&mut self, family: &Family, snapshot_id: i64) {
        let (info, dir_hash, dir_ref) =
            match self.snapshot_index
                      .send_reply(snapshot::Msg::Lookup(family.name.clone(), snapshot_id)) {
                snapshot::Reply::Snapshot(opt) => {
                    match opt {
                        Some((i, h, Some(r))) => (i, h, r),
                        _ => {
                            panic!("No complete snapshot found for family {} with id {:?}",
                                   family.name,
                                   snapshot_id)
                        }
                    }
                }
                _ => panic!("Unexpected reply from snapshot index."),
            };

        // Make the snapshot to enable resuming.
        match self.snapshot_index.send_reply(snapshot::Msg::WillDelete(info.clone())) {
            snapshot::Reply::UpdateOk => (),
            _ => panic!("Unexpected reply from snapshot index."),
        }
        self.flush_snapshot_index();

        let local_family = family.clone();
        let local_hash_index = self.hash_index.clone();
        let local_hash_backend = self.hash_backend.clone();
        let local_dir_hash = dir_hash.clone();
        let listing = Box::new(move || {
            let (sender, receiver) = mpsc::channel();
            list_snapshot(&local_hash_backend,
                          &sender,
                          &local_family,
                          local_dir_hash.clone(),
                          dir_ref);
            drop(sender);

            let (id_sender, id_receiver) = mpsc::channel();
            for hash in receiver.iter() {
                match local_hash_index.send_reply(hash::Msg::GetID(hash)) {
                    hash::Reply::HashID(id) => id_sender.send(id).unwrap(),
                    _ => panic!("Unexpected reply from hash index."),
                }
            }
            match local_hash_index.send_reply(hash::Msg::GetID(local_dir_hash)) {
                hash::Reply::HashID(id) => id_sender.send(id).unwrap(),
                _ => panic!("Unexpected reply from hash index."),
            }
            id_receiver
        });

        let final_ref = get_hash_id(&self.hash_index, dir_hash)
                            .expect("Snapshot hash does not exist");
        self.gc.deregister(info.clone(), final_ref, listing);
        family.flush();

        self.deregister_finalize(family, info, final_ref);
    }

    fn deregister_finalize_by_name(&mut self,
                                   family_name: String,
                                   snap_info: snapshot::Info,
                                   hash_id: gc::Id) {
        let family = self.open_family(family_name).expect("Unknown family name");
        self.deregister_finalize(&family, snap_info, hash_id);
    }

    fn deregister_finalize(&mut self,
                           family: &Family,
                           snap_info: snapshot::Info,
                           final_ref: gc::Id) {
        // Mark the snapshot to enable resuming.
        match self.snapshot_index.send_reply(snapshot::Msg::ReadyDelete(snap_info.clone())) {
            snapshot::Reply::UpdateOk => (),
            _ => panic!("Unexpected reply from snapshot index."),
        }
        self.flush_snapshot_index();

        // Clear GC state.
        self.gc.register_cleanup(snap_info.clone(), final_ref);
        family.flush();

        // Delete snapshot metadata.
        self.snapshot_index.send_reply(snapshot::Msg::Delete(snap_info));
        self.flush_snapshot_index();

        match self.snapshot_index.send_reply(snapshot::Msg::Flush) {
            snapshot::Reply::FlushOk => (),
            _ => panic!("Unexpected reply from snapshot index."),
        };
        self.flush_snapshot_index();
    }

    pub fn gc(&mut self) -> (i64, i64) {
        // Remove unused hashes.
        let mut deleted_hashes = 0;
        let (sender, receiver) = mpsc::channel();
        self.gc.list_unused_ids(sender);
        for id in receiver.iter() {
            deleted_hashes += 1;
            match self.hash_index.send_reply(hash::Msg::Delete(id)) {
                hash::Reply::Ok => (),
                _ => panic!("Unexpected reply from hash index."),
            }
        }
        match self.hash_index.send_reply(hash::Msg::Flush) {
            hash::Reply::CommitOk => (),
            _ => panic!("Unexpected reply from hash index."),
        }
        // Mark used blobs.
        let entries = match self.hash_index.send_reply(hash::Msg::List) {
            hash::Reply::Listing(ch) => ch,
            _ => panic!("Unexpected reply from hash index."),
        };
        match self.blob_store.send_reply(blob::Msg::TagAll(tags::Tag::InProgress)) {
            blob::Reply::Ok => (),
            _ => panic!("Unexpected reply from blob store."),
        }
        let mut live_blobs = 0;
        for entry in entries.iter() {
            if let Some(pref) = entry.persistent_ref {
                live_blobs += 1;
                match self.blob_store.send_reply(blob::Msg::Tag(pref, tags::Tag::Reserved)) {
                    blob::Reply::Ok => (),
                    _ => panic!("Unexpected reply from blob store."),
                }
            }
        }
        // Anything still marked "in progress" is not referenced by any hash.
        match self.blob_store.send_reply(blob::Msg::DeleteByTag(tags::Tag::InProgress)) {
            blob::Reply::Ok => (),
            _ => panic!("Unexpected reply from blob store."),
        }
        match self.blob_store.send_reply(blob::Msg::TagAll(tags::Tag::Done)) {
            blob::Reply::Ok => (),
            _ => panic!("Unexpected reply from blob store."),
        }
        match self.blob_store.send_reply(blob::Msg::Flush) {
            blob::Reply::FlushOk => (),
            _ => panic!("Unexpected reply from blob store."),
        }
        return (deleted_hashes, live_blobs);
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

struct FileIterator {
    file: Option<fs::File>,
    contents: Option<Vec<u8>>,
}

impl FileIterator {
    fn new(path: &PathBuf) -> io::Result<FileIterator> {
        match fs::File::open(path) {
            Ok(f) => {
                Ok(FileIterator {
                    file: Some(f),
                    contents: None,
                })
            }
            Err(e) => Err(e),
        }
    }
    fn from_bytes(contents: Vec<u8>) -> FileIterator {
        FileIterator {
            file: None,
            contents: Some(contents),
        }
    }
}

impl Iterator for FileIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Vec<u8>> {
        let chunk_size = 128 * 1024;
        let empty = vec![];
        match self.file {
            Some(ref mut f) => {
                let mut buf = vec![0u8; chunk_size];
                match f.read(&mut buf[..]) {
                    Err(_) => None,
                    Ok(size) if size == 0 => None,
                    Ok(size) => Some(buf[..size].to_vec()),
                }
            }
            None => {
                match self.contents.to_owned() {
                    Some(ref c) if c.is_empty() => None,
                    Some(ref c) => {
                        let (out, rest) = if c.len() <= chunk_size {
                            (c.as_slice(), empty.as_slice())
                        } else {
                            c.split_at(chunk_size)
                        };
                        self.contents = Some(rest.to_owned());
                        Some(out.to_owned())
                    }
                    None => None,
                }
            }
        }
    }
}


#[derive(Clone)]
struct InsertPathHandler {
    count: sync::Arc<sync::atomic::AtomicIsize>,
    last_print: sync::Arc<sync::Mutex<time::Timespec>>,
    key_store: key::StoreProcess<FileIterator>,
}

impl InsertPathHandler {
    pub fn new(key_store: key::StoreProcess<FileIterator>) -> InsertPathHandler {
        InsertPathHandler {
            count: sync::Arc::new(sync::atomic::AtomicIsize::new(0)),
            last_print: sync::Arc::new(sync::Mutex::new(time::now().to_timespec())),
            key_store: key_store,
        }
    }
}

impl listdir::PathHandler<Option<u64>> for InsertPathHandler {
    fn handle_path(&self, parent: Option<u64>, path: PathBuf) -> Option<Option<u64>> {
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

        match FileEntry::new(path.clone(), parent) {
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

                match self.key_store.send_reply(
                    key::Msg::Insert(
                      file_entry.key_entry,
                      if is_directory { None }
                      else { Some(Box::new(move|| {
                              match local_file_entry.file_iterator() {
                                  Err(e) => {
                                      println!("Skipping '{}': {}",
                                               local_root.display(), e.to_string());
                                      None
                                  },
                                  Ok(it) => { Some(it) }
                                }
                            }))
                            }))
        {
          key::Reply::Id(id) => {
            if is_directory { return Some(Some(id)) }
          },
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
        let mut handler = InsertPathHandler::new(self.key_store_process.clone());
        listdir::iterate_recursively((PathBuf::from(&dir), None), &mut handler);
    }

    pub fn snapshot_direct(&self,
                           file: key::Entry,
                           is_directory: bool,
                           contents: Option<Vec<u8>>) {
        match self.key_store_process.send_reply(
                key::Msg::Insert(file,
                      if is_directory { None }
                      else { Some(Box::new(move|| {
                          contents.map(|b| FileIterator::from_bytes(b))
                      }))}))
        {
            key::Reply::Id(..) => return,
            _ => panic!("Unexpected reply from key store"),
        }
    }

    pub fn flush(&self) {
        self.key_store_process.send_reply(key::Msg::Flush);
    }

  fn write_file_chunks<HTB: hash::tree::HashTreeBackend + Clone>(
    &self, fd: &mut fs::File, tree: hash::tree::ReaderResult<HTB>)
  {
        for chunk in tree {
            try_a_few_times_then_panic(|| fd.write_all(&chunk[..]).is_ok(),
                                       "Could not write chunk.");
        }
        try_a_few_times_then_panic(|| fd.flush().is_ok(), "Could not flush file.");
    }

    pub fn checkout_in_dir(&self, output_dir: PathBuf, dir_id: Option<u64>) {
        let mut path = output_dir;
        for (entry, _ref, read_fn_opt) in self.list_from_key_store(dir_id).into_iter() {
            // Extend directory with filename:
            path.push(str::from_utf8(&entry.name[..]).unwrap());

            match read_fn_opt {
                None => {
                    // This is a directory, recurse!
                    fs::create_dir_all(&path).unwrap();
                    self.checkout_in_dir(path.clone(), entry.id);
                }
                Some(read_fn) => {
                    // This is a file, write it
                    let mut fd = fs::File::create(&path).unwrap();
                    if let Some(tree) = read_fn() {
                        self.write_file_chunks(&mut fd, tree);
                    }
                }
            }

            // Prepare for next filename:
            path.pop();
        }
    }

    pub fn list_from_key_store(&self, dir_id: Option<u64>) -> Vec<key::DirElem> {
        match self.key_store_process.send_reply(key::Msg::ListDir(dir_id)) {
            key::Reply::ListResult(ls) => ls,
            _ => panic!("Unexpected result from key store."),
        }
    }

    pub fn fetch_dir_data<HTB: hash::tree::HashTreeBackend + Clone>
        (&self,
         dir_hash: hash::Hash,
         dir_ref: blob::ChunkRef,
         backend: HTB)
         -> Vec<(key::Entry, hash::Hash, blob::ChunkRef)> {
        let mut out = Vec::new();
        let it = hash::tree::SimpleHashTreeReader::open(backend, dir_hash, Some(dir_ref))
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

        out
    }

    pub fn commit(&mut self, hash_ch: mpsc::Sender<hash::Hash>) -> (hash::Hash, blob::ChunkRef) {
        let mut top_tree = self.key_store.hash_tree_writer();
        self.commit_to_tree(&mut top_tree, None, hash_ch);

        top_tree.hash()
    }

    pub fn commit_to_tree(&mut self,
                          tree: &mut hash::tree::SimpleHashTreeWriter<key::HashStoreBackend>,
                          dir_id: Option<u64>,
                          hash_ch: mpsc::Sender<hash::Hash>) {

        let files_at_a_time = 1024;
        let mut it = self.list_from_key_store(dir_id).into_iter();

        loop {
            let mut current_msg_is_empty = true;
            let mut files_msg = capnp::message::Builder::new_default();

            {
                let files_root = files_msg.init_root::<root_capnp::file_list::Builder>();
                let mut files = files_root.init_files(files_at_a_time as u32);

                for (idx, (entry, data_ref, _data_res_open)) in it.by_ref().take(files_at_a_time).enumerate() {
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
                        file_msg.borrow()
                                .init_content()
                                .set_data(hash_ref_root.as_reader())
                                .unwrap();
                        hash_ch.send(hash::Hash { bytes: hash_bytes }).unwrap();
                    } else {
                        drop(data_ref);  // May not use data reference without hash.

                        // This is a directory, recurse!
                        let mut inner_tree = self.key_store.hash_tree_writer();
                        self.commit_to_tree(&mut inner_tree, entry.id, hash_ch.clone());
                        // Store a reference for the sub-tree in our tree:
                        let (dir_hash, dir_ref) = inner_tree.hash();

                        let mut hash_ref_msg = capnp::message::Builder::new_default();
                        let mut hash_ref_root =
                            hash_ref_msg.init_root::<root_capnp::hash_ref::Builder>();

                        // Populate directory hash and ChunkRef.
                        hash_ref_root.set_hash(&dir_hash.bytes);
                        dir_ref.populate_msg(hash_ref_root.borrow().init_chunk_ref());
                        // Set as directory content.
                        file_msg.borrow()
                                .init_content()
                                .set_directory(hash_ref_root.as_reader())
                                .unwrap();

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
                capnp::serialize_packed::write_message(&mut buf, &files_msg).unwrap();
                tree.append(buf);
            }
        }
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    use blob::StoreBackend;
    use blob::tests::MemoryBackend;
    use key;

    fn setup_hat<B: Clone + Send + StoreBackend + 'static>(backend: B) -> Hat<B> {
        let max_blob_size = 1024 * 1024;
        Hat::new_for_testing(backend.clone(), max_blob_size)
    }

    fn setup_family() -> (MemoryBackend, Hat<MemoryBackend>, Family) {
        let backend = MemoryBackend::new();
        let hat = setup_hat(backend.clone());

        let family = "familyname".to_string();
        let fam = hat.open_family(family.clone()).unwrap();

        (backend, hat, fam)
    }

    fn entry(name: Vec<u8>) -> key::Entry {
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

    fn snapshot_files(family: &Family, files: Vec<(&str, Vec<u8>)>) {
        for (name, contents) in files {
            family.snapshot_direct(entry(name.bytes().collect()), false, Some(contents));
        }
    }

    #[test]
    fn snapshot_commit() {
        let (_, mut hat, fam) = setup_family();

        snapshot_files(&fam,
                       vec![("name1", vec![0; 1000000]),
                            ("name2", vec![1; 1000000]),
                            ("name3", vec![2; 1000000])]);

        fam.flush();
        hat.commit(&fam, None);
        hat.meta_commit();

        let (deleted, live) = hat.gc();
        assert_eq!(deleted, 0);
        assert!(live > 0);
    }

    #[test]
    fn snapshot_commit_many_empty_files() {
        let (_, mut hat, fam) = setup_family();

        let names: Vec<String> = (0..10000).map(|i| format!("name-{}", i)).collect();
        snapshot_files(&fam, names.iter().map(|n| (n.as_str(), vec![])).collect());

        fam.flush();
        hat.commit(&fam, None);
        hat.meta_commit();

        let (deleted, live) = hat.gc();
        assert_eq!(deleted, 0);
        assert!(live > 0);
    }

    #[test]
    fn snapshot_gc() {
        let (_, mut hat, fam) = setup_family();

        snapshot_files(&fam,
                       vec![("name1", vec![0; 1000000]),
                            ("name2", vec![1; 1000000]),
                            ("name3", vec![2; 1000000])]);

        fam.flush();

        // No commit so everything is deleted.
        let (deleted, live) = hat.gc();
        assert!(deleted > 0);
        assert_eq!(live, 0);
    }

    #[test]
    fn recover() {
        // Prepare a snapshot.
        let (backend, mut hat, fam) = setup_family();

        snapshot_files(&fam,
                       vec![("name1", vec![0; 1000000]),
                            ("name2", vec![1; 1000000]),
                            ("name3", vec![2; 1000000])]);
        fam.flush();
        hat.commit(&fam, None);
        hat.meta_commit();

        let (deleted, live1) = hat.gc();
        assert_eq!(deleted, 0);
        assert!(live1 > 0);

        // Create a new hat to wipe the index states.
        let mut hat2 = setup_hat(backend.clone());

        // Recover index states.
        hat2.recover();

        // Check that we now reference all the blobs.
        let (deleted, live2) = hat2.gc();
        assert_eq!(deleted, 0);
        assert_eq!(live1, live2);

        // Check that we can delete the snapshot.
        hat2.deregister(&fam, 1);

        let (deleted, live3) = hat2.gc();
        assert!(deleted > 0);
        assert_eq!(live3, 0);
    }
}
