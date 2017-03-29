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


use backend::StoreBackend;
use blob;
use capnp;
use db;
use errors::HatError;
use filetime;
use gc::{self, Gc, GcRc};
use hash;
use key;
use root_capnp;
use snapshot;
use std::fs;
use std::path::PathBuf;
use std::str;
use std::sync::{Arc, mpsc};
use tags;
use util::{Process, FileIterator};
use void::Void;
use rustc_serialize::hex::ToHex;

mod family;
mod insert_path_handler;
mod walker;
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

    fn get_data(&self, hash_id: gc::Id, family_id: gc::Id) -> Result<db::GcData, Self::Err> {
        Ok(self.hash_index.read_gc_data(hash_id, family_id))
    }
    fn update_data<F: db::UpdateFn>(&mut self,
                                    hash_id: gc::Id,
                                    family_id: gc::Id,
                                    f: F)
                                    -> Result<db::GcData, Self::Err> {
        Ok(self.hash_index.update_gc_data(hash_id, family_id, f))
    }
    fn update_all_data_by_family<F: db::UpdateFn, I: Iterator<Item = F>>
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

fn hash_index_name(root: PathBuf) -> String {
    concat_filename(root, "hash_index.sqlite3")
}

fn synthetic_roots_family() -> String {
    From::from("__hat__roots__")
}

struct SnapshotLister<'a, B: StoreBackend> {
    backend: &'a key::HashStoreBackend<B>,
    family: &'a Family<B>,
    // Invariant: Only save the chunkref if it is a directory
    queue: Vec<(hash::tree::HashRef, bool)>,
}

impl<'a, B: StoreBackend> SnapshotLister<'a, B> {
    fn fetch(&mut self, hash_ref: hash::tree::HashRef) -> Result<(), HatError> {
        let res = self.family.fetch_dir_data(hash_ref, self.backend.clone())?;
        for (entry, hash_ref) in res.into_iter().rev() {
            self.queue.push((hash_ref, !entry.data_hash.is_some()));
        }
        Ok(())
    }
}

impl<'a, B: StoreBackend> Iterator for SnapshotLister<'a, B> {
    type Item = Result<hash::tree::HashRef, HatError>;
    fn next(&mut self) -> Option<Result<hash::tree::HashRef, HatError>> {
        match self.queue.pop() {
            Some((ref hash_ref, ref fetch_dir)) if *fetch_dir => {
                if let Err(e) = self.fetch(hash_ref.clone()) {
                    // We yield the error now, but save the hash so
                    // we can output it next time (without recursion)
                    self.queue.push((hash_ref.clone(), false));
                    return Some(Err(e));
                }
                Some(Ok(hash_ref.clone()))
            }
            Some((ref hash_ref, _)) => Some(Ok(hash_ref.clone())),
            None => None,
        }
    }
}

fn list_snapshot<'a, B: StoreBackend>(backend: &'a key::HashStoreBackend<B>,
                                      family: &'a Family<B>,
                                      dir_hash: hash::tree::HashRef)
                                      -> SnapshotLister<'a, B> {
    SnapshotLister {
        backend: backend,
        family: family,
        queue: vec![(dir_hash, true)],
    }
}


impl<B: StoreBackend> HatRc<B> {
    pub fn open_repository(repository_root: PathBuf,
                           backend: Arc<B>,
                           max_blob_size: usize)
                           -> Result<HatRc<B>, HatError> {
        let hash_index_path = hash_index_name(repository_root.clone());
        let db_p = Arc::new(db::Index::new(&hash_index_path)?);
        let si_p = snapshot::SnapshotIndex::new(db_p.clone());
        let hi_p = Arc::new(hash::HashIndex::new(db_p.clone())?);
        let bi_p = Arc::new(blob::BlobIndex::new(db_p.clone())?);

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
        hat.resume()?;

        Ok(hat)
    }

    #[cfg(test)]
    pub fn new_for_testing(backend: Arc<B>, max_blob_size: usize) -> Result<HatRc<B>, HatError> {
        let db_p = Arc::new(db::Index::new_for_testing());
        let si_p = snapshot::SnapshotIndex::new(db_p.clone());
        let bi_p = Arc::new(blob::BlobIndex::new(db_p.clone()).unwrap());
        let hi_p = Arc::new(hash::HashIndex::new(db_p).unwrap());

        let bs_p = Arc::new(blob::BlobStore::new(bi_p.clone(), backend.clone(), max_blob_size));

        let gc_backend = GcBackend { hash_index: hi_p.clone() };
        let gc = gc::Gc::new(gc_backend);

        let mut hat = Hat {
            repository_root: None,
            snapshot_index: si_p,
            hash_index: hi_p,
            blob_index: bi_p,
            blob_store: bs_p,
            blob_max_size: max_blob_size,
            backend: backend,
            gc: gc,
        };

        // Resume any unfinished commands.
        hat.resume()?;

        Ok(hat)
    }

    pub fn hash_tree_writer(&self,
                            leaf: blob::LeafType)
                            -> hash::tree::SimpleHashTreeWriter<key::HashStoreBackend<B>> {
        hash::tree::SimpleHashTreeWriter::new(leaf, 8, self.hash_backend())
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

        let ki_p = Arc::new(key::KeyIndex::new(&key_index_path)?);

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

    pub fn delete_all_snapshots(&mut self) -> Result<(), HatError> {
        // This function deletes ALL snapshots from ALL families, including meta snapshots used
        // for recovery. After calling this function and running the GC, all blobs should be gone.
        for snapshot in self.snapshot_index.list_all() {
            let id = snapshot.info.snapshot_id;
            self.deregister_by_name(snapshot.family_name, id)?;
        }
        Ok(())
    }

    pub fn meta_commit(&mut self) -> Result<(), HatError> {
        let all_snapshots = self.snapshot_index.list_all();

        // FIXME(jos): Split into N-entries per append().
        let mut message = capnp::message::Builder::new_default();
        let mut all_root_ids = vec![];

        {
            let root = message.init_root::<root_capnp::snapshot_list::Builder>();
            let mut snapshots = root.init_snapshots(all_snapshots.len() as u32);

            for (i, snapshot) in all_snapshots.into_iter().enumerate() {
                let mut s = snapshots.borrow().get(i as u32);
                s.set_id(snapshot.info.snapshot_id);
                s.set_family_name(&snapshot.family_name);
                s.set_msg(&snapshot.msg.unwrap_or("".to_owned()));
                let hash_ref = snapshot.hash_ref.unwrap();
                hash::tree::HashRef::from_bytes(&mut hash_ref.as_ref())
                    ?
                    .populate_msg(s.init_hash_ref());

                if snapshot.family_name == synthetic_roots_family() {
                    all_root_ids.push(snapshot.info.snapshot_id);
                }
            }
        }

        let mut listing = Vec::new();
        capnp::serialize_packed::write_message(&mut listing, &message).unwrap();

        let family = self.open_family(synthetic_roots_family()).unwrap();
        family.snapshot_direct(key::Entry::new(None, From::from("root"), None),
                             false,
                             Some(FileIterator::from_bytes(listing)))?;

        family.flush()?;
        self.commit(&family, None)?;

        // Delete old root snapshots, but always keep the past 10.
        // FIXME(jos): Number of meta snapshots to keep to be configurable.
        all_root_ids.sort();
        for id in all_root_ids.iter().rev().skip(10) {
            self.deregister_by_name(synthetic_roots_family(), *id)?;
        }

        Ok(())
    }

    fn recover_root(&mut self) -> Result<Option<hash::tree::HashRef>, HatError> {
        let blobs = self.blob_store.list_by_tag(tags::Tag::Done);
        info!("{} blobs to investigate", blobs.len());
        for b in blobs.into_iter() {
            info!("Inspecting blob: {}", b.name.to_hex());
            for r in self.blob_store.retrieve_refs(b)?.unwrap_or(vec![]) {
                match (r.leaf, r.info.as_ref()) {
                    (blob::LeafType::TreeList, Some(ref i)) if i.hat_snapshot_top &&
                                                               i.name == b"__hat__roots__" => {
                        // FIXME(jos): Allow skipping first root in case it is not working.
                        return Ok(Some(r.clone()));
                    }
                    // FIXME(jos): Recover file-listings stored after commit
                    (blob::LeafType::TreeList, _) => {
                        warn!("Skipping directory listing: {}", r.hash.bytes.to_hex())
                    }
                    (blob::LeafType::FileChunk, _) => {
                        warn!("Skipping file contents: {}", r.hash.bytes.to_hex())
                    }
                }
            }
        }
        Ok(None)
    }

    pub fn recover(&mut self) -> Result<(), HatError> {
        self.blob_store.recover()?;
        let root_href = self.recover_root()?.expect("Failed to find a commit-ed root.");

        info!("Recovering using root: {}", root_href.hash.bytes.to_hex());
        info!(".. from blob: {}",
              root_href.persistent_ref.blob_name.to_hex());

        let family = self.open_family(synthetic_roots_family()).unwrap();
        let list = family.fetch_dir_data(root_href.clone(), self.hash_backend())?;
        assert_eq!(list.len(), 1);

        let (entry, list_ref) = list.into_iter().next().unwrap();
        assert_eq!(entry.info.name, b"root");

        for msg in hash::tree::LeafIterator::new(self.hash_backend(), list_ref)?.unwrap() {
            let message_reader =
                capnp::serialize_packed::read_message(&mut &msg[..],
                                                      capnp::message::ReaderOptions::new())
                    .unwrap();
            let snapshot_list = message_reader.get_root::<root_capnp::snapshot_list::Reader>()
                .unwrap();

            for s in snapshot_list.get_snapshots().unwrap().iter() {
                let hash_ref = hash::tree::HashRef::read_msg(&s.get_hash_ref().unwrap()).unwrap();
                self.snapshot_index
                    .recover(s.get_id(),
                             s.get_family_name()
                                 .unwrap(),
                             s.get_msg().unwrap(),
                             &hash_ref,
                             Some(db::SnapshotWorkStatus::RecoverInProgress));
            }
        }

        self.flush_snapshot_index();
        self.resume()?;

        // Register the newly found root. This is needed because root cannot contain itself.
        let latest = self.snapshot_index.latest(&synthetic_roots_family());
        let next_id = match latest {
            Some((_, _, Some(ref href))) if href.hash == root_href.hash => return Ok(()),  // already have it.
            Some((info, _, _)) => info.snapshot_id + 1,
            None => 1,
        };

        self.snapshot_index.recover(next_id,
                                    &synthetic_roots_family(),
                                    "",
                                    &root_href,
                                    Some(db::SnapshotWorkStatus::RecoverInProgress));
        self.flush_snapshot_index();
        self.resume()?;

        Ok(())
    }

    fn recover_snapshot(&mut self,
                        family_name: String,
                        info: db::SnapshotInfo,
                        final_hash: &hash::tree::HashRef)
                        -> Result<(), HatError> {
        fn recover_entry<B: StoreBackend>(hashes: &hash::HashIndex,
                                          blobs: &blob::BlobStore<B>,
                                          node: family::recover::Node) {
            let mut pref = node.href.persistent_ref.clone();
            pref.blob_id = blobs.find(&pref.blob_name).map(|b| b.id);

            fn entry(href: hash::tree::HashRef, childs: Option<Vec<i64>>) -> hash::Entry {
                hash::Entry {
                    hash: href.hash,
                    persistent_ref: Some(href.persistent_ref),
                    node: href.node,
                    leaf: href.leaf,
                    childs: childs,
                }
            }

            // Convert child hashes to child IDs.
            let child_ids = match node.childs {
                Some(ref hs) => {
                    Some(hs.iter()
                        .map(|h| match hashes.reserve(&entry(h.clone(), None)) {
                            hash::ReserveResult::HashKnown(id) |
                            hash::ReserveResult::ReserveOk(id) => id,
                        })
                        .collect())
                }
                None => None,
            };

            // Now insert the hash information if needed.
            let entry = hash::Entry {
                hash: node.href.hash,
                persistent_ref: Some(pref.clone()),
                node: node.href.node,
                leaf: node.href.leaf,
                childs: child_ids,
            };

            if let hash::ReserveResult::HashKnown(..) = hashes.reserve(&entry) {
                if hashes.reserved_id(&entry).is_none() {
                    // This is a repeat hash that was already fully committed.
                    return;
                }
                // Update previously reserved hash.
                hashes.update_reserved(entry.clone());
            }
            // Commit hash.
            hashes.commit(&entry.hash, pref);
        }

        let mut dir_v = family::recover::DirVisitor::new();
        let mut file_v = family::recover::FileVisitor::new();
        let mut walk = walker::Walker::new(self.hash_backend(), final_hash.clone())?;

        let mut tops = vec![];
        while {
            for node in file_v.nodes() {
                recover_entry(&self.hash_index, &self.blob_store, node);
            }
            tops.append(&mut file_v.tops());
            for node in dir_v.nodes() {
                recover_entry(&self.hash_index, &self.blob_store, node);
            }
            walk.resume(&mut file_v, &mut dir_v)?
        } {}

        let family = self.open_family(family_name.clone())
            .expect(&format!("Could not open family '{}'", family_name));

        // Recover hashes for tree-tops. These are also registered with the GC.
        self.hash_index.set_all_tags(tags::Tag::Done);
        for hash in tops {
            if hash != final_hash.hash {
                let id = self.hash_index.get_id(&hash).unwrap();
                self.hash_index.set_tag(id, tags::Tag::Reserved);
            }
        }
        self.flush_blob_store();

        let final_id = self.hash_index.get_id(&final_hash.hash).expect("final hash has no id");
        self.gc.register_final(&info, final_id)?;
        self.commit_finalize(&family, info, &final_hash.hash)?;
        Ok(())
    }

    pub fn resume(&mut self) -> Result<(), HatError> {
        let need_work = self.snapshot_index.list_not_done();

        for snapshot in need_work {
            match snapshot.status {
                db::SnapshotWorkStatus::CommitInProgress |
                db::SnapshotWorkStatus::RecoverInProgress => {
                    let done_hash_opt = match snapshot.hash {
                        None => None,
                        Some(ref h) => {
                            let status_res = self.hash_index
                                .get_id(h)
                                .map(|id| self.gc.status(id));
                            let status_opt = match status_res {
                                None => None,
                                Some(res) => res?,
                            };
                            match status_opt {
                                None => None,  // We did not fully commit.
                                Some(gc::Status::InProgress) |
                                Some(gc::Status::Complete) => Some(h),
                            }
                        }
                    };
                    match (done_hash_opt, snapshot.status) {
                        (Some(hash), db::SnapshotWorkStatus::CommitInProgress) => {
                            self.commit_finalize_by_name(snapshot.family_name, snapshot.info, hash)?
                        }
                        (None, db::SnapshotWorkStatus::CommitInProgress) => {
                            println!("Resuming commit of: {}", snapshot.family_name);
                            self.commit_by_name(snapshot.family_name, Some(snapshot.info))?
                        }
                        (None, db::SnapshotWorkStatus::RecoverInProgress) => {
                            println!("Resuming recovery of: {}", snapshot.family_name);
                            let hash_ref_bytes = snapshot.hash_ref
                                .ok_or("Recovered hash tree has no root hash")?;
                            let hash_ref =
                                hash::tree::HashRef::from_bytes(&mut &hash_ref_bytes[..])?;
                            self.recover_snapshot(snapshot.family_name, snapshot.info, &hash_ref)?
                        }
                        (hash, status) => {
                            return Err(From::from(format!("unexpected state: ({:?}, {:?})",
                                                          hash,
                                                          status)))
                        }
                    }
                }
                db::SnapshotWorkStatus::CommitComplete => {
                    match snapshot.hash {
                        Some(ref h) => {
                            self.commit_finalize_by_name(snapshot.family_name, snapshot.info, h)?
                        }
                        None => {
                            // This should not happen.
                            return Err(From::from(format!("Snapshot {:?} is fully registered \
                                                           in GC, but has no hash",
                                                          snapshot)));
                        }
                    }
                }
                db::SnapshotWorkStatus::DeleteInProgress => {
                    let hash = snapshot.hash.expect("Snapshot has no hash");
                    let hash_id = self.hash_index
                        .get_id(&hash)
                        .expect("Snapshot hash not recognized");
                    let status = self.gc.status(hash_id)?;
                    match status {
                        None |
                        Some(gc::Status::InProgress) => {
                            println!("Resuming delete of: {} #{:?}",
                                     snapshot.family_name,
                                     snapshot.info.snapshot_id);
                            self.deregister_by_name(snapshot.family_name,
                                                    snapshot.info.snapshot_id)?
                        }
                        Some(gc::Status::Complete) => {
                            self.deregister_finalize_by_name(snapshot.family_name,
                                                             snapshot.info,
                                                             hash_id)?
                        }
                    }
                }
                db::SnapshotWorkStatus::DeleteComplete => {
                    let hash = snapshot.hash.expect("Snapshot has no hash");
                    let hash_id = self.hash_index
                        .get_id(&hash)
                        .expect("Snapshot hash not recognized");
                    self.deregister_finalize_by_name(snapshot.family_name, snapshot.info, hash_id)?
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
                          resume_info: Option<db::SnapshotInfo>)
                          -> Result<(), HatError> {
        let family = self.open_family(family_name)?;
        self.commit(&family, resume_info)?;

        Ok(())
    }

    pub fn commit(&mut self,
                  family: &Family<B>,
                  resume_info: Option<db::SnapshotInfo>)
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

        // Commit metadata while registering needed data-hashes (files and dirs).
        let top_ref = {
            let mut local_family: Family<B> = (*family).clone();

            let local_hash_index = self.hash_index.clone();
            local_family.commit(&|hash| {
                    let id = local_hash_index.get_id(hash).expect("Top hash");
                    local_hash_index.set_tag(id, tags::Tag::Reserved);
                })?
        };

        // Push any remaining data to external storage.
        // This also flushes our hashes from the memory index, so we can tag them.
        self.flush_blob_store();

        // Tag 2:
        // We update the snapshot entry with the tree hash, which we then register.
        // When the GC has seen the final hash, we flush everything so far.
        self.snapshot_index.update(&snap_info, &top_ref.hash, &top_ref);
        self.flush_snapshot_index();

        // Register the final hash.
        // At this point, the GC should still be able to either resume or rollback safely.
        // After a successful flush, all GC work is done.
        // The GC must be able to tell if it has completed or not.
        let hash_id = self.hash_index.get_id(&top_ref.hash).expect("Hash does not exist");
        self.gc.register_final(&snap_info, hash_id)?;
        family.flush()?;
        self.commit_finalize(family, snap_info, &top_ref.hash)?;

        Ok(())
    }

    fn commit_finalize_by_name(&mut self,
                               family_name: String,
                               snap_info: db::SnapshotInfo,
                               hash: &hash::Hash)
                               -> Result<(), HatError> {
        let family = self.open_family(family_name)?;
        self.commit_finalize(&family, snap_info, &hash)?;

        Ok(())
    }

    fn commit_finalize(&mut self,
                       family: &Family<B>,
                       snap_info: db::SnapshotInfo,
                       hash: &hash::Hash)
                       -> Result<(), HatError> {
        // Commit locally. Let the GC perform any needed cleanup.
        self.snapshot_index.ready_commit(&snap_info);
        self.flush_snapshot_index();

        let hash_id = self.hash_index.get_id(hash).expect("Hash does not exist");
        self.gc.register_cleanup(&snap_info, hash_id)?;
        family.flush()?;

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
        let (_info, _dir_hash, dir_ref) = match self.snapshot_index.latest(&family_name) {
            Some((i, h, Some(r))) => (i, h, r),
            _ => {
                panic!("Tried to checkout family '{}' before first completed commit",
                       family_name)
            }
        };

        let family = self.open_family(family_name.clone())
            .expect(&format!("Could not open family '{}'", family_name));

        let mut output_dir = output_dir;
        self.checkout_dir_ref(&family, &mut output_dir, dir_ref)
    }

    fn checkout_dir_ref(&self,
                        family: &Family<B>,
                        output: &mut PathBuf,
                        dir_hash: hash::tree::HashRef)
                        -> Result<(), HatError> {
        fs::create_dir_all(&output).unwrap();
        for (entry, hash_ref) in family.fetch_dir_data(dir_hash, self.hash_backend())? {
            assert!(entry.info.name.len() > 0);

            output.push(str::from_utf8(&entry.info.name[..]).unwrap());
            println!("{}", output.display());

            if entry.data_hash.is_some() {
                let mut fd = fs::File::create(&output).unwrap();
                let tree_opt = hash::tree::LeafIterator::new(self.hash_backend(), hash_ref)?;
                if let Some(tree) = tree_opt {
                    family.write_file_chunks(&mut fd, tree);
                }
            } else {
                self.checkout_dir_ref(family, output, hash_ref)?;
            }

            if let Some(perms) = entry.info.permissions {
                fs::set_permissions(&output, perms)?;
            }

            if let (Some(m), Some(a)) = (entry.info.modified_ts_secs, entry.info.accessed_ts_secs) {
                let atime = filetime::FileTime::from_seconds_since_1970(a, 0 /* nanos */);
                let mtime = filetime::FileTime::from_seconds_since_1970(m, 0 /* nanos */);
                filetime::set_file_times(&output, atime, mtime).unwrap();
            }

            output.pop();
        }
        Ok(())
    }

    pub fn deregister_by_name(&mut self,
                              family_name: String,
                              snapshot_id: i64)
                              -> Result<(), HatError> {
        let family = self.open_family(family_name)?;
        self.deregister(&family, snapshot_id)?;

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
                for hash in list_snapshot(&hash_backend, &family, dir_ref) {
                    let hash = hash.expect("Invalid hash ref");
                    match hash_index.get_id(&hash.hash) {
                        Some(id) => id_sender.send(id).unwrap(),
                        None => panic!("Unexpected reply from hash index."),
                    }
                }
                id_receiver
            };

            gc.deregister(&info, final_ref, listing)?;
        }
        family.flush()?;

        self.deregister_finalize(family, info, final_ref)
    }

    fn deregister_finalize_by_name(&mut self,
                                   family_name: String,
                                   snap_info: db::SnapshotInfo,
                                   hash_id: gc::Id)
                                   -> Result<(), HatError> {
        let family = self.open_family(family_name)?;
        self.deregister_finalize(&family, snap_info, hash_id)?;

        Ok(())
    }

    fn deregister_finalize(&mut self,
                           family: &Family<B>,
                           snap_info: db::SnapshotInfo,
                           final_ref: gc::Id)
                           -> Result<(), HatError> {
        // Mark the snapshot to enable resuming.
        self.snapshot_index.ready_delete(&snap_info);
        self.flush_snapshot_index();

        // Clear GC state.
        self.gc.register_cleanup(&snap_info, final_ref)?;
        family.flush()?;

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
        self.gc.list_unused_ids(sender)?;
        for id in receiver.iter() {
            deleted_hashes += 1;
            self.hash_index.delete(id);
        }
        self.hash_index.flush();
        // Mark used blobs.
        let entries = self.hash_index.list();
        self.blob_store.tag_all(tags::Tag::InProgress);

        let mut live_blobs = 0;
        for entry in entries {
            if let Some(pref) = entry.persistent_ref {
                live_blobs += 1;
                self.blob_store.tag(pref, tags::Tag::Reserved);
            }
        }
        // Anything still marked "in progress" is not referenced by any hash.
        self.blob_store.delete_by_tag(tags::Tag::InProgress)?;
        self.blob_store.tag_all(tags::Tag::Done);
        self.blob_store.flush();

        Ok((deleted_hashes, live_blobs))
    }

    fn hash_backend(&self) -> key::HashStoreBackend<B> {
        key::HashStoreBackend::new(self.hash_index.clone(), self.blob_store.clone())
    }
}
