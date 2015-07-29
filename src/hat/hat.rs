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


use rustc_serialize::{json};
use rustc_serialize::json::{ToJson};
use std::str;
use std::collections::{BTreeMap};

use process::{Process};
use tags;

use blob_index::{BlobIndex};
use blob_store::{BlobStore, BlobStoreProcess, BlobStoreBackend};

use hash_index;
use hash_index::{GcData, Hash, HashIndex, HashIndexProcess};
use key_index::{KeyIndex, KeyEntry};
use key_store::{KeyStore, KeyStoreProcess};
use key_store;
use snapshot_index::{SnapshotInfo, SnapshotIndex, SnapshotIndexProcess};
use snapshot_index;

use hash_tree;
use listdir;
use gc;
use gc_noop;

use std::path::PathBuf;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::io::{self, Read, Write};

use std::sync::{self, mpsc, atomic};
use std::thread;

use time;


struct GcBackend{
  hash_index: HashIndexProcess,
}

impl gc::GcBackend for GcBackend {
  fn get_data(&self, hash_id: i64, family_id: i64) -> GcData {
    match self.hash_index.send_reply(hash_index::Msg::ReadGcData(hash_id, family_id)) {
      hash_index::Reply::CurrentGcData(data) => data,
      _ => panic!("Unexpected reply from hash index."),
    }
  }
  fn update_data(&self, hash_id: i64, family_id: i64, f: gc::UpdateFn) -> GcData {
    match self.hash_index.send_reply(hash_index::Msg::UpdateGcData(hash_id, family_id, f)) {
      hash_index::Reply::CurrentGcData(data) => data,
      _ => panic!("Unexpected reply from hash index."),
    }
  }
  fn update_all_data_by_family(&self, family_id: i64, fs: mpsc::Receiver<gc::UpdateFn>)
  {
    match self.hash_index.send_reply(hash_index::Msg::UpdateFamilyGcData(family_id, fs)) {
      hash_index::Reply::Ok => (),
      _ => panic!("Unexpected reply from hash index."),
    }
  }

  fn get_tag(&self, hash_id: i64) -> Option<tags::Tag> {
    match self.hash_index.send_reply(hash_index::Msg::GetTag(hash_id)) {
      hash_index::Reply::HashTag(tag) => tag,
      _ => panic!("Unexpected reply from hash index."),
    }
  }

  fn set_tag(&self, hash_id: i64, tag: tags::Tag) {
    match self.hash_index.send_reply(hash_index::Msg::SetTag(hash_id, tag)) {
      hash_index::Reply::Ok => (),
      _ => panic!("Unexpected reply from hash index."),
    }
  }

  fn set_all_tags(&self, tag: tags::Tag) {
    match self.hash_index.send_reply(hash_index::Msg::SetAllTags(tag)) {
      hash_index::Reply::Ok => (),
      _ => panic!("Unexpected reply from hash index."),
    }
  }

  fn reverse_refs(&self, hash_id: i64) -> Vec<i64> {
    panic!("Not implemented yet")
  }

  fn list_ids_by_tag(&self, tag: tags::Tag) -> mpsc::Receiver<i64> {
    panic!("Not implemented yet")
  }

  fn list_snapshot_refs(&self, snapshot: SnapshotInfo) -> mpsc::Receiver<i64> {
    panic!("Not implemented yet")
  }
}


pub struct Hat<B> {
  repository_root: PathBuf,
  snapshot_index: SnapshotIndexProcess,
  blob_store: BlobStoreProcess,
  hash_index: HashIndexProcess,
  blob_backend: B,
  hash_backend: key_store::HashStoreBackend,
  gc: Box<gc::Gc>,
  max_blob_size: usize,
}

fn concat_filename(a: &PathBuf, b: String) -> String {
  let mut result = a.clone();
  result.push(&b);
  result.into_os_string().into_string().unwrap()
}

fn snapshot_index_name(root: &PathBuf) -> String {
  concat_filename(root, "snapshot_index.sqlite3".to_string())
}

fn blob_index_name(root: &PathBuf) -> String {
  concat_filename(root, "blob_index.sqlite3".to_string())
}

fn hash_index_name(root: &PathBuf) -> String {
  concat_filename(root, "hash_index.sqlite3".to_string())
}

impl <B: 'static + BlobStoreBackend + Clone + Send> Hat<B> {
  pub fn open_repository(repository_root: &PathBuf, backend: B, max_blob_size: usize)
                        -> Hat<B> {
    let snapshot_index_path = snapshot_index_name(repository_root);
    let blob_index_path = blob_index_name(repository_root);
    let hash_index_path = hash_index_name(repository_root);
    let si_p = Process::new(Box::new(move|| { SnapshotIndex::new(snapshot_index_path) }));
    let bi_p = Process::new(Box::new(move|| { BlobIndex::new(blob_index_path) }));
    let hi_p = Process::new(Box::new(move|| { HashIndex::new(hash_index_path) }));

    let local_blob_index = bi_p.clone();
    let local_backend = backend.clone();
    let bs_p = Process::new(Box::new(move|| {
      BlobStore::new(local_blob_index, local_backend, max_blob_size) }));

    let gc_backend = GcBackend{hash_index: hi_p.clone()};
    let gc = gc_noop::GcNoop::new(Box::new(gc_backend));

    Hat{repository_root: repository_root.clone(),
        snapshot_index: si_p,
        hash_index: hi_p.clone(),
        blob_store: bs_p.clone(),
        blob_backend: backend.clone(),
        hash_backend: key_store::HashStoreBackend::new(hi_p, bs_p),
        gc: Box::new(gc),
        max_blob_size: max_blob_size,
    }
  }

  pub fn open_family(&self, name: String) -> Option<Family> {
    // We setup a standard pipeline of processes:
    // KeyStore -> KeyIndex
    //          -> HashIndex
    //          -> BlobStore -> BlobIndex

    let key_index_path = concat_filename(&self.repository_root, name.clone());
    let ki_p = Process::new(Box::new(move|| { KeyIndex::new(key_index_path) }));

    let local_ks = KeyStore::new(ki_p.clone(), self.hash_index.clone(), self.blob_store.clone());
    let ks_p = Process::new(Box::new(move|| { local_ks }));

    let ks = KeyStore::new(ki_p, self.hash_index.clone(), self.blob_store.clone());
    Some(Family{name: name, key_store: ks, key_store_process: ks_p})
  }

  pub fn commit(&self, family_name: String) {
    let family = self.open_family(family_name.clone()).expect(
      &format!("Could not open family '{}'", family_name));

    //  Tag 1:
    //  Reserve the snapshot and commit the reservation.
    //  Register all but the last hashes.
    //  (the last hash is a special-case, as the GC use it to save meta-data for resuming)
    let snapshot_info = match self.snapshot_index.send_reply(
      snapshot_index::Msg::Reserve(family_name.clone())) {
        snapshot_index::Reply::Reserved(info) => info,
        _ => panic!("Invalid reply from snapshot index"),
    };
    self.flush_snapshot_index();

    fn get_hash_id(index: &HashIndexProcess, hash: Hash) -> Result<i64, String> {
      match index.send_reply(hash_index::Msg::GetID(hash)) {
        hash_index::Reply::HashID(id) => return Ok(id),
        _ => return Err("Tried to register an unknown hash".to_string()),
      }
    }

    // Prepare.
    let (hash_sender, hash_receiver) = mpsc::channel();
    let (hash_id_sender, hash_id_receiver) = mpsc::channel();

    let local_hash_index = self.hash_index.clone();
    thread::spawn(move|| {
      for hash in hash_receiver.iter() {
        hash_id_sender.send(get_hash_id(&local_hash_index, hash).unwrap()).unwrap();
      }
    });

    // Commit meta data-while registering needed data-hashes (files and dirs).
    let (hash, top_ref) = {
      let mut local_family = family.clone();
      let mut fut = sync::Future::spawn(move|| local_family.commit(hash_sender));

      self.gc.register(snapshot_info.clone(), hash_id_receiver);

      fut.get()
    };

    // Tag 2:
    // We update the snapshot entry with the tree hash, which we then register.
    // When the GC has seen the final hash, we flush everything so far.
    match self.snapshot_index.send_reply(
      snapshot_index::Msg::Update(snapshot_info.clone(), hash.clone(), top_ref)) {
        snapshot_index::Reply::UpdateOk => (),
        _ => panic!("Snapshot index update failed"),
    };
    self.flush_snapshot_index();

    // Register the final hash.
    // At this point, the GC should still be able to either resume or rollback safely.
    // After a successful flush, all GC work is done.
    // The GC must be able to tell if it has completed or not.
    let hash_id = get_hash_id(&self.hash_index, hash).unwrap();
    self.gc.register_final(snapshot_info.clone(), hash_id);
    family.flush();
    self.flush_snapshot_index();

    // Tag 3:
    // Commit locally. Let the GC perform any needed cleanup.
    // If we resume at this stage, we know that commit was successful and all work was completed.
    // Only cleanup remains.
    match self.snapshot_index.send_reply(snapshot_index::Msg::ReadyCommit(snapshot_info.clone())) {
      snapshot_index::Reply::UpdateOk => (),
      _ => panic!("Invalid reply from snapshot index"),
    };
    self.flush_snapshot_index();

    self.gc.register_cleanup(snapshot_info.clone(), hash_id);
    family.flush();
    self.flush_snapshot_index();

    // Tag 0: All is done.
    match self.snapshot_index.send_reply(snapshot_index::Msg::Commit(snapshot_info)) {
      snapshot_index::Reply::CommitOk => (),
      _ => panic!("Invalid reply from snapshot index"),
    };
    self.flush_snapshot_index();
  }

  pub fn flush_snapshot_index(&self) {
    match self.snapshot_index.send_reply(snapshot_index::Msg::Flush) {
      snapshot_index::Reply::FlushOk => (),
      _ => panic!("Invalid reply from snapshot index"),
    };
  }

  pub fn checkout_in_dir(&self, family_name: String, output_dir: PathBuf) {
    // Extract latest snapshot info:
    let (dir_hash, dir_ref) =
      match self.snapshot_index.send_reply(snapshot_index::Msg::Latest(family_name.clone())) {
        snapshot_index::Reply::Latest(Some((h, r))) => (h, r),
        snapshot_index::Reply::Latest(None) =>
          panic!("Tried to checkout family '{}' before first commit", family_name),
        _ => panic!("Unexpected result from snapshot index"),
      };

    let family = self.open_family(family_name.clone()).expect(
      &format!("Could not open family '{}'", family_name));

    let mut output_dir = output_dir;
    self.checkout_dir_ref(&family, &mut output_dir, dir_hash, dir_ref);
  }

  fn checkout_dir_ref(&self, family: &Family, output: &mut PathBuf, dir_hash: Hash, dir_ref: Vec<u8>) {
    fs::create_dir_all(&output).unwrap();
    for o in family.fetch_dir_data(dir_hash, dir_ref, self.hash_backend.clone()) {
      for d in o.as_array().unwrap().iter() {
        if let Some(ref m) = d.as_object() {
          let name: Vec<u8> = m.get("name").unwrap().as_array()
            .unwrap().iter().map(|x| x.as_i64().unwrap() as u8).collect();
          output.push(str::from_utf8(&name[..]).unwrap());
          println!("{}", output.display());

          // TODO(jos): Replace all uses of JSON with either protocol bufffers or cap'n proto.
          let bytes = m.get("dir_hash").or(m.get("data_hash"))
            .and_then(|x| x.as_array()).unwrap().iter().map(|y| y.as_i64().unwrap() as u8);
          let hash = Hash{bytes: bytes.collect()};

          let pref = m.get("dir_ref").or(m.get("data_ref"))
            .and_then(|x| x.as_array()).unwrap().iter().map(|x| x.as_i64().unwrap() as u8).collect();

          if m.contains_key("dir_hash") {
            self.checkout_dir_ref(family, output, hash, pref);
          } else if m.contains_key("data_hash") {
            let mut fd = fs::File::create(&output).unwrap();
            let tree_opt = hash_tree::SimpleHashTreeReader::open(self.hash_backend.clone(), hash, pref);
            if let Some(tree) = tree_opt {
              family.write_file_chunks(&mut fd, tree);
            }
          }
          output.pop();
        }
      }
    }
  }
}

struct FileEntry {
  name: Vec<u8>,
  id: Option<u64>,
  parent_id: Option<u64>,
  metadata: fs::Metadata,
  full_path: PathBuf,
  link_path: Option<PathBuf>,
}

impl FileEntry {
  fn new(full_path: PathBuf,
         parent: Option<u64>) -> Result<FileEntry, String> {
    let filename_opt = full_path.file_name().and_then(|n| n.to_str());
    let link_path = fs::read_link(&full_path).ok();

    if filename_opt.is_some() {
      Ok(FileEntry{
        name: filename_opt.unwrap().bytes().collect(),
        id: None,
        parent_id: parent.clone(),
        metadata: fs::metadata(&full_path).unwrap(),
        full_path: full_path.clone(),
        link_path: link_path,
      })
    }
    else { Err("Could not parse filename."[..].to_string()) }
  }

  fn file_iterator(&self) -> io::Result<FileIterator> {
    FileIterator::new(&self.full_path)
  }

  fn is_directory(&self) -> bool { self.metadata.is_dir() }
  fn is_symlink(&self) -> bool { self.link_path.is_some() }
  fn is_file(&self) -> bool { self.metadata.is_file() }
}

impl Clone for FileEntry {
  fn clone(&self) -> FileEntry {
    FileEntry{
      name:self.name.clone(),
      id: self.id.clone(),
      parent_id:self.parent_id.clone(),
      metadata: fs::metadata(&self.full_path).unwrap(),
      full_path: self.full_path.clone(),
      link_path: self.link_path.clone(),
    }
  }
}

impl KeyEntry<FileEntry> for FileEntry {
  fn name(&self) -> Vec<u8> {
    self.name.clone()
  }
  fn id(&self) -> Option<u64> {
    self.id.clone()
  }
  fn parent_id(&self) -> Option<u64> {
    self.parent_id.clone()
  }

  fn size(&self) -> Option<u64> {
    Some(self.metadata.len())
  }

  fn created(&self) -> Option<u64> {
    None
  }
  fn modified(&self) -> Option<u64> {
    Some(self.metadata.mtime() as u64)
  }
  fn accessed(&self) -> Option<u64> {
    Some(self.metadata.atime() as u64)
  }

  fn permissions(&self) -> Option<u64> {
    None
  }
  fn user_id(&self) -> Option<u64> {
    None
  }
  fn group_id(&self) -> Option<u64> {
    None
  }
  fn with_id(self, id: Option<u64>) -> FileEntry {
    let mut x = self;
    x.id = id;
    return x;
  }
}

struct FileIterator {
  file: fs::File
}

impl FileIterator {
  fn new(path: &PathBuf) -> io::Result<FileIterator> {
    match fs::File::open(path) {
      Ok(f) => Ok(FileIterator{file: f}),
      Err(e) => Err(e),
    }
  }
}

impl Iterator for FileIterator {
  type Item = Vec<u8>;

  fn next(&mut self) -> Option<Vec<u8>> {
    let mut buf = vec![0u8; 128*1024];
    match self.file.read(&mut buf[..]) {
      Err(_) => None,
      Ok(size) if size == 0 => None,
      Ok(size) => Some(buf[..size].to_vec()),
    }
  }
}


#[derive(Clone)]
struct InsertPathHandler {
  count: sync::Arc<sync::atomic::AtomicIsize>,
  last_print: sync::Arc<sync::Mutex<time::Timespec>>,
  key_store: KeyStoreProcess<FileEntry, FileIterator>,
}

impl InsertPathHandler {
  pub fn new(key_store: KeyStoreProcess<FileEntry, FileIterator>)
             -> InsertPathHandler {
    InsertPathHandler{
      count: sync::Arc::new(sync::atomic::AtomicIsize::new(0)),
      last_print: sync::Arc::new(sync::Mutex::new(time::now().to_timespec())),
      key_store: key_store,
    }
  }
}

impl listdir::PathHandler<Option<u64>> for InsertPathHandler
{
  fn handle_path(&self, parent: Option<u64>, path: PathBuf) -> Option<Option<u64>> {
    let count = self.count.fetch_add(1, atomic::Ordering::SeqCst) + 1;

    if count % 16 == 0 {  // don't hammer the mutex
      let mut guarded_last_print = self.last_print.lock().unwrap();
      let now = time::now().to_timespec();
      if guarded_last_print.sec <= now.sec - 1 {
        println!("#{}: {}", count, path.display());
        *guarded_last_print = now;
      }
    }

    match FileEntry::new(path.clone(), parent) {
      Err(e) => {
        println!("Skipping '{}': {}", path.display(), e.to_string());
      },
      Ok(file_entry) => {
        if file_entry.is_symlink() {
          return None;
        }
        let is_directory = file_entry.is_directory();
        let local_root = path;
        let local_file_entry = file_entry.clone();

        match self.key_store.send_reply(key_store::Msg::Insert(
          file_entry,
          if is_directory { None }
          else { Some(Box::new(move|| {
            match local_file_entry.file_iterator() {
              Err(e) => {println!("Skipping '{}': {}", local_root.display(), e.to_string());
                         None},
              Ok(it) => { Some(it) }
            }
          }))
          }))
        {
          key_store::Reply::Id(id) => {
            if is_directory { return Some(Some(id)) }
          },
          _ => panic!("Unexpected reply from key store."),
        }
      }
    }

    return None;
  }
}


fn try_a_few_times_then_panic<F>(f: F, msg: &str) where F: FnMut() -> bool {
  let mut f = f;
  for _ in (1 as i32..5) {
    if f() { return }
  }
  panic!(msg.to_string());
}


#[derive(Clone)]
struct Family {
  name: String,
  key_store: KeyStore<FileEntry>,
  key_store_process: KeyStoreProcess<FileEntry, FileIterator>,
}

impl Family
{
  pub fn snapshot_dir(&self, dir: PathBuf) {
    let mut handler = InsertPathHandler::new(self.key_store_process.clone());
    listdir::iterate_recursively((PathBuf::from(&dir), None), &mut handler);
  }

  pub fn flush(&self) {
    self.key_store_process.send_reply(key_store::Msg::Flush);
  }

  fn write_file_chunks<HTB: hash_tree::HashTreeBackend + Clone>(
    &self, fd: &mut fs::File, tree: hash_tree::ReaderResult<HTB>)
  {
    for chunk in tree {
      try_a_few_times_then_panic(|| fd.write_all(&chunk[..]).is_ok(), "Could not write chunk.");
    }
    try_a_few_times_then_panic(|| fd.flush().is_ok(), "Could not flush file.");
  }

  pub fn checkout_in_dir(&self, output_dir: PathBuf, dir_id: Option<u64>) {
    let mut path = output_dir;
    for (id, name, _, _, _, hash, _, data_res_opt) in self.list_from_key_store(dir_id).into_iter() {
      // Extend directory with filename:
      path.push(str::from_utf8(&name[..]).unwrap());

      if hash.len() == 0 {
        // This is a directory, recurse!
        fs::create_dir_all(&path).unwrap();
        self.checkout_in_dir(path.clone(), Some(id));
      } else {
        // This is a file, write it
        let mut fd = fs::File::create(&path).unwrap();
        if let Some(data_res) = data_res_opt {
          self.write_file_chunks(&mut fd, data_res);
        }
      }
      // Prepare for next filename:
      path.pop();
    }
  }

  pub fn list_from_key_store(&self, dir_id: Option<u64>) -> Vec<key_store::DirElem> {
    let listing = match self.key_store_process.send_reply(key_store::Msg::ListDir(dir_id)) {
      key_store::Reply::ListResult(ls) => ls,
      _ => panic!("Unexpected result from key store."),
    };

    return listing;
  }

  pub fn fetch_dir_data<HTB: hash_tree::HashTreeBackend + Clone>(
    &self, dir_hash: Hash, dir_ref: Vec<u8>, backend: HTB
    ) -> Vec<json::Json> {
    let mut out = Vec::new();
    let it = hash_tree::SimpleHashTreeReader::open(backend, dir_hash, dir_ref)
      .expect("unable to open dir");
    for chunk in it {
      if chunk.len() == 0 {
        continue;
      }
      let m = json::Json::from_str(str::from_utf8(&chunk[..]).unwrap()).unwrap();
      out.push(m);
    }
    return out;
  }

  pub fn commit(&mut self, hash_ch: mpsc::Sender<Hash>)  -> (Hash, Vec<u8>) {
    let mut top_tree = self.key_store.hash_tree_writer();
    self.commit_to_tree(&mut top_tree, None, hash_ch);
    return top_tree.hash();
  }

  pub fn commit_to_tree(&mut self,
                        tree: &mut hash_tree::SimpleHashTreeWriter<key_store::HashStoreBackend>,
                        dir_id: Option<u64>, hash_ch: mpsc::Sender<Hash>) {
    let mut keys = Vec::new();

    for (id, name, ctime, _mtime, atime, hash, data_ref, _) in self.list_from_key_store(dir_id).into_iter() {
      let mut m = BTreeMap::new();
      m.insert("id".to_string(), id.to_json());
      m.insert("name".to_string(), name.to_json());
      m.insert("ct".to_string(), ctime.to_json());
      m.insert("at".to_string(), atime.to_json());

      if hash.len() > 0 {
        // This is a file, store its data hash:
        m.insert("data_hash".to_string(), hash.to_json());
        m.insert("data_ref".to_string(), data_ref.to_json());
        hash_ch.send(Hash{bytes: hash}).unwrap();
      } else if hash.len() == 0 {
        drop(hash);
        drop(data_ref);
        // This is a directory, recurse!
        let mut inner_tree = self.key_store.hash_tree_writer();
        self.commit_to_tree(&mut inner_tree, Some(id), hash_ch.clone());
        // Store a reference for the sub-tree in our tree:
        let (dir_hash, dir_ref) = inner_tree.hash();
        m.insert("dir_hash".to_string(), dir_hash.bytes.to_json());
        m.insert("dir_ref".to_string(), dir_ref.to_json());
        hash_ch.send(dir_hash).unwrap();
      }

      keys.push(json::Json::Object(m).to_json());

      // Flush to our own tree when we have a decent amount.
      // The tree prevents large directories from clogging ram.
      if keys.len() >= 1000 {
        tree.append(keys.to_json().to_string().as_bytes().to_vec());
        keys.clear();
      }
    }
    if keys.len() > 0 {
      tree.append(keys.to_json().to_string().as_bytes().to_vec());
    }
  }

}
