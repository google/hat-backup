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
use std::thunk::Thunk;
use std::collections::{BTreeMap};

use process::{Process};

use blob_index::{BlobIndex};
use blob_store::{BlobStore, BlobStoreProcess, BlobStoreBackend};

use hash_index::{Hash, HashIndex, HashIndexProcess};
use key_index::{KeyIndex, KeyEntry};
use key_store::{KeyStore, KeyStoreProcess};
use key_store;
use snapshot_index::{SnapshotIndex, SnapshotIndexProcess};
use snapshot_index;

use hash_tree;
use listdir;

use std::path::PathBuf;
use std::fs;
use std::io;
use std::io::{Read, Write};

use std::hash::{hash, SipHasher};
use std::sync;
use std::sync::atomic;

use time;


pub struct Hat<B> {
  repository_root: PathBuf,
  snapshot_index: SnapshotIndexProcess,
  blob_store: BlobStoreProcess,
  hash_index: HashIndexProcess,
  blob_backend: B,
  hash_backend: key_store::HashStoreBackend,
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
    let si_p = Process::new(Thunk::new(move|| { SnapshotIndex::new(snapshot_index_path) }));
    let bi_p = Process::new(Thunk::new(move|| { BlobIndex::new(blob_index_path) }));
    let hi_p = Process::new(Thunk::new(move|| { HashIndex::new(hash_index_path) }));

    let local_blob_index = bi_p.clone();
    let local_backend = backend.clone();
    let bs_p = Process::new(Thunk::new(move|| {
      BlobStore::new(local_blob_index, local_backend, max_blob_size) }));

    Hat{repository_root: repository_root.clone(),
        snapshot_index: si_p,
        hash_index: hi_p.clone(),
        blob_store: bs_p.clone(),
        blob_backend: backend.clone(),
        hash_backend: key_store::HashStoreBackend::new(hi_p, bs_p),
        max_blob_size: max_blob_size,
    }
  }

  pub fn open_family(&self, name: String) -> Option<Family> {
    // We setup a standard pipeline of processes:
    // KeyStore -> KeyIndex
    //          -> HashIndex
    //          -> BlobStore -> BlobIndex

    let key_index_path = concat_filename(&self.repository_root, name.clone());
    let ki_p = Process::new(Thunk::new(move|| { KeyIndex::new(key_index_path) }));

    let local_ks = KeyStore::new(ki_p.clone(), self.hash_index.clone(), self.blob_store.clone());
    let ks_p = Process::new(Thunk::new(move|| { local_ks }));

    let ks = KeyStore::new(ki_p, self.hash_index.clone(), self.blob_store.clone());
    Some(Family{name: name, key_store: ks, key_store_process: ks_p})
  }

  pub fn commit(&self, family_name: String) {
    let mut family = self.open_family(family_name.clone()).expect(
      format!("Could not open family '{}'", family_name).as_slice());

    // Commit snapshot:
    let (hash, top_ref) = family.commit();
    family.flush();

    // Update to snapshot index:
    self.snapshot_index.send_reply(snapshot_index::Msg::Add(family_name, hash, top_ref));
    self.snapshot_index.send_reply(snapshot_index::Msg::Flush);
  }

  pub fn checkout_in_dir(&self, family_name: String, output_dir: PathBuf) {
    // Extract latest snapshot info:
    let (dir_hash, dir_ref) =
      match self.snapshot_index.send_reply(snapshot_index::Msg::Latest(family_name.clone())) {
        snapshot_index::Reply::Latest(Some((h, r))) => (h, r),
        snapshot_index::Reply::Latest(None) =>
          panic!("Tries to checkout family '{}' before first commit", family_name),
        _ => panic!("Unexpected result from snapshot index"),
      };

    let family = self.open_family(family_name.clone()).expect(
      format!("Could not open family '{}'", family_name).as_slice());

    let mut output_dir = output_dir;
    self.checkout_dir_ref(&family, &mut output_dir, dir_hash, dir_ref);
  }

  fn checkout_dir_ref(&self, family: &Family, output: &mut PathBuf, dir_hash: Hash, dir_ref: Vec<u8>) {
    fs::create_dir_all(output).unwrap();
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
            let mut fd = fs::File::create(output).unwrap();
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
    Some(self.metadata.modified())
  }
  fn accessed(&self) -> Option<u64> {
    Some(self.metadata.accessed())
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
  fn with_id(&self, id: u64) -> FileEntry {
    let mut x = self.clone();
    x.id = Some(id);
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
        let create_file_it = Thunk::new(move|| {
          match local_file_entry.file_iterator() {
            Err(e) => {println!("Skipping '{}': {}", local_root.display(), e.to_string());
                       None},
            Ok(it) => { Some(it) }
          }
        });
        let create_file_it_opt = if is_directory { None }
                                 else { Some(create_file_it) };

        match self.key_store.send_reply(key_store::Msg::Insert(file_entry, create_file_it_opt))
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
  for _ in range(1 as i32, 5) {
    if f() { return }
  }
  panic!(msg.to_string());
}


struct Family {
  name: String,
  key_store: KeyStore<FileEntry>,
  key_store_process: KeyStoreProcess<FileEntry, FileIterator>,
}

impl Family
{
  pub fn snapshot_dir(&self, dir: PathBuf) {
    let mut handler = InsertPathHandler::new(self.key_store_process.clone());
    listdir::iterate_recursively((PathBuf::new(&dir), None), &mut handler);
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
    for (id, name, _, _, _, hash, _, data_res_opt) in self.listFromKeyStore(dir_id).into_iter() {
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

  pub fn listFromKeyStore(&self, dir_id: Option<u64>) -> Vec<key_store::DirElem> {
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

  pub fn commit(&mut self) -> (Hash, Vec<u8>) {
    let mut top_tree = self.key_store.hash_tree_writer();
    self.commit_to_tree(&mut top_tree, None);
    return top_tree.hash();
  }

  pub fn commit_to_tree(&mut self,
                        tree: &mut hash_tree::SimpleHashTreeWriter<key_store::HashStoreBackend>,
                        dir_id: Option<u64>) {
    let mut keys = Vec::new();

    for (id, name, ctime, mtime, atime, hash, data_ref, _) in self.listFromKeyStore(dir_id).into_iter() {
      let mut m = BTreeMap::new();
      m.insert("id".to_string(), id.to_json());
      m.insert("name".to_string(), name.to_json());
      m.insert("ct".to_string(), ctime.to_json());
      m.insert("at".to_string(), atime.to_json());

      if hash.len() > 0 {
        // This is a file, store its data hash:
        m.insert("data_hash".to_string(), hash.to_json());
        m.insert("data_ref".to_string(), data_ref.to_json());
      } else if hash.len() == 0 {
        drop(hash);
        drop(data_ref);
        // This is a directory, recurse!
        let mut inner_tree = self.key_store.hash_tree_writer();
        self.commit_to_tree(&mut inner_tree, Some(id));
        // Store a reference for the sub-tree in our tree:
        let (dir_hash, dir_ref) = inner_tree.hash();
        m.insert("dir_hash".to_string(), dir_hash.bytes.to_json());
        m.insert("dir_ref".to_string(), dir_ref.to_json());
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
