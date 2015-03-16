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
use std::thunk::Thunk;
use std::collections::{BTreeMap};

use process::{Process};

use blob_index::{BlobIndex, BlobIndexProcess};
use blob_store::{BlobStore, BlobStoreBackend};

use hash_index::{Hash, HashIndex, HashIndexProcess};
use key_index::{KeyIndex, KeyEntry};
use key_store::{KeyStore, KeyStoreProcess};
use key_store;
use snapshot_index::{SnapshotIndex, SnapshotIndexProcess};
use snapshot_index;

use hash_tree;
use listdir;

use std::old_io;
use std::old_io::{Reader, IoResult, USER_DIR, FileStat};
use std::old_io::FileType::{Directory, Symlink, RegularFile};
use std::old_io::fs::{lstat, File, mkdir_recursive};
use std::sync;
use std::sync::atomic;

use time;


pub struct Hat<B> {
  repository_root: Path,
  snapshot_index: SnapshotIndexProcess,
  blob_index: BlobIndexProcess,
  hash_index: HashIndexProcess,
  backend: B,
  max_blob_size: usize,
}

fn concat_filename(a: &Path, b: String) -> String {
  let mut result = a.clone();
  result.push(Path::new(b));
  result.as_str().expect("Unable to decode repository_root.").to_string()
}

fn snapshot_index_name(root: &Path) -> String {
  concat_filename(root, "snapshot_index.sqlite3".to_string())
}

fn blob_index_name(root: &Path) -> String {
  concat_filename(root, "blob_index.sqlite3".to_string())
}

fn hash_index_name(root: &Path) -> String {
  concat_filename(root, "hash_index.sqlite3".to_string())
}

impl <B: 'static + BlobStoreBackend + Clone + Send> Hat<B> {
  pub fn open_repository(repository_root: &Path, backend: B, max_blob_size: usize)
                        -> Option<Hat<B>> {
    repository_root.as_str().map(|_| {
      let snapshot_index_path = snapshot_index_name(repository_root);
      let blob_index_path = blob_index_name(repository_root);
      let hash_index_path = hash_index_name(repository_root);
      let si_p = Process::new(Thunk::new(move|| { SnapshotIndex::new(snapshot_index_path) }));
      let bi_p = Process::new(Thunk::new(move|| { BlobIndex::new(blob_index_path) }));
      let hi_p = Process::new(Thunk::new(move|| { HashIndex::new(hash_index_path) }));
      Hat{repository_root: repository_root.clone(),
          snapshot_index: si_p,
          hash_index: hi_p,
          blob_index: bi_p,
          backend: backend.clone(),
          max_blob_size: max_blob_size,
      }
    })
  }

  pub fn open_family(&self, name: String) -> Option<Family> {
    // We setup a standard pipeline of processes:
    // KeyStore -> KeyIndex
    //          -> HashIndex
    //          -> BlobStore -> BlobIndex

    let local_blob_index = self.blob_index.clone();
    let local_backend = self.backend.clone();
    let local_max_blob_size = self.max_blob_size;
    let bs_p = Process::new(Thunk::new(move|| {
      BlobStore::new(local_blob_index, local_backend, local_max_blob_size) }));

    let key_index_path = concat_filename(&self.repository_root, name.clone());
    let ki_p = Process::new(Thunk::new(move|| { KeyIndex::new(key_index_path) }));

    let local_ks = KeyStore::new(ki_p.clone(), self.hash_index.clone(), bs_p.clone());
    let ks_p = Process::new(Thunk::new(move|| { local_ks }));

    let ks = KeyStore::new(ki_p, self.hash_index.clone(), bs_p);
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
}

struct FileEntry {
  name: Vec<u8>,
  parent_id: Option<Vec<u8>>,
  stat: FileStat,
  full_path: Path,
}

impl FileEntry {
  fn new(full_path: Path,
         parent: Option<Vec<u8>>) -> Result<FileEntry, old_io::IoError> {
    let filename_opt = full_path.filename();
    if filename_opt.is_some() {
      lstat(&full_path).map(|st| {
        FileEntry{
          name: filename_opt.unwrap().to_vec(),
          parent_id: parent.clone(),
          stat: st,
          full_path: full_path.clone()}
      })
    }
    else { Err(old_io::IoError{kind: old_io::OtherIoError,
                               desc: "Could not parse filename.",
                               detail: None }) }
  }

  fn file_iterator(&self) -> IoResult<FileIterator> {
    FileIterator::new(&self.full_path)
  }

  fn is_directory(&self) -> bool { self.stat.kind == Directory }
  fn is_symlink(&self) -> bool { self.stat.kind == Symlink }
  fn is_file(&self) -> bool { self.stat.kind == RegularFile }
}

impl Clone for FileEntry {
  fn clone(&self) -> FileEntry {
    FileEntry{
      name:self.name.clone(), parent_id:self.parent_id.clone(),
      stat: FileStat{
        size: self.stat.size,
        kind: self.stat.kind,
        perm: self.stat.perm,
        created: self.stat.created,
        modified: self.stat.modified,
        accessed: self.stat.accessed,
        unstable: self.stat.unstable,
      },
      full_path:self.full_path.clone()}
  }
}

impl KeyEntry<FileEntry> for FileEntry {
  fn name(&self) -> Vec<u8> {
    self.name.clone()
  }
  fn id(&self) -> Option<Vec<u8>> {
    Some(format!("d{}i{}",
                 self.stat.unstable.device,
                 self.stat.unstable.inode).as_bytes().to_vec())
  }
  fn parent_id(&self) -> Option<Vec<u8>> {
    self.parent_id.clone()
  }

  fn size(&self) -> Option<u64> {
    Some(self.stat.size)
  }

  fn created(&self) -> Option<u64> {
    Some(self.stat.created)
  }
  fn modified(&self) -> Option<u64> {
    Some(self.stat.modified)
  }
  fn accessed(&self) -> Option<u64> {
    Some(self.stat.accessed)
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
  fn with_id(&self, id: Vec<u8>) -> FileEntry {
    assert_eq!(Some(id), self.id());
    self.clone()
  }
}

struct FileIterator {
  file: File
}

impl FileIterator {
  fn new(path: &Path) -> IoResult<FileIterator> {
    match File::open(path) {
      Ok(f) => Ok(FileIterator{file: f}),
      Err(e) => Err(e),
    }
  }
}

impl Iterator for FileIterator {
  type Item = Vec<u8>;

  fn next(&mut self) -> Option<Vec<u8>> {
    let mut buf = vec![0u8; 128*1024];
    match self.file.read(buf.as_mut_slice()) {
      Err(_) => None,
      Ok(size) => Some(buf.slice_to(size).to_vec()),
    }
  }
}


#[derive(Clone)]
struct InsertPathHandler {
  count: sync::Arc<sync::atomic::AtomicInt>,
  last_print: sync::Arc<sync::Mutex<time::Timespec>>,
  key_store: KeyStoreProcess<FileEntry, FileIterator>,
}

impl InsertPathHandler {
  pub fn new(key_store: KeyStoreProcess<FileEntry, FileIterator>)
             -> InsertPathHandler {
    InsertPathHandler{
      count: sync::Arc::new(sync::atomic::AtomicInt::new(0)),
      last_print: sync::Arc::new(sync::Mutex::new(time::now().to_timespec())),
      key_store: key_store,
    }
  }
}

impl listdir::PathHandler<Option<Vec<u8>>> for InsertPathHandler
{
  fn handle_path(&self, parent: Option<Vec<u8>>, path: Path) -> Option<Option<Vec<u8>>> {
    let count = self.count.fetch_add(1, atomic::Ordering::SeqCst) + 1;

    if count % 16 == 0 {  // don't hammer the mutex
      let mut guarded_last_print = self.last_print.lock().unwrap();
      let now = time::now().to_timespec();
      if guarded_last_print.sec <= now.sec - 1 {
        println!("#{}: {}", count, path.display());
        *guarded_last_print = now;
      }
    }

    let fileEntry_opt = FileEntry::new(path.clone(), parent);
    match fileEntry_opt {
      Err(e) => {
        println!("Skipping '{}': {}", path.display(), e.to_string());
      },
      Ok(fileEntry) => {
        if fileEntry.is_symlink() {
          return None;
        }
        let is_directory = fileEntry.is_directory();
        let local_root = path;
        let local_fileEntry = fileEntry.clone();
        let create_file_it = Thunk::new(move|| {
          match local_fileEntry.file_iterator() {
            Err(e) => {println!("Skipping '{}': {}", local_root.display(), e.to_string());
                       None},
            Ok(it) => { Some(it) }
          }
        });
        let create_file_it_opt = if is_directory { None }
                                 else { Some(create_file_it) };

        match self.key_store.send_reply(key_store::Msg::Insert(fileEntry, create_file_it_opt))
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
  pub fn snapshot_dir(&self, dir: Path) {
    let mut handler = InsertPathHandler::new(self.key_store_process.clone());
    listdir::iterate_recursively((Path::new(dir.clone()), None), &mut handler);
  }

  pub fn flush(&self) {
    self.key_store_process.send_reply(key_store::Msg::Flush);
  }

  fn write_file_chunks<HTB: hash_tree::HashTreeBackend + Clone>(
    &self, fd: &mut File, tree: hash_tree::ReaderResult<HTB>)
  {
    let mut it = match tree {
      hash_tree::ReaderResult::NoData => panic!("Trying to read data where none exist."),
      hash_tree::ReaderResult::SingleBlock(chunk) => {
        try_a_few_times_then_panic(|| fd.write(chunk.as_slice()).is_ok(),
                                   "Could not write chunk.");
        return;
      },
      hash_tree::ReaderResult::Tree(it) => it,
    };
    // We have a tree
    for chunk in it {
      try_a_few_times_then_panic(|| fd.write(chunk.as_slice()).is_ok(), "Could not write chunk.");
    }
    try_a_few_times_then_panic(|| fd.flush().is_ok(), "Could not flush file.");
  }

  pub fn checkout_in_dir(&self, output_dir: Path, dir_id: Option<Vec<u8>>) {
    let mut path = output_dir;
    for (id, name, _, _, _, hash, _, data_res) in self.listFromKeyStore(dir_id).into_iter() {
      // Extend directory with filename:
      path.push(name.clone());

      if hash.len() == 0 {
        // This is a directory, recurse!
        mkdir_recursive(&path, USER_DIR).unwrap();
        self.checkout_in_dir(path.clone(), Some(id.clone()));
      } else {
        // This is a file, write it
        let mut fd = File::create(&path).unwrap();
        self.write_file_chunks(&mut fd, data_res);
      }
      // Prepare for next filename:
      path.pop();
    }
  }

  pub fn listFromKeyStore(&self, dir_id: Option<Vec<u8>>) -> Vec<key_store::DirElem> {
    let listing = match self.key_store_process.send_reply(key_store::Msg::ListDir(dir_id.clone())) {
      key_store::Reply::ListResult(ls) => ls,
      _ => panic!("Unexpected result from key store."),
    };

    return listing;
  }

  pub fn commit(&mut self) -> (Hash, Vec<u8>) {
    let mut top_tree = self.key_store.hash_tree_writer();
    self.commit_to_tree(&mut top_tree, None);
    return top_tree.hash();
  }

  pub fn commit_to_tree(&mut self,
                        tree: &mut hash_tree::SimpleHashTreeWriter<key_store::HashStoreBackend>,
                        dir_id: Option<Vec<u8>>) {
    let mut keys = Vec::new();

    for (id, name, ctime, mtime, atime, hash, data_ref, _) in self.listFromKeyStore(dir_id).into_iter() {
      let mut m = BTreeMap::new();
      m.insert("id".to_string(), id.to_json());
      m.insert("name".to_string(), name.to_json());
      m.insert("ct".to_string(), ctime.to_json());
      m.insert("at".to_string(), atime.to_json());

      if hash.len() > 0 {
        // This is file, store its data hash:
        m.insert("data_hash".to_string(), hash.to_json());
        m.insert("data_ref".to_string(), data_ref.to_json());
      } else if hash.len() == 0 {
        // This is a directory, recurse!
        let mut inner_tree = self.key_store.hash_tree_writer();
        self.commit_to_tree(&mut inner_tree, Some(id));
        // Store a reference for the sub-tree in our tree:
        let (dir_hash, dir_ref) = inner_tree.hash();
        m.insert("dir_hash".to_string(), hash.to_json());
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
