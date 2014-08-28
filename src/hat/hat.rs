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

extern crate std;
extern crate sync;
extern crate time;


use process::{Process};

use blob_index::{BlobIndex, BlobIndexProcess};
use blob_store::{BlobStore, BlobStoreBackend};

use hash_index::{HashIndex, HashIndexProcess};
use hash_store::{HashStore};
use hash_tree;

use key_index::{KeyIndex, KeyEntry};

use key_store::{KeyStore, KeyStoreProcess};
use key_store;

use listdir;
use std::io::stdio::{println};

use std::io::{Reader, IoResult, UserDir,
              TypeDirectory, TypeSymlink, TypeFile, FileStat};
use std::io::fs::{lstat, File, mkdir_recursive};


pub struct Hat<'db, B> {
  repository_root: Path,
  blob_index: Box<BlobIndexProcess>,
  hash_index: Box<HashIndexProcess<'db>>,

  backend: B,
  max_blob_size: uint,
}

fn concat_filename(a: &Path, b: ~str) -> ~str {
  let mut result = a.clone();
  result.push(Path::new(b));
  result.as_str().expect("Unable to decode repository_root.").to_owned()
}

fn blob_index_name(root: &Path) -> ~str {
  concat_filename(root, "blob_index.sqlite3".to_owned())
}

fn hash_index_name(root: &Path) -> ~str {
  concat_filename(root, "hash_index.sqlite3".to_owned())
}

impl <'db, B: BlobStoreBackend + Clone + Send> Hat<'db, B> {
  pub fn openRepository(repository_root: &Path, backend: B, max_blob_size: uint)
                        -> Option<Hat<'db, B>> {
    repository_root.as_str().map(|_| {
      let blob_index_path = blob_index_name(repository_root);
      let hash_index_path = hash_index_name(repository_root);
      let biP = Process::new(proc() { BlobIndex::new(blob_index_path) });
      let hiP = Process::new(proc() { HashIndex::new(hash_index_path) });
      Hat{repository_root: repository_root.clone(),
                hash_index: hiP,
                blob_index: biP,
                backend: backend.clone(),
                max_blob_size: max_blob_size,
      }
    })
  }

  pub fn openFamily(&self, name: ~str) -> Option<Family<'db, B>> {
    // We setup a standard pipeline of processes:
    // KeyStore -> KeyIndex
    //          -> HashStore -> HashIndex
    //                       -> BlobStore -> BlobIndex

    let local_blob_index = self.blob_index.clone();
    let local_backend = self.backend.clone();
    let local_max_blob_size = self.max_blob_size;
    let bsP = Process::new(proc() {
      BlobStore::new(local_blob_index, local_backend, local_max_blob_size) });

    let local_hash_index = self.hash_index.clone();
    let hsP = Process::new(proc() { HashStore::new(local_hash_index, bsP) });

    let key_index_path = concat_filename(&self.repository_root, name.clone());
    let kiP = Process::new(proc() { KeyIndex::new(key_index_path) });
    let ksP = Process::new(proc() { KeyStore::new(kiP, hsP) });

    Some(Family{name: name,
                key_store: ksP})
  }
}



struct FileEntry {
  name: ~[u8],

  parentId: Option<~[u8]>,

  stat: FileStat,
  full_path: Path,
}

impl FileEntry {
  fn new(full_path: Path,
         parent: Option<~[u8]>) -> Result<FileEntry, std::io::IoError> {
    let filename_opt = full_path.filename();
    if filename_opt.is_some() {
      lstat(&full_path).map(|st| {
        FileEntry{
          name: filename_opt.unwrap().into_owned(),
          parentId: parent.clone(),
          stat: st,
          full_path: full_path.clone()}
      })
    }
    else { Err(std::io::IoError{kind: std::io::OtherIoError,
                                desc: "Could not parse filename.",
                                detail: None
                               }) }
  }

  fn fileIterator(&self) -> IoResult<FileIterator> {
    FileIterator::new(&self.full_path)
  }

  fn isDirectory(&self) -> bool { self.stat.kind == TypeDirectory }
  fn isSymlink(&self) -> bool { self.stat.kind == TypeSymlink }
  fn isFile(&self) -> bool { self.stat.kind == TypeFile }
}

impl Clone for FileEntry {
  fn clone(&self) -> FileEntry {
    FileEntry{
      name:self.name.clone(), parentId:self.parentId.clone(),
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
  fn name(&self) -> ~[u8] {
    self.name.clone()
  }
  fn id(&self) -> Option<~[u8]> {
    Some(format!("d{:u}i{:u}",
                 self.stat.unstable.device,
                 self.stat.unstable.inode).as_bytes().into_owned())
  }
  fn parentId(&self) -> Option<~[u8]> {
    self.parentId.clone()
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
  fn userId(&self) -> Option<u64> {
    None
  }
  fn groupId(&self) -> Option<u64> {
    None
  }
  fn withId(&self, id: ~[u8]) -> FileEntry {
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

impl Iterator<~[u8]> for FileIterator {
  fn next(&mut self) -> Option<~[u8]> {
    let mut buf = ~[0, .. 128*1024];
    match self.file.read(buf) {
      Err(_) => None,
      Ok(size) => Some(buf.slice_to(size).into_owned()),
    }
  }
}


#[deriving(Clone)]
struct InsertPathHandler<'db, B> {
  count: sync::Arc<sync::Mutex<uint>>,
  last_print: sync::Arc<sync::Mutex<time::Timespec>>,
  my_last_print: time::Timespec,

  key_store: Box<KeyStoreProcess<'db, FileEntry, FileIterator, B>>,
}

impl <'db, B> InsertPathHandler<'db, B> {
  pub fn new(key_store: Box<KeyStoreProcess<'db, FileEntry, FileIterator, B>>)
             -> InsertPathHandler<'db, B> {
    InsertPathHandler{
      count: sync::Arc::new(sync::Mutex::new(0)),
      last_print: sync::Arc::new(sync::Mutex::new(time::now().to_timespec())),
      my_last_print: time::now().to_timespec(),
      key_store: key_store,
    }
  }
}

impl <'db, B: BlobStoreBackend + Clone> listdir::PathHandler<Option<~[u8]>>
  for InsertPathHandler<'db, B> {
  fn handlePath(&mut self, parent: Option<~[u8]>, path: Path) -> Option<Option<~[u8]>> {
    let mut count = 0;
    {
      let mut c = self.count.lock();
      *c += 1;
      count = *c;
    }

    if self.my_last_print.sec <= time::now().to_timespec().sec - 1 {
      let mut t = self.last_print.lock();
      let now = time::now().to_timespec();
      if t.sec <= now.sec - 1 {
        println(format!("\\#{}: {}", count, path.display()));
        *t = now;
      }
      self.my_last_print = now;
    }

    let fileEntry_opt = FileEntry::new(path.clone(), parent);
    match fileEntry_opt {
      Err(e) => {
        println(format!("Skipping '{}': {}", path.display(), e.to_str()));
      },
      Ok(fileEntry) => {
        if fileEntry.isSymlink() {
          return None;
        }
        let isDirectory = fileEntry.isDirectory();
        let local_root = path.clone();
        let local_fileEntry = fileEntry.clone();
        let create_file_it = proc() {
          match local_fileEntry.fileIterator() {
            Err(e) => {println(format!("Skipping '{}': {}",
                                       local_root.display(), e.to_str()));
                       None},
            Ok(it) => { Some(it) }
          }
        };
        let create_file_it_opt = if isDirectory { None }
                                 else { Some(create_file_it) };

        match self.key_store.sendReply(
          key_store::Insert(fileEntry, create_file_it_opt))
        {
          key_store::Id(id) => {
            if isDirectory { return Some(Some(id)) }
          },
          _ => fail!("Unexpected reply from key store."),
        }
      }
    }

    return None;
  }
}


struct Family<'db, B> {
  name: ~str,
  key_store: Box<KeyStoreProcess<'db, FileEntry, FileIterator, B>>,
}

impl <'db, B: BlobStoreBackend + Clone> Family<'db, B> {

  pub fn snapshotDir(&self, dir: Path) {
    let mut handler = InsertPathHandler::new(self.key_store.clone());
    listdir::iterateRecursively((Path::new(dir.clone()), None), &mut handler);
  }

  pub fn flush(&self) {
    self.key_store.sendReply(key_store::Flush);
  }

  pub fn checkoutInDir(&self, output_dir: &mut Path, dir_id: Option<~[u8]>) {

    fn put_chunks<B: hash_tree::HashTreeBackend + Clone>(
      fd: &mut File, tree: hash_tree::ReaderResult<B>)
    {
      let mut it = match tree {
        hash_tree::NoData => fail!("Trying to read data where none exist."),
        hash_tree::SingleBlock(chunk) => {
          fd.write(chunk).unwrap();
          return;
        },
        hash_tree::Tree(it) => it,
      };
      // We have a tree
      for chunk in it {
        fd.write(chunk).unwrap();
      }
    }

    // create output_dir
    mkdir_recursive(output_dir, UserDir).unwrap();

    let listing = match self.key_store.sendReply(key_store::ListDir(dir_id)) {
      key_store::ListResult(ls) => ls,
      _ => fail!("Unexpected result from key store."),
    };

    for (id, name, _, _, _, hash, _, data_res) in listing.move_iter() {

      output_dir.push(name);
      println(format!("{}", output_dir.display()));

      if hash.len() == 0 {
        // This is a directory, recurse!
        self.checkoutInDir(output_dir, Some(id));
      } else {
        // This is a file, write it
        let mut fd = File::create(output_dir).unwrap();
        put_chunks(&mut fd, data_res);
      }

      output_dir.pop();
    }

  }
}