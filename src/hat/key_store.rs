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

//! External API for creating and manipulating snapshots.

use blob_store::{BlobStoreBackend};
use hash_tree::{SimpleHashTreeWriter, HashTreeBackend,
                SimpleHashTreeReader, ReaderResult};
use hash_index;
use hash_store::{HashStoreProcess};
use hash_store;

use process::{Process, MsgHandler};

use key_index::{KeyIndexProcess, KeyEntry};
use key_index;

use hash_store::{HashStore};

#[cfg(test)]
use key_index::{KeyIndex};


pub type KeyStoreProcess<'db, KE, IT, B> = Process<Msg<KE, IT>, Reply<'db, B>, KeyStore<'db, KE, IT, B>>;

// Public structs
pub enum Msg<KE, IT> {

  /// Insert a key into the index. If this key has associated data a "chunk-iterator creator" can be
  /// passed along with it. If the data turns out to be unreadable, this iterator proc can return
  /// `None`.
  /// Returns `Id` with the new entry ID.
  Insert(KE, Option<proc():Send -> Option<IT>>),

  /// List a "directory" (aka. a `level`) in the index.
  /// Returns `ListResult` with all the entries under the given parent.
  ListDir(Option<Vec<u8>>),

  /// Flush this key store and its dependencies.
  /// Returns `FlushOK`.
  Flush,
}

pub enum Reply<'db, B> {
  Id(Vec<u8>),
  ListResult(Vec<(Vec<u8>, Vec<u8>, u64, u64, u64, Vec<u8>, Vec<u8>,
                  ReaderResult<HashStoreBackend<'db, B>>)>),
  FlushOK,
}

pub struct KeyStore<'db, KE, IT, B> {
  index: KeyIndexProcess<KE>,
  hash_store: HashStoreProcess<'db, B>,
}

// Implementations
impl <'db, KE: KeyEntry<KE> + Send, IT: Iterator<Vec<u8>>, B: BlobStoreBackend + Send>
  KeyStore<'db, KE, IT, B> {
  pub fn new(index: KeyIndexProcess<KE>, hash_store: HashStoreProcess<B>) -> KeyStore<KE, IT, B> {
    KeyStore{index: index,
             hash_store: hash_store}
  }

  #[cfg(test)]
  pub fn new_for_testing(backend: B) -> KeyStore<KE, IT, B> {
    let kiP = Process::new(proc() { KeyIndex::new_for_testing() });
    let hsP = Process::new(proc() { HashStore::new_for_testing(backend) });
    KeyStore{index: kiP, hash_store: hsP}
  }

  pub fn flush(&mut self) {
    self.hash_store.send_reply(hash_store::Flush);
    self.index.send_reply(key_index::Flush);
  }
}

#[deriving(Clone)]
pub struct HashStoreBackend<'db, B> {
  hash_store: HashStoreProcess<'db, B>,
}

impl <'db, B> HashStoreBackend<'db, B> {
  fn new(hash_store: Process<hash_store::Msg, hash_store::Reply, HashStore<B>>)
  -> HashStoreBackend<B> {
    HashStoreBackend{hash_store:hash_store}
  }
}

impl <'db, B: BlobStoreBackend + Clone> HashTreeBackend for HashStoreBackend<'db, B>
{
  fn fetch_chunk(&mut self, hash: hash_index::Hash) -> Option<Vec<u8>> {
    assert!(hash.bytes.len() > 0);

    match self.hash_store.send_reply(hash_store::FetchChunkFromHash(hash)) {
      hash_store::Chunk(bytes) => Some(bytes),
      _ => None,
    }
  }

  fn fetch_persistent_ref(&mut self, hash: hash_index::Hash) -> Option<Vec<u8>> {
    assert!(hash.bytes.len() > 0);

    loop {
      match self.hash_store.send_reply(
        hash_store::FetchPersistentRef(hash.clone())
      )
      {
        hash_store::PersistentRef(r) => { return Some(r) }, // done
        hash_store::HashNotKnown => { return None }, // done
        hash_store::Retry => (),  // continue loop
        _ => fail!("Unexpected reply from hash store."),
      }
    };
  }

  fn fetch_payload(&mut self, hash: hash_index::Hash) -> Option<Vec<u8>> {
    match self.hash_store.send_reply(hash_store::FetchPayload(hash.clone()))
    {
      hash_store::Payload(p) => { return p }, // done
      hash_store::HashNotKnown => { return None }, // done
      _ => fail!("Unexpected reply from hash store."),
    }
  }

  fn insert_chunk(&mut self, hash: hash_index::Hash,
                  level: i64, payload: Option<Vec<u8>>,
                  chunk: Vec<u8>) -> Vec<u8> {
    match self.hash_store.send_reply(
      hash_store::Insert(hash, level, payload, chunk))
    {
      hash_store::InsertOK(blob_ref) => blob_ref,
      _ => fail!("Unexpected answer from hash store."),
    }
  }
}

impl <'db, KE: KeyEntry<KE> + Clone + Send, IT: Iterator<Vec<u8>> + Send,
      B: BlobStoreBackend + Clone + Send>
        MsgHandler<Msg<KE, IT>, Reply<'db, B>> for KeyStore<'db, KE, IT, B>
{
  fn handle(&mut self, msg: Msg<KE, IT>, reply: |Reply<'db, B>|) {
    match msg {
      Flush => {
        self.flush();
        return reply(FlushOK);
      },

      ListDir(parent) => {
        match self.index.send_reply(key_index::ListDir(parent)) {
          key_index::ListResult(entries) => {
            // TODO(jos): Rewrite this tuple hell
            let mut my_entries = Vec::with_capacity(entries.len());
            for (id, name, created, modified, accessed, hash, persistent_ref) in entries.move_iter()
            {
              let local_hash_store = self.hash_store.clone();
              let local_hash = hash_index::Hash{bytes: hash.clone()};
              let local_ref = persistent_ref.clone();

              my_entries.push(
                (id, name, created, modified, accessed, hash, persistent_ref,
                 // proc() {
                   SimpleHashTreeReader::new(
                     HashStoreBackend::new(local_hash_store), local_hash, local_ref)
                 // }
                 ));
            }
            return reply(ListResult(my_entries));
          },
          _ => fail!("Unexpected result from key index."),
        }
      },

      Insert(org_entry, chunk_it_opt) => {
        match self.index.send_reply(key_index::LookupExact(org_entry.clone())) {

          key_index::Id(entryId) => {
            return reply(Id(entryId));
          },

          _ => {
            let id = match self.index.send_reply(key_index::Insert(org_entry.clone()))
            {
              key_index::Id(entryId) => entryId,
              _ => fail!("No ID returned from key index Insert()."),
            };

            // Send out the ID early to allow the client to continue its key discovery routine.
            // The bounded input-channel will prevent the client from overflowing us.
            reply(Id(id.clone()));

            // Setup hash tree structure
            let backend = HashStoreBackend::new(self.hash_store.clone());
            let mut tree = SimpleHashTreeWriter::new(8, backend);

            // Read and append all file chunks
            let it_opt = chunk_it_opt.and_then(|p| p());
            if it_opt.is_some() {

              let mut bytes_read = 0u64;
              it_opt.unwrap().map(|chunk: Vec<u8>| {
                bytes_read += chunk.len() as u64;
                tree.append(chunk);
              }).last();

              // Check that we read the whole file:
              org_entry.size().map(|size| {
                if size > bytes_read {
                  println!("Warning: File grew while reading it: {} (wanted {}, got {})",
                           org_entry.name(), size, bytes_read) }
                else if size > bytes_read {
                  println!(
                    "Warning: Could not read whole file (or it shrank): {} (wanted {}, got {})",
                    org_entry.name(), size, bytes_read) }
              });

              // Get top tree hash:
              let (hash, persistent_ref) = tree.hash();

              let local_index = self.index.clone();
              self.hash_store.send_reply(
                hash_store::CallAfterHashIsComitted(
                  hash.clone(),
                  proc() {
                    local_index.send_reply(key_index::UpdateDataHash(org_entry.with_id(id),
                                                                    Some(hash.bytes.clone()),
                                                                    Some(persistent_ref)));
                  }
                )
              );
            } else {
              // No data is associated with this entry
              self.index.send_reply(
                key_index::UpdateDataHash(org_entry, None, None));
            }
          }
        }
      }
    }
  }
}


#[cfg(test)]
mod tests {
  use super::*;

  use key_index::{KeyEntry};
  use blob_store::tests::{MemoryBackend, DevNullBackend};
  use hash_tree;

  use std::rand::{Rng, task_rng};
  use quickcheck::{Config, Testable, gen};
  use quickcheck::{quickcheck_config};
  use process::{Process};

  use test::{Bencher};

  // QuickCheck configuration
  static SIZE: uint = 50;
  static CONFIG: Config = Config {
    tests: 10,
    max_tests: 100,
  };

  // QuickCheck helpers:
  fn qcheck<A: Testable>(f: A) {
    quickcheck_config(CONFIG, &mut gen(task_rng(), SIZE), f)
  }

  fn random_ascii_bytes() -> Vec<u8> {
    let ascii: String = task_rng().gen_ascii_chars().take(32).collect();
    ascii.into_bytes()
  }

  #[deriving(Clone)]
  struct KeyEntryStub {
    parent_id: Option<Vec<u8>>,

    id: Vec<u8>,
    name: Vec<u8>,

    data: Option<Vec<Vec<u8>>>,

    modified: Option<u64>,
  }

  #[deriving(Eq)]
  impl KeyEntryStub {
    fn new(parent: Option<Vec<u8>>, name: Vec<u8>, data: Option<Vec<Vec<u8>>>,
           modified: Option<u64>) ->
      KeyEntryStub
    {
      KeyEntryStub{parent_id: parent,
                   id: random_ascii_bytes(),
                   name: name,
                   data: data,
                   modified: modified}
    }
  }

  impl KeyEntry<KeyEntryStub> for KeyEntryStub {

    fn id(&self) -> Option<Vec<u8>> {
      Some(self.id.clone())
    }

    fn parent_id(&self) -> Option<Vec<u8>> {
      self.parent_id.clone()
    }

    fn name(&self) -> Vec<u8> {
      self.name.as_slice().into_owned()
    }

    fn size(&self) -> Option<u64> {
      None
    }

    fn permissions(&self) -> Option<u64> {
      None
    }

    fn created(&self) -> Option<u64> {
      None
    }

    fn accessed(&self) -> Option<u64> {
      None
    }

    fn modified(&self) -> Option<u64> {
      self.modified
    }

    fn user_id(&self) -> Option<u64> {
      None
    }

    fn group_id(&self) -> Option<u64> {
      None
    }

    fn with_id(&self, id: Vec<u8>) -> KeyEntryStub {
      assert_eq!(self.id, id);
      self.clone()
    }

  }

  impl Iterator<Vec<u8>> for KeyEntryStub {
    fn next(&mut self) -> Option<Vec<u8>> {
      if self.data.is_none() { return None }
      self.data.get_mut_ref().shift()
    }
  }

  #[deriving(Clone)]
  struct FileSystem {
    file: KeyEntryStub,
    filelist: Vec<FileSystem>,
  }

  fn rng_filesystem(size: uint) -> FileSystem
  {
    fn create_files(parent_id: Option<Vec<u8>>, size: uint) -> Vec<FileSystem> {
      let children = size as f32 / 10.0;

      // dist_factor * i for i in range(children) + children == size
      let dist_factor: f32 = (size as f32 - children) / ((children * (children - 1.0)) / 2.0);

      let mut child_size = 0.0 as f32;

      let mut files = Vec::new();
      for _ in range(0, children as uint) {

        let data_opt = if task_rng().gen() { None }
                       else { Some(Vec::from_fn(8, |_| random_ascii_bytes()))
                       };

        let modified: u64 = task_rng().gen();
        let new_root = KeyEntryStub::new(
          parent_id.clone(),
          random_ascii_bytes(),
          data_opt,
          Some(modified % 2147483648)
        );

        let new_root_id = new_root.id();

        files.push(FileSystem{file: new_root,
                              filelist: create_files(new_root_id, child_size as uint)});
        child_size += dist_factor;
      }
      files
    }

    let root = KeyEntryStub::new(None,
                                 b"root".into_owned(),
                                 None, Some(123));
    let root_id = root.id();
    FileSystem{file: root,
               filelist: create_files(root_id, size)}
  }

  fn insert_fs(fs: &FileSystem, ksP: KeyStoreProcess<KeyEntryStub, KeyEntryStub, MemoryBackend>) {

    let local_file = fs.file.clone();
    ksP.send_reply(Insert(fs.file.clone(),
                         if fs.file.data.is_some() {
                           Some(proc() { Some(local_file) }) } else { None }));

    for fs in fs.filelist.iter() {
      insert_fs(fs, ksP.clone());
    }
  }

  fn verify_filesystem(fs: &FileSystem,
                       ksP: KeyStoreProcess<KeyEntryStub, KeyEntryStub, MemoryBackend>) -> uint {

    let listing = match ksP.send_reply(ListDir(fs.file.id())) {
      ListResult(ls) => ls,
      _ => fail!("Unexpected result from key store."),
    };

    assert_eq!(fs.filelist.len(), listing.len());

    for (id, name, created, modified, accessed, hash, persistent_ref, tree_data)
      in listing.move_iter() {
      let mut found = false;

      for dir in fs.filelist.iter() {
        if dir.file.name() == name {
          found = true;

          assert_eq!(dir.file.id().unwrap(), id);
          assert_eq!(dir.file.created().unwrap_or(0), created);
          assert_eq!(dir.file.accessed().unwrap_or(0), accessed);
          assert_eq!(dir.file.modified().unwrap_or(0), modified);

          match dir.file.data {
            Some(ref original) => {
              let it = match tree_data {
                hash_tree::NoData => fail!("No data."),
                hash_tree::SingleBlock(chunk) => {
                  assert!(original.len() <= 1);
                  assert_eq!(original.last().unwrap_or(&b"".into_owned()),
                             &chunk);
                  break
                },
                hash_tree::Tree(it) => it,
              };
              let mut chunk_count = 0;
              for (i, chunk) in it.enumerate() {
                assert_eq!(original.get(i), &chunk);
                chunk_count += 1;
              }
              assert_eq!(original.len(), chunk_count);
            },
            None => {
              assert_eq!(hash, b"".into_owned());
              assert_eq!(persistent_ref, b"".into_owned());
            }
          }

          break;  // Proceed to check next file
        }
      }
      assert_eq!(true, found);
    }

    let mut count = fs.filelist.len();
    for dir in fs.filelist.iter() {
      count += verify_filesystem(dir, ksP.clone());
    }

    count
  }

  #[test]
  fn identity() {
    fn prop(size: u8) -> bool {
      let fs = rng_filesystem(size as uint);

      let backend = MemoryBackend::new();
      let ksP = Process::new(proc() { KeyStore::new_for_testing(backend) });

      insert_fs(&fs, ksP.clone());

      match ksP.send_reply(Flush) {
        FlushOK => (),
        _ => fail!("Unexpected result from key store."),
      }

      verify_filesystem(&fs, ksP.clone());

      true
    }

    qcheck(prop);
  }


  #[bench]
  fn insert_1_key_x_128000_zeros(bench: &mut Bencher) {
    let backend = DevNullBackend;
    let ksP : KeyStoreProcess<KeyEntryStub, KeyEntryStub, DevNullBackend>
      = Process::new(proc() { KeyStore::new_for_testing(backend) });

    let bytes = Vec::from_elem(128*1024, 0u8);

    let mut i = 0u;
    bench.iter(|| {
      i += 1;

      let entry = KeyEntryStub::new(None, format!("{}", i).as_bytes().into_owned(),
                                    Some(vec![bytes.clone()]), None);
      ksP.send_reply(Insert(entry.clone(), Some(proc() { Some(entry) })));

    });

    bench.bytes = 128 * 1024;

  }


  #[bench]
  fn insert_1_key_x_128000_unique(bench: &mut Bencher) {
    let backend = DevNullBackend;
    let ksP : KeyStoreProcess<KeyEntryStub, KeyEntryStub, DevNullBackend>
      = Process::new(proc() { KeyStore::new_for_testing(backend) });

    let bytes = Vec::from_elem(128*1024, 0u8);

    let mut i = 0u;
    bench.iter(|| {
      i += 1;

      let mut my_bytes = bytes.clone();
      {
        let mut mut_view = my_bytes.as_mut_slice();
        mut_view[0] = i as u8;
        mut_view[1] = (i / 256) as u8;
        mut_view[2] = (i / 65536) as u8;
      }
      let my_bytes = my_bytes;

      let entry = KeyEntryStub::new(None, format!("{}", i).as_bytes().into_owned(),
                                    Some(vec!(my_bytes)), None);
      ksP.send_reply(Insert(entry.clone(), Some(proc() { Some(entry) })));

    });

    bench.bytes = 128 * 1024;

  }


  #[bench]
  fn insert_1_key_x_16_x_128000_zeros(bench: &mut Bencher) {
    let backend = DevNullBackend;
    let ksP : KeyStoreProcess<KeyEntryStub, KeyEntryStub, DevNullBackend>
      = Process::new(proc() { KeyStore::new_for_testing(backend) });

    bench.iter(|| {
      let bytes = Vec::from_elem(128*1024, 0u8);
      let entry = KeyEntryStub::new(None,
                                    vec![1u8, 2, 3].as_slice().into_owned(),
                                    Some(Vec::from_elem(16, bytes)),
                                    None);

      ksP.send_reply(Insert(entry.clone(), Some(proc() { Some(entry) })));

      match ksP.send_reply(Flush) {
        FlushOK => (),
        _ => fail!("Unexpected result from key store."),
      }
    });

    bench.bytes = 16 * (128 * 1024);
  }



  #[bench]
  fn insert_1_key_x_16_x_128000_unique(bench: &mut Bencher) {
    let backend = DevNullBackend;
    let ksP : KeyStoreProcess<KeyEntryStub, KeyEntryStub, DevNullBackend>
      = Process::new(proc() { KeyStore::new_for_testing(backend) });

    let bytes = Vec::from_elem(128*1024, 0u8);
    let mut i = 0;

    bench.iter(|| {
      i += 1;

      let mut my_bytes = bytes.clone();
      {
        let mut_view = my_bytes.as_mut_slice();
        mut_view[0] = i as u8;
        mut_view[1] = (i / 256) as u8;
        mut_view[2] = (i / 65536) as u8;
      }
      let my_bytes = my_bytes;

      let entry = KeyEntryStub::new(None,
                                    vec![1u8, 2, 3].as_slice().into_owned(),
                                    Some(Vec::from_fn(16, |i| {
                                      let mut local_bytes = my_bytes.clone();
                                      local_bytes.as_mut_slice()[3] = i as u8;
                                      local_bytes
                                    })),
                                    None);

      ksP.send_reply(Insert(entry.clone(), Some(proc() { Some(entry) })));

      match ksP.send_reply(Flush) {
        FlushOK => (),
        _ => fail!("Unexpected result from key store."),
      }
    });

    bench.bytes = 16 * (128 * 1024);
  }

}
