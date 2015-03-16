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

use std::thunk::Thunk;

use blob_store;
use hash_tree::{SimpleHashTreeWriter, HashTreeBackend,
                SimpleHashTreeReader, ReaderResult};
use hash_index;

use process::{Process, MsgHandler};

use key_index::{KeyIndexProcess, KeyEntry};
use key_index;


#[cfg(test)]
use key_index::{KeyIndex};

pub type KeyStoreProcess<KE, IT> = Process<Msg<KE, IT>, Reply>;

pub type DirElem = (Vec<u8>, Vec<u8>, u64, u64, u64, Vec<u8>, Vec<u8>,
                    ReaderResult<HashStoreBackend>);

// Public structs
pub enum Msg<KE, IT> {

  /// Insert a key into the index. If this key has associated data a "chunk-iterator creator" can be
  /// passed along with it. If the data turns out to be unreadable, this iterator proc can return
  /// `None`.
  /// Returns `Id` with the new entry ID.
  Insert(KE, Option<Thunk<'static, (), Option<IT>>>),

  /// List a "directory" (aka. a `level`) in the index.
  /// Returns `ListResult` with all the entries under the given parent.
  ListDir(Option<Vec<u8>>),

  /// Flush this key store and its dependencies.
  /// Returns `FlushOK`.
  Flush,
}

pub enum Reply {
  Id(Vec<u8>),
  ListResult(Vec<DirElem>),
  FlushOK,
}

pub struct KeyStore<KE> {
  index: KeyIndexProcess<KE>,
  hash_index: hash_index::HashIndexProcess,
  blob_store: blob_store::BlobStoreProcess,
}

// Implementations
impl <KE: 'static + KeyEntry<KE> + Send> KeyStore<KE>
{
  pub fn new(index: KeyIndexProcess<KE>,
             hash_index: hash_index::HashIndexProcess,
             blob_store: blob_store::BlobStoreProcess) -> KeyStore<KE> {
    KeyStore{index: index, hash_index: hash_index, blob_store: blob_store}
  }

  #[cfg(test)]
  pub fn new_for_testing<B:'static + blob_store::BlobStoreBackend + Send + Clone>(backend: B) -> KeyStore<KE> {
    let ki_p = Process::new(Thunk::new(move|| { KeyIndex::new_for_testing() }));
    let hi_p = Process::new(Thunk::new(move|| { hash_index::HashIndex::new_for_testing() }));
    let bs_p = Process::new(Thunk::new(move|| { blob_store::BlobStore::new_for_testing(backend, 1024) }));
    KeyStore{index: ki_p, hash_index: hi_p, blob_store: bs_p}
  }

  pub fn flush(&mut self) {
    self.blob_store.send_reply(blob_store::Msg::Flush);
    self.hash_index.send_reply(hash_index::Msg::Flush);
    self.index.send_reply(key_index::Msg::Flush);
  }

  pub fn hash_tree_writer(&mut self) -> SimpleHashTreeWriter<HashStoreBackend> {
    let backend = HashStoreBackend::new(self.hash_index.clone(), self.blob_store.clone());
    return SimpleHashTreeWriter::new(8, backend);
  }
}

#[derive(Clone)]
pub struct HashStoreBackend {
  hash_index: hash_index::HashIndexProcess,
  blob_store: blob_store::BlobStoreProcess,
}

impl HashStoreBackend {
  fn new(hash_index: hash_index::HashIndexProcess,
         blob_store: blob_store::BlobStoreProcess)
         -> HashStoreBackend {
    HashStoreBackend{hash_index: hash_index, blob_store: blob_store}
  }

  fn fetch_chunk_from_hash(&mut self, hash: hash_index::Hash) -> Option<Vec<u8>> {
    assert!(hash.bytes.len() > 0);
    match self.hash_index.send_reply(hash_index::Msg::FetchPersistentRef(hash)) {
      hash_index::Reply::PersistentRef(chunk_ref_bytes) => {
        let chunk_ref = blob_store::BlobID::from_bytes(chunk_ref_bytes);
        self.fetch_chunk_from_persistent_ref(chunk_ref)
      },
      _ => None  // TODO: Do we need to distinguish `missing` from `unknown ref`?
    }
  }

  fn fetch_chunk_from_persistent_ref(&mut self, chunk_ref: blob_store::BlobID) -> Option<Vec<u8>> {
    match self.blob_store.send_reply(blob_store::Msg::Retrieve(chunk_ref)) {
      blob_store::Reply::RetrieveOK(chunk) => Some(chunk),
      _ => None
    }
  }
}

impl HashTreeBackend for HashStoreBackend
{
  fn fetch_chunk(&mut self, hash: hash_index::Hash) -> Option<Vec<u8>> {
    assert!(hash.bytes.len() > 0);
    return self.fetch_chunk_from_hash(hash);
  }

  fn fetch_persistent_ref(&mut self, hash: hash_index::Hash) -> Option<Vec<u8>> {
    assert!(hash.bytes.len() > 0);
    loop {
      match self.hash_index.send_reply(hash_index::Msg::FetchPersistentRef(hash.clone())) {
        hash_index::Reply::PersistentRef(r) => { return Some(r) }, // done
        hash_index::Reply::HashNotKnown => { return None }, // done
        hash_index::Reply::Retry => (),  // continue loop
        _ => panic!("Unexpected reply from hash index."),
      }
    };
  }

  fn fetch_payload(&mut self, hash: hash_index::Hash) -> Option<Vec<u8>> {
    match self.hash_index.send_reply(hash_index::Msg::FetchPayload(hash)) {
      hash_index::Reply::Payload(p) => { return p }, // done
      hash_index::Reply::HashNotKnown => { return None }, // done
      _ => panic!("Unexpected reply from hash index."),
    }
  }

  fn insert_chunk(&mut self, hash: hash_index::Hash, level: i64, payload: Option<Vec<u8>>,
                  chunk: Vec<u8>) -> Vec<u8> {
    assert!(hash.bytes.len() > 0);

    let mut hash_entry = hash_index::HashEntry{hash:hash.clone(), level:level, payload:payload,
                                               persistent_ref: None};

    match self.hash_index.send_reply(hash_index::Msg::Reserve(hash_entry.clone())) {
      hash_index::Reply::HashKnown => {
        // Someone came before us: piggyback on their result.
        return self.fetch_persistent_ref(hash).expect(
          "Could not find persistent_ref for known chunk.");
      },
      hash_index::Reply::ReserveOK => {
        // We came first: this data-chunk is ours to process.
        let local_hash_index = self.hash_index.clone();

        let callback = Thunk::with_arg(move|blobid: blob_store::BlobID| {
          local_hash_index.send_reply(hash_index::Msg::Commit(hash, blobid.as_bytes()));
        });
        match self.blob_store.send_reply(blob_store::Msg::Store(chunk, callback)) {
          blob_store::Reply::StoreOK(blob_ref) => {
            hash_entry.persistent_ref = Some(blob_ref.as_bytes());
            self.hash_index.send_reply(hash_index::Msg::UpdateReserved(hash_entry));
            return blob_ref.as_bytes();
          },
          _ => panic!("Unexpected reply from BlobStore."),
        };
      },
      _ => panic!("Unexpected HashIndex reply."),
    };
  }
}

fn file_size_warning(name: Vec<u8>, wanted: u64, got: u64) {
  if wanted < got {
    println!("Warning: File grew while reading it: {:?} (wanted {}, got {})", name, wanted, got)
  } else if wanted > got {
    println!("Warning: Could not read whole file (or it shrank): {:?} (wanted {}, got {})",
             name, wanted, got)
  }
}

impl
  <KE: 'static + KeyEntry<KE> + Send + Clone, IT: Iterator<Item=Vec<u8>>>
  MsgHandler<Msg<KE, IT>, Reply> for KeyStore<KE>
{
  fn handle(&mut self, msg: Msg<KE, IT>, reply: Box<Fn(Reply)>) {
    match msg {
      Msg::Flush => {
        self.flush();
        return reply(Reply::FlushOK);
      },

      Msg::ListDir(parent) => {
        match self.index.send_reply(key_index::Msg::ListDir(parent)) {
          key_index::Reply::ListResult(entries) => {
            // TODO(jos): Rewrite this tuple hell
            let mut my_entries = Vec::with_capacity(entries.len());
            for (id, name, created, modified, accessed, hash, persistent_ref) in entries.into_iter()
            {
              let local_hash = hash_index::Hash{bytes: hash.clone()};
              let local_ref = persistent_ref.clone();

              my_entries.push(
                (id, name, created, modified, accessed, hash, persistent_ref,
                   SimpleHashTreeReader::new(
                     HashStoreBackend::new(self.hash_index.clone(), self.blob_store.clone()),
                     local_hash, local_ref)
                 ));
            }
            return reply(Reply::ListResult(my_entries));
          },
          _ => panic!("Unexpected result from key index."),
        }
      },

      Msg::Insert(org_entry, chunk_it_opt) => {
        match self.index.send_reply(key_index::Msg::LookupExact(org_entry.clone())) {

          key_index::Reply::Id(entry_id) => {
            return reply(Reply::Id(entry_id));
          },

          _ => {
            let id = match self.index.send_reply(key_index::Msg::Insert(org_entry.clone())) {
              key_index::Reply::Id(entry_id) => entry_id,
              _ => panic!("No ID returned from key index Insert()."),
            };

            // Send out the ID early to allow the client to continue its key discovery routine.
            // The bounded input-channel will prevent the client from overflowing us.
            reply(Reply::Id(id.clone()));

            // Setup hash tree structure
            let mut tree = self.hash_tree_writer();

            // Check if we have an data source:
            let it_opt = chunk_it_opt.and_then(|p| p.invoke(()));
            if it_opt.is_none() {
              // No data is associated with this entry.
              self.index.send_reply(key_index::Msg::UpdateDataHash(org_entry, None, None));
              // Bail out before storing data that does not exist:
              return;
            }

            // Read and insert all file chunks:
            // (see HashStoreBackend::insert_chunk above)
            let mut bytes_read = 0u64;
            for chunk in it_opt.unwrap() {
              bytes_read += chunk.len() as u64;
              tree.append(chunk);
            }

            // Warn the user if we did not read the expected size:
            org_entry.size().map(|s| { file_size_warning(org_entry.name(), s, bytes_read); });

            // Get top tree hash:
            let (hash, persistent_ref) = tree.hash();

            // Install a callback for updating the entry's data hash once the data has been stored:
            let new_entry = org_entry.with_id(id);
            let local_index = self.index.clone();
            let hash_bytes = hash.bytes.clone();
            let callback = Thunk::new(move|| {
              local_index.send_reply(
                key_index::Msg::UpdateDataHash(new_entry, Some(hash_bytes), Some(persistent_ref)));
            });
            self.hash_index.send_reply(hash_index::Msg::CallAfterHashIsComitted(hash, callback));
          }
        }
      }
    }
  }
}


#[cfg(test)]
mod tests {
  use super::*;

  use std::thunk::Thunk;

  use key_index::{KeyEntry};
  use blob_store::tests::{MemoryBackend, DevNullBackend};
  use hash_tree;

  use process::{Process};

  use rand::Rng;
  use rand::thread_rng;

  use test::{Bencher};

  fn random_ascii_bytes() -> Vec<u8> {
    let ascii: String = thread_rng().gen_ascii_chars().take(32).collect();
    ascii.into_bytes()
  }

  #[derive(Clone, Debug)]
  struct KeyEntryStub {
    parent_id: Option<Vec<u8>>,

    id: Vec<u8>,
    name: Vec<u8>,

    data: Option<Vec<Vec<u8>>>,

    modified: Option<u64>,
  }

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
      self.name.as_slice().to_vec()
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

  impl Iterator for KeyEntryStub {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Vec<u8>> {
      match self.data.as_mut() {
        Some(x) => if x.len() > 0 { Some(x.remove(0)) } else { None },
        None => None,
      }
    }
  }

  #[derive(Clone, Debug)]
  struct FileSystem {
    file: KeyEntryStub,
    filelist: Vec<FileSystem>,
  }

  fn rng_filesystem(size: usize) -> FileSystem
  {
    fn create_files(parent_id: Option<Vec<u8>>, size: usize) -> Vec<FileSystem> {
      let children = size as f32 / 10.0;

      // dist_factor * i for i in range(children) + children == size
      let dist_factor: f32 = (size as f32 - children) / ((children * (children - 1.0)) / 2.0);

      let mut child_size = 0.0 as f32;

      let mut files = Vec::new();
      for _ in range(0, children as usize) {

        let data_opt = if thread_rng().gen() { None } else {
          let mut v = vec![];
          for i in range(0, 8) {
            v.push(random_ascii_bytes())
          }
          Some(v)
         };

        let modified: u64 = thread_rng().gen();
        let new_root = KeyEntryStub::new(
          parent_id.clone(),
          random_ascii_bytes(),
          data_opt,
          Some(modified % 2147483648)
        );

        let new_root_id = new_root.id();

        files.push(FileSystem{file: new_root,
                              filelist: create_files(new_root_id, child_size as usize)});
        child_size += dist_factor;
      }
      files
    }

    let root = KeyEntryStub::new(None,
                                 b"root".to_vec(),
                                 None, Some(123));
    let root_id = root.id();
    FileSystem{file: root,
               filelist: create_files(root_id, size)}
  }

  fn insert_fs(fs: &FileSystem, ks_p: KeyStoreProcess<KeyEntryStub, KeyEntryStub>) {

    let local_file = fs.file.clone();
    ks_p.send_reply(Msg::Insert(fs.file.clone(),
                               if fs.file.data.is_some() {
                                 Some(Thunk::new(move|| { Some(local_file) }))
                               } else { None }));

    for fs in fs.filelist.iter() {
      insert_fs(fs, ks_p.clone());
    }
  }

  fn verify_filesystem(fs: &FileSystem,
                       ks_p: KeyStoreProcess<KeyEntryStub, KeyEntryStub>) -> usize {

    let listing = match ks_p.send_reply(Msg::ListDir(fs.file.id())) {
      Reply::ListResult(ls) => ls,
      _ => panic!("Unexpected result from key store."),
    };

    assert_eq!(fs.filelist.len(), listing.len());

    for (id, name, created, modified, accessed, hash, persistent_ref, tree_data) in listing {
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
                hash_tree::ReaderResult::NoData => panic!("No data."),
                hash_tree::ReaderResult::SingleBlock(chunk) => {
                  assert!(original.len() <= 1);
                  assert_eq!(original.last().unwrap_or(&b"".to_vec()),
                             &chunk);
                  break
                },
                hash_tree::ReaderResult::Tree(it) => it,
              };
              let mut chunk_count = 0;
              for (i, chunk) in it.enumerate() {
                assert_eq!(original.get(i), Some(&chunk));
                chunk_count += 1;
              }
              assert_eq!(original.len(), chunk_count);
            },
            None => {
              assert_eq!(hash, b"".to_vec());
              assert_eq!(persistent_ref, b"".to_vec());
            }
          }

          break;  // Proceed to check next file
        }
      }
      assert_eq!(true, found);
    }

    let mut count = fs.filelist.len();
    for dir in fs.filelist.iter() {
      count += verify_filesystem(dir, ks_p.clone());
    }

    count
  }

  #[quickcheck]
  fn identity(size: u8) -> bool {
    let fs = rng_filesystem(size as usize);

    let backend = MemoryBackend::new();
    let ks_p = Process::new(Thunk::new(move|| { KeyStore::new_for_testing(backend) }));

    insert_fs(&fs, ks_p.clone());

    match ks_p.send_reply(Msg::Flush) {
      Reply::FlushOK => (),
      _ => panic!("Unexpected result from key store."),
    }

    verify_filesystem(&fs, ks_p.clone());

    true
  }


  #[bench]
  fn insert_1_key_x_128000_zeros(bench: &mut Bencher) {
    let backend = DevNullBackend;
    let ks_p : KeyStoreProcess<KeyEntryStub, KeyEntryStub>
      = Process::new(Thunk::new(move|| { KeyStore::new_for_testing(backend) }));

    let bytes = vec![0u8; 128*1024];

    let mut i = 0i32;
    bench.iter(|| {
      i += 1;

      let entry = KeyEntryStub::new(None, format!("{}", i).as_bytes().to_vec(),
                                    Some(vec![bytes.clone()]), None);
      ks_p.send_reply(Msg::Insert(entry.clone(), Some(Thunk::new(move|| { Some(entry) }))));

    });

    bench.bytes = 128 * 1024;

  }


  #[bench]
  fn insert_1_key_x_128000_unique(bench: &mut Bencher) {
    let backend = DevNullBackend;
    let ks_p : KeyStoreProcess<KeyEntryStub, KeyEntryStub>
      = Process::new(Thunk::new(move|| { KeyStore::new_for_testing(backend) }));

    let bytes = vec![0u8; 128*1024];

    let mut i = 0i32;
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

      let entry = KeyEntryStub::new(None, format!("{}", i).as_bytes().to_vec(),
                                    Some(vec!(my_bytes)), None);
      ks_p.send_reply(Msg::Insert(entry.clone(), Some(Thunk::new(move|| { Some(entry) }))));

    });

    bench.bytes = 128 * 1024;

  }


  #[bench]
  fn insert_1_key_x_16_x_128000_zeros(bench: &mut Bencher) {
    let backend = DevNullBackend;
    let ks_p : KeyStoreProcess<KeyEntryStub, KeyEntryStub>
      = Process::new(Thunk::new(move|| { KeyStore::new_for_testing(backend) }));

    bench.iter(|| {
      let bytes = vec![0u8; 128*1024];
      let entry = KeyEntryStub::new(None,
                                    vec![1u8, 2, 3].to_vec(),
                                    Some(vec![bytes; 16]),
                                    None);

      ks_p.send_reply(Msg::Insert(entry.clone(), Some(Thunk::new(move|| { Some(entry) }))));

      match ks_p.send_reply(Msg::Flush) {
        Reply::FlushOK => (),
        _ => panic!("Unexpected result from key store."),
      }
    });

    bench.bytes = 16 * (128 * 1024);
  }



  #[bench]
  fn insert_1_key_x_16_x_128000_unique(bench: &mut Bencher) {
    let backend = DevNullBackend;
    let ks_p : KeyStoreProcess<KeyEntryStub, KeyEntryStub>
      = Process::new(Thunk::new(move|| { KeyStore::new_for_testing(backend) }));

    let bytes = vec![0u8; 128*1024];
    let mut i = 0i32;

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

      let mut chunks = vec![];
      for i in range(0, 16) {
        let mut local_bytes = my_bytes.clone();
        local_bytes.as_mut_slice()[3] = i as u8;
        chunks.push(local_bytes);
      }

      let entry = KeyEntryStub::new(None, vec![1u8, 2, 3], Some(chunks), None);

      ks_p.send_reply(Msg::Insert(entry.clone(), Some(Thunk::new(move|| { Some(entry) }))));

      match ks_p.send_reply(Msg::Flush) {
        Reply::FlushOK => (),
        _ => panic!("Unexpected result from key store."),
      }
    });

    bench.bytes = 16 * (128 * 1024);
  }

}
