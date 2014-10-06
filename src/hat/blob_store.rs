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

//! Combines data chunks into larger blobs to be stored externally.

use std::sync::{Arc, Mutex};

use serialize::{json, Encodable, Decodable};
use serialize::hex::{ToHex};
use serialize::json::{Json, ToJson, Decoder, from_str};

use std::collections::treemap::{TreeMap};
use std::collections::lru_cache::{LruCache};

use std::io::{File};
use std::str;

use process::{Process, MsgHandler};

use blob_index;
use blob_index::{BlobIndexProcess};

#[cfg(test)]
use blob_index::{BlobIndex};


pub type BlobStoreProcess<B> = Process<Msg, Reply, BlobStore<B>>;

pub trait BlobStoreBackend {
  fn store(&mut self, name: &[u8], data: &[u8]) -> Result<(), String>;
  fn retrieve(&mut self, name: &[u8]) -> Result<Vec<u8>, String>;
}


#[deriving(Clone)]
pub struct FileBackend {
  root: Path,
  read_cache: Arc<Mutex<LruCache<Vec<u8>, Result<Vec<u8>, String>>>>,
}

impl FileBackend {

  pub fn new(root: Path) -> FileBackend {
    FileBackend{root: root, read_cache: Arc::new(Mutex::new(LruCache::new(10)))}
  }

  fn guarded_cache_get(&self, name: &Vec<u8>) -> Option<Result<Vec<u8>, String>> {
    self.read_cache.lock().get(name).map(|v| v.clone())
  }

  fn guarded_cache_put(&mut self, name: Vec<u8>, result: Result<Vec<u8>, String>) {
    self.read_cache.lock().put(name, result);
  }
}

impl BlobStoreBackend for FileBackend {

  fn store(&mut self, name: &[u8], data: &[u8]) -> Result<(), String> {
    let mut path = self.root.clone();
    path.push(name.to_hex());

    let mut file = match File::create(&path) {
      Err(e) => return Err(e.to_str()),
      Ok(f) => f,
    };

    match file.write(data) {
      Err(e) => Err(e.to_str()),
      Ok(()) => Ok(()),
    }
  }

  fn retrieve(&mut self, name: &[u8]) -> Result<Vec<u8>, String> {
    // Check for key in cache:
    let name = name.into_owned();
    let value_opt = self.guarded_cache_get(&name);
    match value_opt {
      Some(result) => return result,
      None => (),
    }

    // Read key:
    let path = { let mut p = self.root.clone();
                 p.push(name.as_slice().to_hex());
                 p };

    let mut fd = File::open(&path).unwrap();

    let res = fd.read_to_end().and_then(|data| {
      Ok(data.into_owned()) }).or_else(|e| Err(e.to_str()));

    // Update cache to contain key:
    self.guarded_cache_put(name, res.clone());

    return res;
  }

}


#[deriving(Show, Clone, Eq, PartialEq, Encodable, Decodable)]
pub struct BlobID {
  name: Vec<u8>,
  begin: uint,
  end: uint,
}

impl BlobID {

  pub fn from_bytes(bytes: Vec<u8>) -> BlobID {
    let mut decoder = Decoder::new(from_str(
      str::from_utf8(bytes.as_slice()).unwrap()).unwrap());
    Decodable::decode(&mut decoder).unwrap()
  }

  pub fn as_bytes(&self) -> Vec<u8> {
    self.to_json().to_str().as_bytes().into_owned()
  }

}

impl ToJson for BlobID {
  fn to_json(&self) -> Json {
    let mut m = box TreeMap::new();
    m.insert("name".to_string(), self.name.to_json());
    m.insert("begin".to_string(), self.begin.to_json());
    m.insert("end".to_string(), self.end.to_json());
    json::Object(m).to_json()
  }
}


pub enum Msg {
  /// Store a new data chunk into the current blob. The callback is triggered after the blob
  /// containing the chunk has been committed to persistent storage (it is then safe to use the
  /// `BlobID` as persistent reference).
  Store(Vec<u8>, proc(BlobID):Send -> ()),
  /// Retrieve the data chunk identified by `BlobID`.
  Retrieve(BlobID),
  /// Flush the current blob, independent of its size.
  Flush,
}


#[deriving(Eq, PartialEq, Show)]
pub enum Reply {
  StoreOK(BlobID),
  RetrieveOK(Vec<u8>),
  FlushOK,
}


pub struct BlobStore<B> {
  backend: B,

  blob_index: BlobIndexProcess,
  blob_desc: blob_index::BlobDesc,

  buffer_data: Vec<(BlobID, Vec<u8>, proc(BlobID):Send -> ())>,
  buffer_data_len: uint,

  max_blob_size: uint,
}


fn empty_blob_desc() -> blob_index::BlobDesc {
  blob_index::BlobDesc{name: b"".into_owned(), id: 0}
}


impl <B: BlobStoreBackend> BlobStore<B> {

  pub fn new(index: BlobIndexProcess, backend: B,
             max_blob_size: uint) -> BlobStore<B> {
    let mut bs = BlobStore{
      backend: backend,
      blob_index: index,
      blob_desc: empty_blob_desc(),
      buffer_data: Vec::new(),
      buffer_data_len: 0,
      max_blob_size: max_blob_size,
    };
    bs.reserve_new_blob();
    bs
  }

  #[cfg(test)]
  pub fn new_for_testing(backend: B, max_blob_size: uint) -> BlobStore<B> {
    let biP = Process::new(proc() { BlobIndex::new_for_testing() });
    let mut bs = BlobStore{backend: backend,
                           blob_index: biP,
                           blob_desc: empty_blob_desc(),
                           buffer_data: Vec::new(),
                           buffer_data_len: 0,
                           max_blob_size: max_blob_size,
                          };
    bs.reserve_new_blob();
    bs
  }

  fn reserve_new_blob(&mut self) -> blob_index::BlobDesc {
    let old_blob_desc = self.blob_desc.clone();

    let res = self.blob_index.send_reply(blob_index::Reserve);
    match res {
      blob_index::Reserved(blob_desc) => {
        self.blob_desc = blob_desc;
      },
      _ => fail!("Could not reserve blob."),
    }

    old_blob_desc
  }

  fn backend_store(&mut self, name: &[u8], blob: &[u8]) {
    match self.backend.store(name, blob) {
      Ok(()) => (),
      Err(s) => fail!(s),
    }
  }

  fn backend_read(&mut self, name: &[u8]) -> Vec<u8> {
    match self.backend.retrieve(name) {
      Ok(data) => data,
      Err(s) => fail!(s),
    }
  }

  fn flush(&mut self) {
    if self.buffer_data_len == 0 { return }

    // Replace blob id
    let old_blob_desc = self.reserve_new_blob();
    self.buffer_data_len = 0;

    // Prepare blob
    let mut ready_callback = Vec::new();
    let mut blob = Vec::new();
    loop {
      match self.buffer_data.shift() {
        Some((chunk_ref, chunk, cb)) => {
          ready_callback.push((chunk_ref, cb));
          blob.push_all(chunk.as_slice());
        },
        None => break,
      }
    }

    self.blob_index.send_reply(blob_index::InAir(old_blob_desc.clone()));
    self.backend_store(old_blob_desc.name.as_slice(), blob.as_slice());
    self.blob_index.send_reply(blob_index::CommitDone(old_blob_desc));

    // Go through callbacks
    for (blobid, cb) in ready_callback.move_iter() {
      cb(blobid);
    }
  }

  fn maybe_flush(&mut self) {
    if self.buffer_data_len >= self.max_blob_size {
      self.flush();
    }
  }
}

impl <B: BlobStoreBackend> MsgHandler<Msg, Reply> for BlobStore<B> {

  fn handle(&mut self, msg: Msg, reply: |Reply|) {
    match msg {

      Store(blob, cb) => {
        if blob.len() == 0 {
          let id = BlobID{name: vec!(0), begin: 0, end: 0};
          let cb_id = id.clone();
          spawn(proc(){ cb(cb_id) });
          return reply(StoreOK(id));
        }

        let new_size = self.buffer_data_len + blob.len();
        let id = BlobID{name: self.blob_desc.name.clone(),
                        begin: self.buffer_data_len,
                        end: new_size};

        self.buffer_data_len = new_size;
        self.buffer_data.push((id.clone(), blob.into_owned(), cb));

        // To avoid unnecessary blocking, we reply with the ID *before* possibly flushing.
        reply(StoreOK(id));

        // Flushing can be expensive, so try not block on it.
        self.maybe_flush();
       },

      Retrieve(id) => {
        if id.begin == 0 && id.end == 0 {
          return reply(RetrieveOK(vec![].into_owned()));
        }
        let blob = self.backend_read(id.name.as_slice());
        let chunk = blob.slice(id.begin, id.end);
        return reply(RetrieveOK(chunk.into_owned()));
      },

      Flush => {
        self.flush();
        return reply(FlushOK)
      },

    }
  }

}


#[cfg(test)]
pub mod tests {
  use super::*;
  use std::rand::{task_rng};
  use quickcheck::{Config, Testable, gen};
  use quickcheck::{quickcheck_config};

  use process::{Process};

  use std::sync::{Arc, Mutex};
  use std::collections::treemap::{TreeMap};

  #[deriving(Clone)]
  pub struct MemoryBackend {
    files: Arc<Mutex<TreeMap<Vec<u8>, Vec<u8>>>>
  }

  impl MemoryBackend {
    pub fn new() -> MemoryBackend {
      MemoryBackend{files: Arc::new(Mutex::new(TreeMap::new()))}
    }

    fn guarded_insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String>{
      let mut guarded_files = self.files.lock();
      if guarded_files.contains_key(&key) {
        return Err(format!("Key already exists: '{}'", key));
      }
      guarded_files.insert(key, value);
      Ok(())
    }

    fn guarded_retrieve(&mut self, key: &[u8]) -> Result<Vec<u8>, String> {
      let value_opt = self.files.lock().find(&key.into_owned()).map(|v| v.clone());
      value_opt.map(|v| Ok(v)).unwrap_or_else(|| Err(format!("Unknown key: '{}'", key)))
    }
  }

  impl BlobStoreBackend for MemoryBackend {

    fn store(&mut self, name: &[u8], data: &[u8]) -> Result<(), String> {
      self.guarded_insert(name.to_owned(), data.into_owned())
    }

    fn retrieve(&mut self, name: &[u8]) -> Result<Vec<u8>, String> {
      self.guarded_retrieve(name)
    }
  }

  #[deriving(Clone)]
  pub struct DevNullBackend;

  impl BlobStoreBackend for DevNullBackend {
    fn store(&mut self, _name: &[u8], _data: &[u8]) -> Result<(), String> {
      Ok(())
    }
    fn retrieve(&mut self, name: &[u8]) -> Result<Vec<u8>, String> {
      Err(format!("Unknown key: '{}'", name))
    }
  }


  // QuickCheck configuration
  static SIZE: uint = 100;
  static CONFIG: Config = Config {
    tests: 200,
    max_tests: 1000,
  };

  // QuickCheck helpers:
  fn qcheck<A: Testable>(f: A) {
    quickcheck_config(CONFIG, &mut gen(task_rng(), SIZE), f)
  }

  #[test]
  fn identity() {
    fn prop(chunks: Vec<Vec<u8>>) -> bool {
      let mut backend = MemoryBackend::new();

      let local_backend = backend.clone();
      let bsP : BlobStoreProcess<MemoryBackend> =
        Process::new(proc() { BlobStore::new_for_testing(local_backend, 1024) });

      let mut ids = Vec::new();
      for chunk in chunks.iter() {
        match bsP.send_reply(Store(chunk.as_slice().into_owned(), proc(_){})) {
          StoreOK(id) => { ids.push((id, chunk)); },
          _ => fail!("Unexpected reply from blob store."),
        }
      }

      assert_eq!(bsP.send_reply(Flush), FlushOK);

      // Non-empty chunks must be in the backend now:
      for &(ref id, chunk) in ids.iter() {
        if chunk.len() > 0 {
          match backend.retrieve(id.name.as_slice()) {
            Ok(_) => (),
            Err(e) => fail!(e),
          }
        }
      }

      // All chunks must be available through the blob store:
      for &(ref id, chunk) in ids.iter() {
        match bsP.send_reply(Retrieve(id.clone())) {
          RetrieveOK(found_chunk) => assert_eq!(found_chunk,
                                                chunk.as_slice().into_owned()),
          _ => fail!("Unexpected reply from blob store."),
        }
      }

      return true;
    }
    qcheck(prop);
  }


  #[test]
  fn identity_with_excessive_flushing() {
    fn prop(chunks: Vec<Vec<u8>>) -> bool {
      let mut backend = MemoryBackend::new();

      let local_backend = backend.clone();
      let bsP: BlobStoreProcess<MemoryBackend> = Process::new(proc() {
        BlobStore::new_for_testing(local_backend, 1024) });

      let mut ids = Vec::new();
      for chunk in chunks.iter() {
        match bsP.send_reply(Store(chunk.as_slice().into_owned(), proc(_){})) {
          StoreOK(id) => { ids.push((id, chunk)); },
          _ => fail!("Unexpected reply from blob store."),
        }
        assert_eq!(bsP.send_reply(Flush), FlushOK);
        let &(ref id, chunk) = ids.last().unwrap();
        assert_eq!(bsP.send_reply(Retrieve(id.clone())), RetrieveOK(chunk.clone()));
      }

      // Non-empty chunks must be in the backend now:
      for &(ref id, chunk) in ids.iter() {
        if chunk.len() > 0 {
          match backend.retrieve(id.name.as_slice()) {
            Ok(_) => (),
            Err(e) => fail!(e),
          }
        }
      }

      // All chunks must be available through the blob store:
      for &(ref id, chunk) in ids.iter() {
        match bsP.send_reply(Retrieve(id.clone())) {
          RetrieveOK(found_chunk) => assert_eq!(found_chunk,
                                                chunk.as_slice().into_owned()),
          _ => fail!("Unexpected reply from blob store."),
        }
      }

      return true;
    }
    qcheck(prop);
  }

  #[test]
  fn blobid_identity() {
    fn prop(name: Vec<u8>, begin: uint, end: uint) -> bool {
      let blob_id = BlobID{name: name.into_owned(),
                           begin: begin, end: end};
      BlobID::from_bytes(blob_id.as_bytes()) == blob_id
    }
    qcheck(prop);
  }

}
