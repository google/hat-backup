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

//! Hat's main way of storing list-like data externally.
//!
//! This module implements two structures for handling hash trees: A streaming hash-tree writer, and
//! a streaming hash-tree reader.

use serialize::{json, Encodable, Decodable};
use serialize::json::{Json, ToJson, Object, List};

use collections::treemap::{TreeMap};
use hash_index::{Hash};
use std::vec::{Vec};
use std::{str};

#[deriving(Clone, Encodable, Decodable)]
struct HashRef {
  pub hash: ~[u8],
  pub persistent_ref: ~[u8],
}

impl HashRef {
  fn new(hash: ~[u8], persistent_ref: ~[u8]) -> HashRef {
    HashRef{hash: hash, persistent_ref: persistent_ref}
  }
}

impl ToJson for HashRef {
  fn to_json(&self) -> Json {
    let mut m = box TreeMap::new();
    m.insert(StrBuf::from_str("hash"), self.hash.to_json());
    m.insert(StrBuf::from_str("persistent_ref"), self.persistent_ref.to_json());
    Object(m).to_json()
  }
}

fn vec_to_json<J: ToJson>(vec: &Vec<J>) -> Json {
  let mut json_vec = Vec::new();
  for x in vec.iter() {
    json_vec.push(x.to_json());
  }
  List(json_vec).to_json()
}

pub trait HashTreeBackend {

  fn fetch_chunk(&mut self, Hash) -> Option<~[u8]>;

  fn fetch_payload(&mut self, Hash) -> Option<~[u8]>;

  fn fetch_persistent_ref(&mut self, Hash) -> Option<~[u8]>;

  fn insert_chunk(&mut self, Hash, i64, Option<~[u8]>, ~[u8]) -> ~[u8];

}


fn hash_refs_to_bytes(refs: &Vec<HashRef>) -> ~[u8] {
  vec_to_json(refs).to_str().as_bytes().into_owned()
}

fn hash_refs_from_bytes(bytes: &[u8]) -> Option<Vec<HashRef>> {
  str::from_utf8(bytes).and_then(|s| {
    json::from_str(s).ok() }).and_then(|json| {
    Decodable::decode(&mut json::Decoder::new(json)).ok()
  })
}


/// A simple implementation of a hash-tree stream writer.
///
/// The hash-tree is "created" as append-only and is streamed from first to last data-block. The
///
/// ```rust
/// use hash_tree::SimpleHashTreeWriter;
///
/// let mut tree = SimpleHashTreeWriter::new(order, backend);
/// for data_chunk in chunk_iterator {
///   tree.append(data_chunk);
/// }
///
/// let (top_hash, top_persisitent_ref) = tree.hash();
/// ```
pub struct SimpleHashTreeWriter<B> {
  backend: B,
  order: uint,
  levels: Vec<Vec<HashRef>>,  // Representation of rightmost path to root
}

impl <B: HashTreeBackend + Clone> SimpleHashTreeWriter<B> {

  /// Create a new hash-tree to be stored through 'backend' with node order 'order'.
  pub fn new(order: uint, backend: B) -> SimpleHashTreeWriter<B> {
    SimpleHashTreeWriter{backend: backend,
                         order: order,
                         levels: Vec::new()}
  }

  fn top_level(&self) -> uint {
    self.levels.len() - 1
  }

  /// Append data-block to hash-tree.
  ///
  /// When reading back the tree, these blocks will be returned exactly as they were written, in the
  /// same order, and split at the same boundaries (i.e. pushing 1-bytes blocks will give 1-byte
  /// blocks when reading; if needed, accummulation of data must be handled by the `backend`).
  pub fn append(&mut self, chunk: ~[u8]) {
    self.append_at_level(0, chunk, None);
  }

  fn append_at_level(&mut self, level: uint, data: ~[u8],
                     metadata: Option<~[u8]>) {
    let hash = Hash::new(metadata.clone().unwrap_or_else(|| data.clone()));
    let persistent_ref = self.backend.insert_chunk(
      hash.clone(), level as i64, metadata, data);

    self.append_hashref_at_level(
      level, HashRef::new(hash.bytes.into_owned(), persistent_ref));
  }

  fn append_hashref_at_level(&mut self, level: uint, hashref: HashRef) {
    assert!(self.levels.len() >= level);
    if self.levels.len() == level {
      self.levels.push(Vec::new());
    }

    let is_full = {
      let level_v = self.levels.get_mut(level);
      level_v.push(hashref);
      level_v.len() == self.order
    };

    if is_full {
      self.collapse_level(level);
    }
  }

  fn collapse_level(&mut self, level: uint) {
    assert!(self.levels.len() > level);

    // Extract-replace level with a new empty level
    self.levels.push(Vec::new());
    let level_v = self.levels.swap_remove(level).unwrap();

    // All data from this level (hashes and references):
    let data = hash_refs_to_bytes(&level_v);

    // The hashes for this level is stored as metadata for future use:
    let metadata_bytes = {
      let mut metadata = Vec::new();
      for hashref in level_v.move_iter() {
        metadata.push_all(hashref.hash);
      }
      metadata.as_slice().into_owned()
    };

    self.append_at_level(level + 1, data, Some(metadata_bytes));
  }

  /// Retrieve the hash and backend persistent reference that identified this tree.
  ///
  /// This also flushes and finalizes the tree. It should be considered frozen after calling
  /// `hash()`, i.e. it's OK to call `hash()` multiple times, but it's **not OK** to call `append()`
  /// after `hash()`.
  pub fn hash(&mut self) -> (Hash, ~[u8]) {
    // Empty hash tree is equivalent to hash tree of one empty block:
    if self.levels.len() == 0 {
      self.append(bytes!().into_owned());
    }
    // Locate first level that isn't empty (has data to collapse)
    let first_non_empty_level = self.levels.iter().take_while(|level| level.len() == 0).len();
    // Unless only root has data, collapse all levels up to top level (which is handled next)
    range(first_non_empty_level, self.top_level()).map(|l| self.collapse_level(l)).last();
    // Collapse top level if possible
    let top_level = self.top_level();
    if self.levels.get(top_level).len() > 1 {
      self.collapse_level(top_level);
    }
    // After this point, only root exists and root has exactly one entry:
    assert_eq!(self.levels.last().map(|x| x.len()), Some(1));

    match self.levels.last().and_then(|x| x.last()) {
      Some(hashref) => (Hash{bytes:hashref.hash.clone()},
                        hashref.persistent_ref.clone()),
      None => fail!("Collapsed hash tree is missing its root node."),
    }
  }
}


/// A structure for reading hash-trees written with `SimpleHashTreeWriter`.
///
/// The hash-tree is "opened" as read-only and is streamed from first to last data-block. The data
/// blocks are read in the same order as they were written. The reader implement a `~[u8]` iterator
/// used for extracting the tree blocks.
///
/// ```rust
/// use hash_tree::SimpleHashTreeReader;
///
/// let tree_it = match SimpleHashTreeReader::new(backend, top_hash, top_persisitent_ref) {
///     SingleBlock(block) => return println(block),
///     Tree(it) => it,
/// };
///
/// for data_chunk in tree_it {
///     println(data_chunk);
/// }
/// ```
pub struct SimpleHashTreeReader<B> {
  backend: B,
  stack: Vec<(HashRef, Vec<HashRef>)>,
}

/// Wrapper for the result of `SimpleHashTreeReader::new()`.
///
/// We wrap the output of `SimpleHashTreeReader::new(...)` in a `ReaderResult` to fast-path the
/// single-block case (alternatively we could build a one-block iterator).
pub enum ReaderResult<B> {

  NoData,

  /// The tree consists of just a single node and its data-chunk is given here.
  SingleBlock(~[u8]),

  /// The tree has more than one node, so a data-chunk iterator is returned.
  Tree(SimpleHashTreeReader<B>),
}

impl <B: HashTreeBackend + Clone> SimpleHashTreeReader<B> {

  /// Creates a new `HashTreeReader` that reads through the `backend` the blocks of the hash tree
  /// defined by `root_hash` and `root_ref`.
  pub fn new(backend: B, root_hash: Hash, root_ref: ~[u8]) ->  ReaderResult<B>
  {
    if root_hash.bytes.len() == 0 {
      return NoData;
    }

    let data = backend.clone().fetch_chunk(root_hash.clone()).expect(
      "Could not find tree root hash.");

    match hash_refs_from_bytes(data) {
      None => SingleBlock(data), // There's no tree top, just a data block
      Some(childs) => Tree(SimpleHashTreeReader{
        stack: vec!((HashRef{hash: root_hash.bytes, persistent_ref: root_ref},
                     childs)),
        backend: backend}),
    }
  }

  fn extract(&mut self) -> Option<~[u8]> {
    while self.stack.len() > 0 {
      let child_opt = self.stack.mut_last().and_then(
        |&(_, ref mut childs)| childs.shift());

      match child_opt {
        Some(child_hash) => {
          let data = match self.backend.fetch_chunk(
            Hash{bytes: child_hash.hash.clone()})
          {
            None => fail!("Invalid hash ref."),
            Some(data) => data,
          };

          match hash_refs_from_bytes(data) {
            None => {
              return Some(data)
            },
            Some(new_childs) => {
              self.stack.push((child_hash, new_childs));
            }
          }
        },
        None => {
          // This node is done, move to parent
          self.stack.pop();
        }
      };
    }

    None
  }

}


impl <B: HashTreeBackend + Clone> Iterator<~[u8]> for SimpleHashTreeReader<B> {

  /// Read the next block of the hash-tree.
  /// This operation can be expensive, as it may require fetching a file through the backend.
  fn next(&mut self) -> Option<~[u8]> {
    self.extract()
  }
}


#[cfg(test)]
mod tests {
  use super::*;
  use test::{Bencher};

  use sync::{Arc, Mutex};

  use std;
  use hash_index::{Hash};
  use collections::hashmap::{HashMap, HashSet};

  use rand::{task_rng};
  use quickcheck::{Config, Testable, gen};
  use quickcheck::{quickcheck_config};
  use std::io::stdio::{println};

  // QuickCheck configuration
  static SIZE: uint = 500;
  static CONFIG: Config = Config {
    tests: 100,
    max_tests: 1000,
  };

  // QuickCheck helpers:
  fn qcheck<A: Testable>(f: A) {
    quickcheck_config(CONFIG, &mut gen(task_rng(), SIZE), f)
  }

  #[deriving(Clone)]
  struct MemoryBackend {
    chunks: Arc<Mutex<HashMap<~[u8], (i64, Option<~[u8]>, ~[u8])>>>,
    seen_chunks: Arc<Mutex<HashSet<~[u8]>>>,
  }

  impl MemoryBackend {
    fn new() -> MemoryBackend {
      MemoryBackend{
        chunks: Arc::new(Mutex::new(HashMap::new())),
        seen_chunks: Arc::new(Mutex::new(HashSet::new()))
      }
    }
    fn saw_chunk(&self, chunk: &~[u8]) -> bool {
      let mut guarded_seen = self.seen_chunks.lock();
      guarded_seen.contains(chunk)
    }
  }

  impl HashTreeBackend for MemoryBackend {

    fn fetch_chunk(&mut self, hash:Hash) -> Option<~[u8]> {
      let mut guarded_chunks = self.chunks.lock();
      guarded_chunks.find(&hash.bytes).map(|&(_, _, ref chunk)| chunk.clone())
    }

    fn fetch_payload(&mut self, hash:Hash) -> Option<~[u8]> {
      let mut guarded_chunks = self.chunks.lock();
      guarded_chunks.find(&hash.bytes).and_then(|&(_, ref payload, _)| payload.clone())
    }

    fn fetch_persistent_ref(&mut self, hash:Hash) -> Option<~[u8]> {
      let mut guarded_chunks = self.chunks.lock();
      if guarded_chunks.contains_key(&hash.bytes) {
        Some(hash.bytes.clone())
      } else {
        None
      }
    }

    fn insert_chunk(&mut self, hash:Hash, level:i64,
                    payload:Option<~[u8]>, chunk:~[u8]) -> ~[u8] {
      let mut guarded_seen = self.seen_chunks.lock();
      guarded_seen.insert(chunk.clone());

      let mut guarded_chunks = self.chunks.lock();
      guarded_chunks.insert(hash.bytes.clone(), (level, payload, chunk));

      hash.bytes
    }
  }

  #[test]
  fn identity_many_small_blocks() {
    fn prop(chunks_count: u8) -> bool {
      let backend = MemoryBackend::new();
      let mut ht = SimpleHashTreeWriter::new(4, backend.clone());

      for _ in range(0, chunks_count) {
        ht.append(bytes!("a").into_owned());
      }

      let (hash, hash_ref) = ht.hash();

      let mut tree_it = match SimpleHashTreeReader::new(backend, hash, hash_ref) {
        NoData => fail!("No data."),
        SingleBlock(found_chunk) => {
          if chunks_count == 0 {
            assert_eq!(found_chunk, bytes!().into_owned());
          } else {
            assert_eq!(chunks_count, 1);
            assert_eq!(found_chunk, bytes!("a").into_owned());
          }
          return true;
        },
        Tree(it) => it,
      };

      // We have a tree, let's investigate!
      let mut actual_count = 0;
      for chunk in tree_it {
        assert_eq!(chunk, bytes!("a").into_owned());
        actual_count += 1;
      }
      assert_eq!(chunks_count, actual_count);

      true
    }
    qcheck(prop);
  }

  #[test]
  fn identity_empty() {
    let block = bytes!().into_owned();

    let backend = MemoryBackend::new();
    let mut ht = SimpleHashTreeWriter::new(4, backend.clone());

    ht.append(block.clone());

    let (hash, hash_ref) = ht.hash();

    match SimpleHashTreeReader::new(backend, hash, hash_ref) {
      NoData => fail!("Expected a single block, got no data."),
      SingleBlock(found_block) => assert_eq!(found_block, block),
      Tree(_) => fail!("Expected a single block, not a tree."),
    };
  }

  #[test]
  fn identity_append1() {
    let block = bytes!("foobar").into_owned();

    let backend = MemoryBackend::new();
    let mut ht = SimpleHashTreeWriter::new(4, backend.clone());

    ht.append(block.clone());

    let (hash, hash_ref) = ht.hash();

    match SimpleHashTreeReader::new(backend, hash, hash_ref) {
      NoData => fail!("Expected a single block, got no data."),
      SingleBlock(found_block) => assert_eq!(found_block, block),
      Tree(_) => fail!("Expected a single block, not a tree."),
    };
  }

  #[test]
  fn identity_implicit_flush() {
    let order = 8;
    let backend = MemoryBackend::new();
    let mut ht = SimpleHashTreeWriter::new(order, backend.clone());

    let mut bytes = ~[0];
    {
      for i in std::iter::range_inclusive(1u8, (order * 4) as u8) {
        bytes[0] = i;
        ht.append(bytes.clone());
      }
    }

    for i in std::iter::range_inclusive(1u8, (order * 4) as u8) {
      bytes[0] = i;
      assert!(backend.saw_chunk(&bytes));
    }

    let (hash, hash_ref) = ht.hash();

    let it = match SimpleHashTreeReader::new(backend, hash, hash_ref) {
      NoData => fail!("Expected a hash tree, got no data."),
      SingleBlock(s) => fail!(format!("Expected a hash tree here, got: {}", s)),
      Tree(it) => it,
    };

    for (i, chunk) in it.enumerate() {
      bytes[0] = (i+1) as u8;
      assert_eq!(bytes, chunk);
    }
  }

  #[test]
  fn identity_1_short_of_flush() {
    let order = 8;
    let backend = MemoryBackend::new();
    let mut ht = SimpleHashTreeWriter::new(order, backend.clone());
    let mut bytes = ~[0];

    for i in std::iter::range_inclusive(1u8, (order - 1) as u8) {
      bytes[0] = i;
      ht.append(bytes.clone());
    }

    let (hash, hash_ref) = ht.hash();

    for i in std::iter::range_inclusive(1u8, (order - 1) as u8) {
      println(format!("{}", i));
      bytes[0] = i;
      assert!(backend.saw_chunk(&bytes));
    }

    let it = match SimpleHashTreeReader::new(backend, hash, hash_ref) {
      NoData => fail!("Expected a hash tree, got no data."),
      SingleBlock(s) => fail!(format!("Expected a hash tree here, got: {}", s)),
      Tree(it) => it,
    };

    for (i, chunk) in it.enumerate() {
      bytes[0] = (i+1) as u8;
      assert_eq!(bytes, chunk);
    }
  }


  #[bench]
  fn append_unknown_16x128KB(bench: &mut Bencher) {
    let mut bytes : ~[u8] = ~[0, ..128*1024];

    bench.iter(|| {
      let mut ht = SimpleHashTreeWriter::new(8, MemoryBackend::new());
      for i in range(0, 16) {
        bytes[0] = i as u8;
        ht.append(bytes.clone());
      };
      ht.hash();
    });

    bench.bytes = 128*1024*16;
  }

  #[bench]
  fn append_known_16x128KB(bench: &mut Bencher) {
    let bytes : ~[u8] = ~[0, ..128*1024];

    bench.iter(|| {
      let mut ht = SimpleHashTreeWriter::new(8, MemoryBackend::new());
      for _ in range(0, 16) {
        ht.append(bytes.clone());
      }
      ht.hash();
    });

    bench.bytes = 128*1024*16;
  }
}
