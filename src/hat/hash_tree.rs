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

// extern crate serialize;
// use serialize::json;

use std::num::Int;
use rustc_serialize::json;
use hash_index::{Hash};
use std::{str};


#[derive(Clone, Debug, Eq, PartialEq, RustcEncodable, RustcDecodable)]
struct HashRef {
  pub hash: Vec<u8>,
  pub persistent_ref: Vec<u8>,
}

impl HashRef {
  fn new(hash: Vec<u8>, persistent_ref: Vec<u8>) -> HashRef {
    HashRef{hash: hash, persistent_ref: persistent_ref}
  }
}


pub trait HashTreeBackend {
  fn fetch_chunk(&mut self, Hash) -> Option<Vec<u8>>;
  fn fetch_payload(&mut self, Hash) -> Option<Vec<u8>>;
  fn fetch_persistent_ref(&mut self, Hash) -> Option<Vec<u8>>;
  fn insert_chunk(&mut self, Hash, i64, Option<Vec<u8>>, Vec<u8>) -> Vec<u8>;
}


fn hash_refs_to_bytes(refs: &Vec<HashRef>) -> Vec<u8> {
  json::encode(refs).unwrap().as_bytes().to_vec()
}

fn hash_refs_from_bytes(bytes: &[u8]) -> Option<Vec<HashRef>> {
  return str::from_utf8(bytes).ok().and_then(|s| { json::decode(s).ok() })
}

#[quickcheck]
fn test_json(count: u8, hash: Vec<u8>, pref: Vec<u8>) -> bool {
  let mut refs = Vec::new();
  for i in range(0, count) {
    refs.push(HashRef::new(hash.clone(), pref.clone()));
  }
  let j = hash_refs_to_bytes(&refs);
  return Some(refs) == hash_refs_from_bytes(&j[..])
}


/// A simple implementation of a hash-tree stream writer.
///
/// The hash-tree is "created" as append-only and is streamed from first to last data-block. The
///
/// ```rust,ignore
/// let mut tree = SimpleHashTreeWriter::new(order, backend);
/// for data_chunk in chunk_iterator {
///   tree.append(data_chunk);
/// }
///
/// let (top_hash, top_persisitent_ref) = tree.hash();
/// ```
pub struct SimpleHashTreeWriter<B> {
  backend: B,
  order: usize,
  levels: Vec<Vec<HashRef>>,  // Representation of rightmost path to root
}

impl <B: HashTreeBackend + Clone> SimpleHashTreeWriter<B> {

  /// Create a new hash-tree to be stored through 'backend' with node order 'order'.
  pub fn new(order: usize, backend: B) -> SimpleHashTreeWriter<B> {
    SimpleHashTreeWriter{backend: backend,
                         order: order,
                         levels: Vec::new()}
  }

  fn top_level(&self) -> Option<usize> {
    self.levels.len().checked_sub(1)
  }

  fn grow_to(&mut self, level: usize) {
    while self.top_level() < Some(level) {
      self.levels.push(Vec::new());
    }
  }

  /// Append data-block to hash-tree.
  ///
  /// When reading back the tree, these blocks will be returned exactly as they were written, in the
  /// same order, and split at the same boundaries (i.e. pushing 1-bytes blocks will give 1-byte
  /// blocks when reading; if needed, accummulation of data must be handled by the `backend`).
  pub fn append(&mut self, chunk: Vec<u8>) {
    let hash = Hash::new(chunk.as_slice());
    self.append_at(0, hash, chunk, None);
  }

  fn append_at(&mut self, level: usize, hash: Hash, data: Vec<u8>, metadata: Option<Vec<u8>>) {
    let persistent_ref = self.backend.insert_chunk(hash.clone(), level as i64, metadata, data);
    let hash_ref = HashRef::new(hash.bytes, persistent_ref);
    self.append_hashref_at(level, hash_ref);
  }

  fn append_hashref_at(&mut self, level: usize, hashref: HashRef) {
    assert!(self.levels.len() >= level);
    self.grow_to(level);

    let new_level_len = {
      let level: &mut Vec<HashRef> = self.levels.get_mut(level).expect("len() >= level");
      level.push(hashref);
      level.len()
    };

    if new_level_len == self.order {
      self.collapse_level(level);
    }
  }

  fn collapse_level(&mut self, level: usize) {
    // Extract-replace level with a new empty level
    assert!(self.levels.len() > level);
    self.levels.push(Vec::new());
    let level_v = self.levels.swap_remove(level);

    // All data from this level (hashes and references):
    let data = hash_refs_to_bytes(&level_v);
    assert_eq!(Some(level_v.clone()), hash_refs_from_bytes(&data[..]));

    // The hashes for this level is stored as metadata for future use:
    let metadata_bytes = {
      let mut metadata = Vec::new();
      for hashref in level_v.into_iter() {
        metadata.extend(hashref.hash.into_iter());
      }
      metadata
    };

    let hash = Hash::new(metadata_bytes.as_slice());
    self.append_at(level + 1, hash, data, Some(metadata_bytes));
  }

  /// Retrieve the hash and backend persistent reference that identified this tree.
  ///
  /// This also flushes and finalizes the tree. It should be considered frozen after calling
  /// `hash()`, i.e. it's OK to call `hash()` multiple times, but it's **not OK** to call `append()`
  /// after `hash()`.
  pub fn hash(&mut self) -> (Hash, Vec<u8>) {
    // Empty hash tree is equivalent to hash tree of one empty block:
    if self.levels.len() == 0 {
      self.append(vec!());
    }

    // Locate first level that isn't empty (has data to collapse)
    let first_non_empty_level_idx = self.levels.iter().take_while(|level| level.len() == 0).count();

    // Unless only root has data, collapse all levels up to top level (which is handled next)
    let top_level = self.top_level().expect("levels.len() > 0");
    range(first_non_empty_level_idx, top_level).map(|l| self.collapse_level(l)).last();

    // Collapse top level if possible
    let top_level = self.top_level().expect("levels.len() > 0");
    if self.levels.get(top_level).expect("top level").len() > 1 {
      self.collapse_level(top_level);
    }

    // After this point, only root exists and root has exactly one entry:
    assert_eq!(self.levels.last().map(|x| x.len()), Some(1));
    let hashref = self.levels.last().and_then(|x| x.last()).expect("asserted");

    (Hash{bytes:hashref.hash.clone()}, hashref.persistent_ref.clone())
  }
}


/// A structure for reading hash-trees written with `SimpleHashTreeWriter`.
///
/// The hash-tree is "opened" as read-only and is streamed from first to last data-block. The data
/// blocks are read in the same order as they were written. The reader implement a `Vec<u8>` iterator
/// used for extracting the tree blocks.
///
/// ```rust,ignore
/// let tree_it = match SimpleHashTreeReader::new(backend, top_hash, top_persisitent_ref) {
///     SingleBlock(block) => return println!("{}", block),
///     Tree(it) => it,
/// };
///
/// for data_chunk in tree_it {
///     println!("{}", data_chunk);
/// }
/// ```
pub struct SimpleHashTreeReader<B> {
  backend: B,
  stack: Vec<HashRef>,
}


/// Wrapper for the result of `SimpleHashTreeReader::new()`.
///
/// We wrap the output of `SimpleHashTreeReader::new(...)` in a `ReaderResult` to fast-path the
/// single-block case (alternatively we could build a one-block iterator).
pub enum ReaderResult<B> {
  Empty,

  /// The tree consists of just a single node and its data-chunk is given here.
  SingleBlock(Vec<u8>),

  /// The tree has more than one node, so a data-chunk iterator is returned.
  Tree(SimpleHashTreeReader<B>),
}

impl <B: HashTreeBackend + Clone> SimpleHashTreeReader<B> {

  /// Creates a new `HashTreeReader` that reads through the `backend` the blocks of the hash tree
  /// defined by `root_hash` and `root_ref`.
  pub fn open(backend: B, root_hash: Hash, root_ref: Vec<u8>) -> Option<ReaderResult<B>>
  {
    if root_hash.bytes.len() == 0 {
      return None
    }

    let data = backend.clone().fetch_chunk(root_hash.clone()).expect(
      "Could not find tree root hash.");

    match hash_refs_from_bytes(data.as_slice()) {
      None => Some(ReaderResult::SingleBlock(data)), // There's no tree top, just a data block
      Some(childs) => {
        let mut childs = childs;
        childs.reverse();
        Some(ReaderResult::Tree(SimpleHashTreeReader{stack: childs, backend: backend}))
      },
    }
  }

  fn extract(&mut self) -> Option<Vec<u8>> {
    while self.stack.len() > 0 {
      let child = self.stack.pop().expect("len() > 0");

      let hash = Hash{bytes: child.hash};
      let data = self.backend.fetch_chunk(hash).expect("Invalid hash ref");

      match hash_refs_from_bytes(data.as_slice()) {
        None => return Some(data),
        Some(new_childs) => {
          let mut new_childs = new_childs;
          new_childs.reverse();
          self.stack.extend(new_childs.into_iter());
        }
      }
    }

    None
  }

}


impl <B: HashTreeBackend + Clone> Iterator for ReaderResult<B> {
  type Item = Vec<u8>;

  /// Read the next block of the hash-tree.
  /// This operation can be expensive, as it may require fetching a file through the backend.
  fn next(&mut self) -> Option<Vec<u8>> {
    let (force_empty, res) = match *self {
      ReaderResult::Tree(ref mut it) => (false, it.extract()),
      ReaderResult::SingleBlock(ref b) => (true, Some(b.clone())),
      ReaderResult::Empty => (true, None),
    };
    if force_empty || res.is_none() {
      *self = ReaderResult::Empty;
    }
    return res;
  }
}


#[cfg(test)]
mod tests {
  use super::*;
  use test::{Bencher};

  use std::sync::{Arc, Mutex};

  use std;
  use hash_index::{Hash};
  use std::collections::{BTreeMap, BTreeSet};

  #[derive(Clone)]
  struct MemoryBackend {
    chunks: Arc<Mutex<BTreeMap<Vec<u8>, (i64, Option<Vec<u8>>, Vec<u8>)>>>,
    seen_chunks: Arc<Mutex<BTreeSet<Vec<u8>>>>,
  }

  impl MemoryBackend {
    fn new() -> MemoryBackend {
      MemoryBackend{
        chunks: Arc::new(Mutex::new(BTreeMap::new())),
        seen_chunks: Arc::new(Mutex::new(BTreeSet::new()))
      }
    }
    fn saw_chunk(&self, chunk: &Vec<u8>) -> bool {
      let mut guarded_seen = self.seen_chunks.lock().unwrap();
      guarded_seen.contains(chunk)
    }
  }

  impl HashTreeBackend for MemoryBackend {

    fn fetch_chunk(&mut self, hash:Hash) -> Option<Vec<u8>> {
      let mut guarded_chunks = self.chunks.lock().unwrap();
      guarded_chunks.get(&hash.bytes).map(|&(_, _, ref chunk)| chunk.clone())
    }

    fn fetch_payload(&mut self, hash:Hash) -> Option<Vec<u8>> {
      let mut guarded_chunks = self.chunks.lock().unwrap();
      guarded_chunks.get(&hash.bytes).and_then(|&(_, ref payload, _)| payload.clone())
    }

    fn fetch_persistent_ref(&mut self, hash:Hash) -> Option<Vec<u8>> {
      let mut guarded_chunks = self.chunks.lock().unwrap();
      if guarded_chunks.contains_key(&hash.bytes) {
        Some(hash.bytes)
      } else {
        None
      }
    }

    fn insert_chunk(&mut self, hash:Hash, level:i64,
                    payload:Option<Vec<u8>>, chunk:Vec<u8>) -> Vec<u8> {
      let mut guarded_seen = self.seen_chunks.lock().unwrap();
      guarded_seen.insert(chunk.clone());

      let mut guarded_chunks = self.chunks.lock().unwrap();
      guarded_chunks.insert(hash.bytes.clone(), (level, payload, chunk));

      hash.bytes
    }
  }

  #[quickcheck]
  fn identity_many_small_blocks(chunks_count: u8) -> bool {
    let backend = MemoryBackend::new();
    let mut ht = SimpleHashTreeWriter::new(4, backend.clone());

    for _ in range(0, chunks_count) {
      ht.append(b"a".to_vec());
    }

    let (hash, hash_ref) = ht.hash();

    let tree_it = SimpleHashTreeReader::open(backend, hash, hash_ref).expect("tree not found");

    if chunks_count == 0 {
      // An empty tree is a tree with a single empty chunk (as opposed to no chunks).
      assert_eq!(vec![vec![]], tree_it.collect());
      return true;
    }

    // We have a tree, let's investigate!
    let mut actual_count = 0;
    for chunk in tree_it {
      assert_eq!(chunk, b"a".to_vec());
      actual_count += 1;
    }
    assert_eq!(chunks_count, actual_count);

    true
  }

  #[test]
  fn identity_empty() {
    let block = Vec::new();

    let backend = MemoryBackend::new();
    let mut ht = SimpleHashTreeWriter::new(4, backend.clone());

    ht.append(block.clone());

    let (hash, hash_ref) = ht.hash();

    let it = SimpleHashTreeReader::open(backend, hash, hash_ref).expect("tree not found");
    assert_eq!(vec![block], it.collect());
  }

  #[test]
  fn identity_append1() {
    let block: Vec<u8> = b"foobar".to_vec();

    let backend = MemoryBackend::new();
    let mut ht = SimpleHashTreeWriter::new(4, backend.clone());

    ht.append(block.clone());

    let (hash, hash_ref) = ht.hash();

    let it = SimpleHashTreeReader::open(backend, hash, hash_ref).expect("tree not found");
    assert_eq!(vec![block], it.collect());
  }

  #[test]
  fn identity_implicit_flush() {
    let order = 8;
    let backend = MemoryBackend::new();
    let mut ht = SimpleHashTreeWriter::new(order, backend.clone());

    let mut bytes = vec!(0u8);
    {
      for i in std::iter::range_inclusive(1u8, (order * 4) as u8) {
        bytes.as_mut_slice()[0] = i;
        ht.append(bytes.clone());
      }
    }

    for i in std::iter::range_inclusive(1u8, (order * 4) as u8) {
      bytes.as_mut_slice()[0] = i;
      assert!(backend.saw_chunk(&bytes));
    }

    let (hash, hash_ref) = ht.hash();

    let it = SimpleHashTreeReader::open(backend, hash, hash_ref).expect("tree not found");

    for (i, chunk) in it.enumerate() {
      bytes.as_mut_slice()[0] = (i+1) as u8;
      assert_eq!(bytes, chunk);
    }
  }

  #[test]
  fn identity_1_short_of_flush() {
    let order = 8;
    let backend = MemoryBackend::new();
    let mut ht = SimpleHashTreeWriter::new(order, backend.clone());
    let mut bytes = vec!(0u8);

    for i in std::iter::range_inclusive(1u8, (order - 1) as u8) {
      bytes.as_mut_slice()[0] = i;
      ht.append(bytes.clone());
    }

    let (hash, hash_ref) = ht.hash();

    for i in std::iter::range_inclusive(1u8, (order - 1) as u8) {
      bytes.as_mut_slice()[0] = i;
      assert!(backend.saw_chunk(&bytes));
    }

    let it = SimpleHashTreeReader::open(backend, hash, hash_ref).expect("tree not found");

    for (i, chunk) in it.enumerate() {
      bytes.as_mut_slice()[0] = (i+1) as u8;
      assert_eq!(bytes, chunk);
    }
  }


  #[bench]
  fn append_unknown_16x128_kb(bench: &mut Bencher) {
    let mut bytes = vec![0u8; 128*1024];

    bench.iter(|| {
      let mut ht = SimpleHashTreeWriter::new(8, MemoryBackend::new());
      for i in range(0u8, 16) {
        bytes.as_mut_slice()[0] = i;
        ht.append(bytes.clone());
      };
      ht.hash();
    });

    bench.bytes = 128*1024*16;
  }

  #[bench]
  fn append_known_16x128_kb(bench: &mut Bencher) {
    let bytes = vec![0u8, 128*1024];

    bench.iter(|| {
      let mut ht = SimpleHashTreeWriter::new(8, MemoryBackend::new());
      for _ in range(0i32, 16) {
        ht.append(bytes.clone());
      }
      ht.hash();
    });

    bench.bytes = 128*1024*16;
  }
}
