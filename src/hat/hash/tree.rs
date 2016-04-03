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

use std::sync::mpsc;

use capnp;
use root_capnp;

use blob::ChunkRef;
use hash::{Entry, Hash, HASHBYTES};

#[cfg(test)]
use quickcheck;


#[derive(Clone, Debug, Eq, PartialEq)]
struct HashRef {
    pub hash: Vec<u8>,
    pub persistent_ref: ChunkRef,
}

impl HashRef {
    fn new(hash: Vec<u8>, persistent_ref: ChunkRef) -> HashRef {
        HashRef {
            hash: hash,
            persistent_ref: persistent_ref,
        }
    }

    fn from_bytes(&self, bytes: &mut &[u8]) -> Result<HashRef, capnp::Error> {
        let reader = try!(capnp::serialize_packed::read_message(bytes,
                                                           capnp::message::ReaderOptions::new()));

        let root = try!(reader.get_root::<root_capnp::hash_ref::Reader>());
        Ok(try!(HashRef::read_msg(&root)))
    }

    fn as_bytes(&self) -> Vec<u8> {
        let mut message = capnp::message::Builder::new_default();
        {
            let mut root = message.init_root::<root_capnp::hash_ref::Builder>();
            self.populate_msg(root.borrow());
        }
        let mut out = Vec::new();
        capnp::serialize_packed::write_message(&mut out, &message).unwrap();
        out
    }

    fn populate_msg(&self, msg: root_capnp::hash_ref::Builder) {
        let mut msg = msg;
        msg.set_hash(&self.hash[..]);
        {
            let mut chunk_ref = msg.init_chunk_ref();
            self.persistent_ref.populate_msg(chunk_ref.borrow());
        }
    }

    fn read_msg(msg: &root_capnp::hash_ref::Reader) -> Result<HashRef, capnp::Error> {
        Ok(HashRef {
            hash: try!(msg.get_hash()).to_owned(),
            persistent_ref: try!(ChunkRef::read_msg(&try!(msg.get_chunk_ref()))),
        })
    }
}


pub trait HashTreeBackend {
    fn fetch_chunk(&mut self, Hash, Option<ChunkRef>) -> Option<Vec<u8>>;
    fn fetch_payload(&mut self, Hash) -> Option<Vec<u8>>;
    fn fetch_persistent_ref(&mut self, Hash) -> Option<ChunkRef>;
    fn insert_chunk(&mut self, Hash, i64, Option<Vec<u8>>, Vec<u8>) -> ChunkRef;
}


fn hash_refs_to_bytes(refs: &Vec<HashRef>) -> Vec<u8> {
    let mut message = capnp::message::Builder::new_default();
    {
        let root = message.init_root::<root_capnp::hash_ref_list::Builder>();
        let mut list = root.init_hash_refs(refs.len() as u32);
        for (i, ref_) in refs.iter().enumerate() {
            ref_.populate_msg(list.borrow().get(i as u32));
        }
    }
    let mut out = Vec::new();
    capnp::serialize_packed::write_message(&mut out, &message).unwrap();
    out
}

fn hash_refs_from_bytes(bytes: &[u8]) -> Option<Vec<HashRef>> {
    let mut out = Vec::new();
    if bytes.is_empty() {
        return Some(out);
    }

    let reader = capnp::serialize_packed::read_message(&mut &bytes[..],
                                                       capnp::message::ReaderOptions::new())
                     .unwrap();
    let msg = reader.get_root::<root_capnp::hash_ref_list::Reader>().unwrap();

    for ref_ in msg.get_hash_refs().unwrap().iter() {
        out.push(HashRef::read_msg(&ref_).unwrap());
    }

    Some(out)
}

#[test]
fn test_hash_refs_identity() {
    fn prop(count: u8, hash: Vec<u8>, blob: Vec<u8>, n: usize) -> bool {
        let chunk_ref = ChunkRef {
            blob_id: blob.clone(),
            offset: n,
            length: n,
        };
        let mut v = vec![];
        for _ in 0..count {
            v.push(HashRef {
                hash: hash.clone(),
                persistent_ref: chunk_ref.clone(),
            });
        }
        let bytes = hash_refs_to_bytes(&v);
        assert_eq!(hash_refs_from_bytes(&bytes), Some(v));

        true
    }
    quickcheck::quickcheck(prop as fn(u8, Vec<u8>, Vec<u8>, usize) -> bool);
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
    levels: Vec<Vec<HashRef>>, // Representation of rightmost path to root
}


pub fn decode_metadata_refs(metadata: &[u8]) -> Vec<Vec<u8>> {
    metadata.chunks(HASHBYTES).map(|c| c.to_vec()).collect()
}


impl<B: HashTreeBackend + Clone> SimpleHashTreeWriter<B> {
    /// Create a new hash-tree to be stored through 'backend' with node order 'order'.
    pub fn new(order: usize, backend: B) -> SimpleHashTreeWriter<B> {
        SimpleHashTreeWriter {
            backend: backend,
            order: order,
            levels: Vec::new(),
        }
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
    /// When reading back the tree, these blocks will be returned exactly as they were written, in
    /// the same order, and split at the same boundaries (i.e. pushing 1-bytes blocks will give
    /// 1-byte blocks when reading; if needed, accummulation of data must be handled by the
    /// `backend`).
    pub fn append(&mut self, mut chunk: Vec<u8>) {
        let hash = Hash::new(&chunk[..]);

        // data-chunks start with 0 to distinguish them from tree nodes.
        // The exception is the empty chunk, which is always a data chunk.
        if !chunk.is_empty() {
            chunk.push(0);
        }

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
        let mut data = hash_refs_to_bytes(&level_v);
        assert_eq!(Some(level_v.clone()), hash_refs_from_bytes(&data[..]));
        data.push(1);

        // The hashes for this level is stored as metadata for future use:
        let metadata_bytes = {
            let mut metadata = Vec::new();
            for hashref in level_v.into_iter() {
                metadata.extend(hashref.hash.into_iter());
            }
            metadata
        };

        let hash = Hash::new(&metadata_bytes[..]);
        self.append_at(level + 1, hash, data, Some(metadata_bytes));
    }

    /// Retrieve the hash and backend persistent reference that identified this tree.
    ///
    /// This also flushes and finalizes the tree. It should be considered frozen after calling
    /// `hash()`, i.e. it's Ok to call `hash()` multiple times, but it's **not Ok** to call
    /// `append()` after `hash()`.
    pub fn hash(&mut self) -> (Hash, ChunkRef) {
        // Empty hash tree is equivalent to hash tree of one empty block:
        if self.levels.is_empty() {
            self.append(vec![]);
        }

        // Locate first level that isn't empty (has data to collapse)
        let first_non_empty_level_idx = self.levels
                                            .iter()
                                            .take_while(|level| level.is_empty())
                                            .count();

        // Unless only root has data, collapse all levels up to top level (which is handled next)
        let top_level = self.top_level().expect("levels.len() > 0");
        (first_non_empty_level_idx..top_level).map(|l| self.collapse_level(l)).last();

        // Collapse top level if possible
        let top_level = self.top_level().expect("levels.len() > 0");
        if self.levels.get(top_level).expect("top level").len() > 1 {
            self.collapse_level(top_level);
        }

        // After this point, only root exists and root has exactly one entry:
        assert_eq!(self.levels.last().map(|x| x.len()), Some(1));
        let hashref = self.levels.last().and_then(|x| x.last()).expect("asserted");

        (Hash { bytes: hashref.hash.clone() },
         hashref.persistent_ref.clone())
    }
}


/// A structure for reading hash-trees written with `SimpleHashTreeWriter`.
///
/// The hash-tree is "opened" as read-only and is streamed from first to last data-block. The data
/// blocks are read in the same order as they were written. The reader implement a `Vec<u8>`
/// iterator used for extracting the tree blocks.
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

impl<B: HashTreeBackend + Clone> SimpleHashTreeReader<B> {
    /// Creates a new `HashTreeReader` that reads through the `backend` the blocks of the hash tree
    /// defined by `root_hash` and `root_ref`.
    pub fn open(backend: B,
                root_hash: Hash,
                root_ref: Option<ChunkRef>)
                -> Option<ReaderResult<B>> {
        if root_hash.bytes.is_empty() {
            return None;
        }

        let mut data = backend.clone()
                              .fetch_chunk(root_hash.clone(), root_ref)
                              .expect("Could not find tree root hash.");

        match data.pop() {
            None | Some(0) => {
                // This is a raw data block.
                Some(ReaderResult::SingleBlock(data))
            }
            _ => {
                // This is a tree node.
                let mut childs = hash_refs_from_bytes(&data[..]).unwrap();
                childs.reverse();
                Some(ReaderResult::Tree(SimpleHashTreeReader {
                    stack: childs,
                    backend: backend,
                }))
            }
        }
    }

    pub fn list_entries(&mut self, out: mpsc::Sender<Entry>) -> (Vec<u8>, i64) {
        let mut level = 1;
        let mut metadata = vec![];
        while !self.stack.is_empty() {
            let child = self.stack.pop().expect("!is_empty()");
            metadata.extend(child.hash.iter());

            let hash = Hash { bytes: child.hash.clone() };
            let mut data = self.backend
                               .fetch_chunk(hash.clone(), Some(child.persistent_ref.clone()))
                               .expect("Invalid hash ref");

            match data.pop() {
                None | Some(0) => {
                    out.send(Entry {
                           hash: hash,
                           persistent_ref: Some(child.persistent_ref),
                           level: 0,
                           payload: None,
                       })
                       .unwrap()
                }
                _ => {
                    let mut new_childs = hash_refs_from_bytes(&data[..]).unwrap();
                    new_childs.reverse();

                    let mut next = SimpleHashTreeReader {
                        stack: new_childs,
                        backend: self.backend.clone(),
                    };
                    let (child_payload, child_level) = next.list_entries(out.clone());
                    out.send(Entry {
                           hash: hash,
                           persistent_ref: Some(child.persistent_ref),
                           level: child_level,
                           payload: Some(child_payload),
                       })
                       .unwrap();

                    let my_level = 1 + child_level;
                    assert!(level == 1 || level == my_level);
                    level = my_level;
                }
            }

        }
        assert!(!metadata.is_empty());
        return (metadata, level);
    }

    fn extract(&mut self) -> Option<Vec<u8>> {
        // Basic cycle detection to spot some programming mistakes.
        let mut cycle_start = None;

        while !self.stack.is_empty() {
            let child = self.stack.pop().expect("!is_empty()");

            match cycle_start {
                None => cycle_start = Some(child.hash.clone()),
                Some(ref hash) => assert!(hash != &child.hash),
            }

            let hash = Hash { bytes: child.hash };
            let mut data = self.backend
                               .fetch_chunk(hash, Some(child.persistent_ref))
                               .expect("Invalid hash ref");

            match data.pop() {
                None | Some(0) => return Some(data),
                _ => {
                    let mut new_childs = hash_refs_from_bytes(&data[..]).unwrap();
                    new_childs.reverse();
                    self.stack.extend(new_childs.into_iter());
                }
            }
        }

        None
    }
}


impl<B: HashTreeBackend + Clone> Iterator for ReaderResult<B> {
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
    use test::Bencher;

    use std::sync::{Arc, Mutex};

    use blob::ChunkRef;
    use hash::Hash;
    use std::collections::{BTreeMap, BTreeSet};
    use quickcheck;

    #[derive(Clone)]
    struct MemoryBackend {
        chunks: Arc<Mutex<BTreeMap<Vec<u8>, (i64, Option<Vec<u8>>, Vec<u8>)>>>,
        seen_chunks: Arc<Mutex<BTreeSet<Vec<u8>>>>,
    }

    impl MemoryBackend {
        fn new() -> MemoryBackend {
            MemoryBackend {
                chunks: Arc::new(Mutex::new(BTreeMap::new())),
                seen_chunks: Arc::new(Mutex::new(BTreeSet::new())),
            }
        }
        fn saw_chunk(&self, chunk: &Vec<u8>) -> bool {
            let guarded_seen = self.seen_chunks.lock().unwrap();
            guarded_seen.contains(chunk)
        }
    }

    impl HashTreeBackend for MemoryBackend {
        fn fetch_chunk(&mut self, hash: Hash, ref_opt: Option<ChunkRef>) -> Option<Vec<u8>> {
            let hash = match ref_opt {
                Some(b) => Hash { bytes: b.blob_id },  // blob names are chunk hashes.
                None => hash,
            };
            let guarded_chunks = self.chunks.lock().unwrap();
            guarded_chunks.get(&hash.bytes).map(|&(_, _, ref chunk)| chunk.clone())
        }

        fn fetch_payload(&mut self, hash: Hash) -> Option<Vec<u8>> {
            let guarded_chunks = self.chunks.lock().unwrap();
            guarded_chunks.get(&hash.bytes).and_then(|&(_, ref payload, _)| payload.clone())
        }

        fn fetch_persistent_ref(&mut self, hash: Hash) -> Option<ChunkRef> {
            let guarded_chunks = self.chunks.lock().unwrap();
            if guarded_chunks.contains_key(&hash.bytes) {
                Some(ChunkRef {
                    blob_id: hash.bytes,
                    offset: 0,
                    length: 0,
                })
            } else {
                None
            }
        }

        fn insert_chunk(&mut self,
                        hash: Hash,
                        level: i64,
                        payload: Option<Vec<u8>>,
                        chunk: Vec<u8>)
                        -> ChunkRef {
            let mut guarded_seen = self.seen_chunks.lock().unwrap();
            guarded_seen.insert(chunk.clone());

            let mut guarded_chunks = self.chunks.lock().unwrap();
            guarded_chunks.insert(hash.bytes.clone(), (level, payload, chunk));

            let len = hash.bytes.len();

            ChunkRef {
                blob_id: hash.bytes,
                offset: 0,
                length: len,
            }
        }
    }

    #[test]
    fn identity_many_small_blocks() {
        fn prop(chunks_count: u8) -> bool {
            let backend = MemoryBackend::new();
            let mut ht = SimpleHashTreeWriter::new(4, backend.clone());

            for _ in 0..chunks_count {
                ht.append(b"a".to_vec());
            }

            let (hash, hash_ref) = ht.hash();

            let mut tree_it = SimpleHashTreeReader::open(backend, hash, Some(hash_ref))
                                  .expect("tree not found");

            if chunks_count == 0 {
                // An empty tree is a tree with a single empty chunk (as opposed to no chunks).
                assert_eq!(Some(vec![]), tree_it.next());
                assert_eq!(0, tree_it.count());
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
        quickcheck::quickcheck(prop as fn(u8) -> bool);
    }

    #[test]
    fn identity_empty() {
        let block = Vec::new();

        let backend = MemoryBackend::new();
        let mut ht = SimpleHashTreeWriter::new(4, backend.clone());

        ht.append(block.clone());

        let (hash, hash_ref) = ht.hash();

        let mut it = SimpleHashTreeReader::open(backend, hash, Some(hash_ref))
                         .expect("tree not found");

        assert_eq!(Some(block), it.next());
        assert_eq!(0, it.count());
    }

    #[test]
    fn identity_append1() {
        let block: Vec<u8> = b"foobar".to_vec();

        let backend = MemoryBackend::new();
        let mut ht = SimpleHashTreeWriter::new(4, backend.clone());

        ht.append(block.clone());

        let (hash, hash_ref) = ht.hash();

        let mut it = SimpleHashTreeReader::open(backend, hash, Some(hash_ref))
                         .expect("tree not found");
        assert_eq!(Some(block), it.next());
        assert_eq!(0, it.count());
    }

    #[test]
    fn identity_append5() {
        let blocks: Vec<Vec<u8>> = vec![b"foo".to_vec(),
                                        b"bar".to_vec(),
                                        b"baz".to_vec(),
                                        b"qux".to_vec(),
                                        b"norf".to_vec()];

        let backend = MemoryBackend::new();
        let mut ht = SimpleHashTreeWriter::new(4, backend.clone());

        for block in blocks.iter() {
            ht.append(block.clone());
        }

        let (hash, hash_ref) = ht.hash();

        let mut it = SimpleHashTreeReader::open(backend.clone(), hash, Some(hash_ref))
                         .expect("tree not found");

        for mut block in blocks {
            assert_eq!(Some(&block), it.next().as_ref());
            block.push(0);
            assert!(backend.saw_chunk(&block));
        }
        assert_eq!(0, it.count());
    }

    #[test]
    fn identity_implicit_flush() {
        let order = 8;
        let backend = MemoryBackend::new();
        let mut ht = SimpleHashTreeWriter::new(order, backend.clone());

        let mut bytes = vec![0u8];
        {
            for i in 1u8..(order * 4 + 1) as u8 {
                bytes[0] = i;
                ht.append(bytes.clone());
            }
        }

        // The tree will append a '0' to our chunks.
        for i in 1u8..(order * 4 + 1) as u8 {
            assert!(backend.saw_chunk(&vec![i, 0]));
        }

        let (hash, hash_ref) = ht.hash();

        let it = SimpleHashTreeReader::open(backend, hash, Some(hash_ref)).expect("tree not found");

        for (i, chunk) in it.enumerate() {
            bytes[0] = (i + 1) as u8;
            assert_eq!(bytes, chunk);
        }
    }

    #[test]
    fn identity_1_short_of_flush() {
        let order = 8;
        let backend = MemoryBackend::new();
        let mut ht = SimpleHashTreeWriter::new(order, backend.clone());
        let mut bytes = vec![0u8];

        for i in 1u8..order as u8 {
            bytes[0] = i;
            ht.append(bytes.clone());
        }

        let (hash, hash_ref) = ht.hash();

        // The tree will append a '0' to our chunks.
        for i in 1u8..order as u8 {
            assert!(backend.saw_chunk(&vec![i, 0]));
        }

        let it = SimpleHashTreeReader::open(backend, hash, Some(hash_ref)).expect("tree not found");

        for (i, chunk) in it.enumerate() {
            bytes[0] = (i + 1) as u8;
            assert_eq!(bytes, chunk);
        }
    }


    #[bench]
    fn append_unknown_16x128_kb(bench: &mut Bencher) {
        let mut bytes = vec![0u8; 128*1024];

        bench.iter(|| {
            let mut ht = SimpleHashTreeWriter::new(8, MemoryBackend::new());
            for i in 0u8..16 {
                bytes[0] = i;
                ht.append(bytes.clone());
            }
            ht.hash();
        });

        bench.bytes = 128 * 1024 * 16;
    }

    #[bench]
    fn append_known_16x128_kb(bench: &mut Bencher) {
        let bytes = vec![0u8; 128*1024];

        bench.iter(|| {
            let mut ht = SimpleHashTreeWriter::new(8, MemoryBackend::new());
            for _ in 0i32..16 {
                ht.append(bytes.clone());
            }
            ht.hash();
        });

        bench.bytes = 128 * 1024 * 16;
    }
}
