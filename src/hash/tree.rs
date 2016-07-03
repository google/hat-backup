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

use std::fmt;

use capnp;
use root_capnp;

use blob::{ChunkRef, Kind};
use hash::{Entry, HASHBYTES, Hash};

#[cfg(test)]
use quickcheck;


#[derive(Clone, Debug, Eq, PartialEq)]
struct HashRef {
    hash: Vec<u8>,
    persistent_ref: ChunkRef,
}

impl HashRef {
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


pub trait HashTreeBackend: Clone {
    type Err: fmt::Debug;

    fn fetch_chunk(&self, &Hash, Option<ChunkRef>) -> Result<Option<Vec<u8>>, Self::Err>;
    fn fetch_payload(&self, &Hash) -> Option<Vec<u8>>;
    fn fetch_persistent_ref(&self, &Hash) -> Option<ChunkRef>;
    fn insert_chunk(&self, &Hash, i64, Option<Vec<u8>>, Vec<u8>) -> Result<ChunkRef, Self::Err>;
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
            kind: Kind::TreeBranch,
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


impl<B: HashTreeBackend> SimpleHashTreeWriter<B> {
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
    pub fn append(&mut self, chunk: Vec<u8>) -> Result<(), B::Err> {
        self.append_at(0, chunk, None)
    }

    fn append_at(&mut self,
                 level: usize,
                 data: Vec<u8>,
                 metadata: Option<Vec<u8>>)
                 -> Result<(), B::Err> {
        let hash = Hash::new(&data[..]);
        let persistent_ref = try!(self.backend.insert_chunk(&hash, level as i64, metadata, data));
        let hash_ref = HashRef {
            hash: hash.bytes,
            persistent_ref: persistent_ref,
        };
        self.append_hashref_at(level, hash_ref)
    }

    fn append_hashref_at(&mut self, level: usize, hashref: HashRef) -> Result<(), B::Err> {
        assert!(self.levels.len() >= level);
        self.grow_to(level);

        let new_level_len = {
            let level: &mut Vec<HashRef> = self.levels.get_mut(level).expect("len() >= level");
            level.push(hashref);
            level.len()
        };

        if new_level_len == self.order {
            try!(self.collapse_level(level));
        }

        Ok(())
    }

    fn collapse_level(&mut self, level: usize) -> Result<(), B::Err> {
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

        self.append_at(level + 1, data, Some(metadata_bytes))
    }

    /// Retrieve the hash and backend persistent reference that identified this tree.
    ///
    /// This also flushes and finalizes the tree. It should be considered frozen after calling
    /// `hash()`, i.e. it's Ok to call `hash()` multiple times, but it's **not Ok** to call
    /// `append()` after `hash()`.
    pub fn hash(&mut self) -> Result<(Hash, ChunkRef), B::Err> {
        // Empty hash tree is equivalent to hash tree of one empty block:
        if self.levels.is_empty() {
            try!(self.append(vec![]));
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
            try!(self.collapse_level(top_level));
        }

        // After this point, only root exists and root has exactly one entry:
        assert_eq!(self.levels.last().map(|x| x.len()), Some(1));
        let hashref = self.levels.last().and_then(|x| x.last()).expect("asserted");

        Ok((Hash { bytes: hashref.hash.clone() }, hashref.persistent_ref.clone()))
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

impl<B: HashTreeBackend> SimpleHashTreeReader<B> {
    /// Creates a new `HashTreeReader` that reads through the `backend` the blocks of the hash tree
    /// defined by `root_hash` and `root_ref`.
    pub fn open(backend: B,
                root_hash: &Hash,
                root_ref: Option<ChunkRef>)
                -> Result<Option<ReaderResult<B>>, B::Err> {
        if root_hash.bytes.is_empty() {
            return Ok(None);
        }

        let pref = if let None = root_ref {
                backend.fetch_persistent_ref(root_hash)
            } else {
                root_ref
            }
            .expect("Could not find tree root hash");
        let kind = pref.kind.clone();

        let data = try!(backend.fetch_chunk(root_hash, Some(pref)))
            .expect("Could not find tree root hash.");
        match kind {
            // This is a raw data block.
            Kind::TreeLeaf => Ok(Some(ReaderResult::SingleBlock(data))),
            Kind::TreeBranch => {
                // This is a tree node.
                let mut childs = hash_refs_from_bytes(&data[..]).unwrap();
                childs.reverse();
                Ok(Some(ReaderResult::Tree(SimpleHashTreeReader {
                    stack: childs,
                    backend: backend,
                })))
            }
        }
    }

    pub fn list_entries(&mut self, out: &mut Vec<Entry>) -> Result<(Vec<u8>, i64), B::Err> {
        let mut level = 1;
        let mut metadata = vec![];
        while !self.stack.is_empty() {
            let child = self.stack.pop().expect("!is_empty()");
            metadata.extend_from_slice(&child.hash);

            let hash = Hash { bytes: child.hash.clone() };
            match child.persistent_ref.kind {
                Kind::TreeLeaf => {
                    out.push(Entry {
                        hash: hash,
                        persistent_ref: Some(child.persistent_ref),
                        level: 0,
                        payload: None,
                    })
                }
                Kind::TreeBranch => {
                    let data = try!(self.backend
                            .fetch_chunk(&hash, Some(child.persistent_ref.clone())))
                        .expect("Invalid hash ref");

                    let mut new_childs = hash_refs_from_bytes(&data[..]).unwrap();
                    new_childs.reverse();

                    let mut next = SimpleHashTreeReader {
                        stack: new_childs,
                        backend: self.backend.clone(),
                    };
                    let (child_payload, child_level) = try!(next.list_entries(out));
                    out.push(Entry {
                        hash: hash,
                        persistent_ref: Some(child.persistent_ref),
                        level: child_level,
                        payload: Some(child_payload),
                    });

                    let my_level = 1 + child_level;
                    assert!(level == 1 || level == my_level);
                    level = my_level;
                }
            }

        }
        assert!(!metadata.is_empty());

        Ok((metadata, level))
    }

    fn extract(&mut self) -> Result<Option<Vec<u8>>, B::Err> {
        // Basic cycle detection to spot some programming mistakes.
        let mut cycle_start = None;

        while !self.stack.is_empty() {
            let child = self.stack.pop().expect("!is_empty()");

            match cycle_start {
                None => cycle_start = Some(child.hash.clone()),
                Some(ref hash) => assert!(hash != &child.hash),
            }

            let hash = Hash { bytes: child.hash };
            let kind = child.persistent_ref.kind.clone();
            let data = try!(self.backend
                    .fetch_chunk(&hash, Some(child.persistent_ref)))
                .expect("Invalid hash ref");

            match kind {
                Kind::TreeLeaf => return Ok(Some(data)),
                Kind::TreeBranch => {
                    let mut new_childs = hash_refs_from_bytes(&data[..]).unwrap();
                    new_childs.reverse();
                    self.stack.extend(new_childs.into_iter());
                }
            }
        }

        Ok(None)
    }
}


impl<B: HashTreeBackend> Iterator for ReaderResult<B> {
    type Item = Vec<u8>;

    /// Read the next block of the hash-tree.
    /// This operation can be expensive, as it may require fetching a file through the backend.
    fn next(&mut self) -> Option<Vec<u8>> {
        let (force_empty, res) = match *self {
            ReaderResult::Tree(ref mut it) => (false, it.extract().unwrap()),
            ReaderResult::SingleBlock(ref b) => (true, Some(b.clone())),
            ReaderResult::Empty => (true, None),
        };
        if force_empty || res.is_none() {
            *self = ReaderResult::Empty;
        }

        res
    }
}
