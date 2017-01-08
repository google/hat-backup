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


use blob::{ChunkRef, Kind, node_height, node_from_height};

use capnp;
use hash::Hash;

#[cfg(test)]
use quickcheck;
use root_capnp;
use std::collections::VecDeque;
use std::fmt;


#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HashRef {
    pub hash: Hash,
    pub kind: Kind,
    pub persistent_ref: ChunkRef,
}

impl HashRef {
    pub fn populate_msg(&self, msg: root_capnp::hash_ref::Builder) {
        let mut msg = msg;
        msg.set_hash(&self.hash.bytes[..]);
        msg.set_height(node_height(&self.kind));
        {
            let mut chunk_ref = msg.init_chunk_ref();
            self.persistent_ref.populate_msg(chunk_ref.borrow());
        }
    }

    pub fn read_msg(msg: &root_capnp::hash_ref::Reader) -> Result<HashRef, capnp::Error> {
        Ok(HashRef {
            hash: Hash { bytes: try!(msg.get_hash()).to_owned() },
            kind: node_from_height(msg.get_height()),
            persistent_ref: try!(ChunkRef::read_msg(&try!(msg.get_chunk_ref()))),
        })
    }

    pub fn from_bytes(bytes: &mut &[u8]) -> Result<HashRef, capnp::Error> {
        let reader = try!(
            capnp::serialize_packed::read_message(bytes, capnp::message::ReaderOptions::new()));
        let root = try!(reader.get_root::<root_capnp::hash_ref::Reader>());
        Ok(try!(HashRef::read_msg(&root)))
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut message = ::capnp::message::Builder::new_default();
        {
            let mut root = message.init_root::<root_capnp::hash_ref::Builder>();
            self.populate_msg(root.borrow());
        }

        let mut out = Vec::new();
        capnp::serialize_packed::write_message(&mut out, &message).unwrap();

        out
    }
}


pub trait HashTreeBackend: Clone {
    type Err: fmt::Debug;

    fn fetch_chunk(&self, &Hash, Option<&ChunkRef>) -> Result<Option<Vec<u8>>, Self::Err>;
    fn fetch_childs(&self, &Hash) -> Option<Vec<i64>>;
    fn fetch_persistent_ref(&self, &Hash) -> Option<ChunkRef>;
    fn insert_chunk(&self,
                    &Hash,
                    i64,
                    Option<Vec<i64>>,
                    &[u8])
                    -> Result<(i64, ChunkRef), Self::Err>;
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
            packing: None,
            key: None,
        };
        let mut v = vec![];
        for i in 1..count + 1 {
            v.push(HashRef {
                hash: Hash { bytes: hash.clone() },
                kind: Kind::TreeBranch(i as i64),
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
    levels: Vec<Vec<(i64, HashRef)>>, // Representation of rightmost path to root
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
    pub fn append(&mut self, chunk: &[u8]) -> Result<(), B::Err> {
        self.append_at(0, &chunk, None)
    }

    fn append_at(&mut self,
                 level: usize,
                 data: &[u8],
                 childs: Option<Vec<i64>>)
                 -> Result<(), B::Err> {
        let hash = Hash::new(&data[..]);
        let (id, chunk_ref) = try!(self.backend.insert_chunk(&hash, level as i64, childs, &data));
        let hash_ref = HashRef {
            hash: hash,
            kind: node_from_height(level as i64),
            persistent_ref: chunk_ref,
        };
        self.append_hashref_at(level, id, hash_ref)
    }

    fn append_hashref_at(&mut self, level: usize, id: i64, hashref: HashRef) -> Result<(), B::Err> {
        assert!(self.levels.len() >= level);
        self.grow_to(level);

        let new_level_len = {
            let level: &mut Vec<(i64, HashRef)> =
                self.levels.get_mut(level).expect("len() >= level");
            level.push((id, hashref));
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
        let ids: Vec<i64> = level_v.iter().map(|&(id, _)| id).collect();
        let data = hash_refs_to_bytes(&level_v.into_iter().map(|(_, hr)| hr).collect());

        self.append_at(level + 1, &data[..], Some(ids))
    }

    /// Retrieve the hash and backend persistent reference that identified this tree.
    ///
    /// This also flushes and finalizes the tree. It should be considered frozen after calling
    /// `hash()`, i.e. it's Ok to call `hash()` multiple times, but it's **not Ok** to call
    /// `append()` after `hash()`.
    pub fn hash(&mut self) -> Result<HashRef, B::Err> {
        // Empty hash tree is equivalent to hash tree of one empty block:
        if self.levels.is_empty() {
            try!(self.append(&[]));
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
        let &(_, ref hashref) = self.levels.last().and_then(|x| x.last()).expect("asserted");

        Ok(hashref.clone())
    }
}


pub trait Visitor {
    fn branch_enter(&mut self, _href: &HashRef, _childs: &Vec<HashRef>) -> bool {
        true
    }
    fn branch_leave(&mut self, _href: &HashRef) -> bool {
        // Do nothing by default.
        false
    }

    fn leaf_enter(&mut self, _href: &HashRef) -> bool {
        true
    }
    fn leaf_leave(&mut self, _chunk: Vec<u8>, _href: &HashRef) -> bool {
        // Do nothing by default.
        false
    }
}

pub enum StackItem {
    Enter(HashRef),
    LeaveBranch(HashRef),
}

pub struct Walker<B> {
    backend: B,
    stack: Vec<StackItem>,
}

impl<B> Walker<B>
    where B: HashTreeBackend
{
    pub fn new(backend: B, root_hash: HashRef) -> Result<Option<Walker<B>>, B::Err> {
        Ok(Some(Walker {
            backend: backend,
            stack: vec![StackItem::Enter(root_hash)],
        }))
    }

    pub fn resume<V>(&mut self, visitor: &mut V) -> Result<bool, B::Err>
        where V: Visitor
    {
        // Basic cycle detection to spot some programming mistakes.
        let mut cycle_start = None;

        while !self.stack.is_empty() {
            let node = match self.stack.pop().expect("!is_empty()") {
                StackItem::Enter(href) => href,
                StackItem::LeaveBranch(href) => {
                    if visitor.branch_leave(&href) {
                        return Ok(true);
                    }
                    continue;
                }
            };

            match cycle_start {
                None => cycle_start = Some(node.hash.clone()),
                Some(ref hash) => assert!(hash != &node.hash),
            }
            let fetch_chunk = |backend: &B, child: &HashRef| {
                backend.fetch_chunk(&child.hash, Some(&child.persistent_ref))
                    .map(|opt| opt.expect("Invalid hash ref"))
            };

            match &node.kind {
                &Kind::TreeLeaf => {
                    if visitor.leaf_enter(&node) {
                        let data = try!(fetch_chunk(&self.backend, &node));
                        if visitor.leaf_leave(data, &node) {
                            return Ok(true);
                        }
                    }
                }
                &Kind::TreeBranch(..) => {
                    let data = try!(fetch_chunk(&self.backend, &node));
                    let mut new_childs = hash_refs_from_bytes(&data[..]).unwrap();
                    if visitor.branch_enter(&node, &new_childs) {
                        self.stack.push(StackItem::LeaveBranch(node));
                        new_childs.reverse();
                        self.stack.extend(new_childs.into_iter().map(|x| StackItem::Enter(x)));
                    }
                }
            }
        }

        Ok(false)
    }
}

pub struct LeafIterator<B> {
    walker: Walker<B>,
    visitor: LeafVisitor,
}

impl<B> LeafIterator<B>
    where B: HashTreeBackend
{
    pub fn new(backend: B, root_ref: HashRef) -> Result<Option<LeafIterator<B>>, B::Err> {
        Ok(try!(Walker::new(backend, root_ref)).map(|w| {
            LeafIterator {
                walker: w,
                visitor: LeafVisitor { leafs: VecDeque::new() },
            }
        }))
    }
}

pub struct LeafVisitor {
    leafs: VecDeque<Vec<u8>>,
}

impl Visitor for LeafVisitor {
    fn leaf_leave(&mut self, leaf: Vec<u8>, _href: &HashRef) -> bool {
        self.leafs.push_back(leaf);
        true
    }
}

impl<B: HashTreeBackend> Iterator for LeafIterator<B> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Vec<u8>> {
        while self.visitor.leafs.is_empty() && self.walker.resume(&mut self.visitor).unwrap() {}
        self.visitor.leafs.pop_front()
    }
}
