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

use models;
use key;
use blob::{ChunkRef, LeafType, NodeType};

use serde_cbor;
use hash::Hash;

#[cfg(test)]
use quickcheck;
use std::collections::VecDeque;
use std::fmt;

#[derive(Clone, Debug)]
pub struct HashRef {
    pub hash: Hash,
    pub node: NodeType, // Where in the tree this reference points.
    pub leaf: LeafType, // What kind of data the tree leafs contain.
    pub persistent_ref: ChunkRef,
    pub info: Option<key::Info>,
}

impl From<models::HashRef> for HashRef {
    fn from(v: models::HashRef) -> HashRef {
        HashRef {
            hash: Hash { bytes: v.hash },
            node: From::from(v.height),
            leaf: v.leaf_type,
            persistent_ref: From::from(v.chunk_ref),
            info: match v.extra {
                models::ExtraInfo::None => None,
                models::ExtraInfo::FileInfo(info) => Some(From::from(info)),
            },
        }
    }
}

impl HashRef {
    pub fn to_model(&self) -> models::HashRef {
        models::HashRef {
            hash: self.hash.bytes.clone(),
            chunk_ref: self.persistent_ref.to_model(),
            height: From::from(self.node),
            leaf_type: self.leaf,
            extra: if let Some(ref info) = self.info {
                models::ExtraInfo::FileInfo(info.to_model())
            } else {
                models::ExtraInfo::None
            },
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<HashRef, serde_cbor::error::Error> {
        let v: models::HashRef = serde_cbor::from_slice(bytes)?;
        Ok(From::from(v))
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        serde_cbor::to_vec(&self.to_model()).unwrap()
    }
}

pub trait HashTreeBackend: Clone {
    type Err: fmt::Debug;

    fn fetch_chunk(&self, &HashRef) -> Result<Option<Vec<u8>>, Self::Err>;
    fn fetch_childs(&self, &Hash) -> Option<Vec<u64>>;
    fn fetch_persistent_ref(&self, &Hash) -> Option<ChunkRef>;
    fn insert_chunk(
        &self,
        &[u8],
        NodeType,
        LeafType,
        Option<Vec<u64>>,
        Option<&key::Info>,
    ) -> Result<(u64, HashRef), Self::Err>;
}

fn hash_refs_to_bytes(refs: &Vec<HashRef>) -> Vec<u8> {
    let value = models::HashRefs {
        refs: refs.iter().map(|hr| hr.to_model()).collect(),
    };
    serde_cbor::to_vec(&value).unwrap()
}

fn hash_refs_from_bytes(bytes: &[u8]) -> Option<Vec<HashRef>> {
    if bytes.is_empty() {
        return Some(vec![]);
    }

    let hr: models::HashRefs = serde_cbor::from_slice(bytes).unwrap();
    Some(hr.refs.into_iter().map(|r| From::from(r)).collect())
}

#[test]
fn test_hash_refs_identity() {
    fn prop(count: u8, hash: Vec<u8>, blob: Vec<u8>, n: usize) -> bool {
        let chunk_ref = ChunkRef {
            blob_id: None,
            blob_name: blob.clone(),
            offset: n,
            length: n,
            packing: None,
            key: None,
        };
        let mut v = vec![];
        for i in 1..count + 1 {
            v.push(HashRef {
                hash: Hash {
                    bytes: hash.clone(),
                },
                node: NodeType::Branch(i as u64),
                leaf: LeafType::FileChunk,
                info: None,
                persistent_ref: chunk_ref.clone(),
            });
        }
        let bytes = hash_refs_to_bytes(&v);
        for (i, r) in hash_refs_from_bytes(&bytes).unwrap().iter().enumerate() {
            assert_eq!(v[i].hash, r.hash);
            assert_eq!(v[i].node, r.node);
            assert_eq!(v[i].leaf, r.leaf);
            assert_eq!(v[i].info, r.info);
            assert!(v[i].persistent_ref.blob_id.is_none());
            assert_eq!(v[i].persistent_ref.blob_name, r.persistent_ref.blob_name);
            assert_eq!(v[i].persistent_ref.offset, r.persistent_ref.offset);
            assert_eq!(v[i].persistent_ref.length, r.persistent_ref.length);
            assert_eq!(v[i].persistent_ref.packing, r.persistent_ref.packing);
            assert!(v[i].persistent_ref.key.is_none());
        }

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
    leaf: LeafType,
    levels: Vec<Vec<(u64, HashRef)>>, // Representation of rightmost path to root
}

impl<B: HashTreeBackend> SimpleHashTreeWriter<B> {
    /// Create a new hash-tree to be stored through 'backend' with node order 'order'.
    pub fn new(leaf_type: LeafType, order: usize, backend: B) -> SimpleHashTreeWriter<B> {
        SimpleHashTreeWriter {
            backend: backend,
            order: order,
            leaf: leaf_type,
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
        self.append_at(0, chunk, None, None)
    }

    fn append_at(
        &mut self,
        level: usize,
        data: &[u8],
        childs: Option<Vec<u64>>,
        info: Option<&key::Info>,
    ) -> Result<(), B::Err> {
        let (id, hash_ref) =
            self.backend
                .insert_chunk(&data, From::from(level as u64), self.leaf, childs, info)?;
        self.append_hashref_at(level, id, hash_ref, info)
    }

    fn append_hashref_at(
        &mut self,
        level: usize,
        id: u64,
        hashref: HashRef,
        info: Option<&key::Info>,
    ) -> Result<(), B::Err> {
        assert!(self.levels.len() >= level);
        self.grow_to(level);

        let new_level_len = {
            let level: &mut Vec<(u64, HashRef)> =
                self.levels.get_mut(level).expect("len() >= level");
            level.push((id, hashref));
            level.len()
        };

        if new_level_len == self.order {
            self.collapse_level(level, info)?;
        }

        Ok(())
    }

    fn collapse_level(&mut self, level: usize, info: Option<&key::Info>) -> Result<(), B::Err> {
        // Extract-replace level with a new empty level
        assert!(self.levels.len() > level);
        self.levels.push(Vec::new());
        let level_v = self.levels.swap_remove(level);

        // All data from this level (hashes and references):
        let ids: Vec<u64> = level_v.iter().map(|&(id, _)| id).collect();
        let data = hash_refs_to_bytes(&level_v.into_iter().map(|(_, hr)| hr).collect());

        self.append_at(level + 1, &data[..], Some(ids), info)
    }

    /// Retrieve the hash and backend persistent reference that identified this tree.
    ///
    /// This also flushes and finalizes the tree. It should be considered frozen after calling
    /// `hash()`, i.e. it's Ok to call `hash()` multiple times, but it's **not Ok** to call
    /// `append()` after `hash()`.
    pub fn hash(&mut self, info: Option<&key::Info>) -> Result<HashRef, B::Err> {
        // Empty hash tree is equivalent to hash tree of one empty block:
        if self.levels.is_empty() {
            self.append(&[])?;
        }

        // Locate first level that isn't empty (has data to collapse)
        let first_non_empty_level_idx = self.levels
            .iter()
            .take_while(|level| level.is_empty())
            .count();

        // Unless only root has data, collapse all levels up to top level (which is handled next)
        let top_level = self.top_level().expect("levels.len() > 0");
        (first_non_empty_level_idx..top_level)
            .map(|l| self.collapse_level(l, None))
            .last();

        // Collapse top level if possible
        let top_level = self.top_level().expect("levels.len() > 0");
        if info.is_some() || self.levels.get(top_level).expect("top level").len() > 1 {
            self.collapse_level(top_level, info)?;
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
where
    B: HashTreeBackend,
{
    pub fn new(backend: B, root_hash: HashRef) -> Result<Option<Walker<B>>, B::Err> {
        Ok(Some(Walker {
            backend: backend,
            stack: vec![StackItem::Enter(root_hash)],
        }))
    }

    pub fn resume<V>(&mut self, visitor: &mut V) -> Result<bool, B::Err>
    where
        V: Visitor,
    {
        if self.stack.is_empty() {
            // All empty, we can never discover more work.
            return Ok(false);
        }

        // Basic cycle detection to spot some programming mistakes.
        let mut cycle_start = None;

        while !self.stack.is_empty() {
            let node = match self.stack.pop().expect("!is_empty()") {
                StackItem::Enter(href) => href,
                StackItem::LeaveBranch(href) => {
                    if visitor.branch_leave(&href) {
                        break;
                    }
                    continue;
                }
            };

            match cycle_start {
                None => cycle_start = Some(node.hash.clone()),
                Some(ref hash) => assert!(hash != &node.hash),
            }
            let fetch_chunk = |backend: &B, child: &HashRef| {
                backend
                    .fetch_chunk(child)
                    .map(|opt| opt.expect("Invalid hash ref"))
            };

            match node.node {
                NodeType::Leaf => {
                    if visitor.leaf_enter(&node) {
                        let data = fetch_chunk(&self.backend, &node)?;
                        if visitor.leaf_leave(data, &node) {
                            break;
                        }
                    }
                }
                NodeType::Branch(..) => {
                    let data = fetch_chunk(&self.backend, &node)?;
                    let mut new_childs = hash_refs_from_bytes(&data[..]).unwrap();
                    if visitor.branch_enter(&node, &new_childs) {
                        self.stack.push(StackItem::LeaveBranch(node));
                        new_childs.reverse();
                        self.stack
                            .extend(new_childs.into_iter().map(StackItem::Enter));
                    }
                }
            }
        }

        // At least 1 work item was processed.
        Ok(true)
    }
}

pub struct LeafIterator<B> {
    walker: Walker<B>,
    visitor: LeafVisitor,
}

impl<B> LeafIterator<B>
where
    B: HashTreeBackend,
{
    pub fn new(backend: B, root_ref: HashRef) -> Result<Option<LeafIterator<B>>, B::Err> {
        Ok(Walker::new(backend, root_ref)?.map(|w| LeafIterator {
            walker: w,
            visitor: LeafVisitor {
                leafs: VecDeque::new(),
            },
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
