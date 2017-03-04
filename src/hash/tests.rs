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


use blob::{ChunkRef, NodeType, LeafType};
use hash::Hash;
use hash::tree::*;
use key;
use quickcheck;

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct MemoryBackend {
    chunks: Arc<Mutex<BTreeMap<Vec<u8>, (NodeType, LeafType, Option<Vec<i64>>, Vec<u8>)>>>,
    seen_chunks: Arc<Mutex<BTreeSet<Vec<u8>>>>,
}

impl MemoryBackend {
    pub fn new() -> MemoryBackend {
        MemoryBackend {
            chunks: Arc::new(Mutex::new(BTreeMap::new())),
            seen_chunks: Arc::new(Mutex::new(BTreeSet::new())),
        }
    }
    pub fn saw_chunk(&self, chunk: &Vec<u8>) -> bool {
        let guarded_seen = self.seen_chunks.lock().unwrap();
        guarded_seen.contains(chunk)
    }
}

impl HashTreeBackend for MemoryBackend {
    type Err = key::MsgError;

    fn fetch_chunk(&self,
                   hash: &Hash,
                   ref_opt: Option<&ChunkRef>)
                   -> Result<Option<Vec<u8>>, Self::Err> {
        let hash = match ref_opt {
            Some(ref b) => Cow::Owned(Hash { bytes: b.blob_name.clone() }), // blob name is chunk hash
            None => Cow::Borrowed(hash),
        };
        let guarded_chunks = self.chunks.lock().unwrap();
        Ok(guarded_chunks.get(&hash.bytes).map(|&(_, _, _, ref chunk)| chunk.clone()))
    }

    fn fetch_childs(&self, hash: &Hash) -> Option<Vec<i64>> {
        let guarded_chunks = self.chunks.lock().unwrap();
        guarded_chunks.get(&hash.bytes).and_then(|&(_, _, ref childs, _)| childs.clone())
    }

    fn fetch_persistent_ref(&self, hash: &Hash) -> Option<ChunkRef> {
        let guarded_chunks = self.chunks.lock().unwrap();
        match guarded_chunks.get(&hash.bytes) {
            Some(&(_, _, _, ref chunk)) => {
                Some(ChunkRef {
                    blob_id: None,
                    blob_name: hash.bytes.clone(),
                    offset: 0,
                    length: chunk.len(),
                    packing: None,
                    key: None,
                })
            }
            None => None,
        }
    }

    fn insert_chunk(&self,
                    chunk: &[u8],
                    node: NodeType,
                    leaf: LeafType,
                    childs: Option<Vec<i64>>,
                    _: Option<&key::Info>)
                    -> Result<(i64, HashRef), Self::Err> {
        let len = chunk.len();
        let mut guarded_seen = self.seen_chunks.lock().unwrap();
        guarded_seen.insert(chunk.to_vec());

        let mut guarded_chunks = self.chunks.lock().unwrap();

        let hash = Hash::new(chunk);
        guarded_chunks.insert(hash.bytes.clone(), (node, leaf, childs, chunk.to_vec()));

        Ok((0,
            HashRef {
                hash: hash.clone(),
                node: node,
                leaf: leaf,
                info: None,
                persistent_ref: ChunkRef {
                blob_id: None,
                blob_name: hash.bytes.clone(),
                offset: 0,
                length: len,
                packing: None,
                key: None,
                }
            }))
    }
}

#[test]
fn identity_many_small_blocks() {
    fn prop(chunks_count: u8) -> bool {
        let backend = MemoryBackend::new();
        let mut ht = SimpleHashTreeWriter::new(LeafType::FileChunk, 4, backend.clone());

        for _ in 0..chunks_count {
            ht.append(b"a").unwrap();
        }

        let hash_ref = ht.hash(None).unwrap();
        assert_eq!(hash_ref.leaf, LeafType::FileChunk);

        let mut tree_it = LeafIterator::new(backend, hash_ref)
            .unwrap()
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
    let mut ht = SimpleHashTreeWriter::new(LeafType::FileChunk, 4, backend.clone());

    ht.append(&block[..]).unwrap();

    let hash_ref = ht.hash(None).unwrap();

    let mut it = LeafIterator::new(backend, hash_ref)
        .unwrap()
        .expect("tree not found");

    assert_eq!(Some(block), it.next());
    assert_eq!(0, it.count());
}

#[test]
fn identity_append1() {
    let block: Vec<u8> = b"foobar".to_vec();

    let backend = MemoryBackend::new();
    let mut ht = SimpleHashTreeWriter::new(LeafType::FileChunk, 4, backend.clone());

    ht.append(&block[..]).unwrap();

    let hash_ref = ht.hash(None).unwrap();

    let mut it = LeafIterator::new(backend, hash_ref)
        .unwrap()
        .expect("tree not found");
    assert_eq!(Some(block), it.next());
    assert_eq!(0, it.count());
}

#[test]
fn identity_append5() {
    let blocks: Vec<Vec<u8>> =
        vec![b"foo".to_vec(), b"bar".to_vec(), b"baz".to_vec(), b"qux".to_vec(), b"norf".to_vec()];

    let backend = MemoryBackend::new();
    let mut ht = SimpleHashTreeWriter::new(LeafType::FileChunk, 4, backend.clone());

    for block in blocks.iter() {
        ht.append(&block[..]).unwrap();
    }

    let hash_ref = ht.hash(None).unwrap();

    let mut it = LeafIterator::new(backend.clone(), hash_ref)
        .unwrap()
        .expect("tree not found");

    for block in blocks {
        assert_eq!(Some(&block), it.next().as_ref());
        assert!(backend.saw_chunk(&block));
    }
    assert_eq!(0, it.count());
}

#[test]
fn identity_implicit_flush() {
    let order = 8;
    let backend = MemoryBackend::new();
    let mut ht = SimpleHashTreeWriter::new(LeafType::FileChunk, order, backend.clone());

    let mut bytes = vec![0u8];
    {
        for i in 1u8..(order * 4 + 1) as u8 {
            bytes[0] = i;
            ht.append(&bytes[..]).unwrap();
        }
    }

    for i in 1u8..(order * 4 + 1) as u8 {
        assert!(backend.saw_chunk(&vec![i]));
    }

    let hash_ref = ht.hash(None).unwrap();

    let it = LeafIterator::new(backend, hash_ref)
        .unwrap()
        .expect("tree not found");

    for (i, chunk) in it.enumerate() {
        bytes[0] = (i + 1) as u8;
        assert_eq!(bytes, chunk);
    }
}

#[test]
fn identity_1_short_of_flush() {
    let order = 8;
    let backend = MemoryBackend::new();
    let mut ht = SimpleHashTreeWriter::new(LeafType::FileChunk, order, backend.clone());
    let mut bytes = vec![0u8];

    for i in 1u8..order as u8 {
        bytes[0] = i;
        ht.append(&bytes[..]).unwrap();
    }

    let hash_ref = ht.hash(None).unwrap();

    for i in 1u8..order as u8 {
        assert!(backend.saw_chunk(&vec![i]));
    }

    let it = LeafIterator::new(backend, hash_ref)
        .unwrap()
        .expect("tree not found");

    for (i, chunk) in it.enumerate() {
        bytes[0] = (i + 1) as u8;
        assert_eq!(bytes, chunk);
    }
}
