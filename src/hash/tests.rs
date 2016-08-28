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

use hash::tree::*;

use std::sync::{Arc, Mutex};

use blob::{ChunkRef, Kind};
use hash::Hash;
use key;

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use quickcheck;

#[derive(Clone)]
pub struct MemoryBackend {
    chunks: Arc<Mutex<BTreeMap<Vec<u8>, (i64, Option<Vec<i64>>, Vec<u8>)>>>,
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
                   ref_opt: Option<ChunkRef>)
                   -> Result<Option<Vec<u8>>, Self::Err> {
        let hash = match ref_opt {
            Some(b) => Cow::Owned(Hash { bytes: b.blob_id }),  // blob names are chunk hashes.
            None => Cow::Borrowed(hash),
        };
        let guarded_chunks = self.chunks.lock().unwrap();
        Ok(guarded_chunks.get(&hash.bytes).map(|&(_, _, ref chunk)| chunk.clone()))
    }

    fn fetch_childs(&self, hash: &Hash) -> Option<Vec<i64>> {
        let guarded_chunks = self.chunks.lock().unwrap();
        guarded_chunks.get(&hash.bytes).and_then(|&(_, ref childs, _)| childs.clone())
    }

    fn fetch_persistent_ref(&self, hash: &Hash) -> Option<ChunkRef> {
        let guarded_chunks = self.chunks.lock().unwrap();
        match guarded_chunks.get(&hash.bytes) {
            Some(&(ref level, _, ref chunk)) => {
                Some(ChunkRef {
                    blob_id: hash.bytes.clone(),
                    offset: 0,
                    length: chunk.len(),
                    kind: if *level == 0 {
                        Kind::TreeLeaf
                    } else {
                        Kind::TreeBranch
                    },
                    packing: None,
                    key: None,
                })
            }
            None => None,
        }
    }

    fn insert_chunk(&self,
                    hash: &Hash,
                    level: i64,
                    childs: Option<Vec<i64>>,
                    chunk: &[u8])
                    -> Result<(i64, HashRef), Self::Err> {
        let mut guarded_seen = self.seen_chunks.lock().unwrap();
        guarded_seen.insert(chunk.to_vec());

        let mut guarded_chunks = self.chunks.lock().unwrap();
        guarded_chunks.insert(hash.bytes.clone(), (level, childs, chunk.to_vec()));

        let len = hash.bytes.len();

        Ok((0,
            (HashRef {
            hash: hash.clone(),
            persistent_ref: ChunkRef {
                blob_id: hash.bytes.clone(),
                offset: 0,
                length: len,
                kind: if level == 0 {
                    Kind::TreeLeaf
                } else {
                    Kind::TreeBranch
                },
                packing: None,
                key: None,
            },
        })))
    }
}

#[test]
fn identity_many_small_blocks() {
    fn prop(chunks_count: u8) -> bool {
        let backend = MemoryBackend::new();
        let mut ht = SimpleHashTreeWriter::new(4, backend.clone());

        for _ in 0..chunks_count {
            ht.append(b"a").unwrap();
        }

        let (hash, hash_ref) = ht.hash().unwrap();

        let mut tree_it = SimpleHashTreeReader::open(backend, &hash, Some(hash_ref))
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
    let mut ht = SimpleHashTreeWriter::new(4, backend.clone());

    ht.append(&block[..]).unwrap();

    let (hash, hash_ref) = ht.hash().unwrap();

    let mut it = SimpleHashTreeReader::open(backend, &hash, Some(hash_ref))
        .unwrap()
        .expect("tree not found");

    assert_eq!(Some(block), it.next());
    assert_eq!(0, it.count());
}

#[test]
fn identity_append1() {
    let block: Vec<u8> = b"foobar".to_vec();

    let backend = MemoryBackend::new();
    let mut ht = SimpleHashTreeWriter::new(4, backend.clone());

    ht.append(&block[..]).unwrap();

    let (hash, hash_ref) = ht.hash().unwrap();

    let mut it = SimpleHashTreeReader::open(backend, &hash, Some(hash_ref))
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
    let mut ht = SimpleHashTreeWriter::new(4, backend.clone());

    for block in blocks.iter() {
        ht.append(&block[..]).unwrap();
    }

    let (hash, hash_ref) = ht.hash().unwrap();

    let mut it = SimpleHashTreeReader::open(backend.clone(), &hash, Some(hash_ref))
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
    let mut ht = SimpleHashTreeWriter::new(order, backend.clone());

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

    let (hash, hash_ref) = ht.hash().unwrap();

    let it = SimpleHashTreeReader::open(backend, &hash, Some(hash_ref))
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
    let mut ht = SimpleHashTreeWriter::new(order, backend.clone());
    let mut bytes = vec![0u8];

    for i in 1u8..order as u8 {
        bytes[0] = i;
        ht.append(&bytes[..]).unwrap();
    }

    let (hash, hash_ref) = ht.hash().unwrap();

    for i in 1u8..order as u8 {
        assert!(backend.saw_chunk(&vec![i]));
    }

    let it = SimpleHashTreeReader::open(backend, &hash, Some(hash_ref))
        .unwrap()
        .expect("tree not found");

    for (i, chunk) in it.enumerate() {
        bytes[0] = (i + 1) as u8;
        assert_eq!(bytes, chunk);
    }
}
