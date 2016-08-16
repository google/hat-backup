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
// limitations under the License

use blob::*;
use backend::{MemoryBackend, StoreBackend};
use hash;

use std::collections::HashSet;
use std::sync::Arc;
use quickcheck;

#[test]
fn identity() {
    fn prop(chunks: Vec<Vec<u8>>) -> bool {
        let backend = Arc::new(MemoryBackend::new());

        let blob_index = Arc::new(BlobIndex::new_for_testing().unwrap());
        let bs_p = BlobStore::new(blob_index, backend.clone(), 1024);

        let mut ids = Vec::new();
        for chunk in chunks.iter() {
            ids.push((bs_p.store(chunk.to_owned(),
                                 hash::Hash::new(chunk),
                                 Kind::TreeLeaf,
                                 Box::new(move |_| {})),
                      chunk));
        }

        bs_p.flush();

        // Non-empty chunks must be in the backend now:
        for &(ref id, chunk) in ids.iter() {
            if chunk.len() > 0 {
                match backend.retrieve(&id.persistent_ref.blob_id[..]) {
                    Ok(_) => (),
                    Err(e) => panic!(e),
                }
            }
        }

        // All chunks must be available through the blob store:
        for &(ref id, chunk) in ids.iter() {
            assert_eq!(bs_p.retrieve(&id.hash, &id.persistent_ref)
                           .unwrap()
                           .unwrap(),
                       &chunk[..]);
        }

        return true;
    }
    quickcheck::quickcheck(prop as fn(Vec<Vec<u8>>) -> bool);
}

#[test]
fn identity_with_excessive_flushing() {
    fn prop(chunks: Vec<Vec<u8>>) -> bool {
        let backend = Arc::new(MemoryBackend::new());

        let blob_index = Arc::new(BlobIndex::new_for_testing().unwrap());
        let bs_p = BlobStore::new(blob_index, backend.clone(), 1024);

        let mut ids = Vec::new();
        for chunk in chunks.iter() {
            ids.push((bs_p.store(chunk.to_owned(),
                                 hash::Hash::new(chunk),
                                 Kind::TreeLeaf,
                                 Box::new(move |_| {})),
                      chunk));
            bs_p.flush();
            let &(ref id, chunk) = ids.last().unwrap();
            assert_eq!(bs_p.retrieve(&id.hash, &id.persistent_ref)
                           .unwrap()
                           .unwrap(),
                       &chunk[..]);
        }

        // Non-empty chunks must be in the backend now:
        for &(ref id, chunk) in ids.iter() {
            if chunk.len() > 0 {
                match backend.retrieve(&id.persistent_ref.blob_id[..]) {
                    Ok(_) => (),
                    Err(e) => panic!(e),
                }
            }
        }

        // All chunks must be available through the blob store:
        for &(ref id, chunk) in ids.iter() {
            assert_eq!(bs_p.retrieve(&id.hash, &id.persistent_ref)
                           .unwrap()
                           .unwrap(),
                       &chunk[..]);
        }

        return true;
    }
    quickcheck::quickcheck(prop as fn(Vec<Vec<u8>>) -> bool);
}

#[test]
fn blobid_identity() {
    fn prop(name: Vec<u8>, offset: usize, length: usize) -> bool {
        let blob_id = ChunkRef {
            blob_id: name.to_vec(),
            offset: offset,
            length: length,
            kind: Kind::TreeBranch,
            packing: None,
            key: None,
        };
        let blob_id_bytes = blob_id.as_bytes();
        ChunkRef::from_bytes(&mut &blob_id_bytes[..]).unwrap() == blob_id
    }
    quickcheck::quickcheck(prop as fn(Vec<u8>, usize, usize) -> bool);
}

#[test]
fn blob_reuse() {
    let mut c1 = hash::tree::HashRef {
        hash: hash::Hash::new(&[]),
        persistent_ref: ChunkRef {
            blob_id: Vec::new(),
            offset: 0,
            length: 0,
            kind: Kind::TreeLeaf,
            packing: None,
            key: None,
        },
    };
    let mut c2 = c1.clone();

    let mut b = Blob::new(1000);
    b.try_append(vec![1, 2, 3], &mut c1).unwrap();
    b.try_append(vec![4, 5, 6], &mut c2).unwrap();

    let mut out = Vec::new();
    b.into_bytes(&mut out);

    assert_eq!(vec![1, 2, 3],
               Blob::read_chunk(&out, &c1.hash, &c1.persistent_ref).unwrap());
    assert_eq!(vec![4, 5, 6],
               Blob::read_chunk(&out, &c2.hash, &c2.persistent_ref).unwrap());

    let mut c3 = c2.clone();

    b.try_append(vec![1, 2], &mut c1).unwrap();
    b.try_append(vec![1, 2], &mut c2).unwrap();
    b.try_append(vec![1, 2], &mut c3).unwrap();

    out.clear();
    b.into_bytes(&mut out);
    assert_eq!(vec![1, 2],
               Blob::read_chunk(&out, &c1.hash, &c1.persistent_ref).unwrap());
    assert_eq!(vec![1, 2],
               Blob::read_chunk(&out, &c2.hash, &c2.persistent_ref).unwrap());
    assert_eq!(vec![1, 2],
               Blob::read_chunk(&out, &c3.hash, &c3.persistent_ref).unwrap());
}

#[test]
fn blob_identity() {
    fn prop(chunks: Vec<Vec<u8>>) -> bool {
        let max_size = 10000;
        let mut b = Blob::new(max_size);
        let mut n = 0;
        for chunk in chunks.iter() {
            let mut cref = hash::tree::HashRef {
                hash: hash::Hash::new(&[]),
                persistent_ref: ChunkRef {
                    blob_id: Vec::new(),
                    offset: 0,
                    length: 0,
                    kind: Kind::TreeLeaf,
                    packing: None,
                    key: None,
                },
            };
            if let Err(_) = b.try_append(chunk.clone(), &mut cref) {
                assert!(b.upperbound_len() + chunk.len() + cref.as_bytes().len() + 50 >= max_size);
                break;
            }
            n = n + 1;
        }

        let mut out = Vec::new();
        b.into_bytes(&mut out);

        if n == 0 {
            assert_eq!(0, out.len());
            return true;
        }

        assert_eq!(max_size, out.len());

        let hrefs = Blob::new(max_size).refs_from_bytes(&out).unwrap();
        assert_eq!(n, hrefs.len());

        // Check recovered ChunkRefs.
        for (i, href) in hrefs.into_iter().enumerate() {
            assert!(chunks[i].len() < href.persistent_ref.length);
            let chunk = Blob::read_chunk(&out, &href.hash, &href.persistent_ref).unwrap();
            assert_eq!(chunks[i].len(), chunk.len());
            assert_eq!(&chunks[i], &chunk);
        }

        true
    }
    quickcheck::quickcheck(prop as fn(Vec<Vec<u8>>) -> bool);
}

fn empty_blocks_blob_ciphertext(blocksize: usize) -> Vec<u8> {
    let mut blob = Blob::new(1024 * 1024);
    let block = vec![0u8; blocksize];
    loop {
        let mut cref = hash::tree::HashRef {
            hash: hash::Hash::new(&block[..]),
            persistent_ref: ChunkRef {
                blob_id: Vec::new(),
                offset: 0,
                length: block.len(),
                kind: Kind::TreeLeaf,
                packing: None,
                key: None,
            },
        };
        match blob.try_append(block.clone(), &mut cref) {
            Ok(()) => continue,
            Err(_) => break,
        }
    }
    let mut out = Vec::new();
    blob.into_bytes(&mut out);
    out
}

#[test]
fn blob_ciphertext_uniqueblocks() {
    // If every inserted block gets a unique (nonce, key) combination, they should produce unique
    // blocks in the out-coming ciphertext (by high enough probability to assert it).
    let mut blocks = HashSet::new();

    for _ in 1..10 {
        for c in empty_blocks_blob_ciphertext(16).chunks(16) {
            let v = c.to_owned();
            assert!(!blocks.contains(&v));
            blocks.insert(v);
        }
    }
}
