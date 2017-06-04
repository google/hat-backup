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

use backend::{MemoryBackend, StoreBackend};
use blob::{Blob, BlobError, BlobIndex, BlobStore, ChunkRef, Key, NodeType, LeafType};
use crypto;
use db;
use hash;
use quickcheck;

use std::collections::HashSet;
use std::sync::Arc;

#[test]
fn identity() {
    fn prop(chunks: Vec<Vec<u8>>) -> bool {
        let backend = Arc::new(MemoryBackend::new());

        let keys = Arc::new(crypto::keys::Keeper::new_for_testing());
        let db = Arc::new(db::Index::new_for_testing());
        let blob_index = Arc::new(BlobIndex::new(keys.clone(), db).unwrap());
        let bs_p = BlobStore::new(keys, blob_index, backend.clone(), 1024);

        let mut ids = Vec::new();
        for chunk in chunks.iter() {
            ids.push((bs_p.store(&chunk[..],
                                 hash::Hash::new(chunk),
                                 NodeType::Leaf,
                                 LeafType::FileChunk,
                                 None,
                                 Box::new(move |_| {})),
                      chunk));
        }

        bs_p.flush();

        // Non-empty chunks must be in the backend now:
        for &(ref id, chunk) in ids.iter() {
            if chunk.len() > 0 {
                match backend.retrieve(&id.persistent_ref.blob_name[..]) {
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

        let keys = Arc::new(crypto::keys::Keeper::new_for_testing());
        let db = Arc::new(db::Index::new_for_testing());
        let blob_index = Arc::new(BlobIndex::new(keys.clone(), db).unwrap());
        let bs_p = BlobStore::new(keys, blob_index, backend.clone(), 1024);

        let mut ids = Vec::new();
        for chunk in chunks.iter() {
            ids.push((bs_p.store(&chunk[..],
                                 hash::Hash::new(chunk),
                                 NodeType::Leaf,
                                 LeafType::FileChunk,
                                 None,
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
                match backend.retrieve(&id.persistent_ref.blob_name[..]) {
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
        let blob_name = ChunkRef {
            blob_id: None,
            blob_name: name.to_vec(),
            offset: offset,
            length: length,
            packing: None,
            key: None,
        };
        let blob_name_bytes = blob_name.as_bytes();
        ChunkRef::from_bytes(&mut &blob_name_bytes[..]).unwrap() == blob_name
    }
    quickcheck::quickcheck(prop as fn(Vec<u8>, usize, usize) -> bool);
}

#[test]
fn blob_reuse() {
    let mut c1 = hash::tree::HashRef {
        hash: hash::Hash::new(&[]),
        node: NodeType::Leaf,
        leaf: LeafType::FileChunk,
        info: None,
        persistent_ref: ChunkRef {
            blob_id: None,
            blob_name: Vec::new(),
            offset: 0,
            length: 0,
            packing: None,
            key: None,
        },
    };
    let mut c2 = c1.clone();

    let keys = Arc::new(crypto::keys::Keeper::new_for_testing());
    let mut b = Blob::new(keys, 1000);
    b.try_append(&[1, 2, 3], &mut c1).unwrap();
    b.try_append(&[4, 5, 6], &mut c2).unwrap();

    let out = b.to_ciphertext().unwrap().to_vec();

    assert_eq!(vec![1, 2, 3],
               Blob::read_chunk(&out, &c1.hash, &c1.persistent_ref).unwrap());
    assert_eq!(vec![4, 5, 6],
               Blob::read_chunk(&out, &c2.hash, &c2.persistent_ref).unwrap());

    let mut c3 = c2.clone();

    b.try_append(&[1, 2], &mut c1).unwrap();
    b.try_append(&[1, 2], &mut c2).unwrap();
    b.try_append(&[1, 2], &mut c3).unwrap();

    let out = b.to_ciphertext().unwrap().to_vec();
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
        let keys = Arc::new(crypto::keys::Keeper::new_for_testing());
        let mut b = Blob::new(keys, max_size);
        let mut n = 0;
        for chunk in chunks.iter() {
            let mut cref = hash::tree::HashRef {
                hash: hash::Hash::new(&[]),
                node: NodeType::Leaf,
                leaf: LeafType::FileChunk,
                info: None,
                persistent_ref: ChunkRef {
                    blob_id: None,
                    blob_name: Vec::new(),
                    offset: 0,
                    length: 0,
                    packing: None,
                    key: None,
                },
            };
            if let Err(_) = b.try_append(&chunk[..], &mut cref) {
                assert!(b.upperbound_len() + chunk.len() + cref.as_bytes().len() + 50 >= max_size);
                break;
            }
            n = n + 1;
        }

        let out = match b.to_ciphertext() {
            None => {
                assert_eq!(n, 0);
                return true;
            }
            Some(ct) => ct.to_vec(),
        };

        assert_eq!(max_size, out.len());

        let keys = Arc::new(crypto::keys::Keeper::new_for_testing());
        let hrefs = Blob::new(keys, max_size).refs_from_bytes(&out).unwrap();
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


#[test]
fn random_input_fails() {
    use sodiumoxide::crypto::secretbox::xsalsa20poly1305;

    fn prop(data: Vec<u8>, hash: Vec<u8>) -> bool {
        let keys = Arc::new(crypto::keys::Keeper::new_for_testing());
        let blob = Blob::new(keys, 128000);
        if let Ok(_) = blob.refs_from_bytes(&data[..]) {
            return false;
        }
        let key = vec![0; 32];
        let hash = hash::Hash { bytes: hash };
        let mut cref = ChunkRef {
            blob_id: None,
            blob_name: Vec::new(),
            offset: 0,
            length: 0,
            packing: None,
            key: Some(Key::XSalsa20Poly1305(xsalsa20poly1305::Key::from_slice(&key[..]).unwrap())),
        };

        for l in 0..data.len() {
            cref.length = l;
            if let Ok(_) = Blob::read_chunk(&data[..], &hash, &cref) {
                return false;
            }
        }

        return true;
    }
    quickcheck::quickcheck(prop as fn(Vec<u8>, Vec<u8>) -> bool);
}

fn empty_blocks_blob_ciphertext(blob: &mut Blob, blocksize: usize) -> Vec<u8> {
    let block = vec![0u8; blocksize];
    loop {
        let mut cref = hash::tree::HashRef {
            hash: hash::Hash::new(&block[..]),
            node: NodeType::Leaf,
            leaf: LeafType::FileChunk,
            info: None,
            persistent_ref: ChunkRef {
                blob_id: None,
                blob_name: Vec::new(),
                offset: 0,
                length: block.len(),
                packing: None,
                key: None,
            },
        };
        match blob.try_append(&block[..], &mut cref) {
            Ok(()) => continue,
            Err(_) => break,
        }
    }
    blob.to_ciphertext().unwrap().to_vec()
}

#[test]
fn blob_ciphertext_uniqueblocks() {
    // If every inserted block gets a unique (nonce, key) combination, they should produce unique
    // blocks in the out-coming ciphertext (by high enough probability to assert it).
    let keys = Arc::new(crypto::keys::Keeper::new_for_testing());
    let mut blob = Blob::new(keys, 1024 * 1024);
    let mut blocks = HashSet::new();

    for _ in 1..10 {
        for c in empty_blocks_blob_ciphertext(&mut blob, 16).chunks(16) {
            let v = c.to_owned();
            assert!(!blocks.contains(&v));
            blocks.insert(v);
        }
    }
}

#[test]
fn blob_ciphertext_authed_allbytes() {
    let keys = Arc::new(crypto::keys::Keeper::new_for_testing());
    let mut blob = Blob::new(keys, 1024);
    let mut bytes = empty_blocks_blob_ciphertext(&mut blob, 1);

    fn verify(blob: &Blob, bs: &[u8]) -> Result<Vec<Vec<u8>>, BlobError> {
        let mut vs = vec![];
        for r in blob.refs_from_bytes(bs)? {
            vs.push(Blob::read_chunk(&bs, &r.hash, &r.persistent_ref)?);
        }
        Ok(vs)
    };

    fn with_modified<F>(mut bytes: &mut [u8],
                        i: usize,
                        b: u8,
                        f: F)
                        -> Result<Vec<Vec<u8>>, BlobError>
        where F: FnOnce(&[u8]) -> Result<Vec<Vec<u8>>, BlobError>
    {
        {
            bytes[i] ^= b;
        }
        let res = f(&bytes);
        {
            bytes[i] ^= b;
        }
        res
    };

    // Blob is valid.
    let vs = verify(&blob, &bytes[..]).unwrap();
    for i in 0..bytes.len() {
        assert!(with_modified(&mut bytes[..], i, 1, |bs| verify(&blob, bs)).is_err());
    }
    // We did not corrupt the blob.
    assert_eq!(vs, verify(&blob, &bytes[..]).unwrap());
}
