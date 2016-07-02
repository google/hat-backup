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
            ids.push((bs_p.store(chunk.to_owned(), Kind::TreeLeaf, Box::new(move |_| {}))
                .unwrap(),
                      chunk));
        }

        assert!(bs_p.flush().is_ok());

        // Non-empty chunks must be in the backend now:
        for &(ref id, chunk) in ids.iter() {
            if chunk.len() > 0 {
                match backend.retrieve(&id.blob_id[..]) {
                    Ok(_) => (),
                    Err(e) => panic!(e),
                }
            }
        }

        // All chunks must be available through the blob store:
        for &(ref id, chunk) in ids.iter() {
            assert_eq!(bs_p.retrieve(id).unwrap().unwrap(), &chunk[..]);
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
            ids.push((bs_p.store(chunk.to_owned(), Kind::TreeLeaf, Box::new(move |_| {}))
                .unwrap(),
                      chunk));
            assert!(bs_p.flush().is_ok());
            let &(ref id, chunk) = ids.last().unwrap();
            assert_eq!(bs_p.retrieve(id).unwrap().unwrap(), &chunk[..]);
        }

        // Non-empty chunks must be in the backend now:
        for &(ref id, chunk) in ids.iter() {
            if chunk.len() > 0 {
                match backend.retrieve(&id.blob_id[..]) {
                    Ok(_) => (),
                    Err(e) => panic!(e),
                }
            }
        }

        // All chunks must be available through the blob store:
        for &(ref id, chunk) in ids.iter() {
            assert_eq!(bs_p.retrieve(id).unwrap().unwrap(), &chunk[..]);
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
        };
        let blob_id_bytes = blob_id.as_bytes();
        ChunkRef::from_bytes(&mut &blob_id_bytes[..]).unwrap() == blob_id
    }
    quickcheck::quickcheck(prop as fn(Vec<u8>, usize, usize) -> bool);
}

#[test]
fn blob_reuse() {
    let c = ChunkRef {
        blob_id: Vec::new(),
        offset: 0,
        length: 1,
        kind: Kind::TreeLeaf,
    };
    let mut b = Blob::new(100);
    b.try_append(vec![1,2,3], &c).unwrap();
    b.try_append(vec![4,5,6], &c).unwrap();

    let mut out = Vec::new();
    b.into_bytes(&mut out);

    assert_eq!(&[1,2,3,4,5,6], &out[0..6]);

    b.try_append(vec![1,2], &c).unwrap();
    b.try_append(vec![1,2], &c).unwrap();
    b.try_append(vec![1,2], &c).unwrap();

    out.clear();
    b.into_bytes(&mut out);
    assert_eq!(&[1,2,1,2,1,2], &out[0..6]);
}

#[test]
fn blob_identity() {
    fn prop(chunks: Vec<Vec<u8>>) -> bool {
        let max_size = 10000;
        let mut b = Blob::new(max_size);
        for chunk in chunks.iter() {
            if let Err(_) = b.try_append(chunk.clone(),
                                         &ChunkRef {
                                             blob_id: Vec::new(),
                                             offset: 0,
                                             length: chunk.len(),
                                             kind: Kind::TreeLeaf,
                                         }) {
                assert!(b.upperbound_len() + chunk.len() + 50 >= max_size);
                break;
            }
        }

        let mut out = Vec::new();
        b.into_bytes(&mut out);

        if chunks.len() == 0 {
            assert_eq!(0, out.len());
            return true;
        }

        assert_eq!(max_size, out.len());

        let crefs = Blob::chunk_refs_from_bytes(&out).unwrap();
        assert_eq!(chunks.len(), crefs.len());

        // Check recovered ChunkRefs.
        let mut pos = 0;
        for (i, cref) in crefs.into_iter().enumerate() {
            assert_eq!(chunks[i].len(), cref.length);
            assert_eq!(&chunks[i][..], &out[pos..pos + cref.length]);
            pos += cref.length;
        }

        true
    }
    quickcheck::quickcheck(prop as fn(Vec<Vec<u8>>) -> bool);
}
