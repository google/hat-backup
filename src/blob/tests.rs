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
