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

use blob::{Blob, ChunkRef, LeafType, NodeType};
use crypto;
use hash::Hash;
use hash::tree::HashRef;

use std::sync::Arc;

use test::Bencher;

const BLOBSIZE: usize = 5 * 1024 * 1024;
const CHUNKSIZE: usize = 128 * 1024;

fn dummy_hashref(keys: &crypto::keys::Keeper) -> HashRef {
    let node = NodeType::Leaf;
    let leaf = LeafType::FileChunk;
    HashRef {
        hash: Hash::new(keys, node, leaf, &[]),
        node: node,
        leaf: leaf,
        persistent_ref: ChunkRef {
            blob_id: None,
            blob_name: vec![],
            offset: 0,
            length: 0,
            packing: None,
            key: None,
        },
        info: None,
    }
}

#[bench]
fn insert_128_kb_chunks(bench: &mut Bencher) {
    let keys = Arc::new(crypto::keys::Keeper::new_for_testing());
    let mut b = Blob::new(keys.clone(), BLOBSIZE);
    let mut href = dummy_hashref(&keys);
    let chunk = [0u8; CHUNKSIZE];
    bench.iter(|| {
        if let Err(()) = b.try_append(&chunk[..], &mut href) {
            b.to_ciphertext();
            b.try_append(&chunk[..], &mut href).unwrap();
        }
    });
    bench.bytes = CHUNKSIZE as u64;
}

#[bench]
fn insert_256_kb_chunks(bench: &mut Bencher) {
    let keys = Arc::new(crypto::keys::Keeper::new_for_testing());
    let chunk = vec![0u8; 2 * CHUNKSIZE];
    let mut b = Blob::new(keys.clone(), BLOBSIZE);
    let mut href = dummy_hashref(&keys);
    bench.iter(|| {
        if let Err(()) = b.try_append(&chunk[..], &mut href) {
            b.to_ciphertext();
            b.try_append(&chunk[..], &mut href).unwrap();
        }
    });
    bench.bytes = 2 * CHUNKSIZE as u64;
}
