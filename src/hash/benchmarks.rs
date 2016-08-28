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

use test::Bencher;

use hash::tree::*;
use hash::tests::MemoryBackend;

#[bench]
fn append_unknown_16x128_kb(bench: &mut Bencher) {
    let mut bytes = vec![0u8; 128*1024];

    bench.iter(|| {
        let mut ht = SimpleHashTreeWriter::new(8, MemoryBackend::new());
        for i in 0u8..16 {
            bytes[0] = i;
            ht.append(&bytes[..]).unwrap();
        }
        ht.hash().unwrap();
    });

    bench.bytes = 128 * 1024 * 16;
}

#[bench]
fn append_known_16x128_kb(bench: &mut Bencher) {
    let bytes = vec![0u8; 128*1024];

    bench.iter(|| {
        let mut ht = SimpleHashTreeWriter::new(8, MemoryBackend::new());
        for _ in 0i32..16 {
            ht.append(&bytes[..]).unwrap();
        }
        ht.hash().unwrap();
    });

    bench.bytes = 128 * 1024 * 16;
}
