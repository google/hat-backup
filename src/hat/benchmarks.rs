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

use std::sync::{Arc, Mutex};
use test::Bencher;

use backend::DevNullBackend;
use hat::HatRc;
use hat::family::Family;
use hat::tests::{entry, setup_hat};
use util::FileIterator;


fn setup_family() -> (HatRc<DevNullBackend>, Family<DevNullBackend>) {
    let empty = vec![];
    let backend = Arc::new(DevNullBackend);
    let hat = setup_hat(backend, &empty[..]);

    let family = "familyname".to_string();
    let fam = hat.open_family(family).unwrap();

    (hat, fam)
}

#[derive(Clone)]
struct UniqueBlockFiller(Arc<Mutex<u32>>);

#[derive(Clone)]
struct UniqueBlockIter {
    filler: UniqueBlockFiller,
    blocksize: usize,
    filesize: i32,
}

impl UniqueBlockFiller {
    fn new(id: u32) -> UniqueBlockFiller {
        UniqueBlockFiller(Arc::new(Mutex::new(id)))
    }

    /// Fill the buffer with 1KB unique blocks of data.
    /// Each block is unique for the given id and among the other blocks.
    fn fill_bytes(&mut self, buf: &mut [u8]) {
        for block in buf.chunks_mut(1024) {
            if block.len() < 4 {
                // Last block is too short to make unique.
                break;
            }
            let mut n = self.0.lock().unwrap();

            block[0] = *n as u8;
            block[1] = (*n >> 8) as u8;
            block[2] = (*n >> 16) as u8;
            block[3] = (*n >> 24) as u8;
            *n += 1;
        }
    }
}

impl UniqueBlockIter {
    fn new(filler: UniqueBlockFiller, blocksize: usize, filesize: i32) -> UniqueBlockIter {
        UniqueBlockIter {
            filler: filler,
            blocksize: blocksize,
            filesize: filesize,
        }
    }
    fn reset_filesize(&mut self, filesize: i32) {
        self.filesize = filesize;
    }
    fn reset_filler(&mut self, id: u32) {
        self.filler = UniqueBlockFiller::new(id);
    }
}

impl Iterator for UniqueBlockIter {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Vec<u8>> {
        if self.filesize <= 0 {
            None
        } else {
            self.filesize -= self.blocksize as i32;
            let mut res = vec![0; self.blocksize];
            self.filler.fill_bytes(&mut res);
            Some(res)
        }
    }
}

fn insert_files(bench: &mut Bencher, filesize: i32, unique: bool) {
    let (_, family) = setup_family();

    let mut filler = UniqueBlockFiller::new(0);
    let mut name = vec![0; 8];

    let mut file_iter = UniqueBlockIter::new(filler.clone(), 128 * 1024, filesize);

    bench.iter(|| {
        filler.fill_bytes(&mut name);

        file_iter.reset_filesize(filesize);
        if !unique {
            // Reset data filler.
            file_iter.reset_filler(0);
        }

        let file = FileIterator::from_iter(Box::new(file_iter.clone()));
        family.snapshot_direct(entry(name.clone()), false, Some(file)).unwrap();
    });

    bench.bytes = filesize as u64;
}

#[bench]
fn insert_small_unique_files(mut bench: &mut Bencher) {
    insert_files(&mut bench, 8, true);
}

#[bench]
fn insert_small_identical_files(mut bench: &mut Bencher) {
    insert_files(&mut bench, 8, false);
}

#[bench]
fn insert_medium_unique_files(mut bench: &mut Bencher) {
    insert_files(&mut bench, 1024 * 1024, true);
}

#[bench]
fn insert_medium_identical_files(mut bench: &mut Bencher) {
    insert_files(&mut bench, 1024 * 1024, false);
}

#[bench]
fn insert_large_unique_files(mut bench: &mut Bencher) {
    insert_files(&mut bench, 8 * 1024 * 1024, true);
}

#[bench]
fn insert_large_identical_files(mut bench: &mut Bencher) {
    insert_files(&mut bench, 8 * 1024 * 1024, false);
}
