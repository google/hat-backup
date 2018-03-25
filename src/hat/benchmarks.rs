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

use backend::DevNullBackend;
use hat::HatRc;
use hat::family::Family;
use hat::tests::{entry, setup_hat};
use std::io;
use std::sync::{Arc, Mutex};
use test::Bencher;
use util::FileIterator;

fn setup_family() -> (HatRc<DevNullBackend>, Family<DevNullBackend>) {
    let backend = Arc::new(DevNullBackend);
    let mut hat = setup_hat(backend);

    let family = "familyname".to_string();
    let fam = hat.open_family(family).unwrap();

    (hat, fam)
}

#[derive(Clone)]
struct UniqueBlockFiller(Arc<Mutex<u32>>);

#[derive(Clone)]
struct UniqueBlockReader {
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

impl UniqueBlockReader {
    fn new(filler: UniqueBlockFiller, blocksize: usize, filesize: i32) -> UniqueBlockReader {
        UniqueBlockReader {
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

impl io::Read for UniqueBlockReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.filesize <= 0 {
            Ok(0)
        } else {
            self.filesize -= self.blocksize as i32;
            self.filler.fill_bytes(&mut buf[..self.blocksize]);
            Ok(self.blocksize)
        }
    }
}

fn insert_files(bench: &mut Bencher, filesize: i32, unique: bool, commit: bool) {
    let (hat, family) = setup_family();

    let mut filler = UniqueBlockFiller::new(0);
    let mut name = vec![0; 8];

    let mut file_reader = UniqueBlockReader::new(filler.clone(), 128 * 1024, filesize);

    bench.iter(|| {
        filler.fill_bytes(&mut name);

        file_reader.reset_filesize(filesize);
        if !unique {
            // Reset data filler.
            file_reader.reset_filler(0);
        }

        let file = FileIterator::from_reader(Box::new(file_reader.clone()));

        if commit {
            family
                .snapshot_direct(entry(name.clone()), false, Some(file))
                .unwrap();
        } else {
            family
                .snapshot_direct_no_commit(entry(name.clone()), false, Some(file))
                .unwrap();
        }
    });

    hat.data_flush().unwrap();
    bench.bytes = filesize as u64;
}

#[bench]
fn insert_empty_files(mut bench: &mut Bencher) {
    insert_files(&mut bench, 0, true, true);
}

#[bench]
fn insert_empty_files_no_commit(mut bench: &mut Bencher) {
    insert_files(&mut bench, 0, true, false);
}

#[bench]
fn insert_small_unique_files(mut bench: &mut Bencher) {
    insert_files(&mut bench, 8, true, true);
}

#[bench]
fn insert_small_identical_files(mut bench: &mut Bencher) {
    insert_files(&mut bench, 8, false, true);
}

#[bench]
fn insert_medium_unique_files(mut bench: &mut Bencher) {
    insert_files(&mut bench, 1024 * 1024, true, true);
}

#[bench]
fn insert_medium_identical_files(mut bench: &mut Bencher) {
    insert_files(&mut bench, 1024 * 1024, false, true);
}

#[bench]
fn insert_large_unique_files(mut bench: &mut Bencher) {
    insert_files(&mut bench, 16 * 1024 * 1024, true, true);
}

#[bench]
fn insert_large_unique_files_no_commit(mut bench: &mut Bencher) {
    insert_files(&mut bench, 16 * 1024 * 1024, true, false);
}

#[bench]
fn insert_large_identical_files(mut bench: &mut Bencher) {
    insert_files(&mut bench, 16 * 1024 * 1024, false, true);
}
