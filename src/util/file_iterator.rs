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

use std::fs;
use std::io;
use std::io::Read;
use std::path::PathBuf;

pub enum FileIterator {
    File(io::BufReader<fs::File>),
    #[cfg(test)]
    Buf(Vec<u8>, usize),
    #[cfg(all(test, feature = "benchmarks"))]
    Iter(Box<Iterator<Item = Vec<u8>> + Send>),
}

impl FileIterator {
    pub fn new(path: &PathBuf) -> io::Result<FileIterator> {
        match fs::File::open(path) {
            Ok(f) => Ok(FileIterator::File(io::BufReader::new(f))),
            Err(e) => Err(e),
        }
    }
    #[cfg(test)]
    pub fn from_bytes(contents: Vec<u8>) -> FileIterator {
        FileIterator::Buf(contents, 0)
    }

    #[cfg(all(test, feature = "benchmarks"))]
    pub fn from_iter<I>(i: Box<I>) -> FileIterator
        where I: Iterator<Item = Vec<u8>> + Send + 'static
    {
        FileIterator::Iter(i)
    }
}

impl Iterator for FileIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Vec<u8>> {
        let chunk_size = 128 * 1024;
        match self {
            &mut FileIterator::File(ref mut f) => {
                let mut buf = vec![0u8; chunk_size];
                match f.read(&mut buf[..]) {
                    Err(_) => None,
                    Ok(size) if size == 0 => None,
                    Ok(size) => Some(buf[..size].to_vec()),
                }
            }
            #[cfg(test)]
            &mut FileIterator::Buf(ref vec, ref mut pos) => {
                use std::cmp;
                if *pos >= vec.len() {
                    None
                } else {
                    let next = &vec[*pos..cmp::min(*pos + chunk_size, vec.len())];
                    *pos += chunk_size;
                    Some(next.to_owned())
                }
            }
            #[cfg(all(test, feature = "benchmarks"))]
            &mut FileIterator::Iter(ref mut inner) => inner.next(),
        }
    }
}
