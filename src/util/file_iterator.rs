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
    Reader(Box<Read + Send>),
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
    pub fn from_reader<R>(r: Box<R>) -> FileIterator
        where R: Read + Send + 'static
    {
        FileIterator::Reader(r)
    }
}

impl Read for FileIterator {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            FileIterator::File(ref mut f) => f.read(buf),
            #[cfg(test)]
            FileIterator::Buf(ref vec, ref mut pos) => {
                use std::cmp;
                if *pos >= vec.len() {
                    Ok(0)
                } else {
                    let next = &vec[*pos..cmp::min(*pos + buf.len(), vec.len())];
                    *pos += next.len();
                    buf[..next.len()].clone_from_slice(&next);
                    Ok(next.len())
                }
            }
            #[cfg(all(test, feature = "benchmarks"))]
            FileIterator::Reader(ref mut r) => r.read(buf),
        }
    }
}
