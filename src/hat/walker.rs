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


use backend::StoreBackend;
use blob;
use capnp;
use errors::HatError;
use hash;
use hat::insert_path_handler::InsertPathHandler;
use key;
use root_capnp;
use std::collections::VecDeque;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::str;
use std::sync::mpsc;
use util::{FileIterator, FnBox, PathHandler};

struct FileEntry(hash::Hash, blob::ChunkRef, key::Entry);

trait Visitor {
    fn enter_file(&mut self, _file: &FileEntry) -> bool {
        true
    }
}

trait HasFiles {
    fn files(&mut self) -> Vec<FileEntry> {
        vec![]
    }
}

struct Walker<B> {
    backend: B,
    tree: hash::tree::Walker<B>,
    child: Option<Box<Walker<B>>>,
    stack: VecDeque<Walker<B>>,
}

impl<B> Walker<B>
    where B: hash::tree::HashTreeBackend
{
    pub fn new(backend: B,
               root_hash: hash::Hash,
               root_ref: Option<blob::ChunkRef>)
               -> Result<Walker<B>, B::Err> {
        let tree = try!(hash::tree::Walker::new(backend.clone(), root_hash, root_ref)).unwrap();
        Ok(Walker {
            backend: backend,
            tree: tree,
            child: None,
            stack: VecDeque::new(),
        })
    }

    pub fn resume_child<V, TV>(&mut self,
                               mut visitor: &mut V,
                               mut tvisitor: &mut TV)
                               -> Result<bool, B::Err>
        where V: Visitor,
              TV: hash::tree::Visitor,
              TV: HasFiles
    {
        if let Some(ref mut c) = self.child.as_mut() {
            if try!(c.resume(visitor, tvisitor)) {
                return Ok(true);
            }
        }
        return Ok(false);
    }


    pub fn resume<V, TV>(&mut self,
                         mut visitor: &mut V,
                         mut tvisitor: &mut TV)
                         -> Result<bool, B::Err>
        where V: Visitor,
              TV: hash::tree::Visitor,
              TV: HasFiles
    {
        if try!(self.resume_child(visitor, tvisitor)) {
            return Ok(true);
        }

        self.child = self.stack.pop_front().map(Box::new);
        if self.child.is_none() {
            if !try!(self.tree.resume(tvisitor)) {
                return Ok(false);
            }
            for file in tvisitor.files() {
                if visitor.enter_file(&file) {
                    self.stack
                        .push_back(try!(Walker::new(self.backend.clone(), file.0, Some(file.1))));
                }
            }
        }
        self.resume(visitor, tvisitor)
    }
}
