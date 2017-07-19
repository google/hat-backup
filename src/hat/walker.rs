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

use std::path::PathBuf;
use hash;
use key;
use std::collections::VecDeque;

#[derive(Clone, Debug)]
pub enum Content {
    Data(hash::tree::HashRef),
    Dir(hash::tree::HashRef),
    Link(PathBuf),
}

#[derive(Clone)]
pub struct FileEntry {
    pub hash_ref: Content,
    pub meta: key::Entry,
}

pub trait LikesFiles {
    fn include_file(&mut self, _file: &FileEntry) -> bool {
        true
    }
    fn include_dir(&mut self, _file: &FileEntry) -> bool {
        true
    }
}

pub trait HasFiles {
    fn files(&mut self) -> Vec<FileEntry> {
        vec![]
    }
}

enum StackItem {
    File(FileEntry),
    Dir(FileEntry),
}

enum Child<B> {
    File(hash::tree::Walker<B>),
    Dir(Box<Walker<B>>),
}

pub struct Walker<B> {
    backend: B,
    tree: hash::tree::Walker<B>,
    child: Option<Child<B>>,
    stack: VecDeque<StackItem>,
}

impl<B> Walker<B>
where
    B: hash::tree::HashTreeBackend,
{
    pub fn new(backend: B, root_hash: hash::tree::HashRef) -> Result<Walker<B>, B::Err> {
        let tree = hash::tree::Walker::new(backend.clone(), root_hash)?
            .unwrap();
        Ok(Walker {
            backend: backend,
            tree: tree,
            child: None,
            stack: VecDeque::new(),
        })
    }

    pub fn resume_child<FV, DV>(
        &mut self,
        mut file_v: &mut FV,
        mut dir_v: &mut DV,
    ) -> Result<bool, B::Err>
    where
        FV: LikesFiles,
        DV: HasFiles,
        FV: hash::tree::Visitor,
        DV: hash::tree::Visitor,
    {
        Ok(match self.child.as_mut() {
            Some(&mut Child::File(ref mut tree)) => tree.resume(file_v)?,
            Some(&mut Child::Dir(ref mut walker)) => walker.resume(file_v, dir_v)?,
            None => false,
        })
    }

    pub fn resume<FV, DV>(
        &mut self,
        mut file_v: &mut FV,
        mut dir_v: &mut DV,
    ) -> Result<bool, B::Err>
    where
        FV: LikesFiles,
        DV: HasFiles,
        FV: hash::tree::Visitor,
        DV: hash::tree::Visitor,
    {
        if self.resume_child(file_v, dir_v)? {
            return Ok(true);
        }

        self.child = match self.stack.pop_front() {
            Some(StackItem::File(f)) => {
                let href = match f.hash_ref {
                    Content::Data(href) => href,
                    _ => unreachable!("Expected file"),
                };
                Some(Child::File(
                    hash::tree::Walker::new(self.backend.clone(), href)?
                        .unwrap(),
                ))
            }
            Some(StackItem::Dir(f)) => {
                let href = match f.hash_ref {
                    Content::Dir(href) => href,
                    _ => unreachable!("Expected dir"),
                };
                Some(Child::Dir(Box::new(
                    Walker::new(self.backend.clone(), href).unwrap(),
                )))
            }
            None => None,
        };

        if self.child.is_some() {
            return self.resume(file_v, dir_v);
        } else {
            if !self.tree.resume(dir_v)? {
                // No child and no tree. We can never do more work.
                return Ok(false);
            }
            for file in dir_v.files() {
                if file_v.include_file(&file) {
                    self.stack.push_back(StackItem::File(file.clone()));
                } else if file_v.include_dir(&file) {
                    self.stack.push_back(StackItem::Dir(file));
                }
            }
        }

        return Ok(true);
    }
}
