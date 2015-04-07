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

use std::thunk::Thunk;
use std::collections::{BTreeMap};
use std::collections::btree_map;


pub struct CallbackContainer<K> {
  callbacks: BTreeMap<K, Vec<Thunk<'static>>>,
  ready: Vec<Thunk<'static>>,
}


impl <K: Ord> CallbackContainer<K> {

  pub fn new() -> CallbackContainer<K> {
    CallbackContainer{callbacks: BTreeMap::new(),
                      ready: vec!()}
  }

  pub fn add(&mut self, k: K, callback: Thunk<'static>) {
    match self.callbacks.entry(k) {
      btree_map::Entry::Occupied(mut entry) => {
        entry.get_mut().push(callback);
      },
      btree_map::Entry::Vacant(space) => {
        space.insert(vec!(callback));
      }
    }
  }

  pub fn allow_flush_of(&mut self, k: &K) {
    self.ready.extend(self.callbacks.remove(k).unwrap_or(vec!()).into_iter());
  }

  pub fn flush(&mut self) {
    while self.ready.len() > 0 {
      let f = self.ready.pop().expect("len() > 0");
      f();
    }
    assert_eq!(self.ready.len(), 0);
  }

  pub fn len(&self) -> usize {
    self.callbacks.len()
  }

}
