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

use std::collections::hashmap::{HashMap};
use std::hash::{Hash};


pub struct CallbackContainer<K> {
  callbacks: HashMap<K, Vec<proc():Send>>,
  ready: Vec<proc():Send>,
}


impl <K: Hash + Eq> CallbackContainer<K> {

  pub fn new() -> CallbackContainer<K> {
    CallbackContainer{callbacks: HashMap::new(),
                      ready: vec!()}
  }

  pub fn add(&mut self, k: K, callback: proc():Send) {
    self.callbacks.find_with_or_insert_with(k, callback,
                                            |_k, cbs, new| cbs.push(new),
                                            |_k, new| vec!(new));
  }

  pub fn allow_flush_of(&mut self, k: K) {
    self.ready.push_all_move(self.callbacks.pop(&k).unwrap_or(vec!()));
  }

  pub fn flush(&mut self) {
    while self.ready.len() > 0 {
      self.ready.pop().expect("len() > 0")();
    }
    assert_eq!(self.ready.len(), 0);
  }

  pub fn len(&self) -> uint {
    self.callbacks.len()
  }

}
