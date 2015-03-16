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

use std::collections::{BTreeMap};
use std::collections::btree_map;


pub struct OrderedCollection<K, V> {
  map: BTreeMap<K, V>
}

impl <K: Clone + Ord, V> OrderedCollection<K, V> {

  pub fn new() -> OrderedCollection<K, V> {
    OrderedCollection{map: BTreeMap::new()}
  }

  pub fn insert(&mut self, k: K, v: V) {
    self.update_value(k, move|v_opt| match v_opt {
      Some(_) => panic!("Key already exists."),
      None => v,
    });
  }

  pub fn update_value<F>(&mut self, k: K, f: F) where F: FnOnce(Option<&V>) -> V {
    match self.map.entry(k) {
      btree_map::Entry::Occupied(mut entry) => {
        let new_v = f(Some(entry.get()));
        entry.insert(new_v);
      },
      btree_map::Entry::Vacant(space) => {
        space.insert(f(None));
      }
    }
  }

  pub fn pop(&mut self, k: &K) -> Option<V> {
    self.map.remove(k)
  }

  pub fn find<'a>(&'a self, k: &K) -> Option<&'a V> {
    self.map.get(k)
  }

  pub fn find_min<'a>(&'a self) -> Option<(&'a K, &'a V)> {
    self.map.iter().next()
  }

  pub fn pop_min_when<F>(&mut self, ready: F) -> Option<(K, V)>
    where F: Fn(&K, &V) -> bool
  {
    let k_opt = self.find_min().and_then(|(k, v)| if ready(k, v) { Some(k.clone()) } else { None });
    k_opt.map(|k| { let v = self.pop(&k).unwrap();
                    (k, v) })
  }

  pub fn len(&self) -> usize {
    self.map.len()
  }

}
