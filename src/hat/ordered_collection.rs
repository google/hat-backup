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

use std::collections::treemap::{TreeMap};
use std::num::{Bounded};


pub struct OrderedCollection<K, V> {
  map: TreeMap<K, V>
}

impl <K: Clone + Ord + Bounded, V: Clone> OrderedCollection<K, V> {

  pub fn new() -> OrderedCollection<K, V> {
    OrderedCollection{map: TreeMap::new()}
  }

  pub fn insert(&mut self, k: K, v: V) {
    self.update_value(k, |v_opt| match v_opt {
      Some(_) => fail!("Key already exists."),
      None => v.clone(),
    });
  }

  pub fn update_value(&mut self, k: K, f: |Option<&V>| -> V) {
    let v_opt = self.map.find(&k).map(|v| v.clone());
    self.map.insert(k, f(v_opt.as_ref()));
  }

  pub fn pop(&mut self, k: &K) -> Option<V> {
    self.map.pop(k)
  }

  pub fn find<'a>(&'a self, k: &K) -> Option<&'a V> {
    self.map.find(k)
  }

  pub fn find_min<'a>(&'a self) -> Option<(&'a K, &'a V)> {
    self.map.lower_bound(&Bounded::min_value()).next()
  }

  pub fn pop_min_when(&mut self, ready: |&K, &V| -> bool) -> Option<(K, V)> {
    let k_opt = self.find_min().and_then(|(k, v)| if ready(k, v) { Some(k.clone()) } else { None });
    k_opt.map(|k| { let v = self.pop(&k).unwrap();
                    (k, v) })
  }

  pub fn len(&self) -> uint {
    self.map.len()
  }

}
