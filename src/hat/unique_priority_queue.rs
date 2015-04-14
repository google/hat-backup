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

#![plugin(quickcheck_macros)]

use std::collections::{BTreeMap};
use std::fmt::{Debug};

use ordered_collection::{OrderedCollection};


#[derive(Clone)]
enum Status<K> {
  Pending(K),
  Ready(K),
}


pub struct UniquePriorityQueue<P, K, V> {
  priority: BTreeMap<P, (Status<K>, Option<V>)>,
  key_to_priority: BTreeMap<K, P>,
}

impl <P: Debug + Clone + Ord, K: Debug + Ord + Clone, V: Clone>
  UniquePriorityQueue<P, K, V> {

  pub fn new() -> UniquePriorityQueue<P, K, V> {
    UniquePriorityQueue{priority: BTreeMap::new(),
                        key_to_priority: BTreeMap::new()}
  }

  pub fn reserve_priority(&mut self, p: P, k: K) -> Result<(), ()> {
    if self.priority.get(&p).is_some() || self.key_to_priority.contains_key(&k) {
      return Err(());
    }
    self.priority.insert_unique(p.clone(), (Status::Pending(k.clone()), None));
    self.key_to_priority.insert(k, p);
    return Ok(());
  }

  pub fn find_key<'a>(&'a self, k: &K) -> Option<&'a P> {
    self.key_to_priority.get(k)
  }

  pub fn put_value(&mut self, k: K, v: V) {
    let prio = self.key_to_priority.get(&k).expect("put_value: Key must exist.");
    self.priority.update_value(prio.clone(), |opt| match opt {
      Some(&(ref status, None)) => (status.clone(), Some(v.clone())),
      _ => unreachable!(),
    });
  }

  pub fn find_value_of_key<'a>(&'a self, k: &K) -> Option<V> {
    let prio_opt = self.key_to_priority.get(k);
    prio_opt.and_then(|prio| self.priority.get(prio).and_then(|&(_, ref v_opt)| v_opt.clone()))
  }

  pub fn update_value<F>(&mut self, k: &K, f: F) where F: Fn(&V) -> V {
    let prio = self.key_to_priority.get(k).expect("update_value: Key must exist.");
    self.priority.update_value(prio.clone(), |opt| match opt {
      Some(&(ref status, Some(ref v))) => (status.clone(), Some(f(v))),
      _ => unreachable!(),
    });
  }

  pub fn set_ready(&mut self, p: P) {
    self.priority.update_value(p, |opt| match opt {
      Some(&(Status::Pending(ref k), ref v_opt)) => (Status::Ready(k.clone()), v_opt.clone()),
      _ => unreachable!(),
    });
  }

  pub fn pop_min_if_complete(&mut self) -> Option<(P, K, V)> {
    let min_opt = self.priority.pop_min_when(|_k, min| match min {
      &(Status::Ready(_), Some(_)) => true,  // We are ready and have a value
      _ => false,
    });
    min_opt.map(|(p, (status, v_opt))| { match status {
      Status::Ready(k) => {
        let v = v_opt.unwrap();
        self.key_to_priority.remove(&k);
        (p, k, v)
      },
      _ => unreachable!(),
    }})
  }

  fn len(&self) -> usize {
    self.priority.len()
  }

}


#[cfg(test)]
mod tests {
  use super::*;

  use std::collections::{BTreeMap};

  #[quickcheck]
  fn insert1(priority: i8, key: isize, value: i8) -> bool {
    let mut upq = UniquePriorityQueue::new();
    assert!(upq.reserve_priority(priority, key).is_ok());
    assert_eq!(upq.pop_min_if_complete(), None);
    upq.put_value(key, value);
    assert_eq!(upq.pop_min_if_complete(), None);
    upq.set_ready(priority);
    assert_eq!(upq.pop_min_if_complete(), Some((priority, key, value)));

    return true;
  }

  #[quickcheck]
  fn insert_many(keys: Vec<(i8, isize, i8)>) -> bool {
    let mut upq = UniquePriorityQueue::new();
    assert_eq!(upq.pop_min_if_complete(), None);

    // Insert priorities
    let mut in_use0 = BTreeMap::new();
    for &(ref p, ref k, ref v) in keys.iter() {
      match upq.reserve_priority(*p, *k) {
        Err(()) => {}  // Already reserved this priority or key; skip
        Ok(()) => {
          in_use0.insert(*p, (*k, *v));
          assert_eq!(upq.find_key(k), Some(p));
        }
      }
    }
    assert_eq!(upq.pop_min_if_complete(), None);

    // Update values
    let mut in_use1 = BTreeMap::new();
    for (p, &(ref k, ref v)) in in_use0.iter() {
      in_use1.insert(*p, (*k, *v));
      upq.put_value(*k, *v);
      assert_eq!(upq.find_value_of_key(k), Some(*v));
    }
    drop(in_use0);
    assert_eq!(upq.pop_min_if_complete(), None);

    // Commit all
    for (p, _) in in_use1.iter() {
      upq.set_ready(*p);
    }

    // Verify that everything is now there, and all entries are complete
    for _ in 0..in_use1.len() {
      let next = upq.pop_min_if_complete();
      assert!(next.is_some());

      let (p, k, v) = next.unwrap();
      assert_eq!(in_use1.get(&p), Some(&(k, v)));
    }

    assert_eq!(upq.pop_min_if_complete(), None);
    return true;
  }
}
