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

use std::mem;
use std::ops;
use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};


pub struct SyncPool<V> {
    vals: Mutex<Vec<V>>,
    cond: Condvar,
}

pub struct SyncPoolGuard<'mutex, V: 'mutex> {
    m: &'mutex SyncPool<V>,
    v: Option<V>,
}

impl<'mutex, V> ops::Deref for SyncPoolGuard<'mutex, V> {
    type Target = V;
    fn deref(&self) -> &V {
        self.v.as_ref().unwrap()
    }
}

impl<'mutex, V> ops::DerefMut for SyncPoolGuard<'mutex, V> {
    fn deref_mut(&mut self) -> &mut V {
        self.v.as_mut().unwrap()
    }
}

impl<'mutex, V> ops::Drop for SyncPoolGuard<'mutex, V> {
    fn drop(&mut self) {
        let mut vals = self.m.vals.lock().unwrap();
        let v = mem::replace(&mut self.v, None);
        vals.push(v.unwrap());
        self.m.cond.notify_one();
    }
}

impl<V> SyncPool<V> {
    pub fn new(vals: Vec<V>) -> SyncPool<V> {
        SyncPool {
            vals: Mutex::new(vals),
            cond: Condvar::new(),
        }
    }

    pub fn lock(&self) -> Result<SyncPoolGuard<V>, PoisonError<MutexGuard<Vec<V>>>> {
        let v = {
            let mut vs = self.vals.lock()?;
            while vs.len() == 0 {
                vs = self.cond.wait(vs)?;
            }
            vs.pop().unwrap()
        };
        Ok(SyncPoolGuard {
               m: self,
               v: Some(v),
           })
    }
}
