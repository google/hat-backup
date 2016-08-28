use std::mem;
use std::ops;
use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};


pub struct MutexSet<V> {
    vals: Mutex<Vec<V>>,
    cond: Condvar,
}

pub struct MutexSetGuard<'mutex, V: 'mutex> {
    m: &'mutex MutexSet<V>,
    v: Option<V>,
}

impl<'mutex, V> ops::Deref for MutexSetGuard<'mutex, V> {
    type Target = V;
    fn deref(&self) -> &V {
        self.v.as_ref().unwrap()
    }
}

impl<'mutex, V> ops::DerefMut for MutexSetGuard<'mutex, V> {
    fn deref_mut(&mut self) -> &mut V {
        self.v.as_mut().unwrap()
    }
}

impl<'mutex, V> ops::Drop for MutexSetGuard<'mutex, V> {
    fn drop(&mut self) {
        let mut vals = self.m.vals.lock().unwrap();
        let v = mem::replace(&mut self.v, None);
        vals.push(v.unwrap());
        self.m.cond.notify_one();
    }
}

impl<V> MutexSet<V> {
    pub fn new(vals: Vec<V>) -> MutexSet<V> {
        MutexSet {
            vals: Mutex::new(vals),
            cond: Condvar::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.vals.lock().unwrap().len()
    }

    pub fn with<F, R>(&self, f: F) -> R
        where F: FnOnce(&mut V) -> R
    {
        let mut v = self.lock().unwrap();
        let r = f(&mut *v);
        r
    }

    pub fn lock(&self) -> Result<MutexSetGuard<V>, PoisonError<MutexGuard<Vec<V>>>> {
        let v = {
            let mut vs = try!(self.vals.lock());
            while vs.len() == 0 {
                vs = try!(self.cond.wait(vs));
            }
            vs.pop().unwrap()
        };
        Ok(MutexSetGuard {
            m: &self,
            v: Some(v),
        })
    }
}
