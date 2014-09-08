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

//! Local state for known hashes and their external location (blob reference).

use serialize::hex::{ToHex};

use callback_container::{CallbackContainer};
use cumulative_counter::{CumulativeCounter};
use unique_priority_queue::{UniquePriorityQueue};
use process::{Process, MsgHandler};

use sqlite3::database::{Database};
use sqlite3::cursor::{Cursor};
use sqlite3::types::{SQLITE_DONE, SQLITE_OK, SQLITE_ROW, Integer64, Blob};
use sqlite3::{open};

use periodic_timer::{PeriodicTimer};

use sodiumoxide::crypto::hash::{sha512};


pub type HashIndexProcess<'db> = Process<Msg, Reply, HashIndex<'db>>;


/// A wrapper around Hash digests.
#[deriving(Clone, Show, Hash, Eq, TotalEq)]
pub struct Hash{
  pub bytes: ~[u8],
}

impl Hash {
  /// Computes `hash(text)` and stores this digest as the `bytes` field in a new `Hash` structure.
  pub fn new(text: &[u8]) -> Hash {
    let sha512::Digest(digest_bytes) = sha512::hash(text);
    Hash{bytes: digest_bytes.slice(0, sha512::HASHBYTES).into_owned()}
  }
}


/// An entry that can be inserted into the hash index.
#[deriving(Clone)]
pub struct HashEntry{

  /// The hash of this entry (unique among all entries in the index).
  pub hash: Hash,

  /// The level in a hash tree that this entry is from. Level `0` represents `leaf`s, i.e. entries
  /// that represent user-data, where levels `1` and up represents `branches` of the tree,
  /// i.e. internal meta-data.
  pub level: i64,

  /// A local payload to store inside the index, along with this entry.
  pub payload: Option<~[u8]>,

  /// A reference to a location in the external persistent storage (a blob reference) that contains
  /// the data for this entry (e.g. an object-name and a byte range).
  pub persistent_ref: Option<~[u8]>,
}

pub enum Msg {
  /// Check whether this `Hash` already exists in the system.
  /// Returns `HashKnown` or `HashNotKnown`.
  HashExists(Hash),

  /// Locate the local payload of the `Hash`. This is currently not used.
  /// Returns `Payload` or `HashNotKnown`.
  FetchPayload(Hash),

  /// Locate the persistent reference (external blob reference) for this `Hash`.
  /// Returns `PersistentRef` or `HashNotKnown`.
  FetchPersistentRef(Hash),

  /// Reserve a `Hash` in the index, while sending its content to external storage.
  /// This is used to ensure that each `Hash` is stored only once.
  /// Returns `ReserveOK` or `HashKnown`.
  Reserve(HashEntry),

  /// Update the info for a reserved `Hash`. The `Hash` remains reserved. This is used to update
  /// the persistent reference (external blob reference) as soon as it is available (to allow new
  /// references to the `Hash` to be created before it is committed).
  /// Returns ReserveOK.
  UpdateReserved(HashEntry),

  /// A `Hash` is committed when it has been `finalized` in the external storage. `Commit` includes
  /// the persistent reference that the content is available at.
  /// Returns CommitOK.
  Commit(Hash, ~[u8]),

  /// Install a "on-commit" handler to be called after `Hash` is committed.
  /// Returns `CallbackRegistered` or `HashNotKnown`.
  CallAfterHashIsComitted(Hash, proc():Send),

  /// Flush the hash index to clear internal buffers and commit the underlying database.
  Flush,
}

pub enum Reply {
  HashKnown,
  HashNotKnown,
  HashEntry(HashEntry),

  Payload(Option<~[u8]>),
  PersistentRef(~[u8]),

  ReserveOK,
  CommitOK,
  CallbackRegistered,

  Retry,
}


#[deriving(Clone)]
struct QueueEntry {
  id: i64,
  level: i64,
  payload: Option<~[u8]>,
  persistent_ref: Option<~[u8]>,
}

pub struct HashIndex<'db> {
  dbh: Database,

  id_counter: CumulativeCounter<i64>,

  queue: UniquePriorityQueue<i64, ~[u8], QueueEntry>,

  callbacks: CallbackContainer<~[u8]>,

  flush_timer: PeriodicTimer,

}

impl <'db> HashIndex<'db> {

  pub fn new(path: ~str) -> HashIndex<'db> {
    let mut hi = match open(path) {
      Ok(dbh) => {
        HashIndex{dbh: dbh,
                  id_counter: CumulativeCounter::new(0i64),
                  queue: UniquePriorityQueue::new(),
                  callbacks: CallbackContainer::new(),
                  flush_timer: PeriodicTimer::new(10 * 1000),
        }
      },
      Err(err) => fail!(err.to_str()),
    };
    hi.execOrDie("CREATE TABLE IF NOT EXISTS
                  hash_index (id        INTEGER PRIMARY KEY,
                              hash      BLOB,
                              height    INTEGER,
                              payload   BLOB,
                              blob_ref  BLOB)");

    hi.execOrDie("CREATE UNIQUE INDEX IF NOT EXISTS " +
                 "HashIndex_UniqueHash " +
                 "ON hash_index(hash)");

    hi.execOrDie("BEGIN");

    hi.refresh_id_counter();
    hi
  }

  #[cfg(test)]
  pub fn newForTesting() -> HashIndex {
    HashIndex::new(":memory:".to_owned())
  }

  fn execOrDie(&self, sql: &str) {
    match self.dbh.exec(sql) {
      Ok(true) => (),
      Ok(false) => fail!("exec: " + self.dbh.get_errmsg()),
      Err(msg) => fail!(format!("exec: {}, {}\nIn sql: '{}'\n",
                                msg.to_str(), self.dbh.get_errmsg(), sql))
    }
  }

  fn prepareOrDie<'a>(&'a self, sql: &str) -> Cursor<'a> {
    match self.dbh.prepare(sql, &None) {
      Ok(s)  => s,
      Err(x) => fail!(format!("sqlite error: {} ({:?})",
                              self.dbh.get_errmsg(), x)),
    }
  }

  fn select1<'a>(&'a self, sql: &str) -> Option<Cursor<'a>> {
    let cursor = self.prepareOrDie(sql);
    if cursor.step() == SQLITE_ROW { Some(cursor) } else { None }
  }

  fn index_locate(&self, hash: &Hash) -> Option<QueueEntry> {
    assert!(hash.bytes.len() > 0);

    let result_opt = self.select1(format!(
      "SELECT id, height, payload, blob_ref FROM hash_index WHERE hash=x'{}'",
      hash.bytes.to_hex()
    ));
    result_opt.map(|result| {
      let payload = result.get_blob(2);
      QueueEntry{id: result.get_int(0) as i64,
                 level: result.get_int(1) as i64,
                 payload: if payload.len() == 0 { None }
                          else {Some(payload.as_slice().into_owned()) },
                 persistent_ref: Some(result.get_blob(3).as_slice().into_owned())
      } })
  }

  fn locate(&self, hash: &Hash) -> Option<QueueEntry> {
    let result_opt = self.queue.find_value_of_key(&hash.bytes);
    result_opt.map(|x| x.clone()).or_else(|| self.index_locate(hash))
  }

  fn refresh_id_counter(&mut self) {
    let id = self.select1("SELECT MAX(id) FROM hash_index").unwrap().get_int(0);
    self.id_counter = CumulativeCounter::new(id as i64);
  }

  fn next_id(&mut self) -> i64 {
    self.id_counter.next()
  }

  fn reserve(&mut self, hash_entry: HashEntry) -> i64 {
    self.maybeFlush();

    let HashEntry{hash, level, payload, persistent_ref} = hash_entry;
    assert!(hash.bytes.len() > 0);

    let my_id = self.next_id();

    assert!(self.queue.reserve_priority(my_id, hash.bytes.clone()).is_ok());
    self.queue.put_value(hash.bytes,
                         QueueEntry{id: my_id,
                                    level: level,
                                    payload: payload,
                                    persistent_ref: persistent_ref
                         });
    my_id
  }

  fn update_reserved(&mut self, hash_entry: HashEntry) {
    let HashEntry{hash, level, payload, persistent_ref} = hash_entry;
    assert!(hash.bytes.len() > 0);
    let old_entry = self.locate(&hash).expect("hash was reserved");

    // If we didn't already commit and pop() the hash, update it:
    let id_opt = self.queue.find_key(&hash.bytes).map(|id| id.clone());
    if id_opt.is_some() {
      assert_eq!(id_opt, Some(old_entry.id));
      self.queue.update_value(&hash.bytes,
                              |qe| QueueEntry{level: level,
                                              payload: payload.clone(),
                                              persistent_ref: persistent_ref.clone(),
                                              ..qe.clone()});
    }
  }

  fn register_hash_callback(&mut self, hash: &Hash, callback: proc():Send) -> bool {
    assert!(hash.bytes.len() > 0);

    if self.queue.find_value_of_key(&hash.bytes).is_some() {
      self.callbacks.add(hash.bytes.clone(), callback);
    } else if self.locate(hash).is_some() {
      // Hash was already committed
      callback();
    } else {
      // We cannot register this callback, since the hash doesn't exist anywhere
      return false
    }

    return true;
  }

  fn insert_completed_in_order(&mut self) {
    let insert_stm = self.dbh.prepare(
      "INSERT INTO hash_index (id, hash, height, payload, blob_ref) VALUES (?, ?, ?, ?, ?)",
      &None).unwrap();
    loop {
      match self.queue.pop_min_if_complete() {
        None => break,
        Some((id, hash_bytes, queue_entry)) => {
          assert_eq!(id, queue_entry.id);

          let child_refs_opt = queue_entry.payload.clone();
          let payload = child_refs_opt.unwrap_or_else(|| bytes!().into_owned());
          let level = queue_entry.level;
          let persistent_ref = queue_entry.persistent_ref.expect("hash was comitted");

          assert_eq!(SQLITE_OK, insert_stm.bind_param(1, &Integer64(id)));
          assert_eq!(SQLITE_OK, insert_stm.bind_param(2, &Blob(Vec::from_slice(hash_bytes.clone()))));
          assert_eq!(SQLITE_OK, insert_stm.bind_param(3, &Integer64(level)));
          assert_eq!(SQLITE_OK, insert_stm.bind_param(4, &Blob(Vec::from_slice(payload))));
          assert_eq!(SQLITE_OK, insert_stm.bind_param(5, &Blob(Vec::from_slice(persistent_ref))));

          assert_eq!(insert_stm.step(), SQLITE_DONE);

          insert_stm.clear_bindings();

          self.callbacks.allow_flush_of(hash_bytes);
        },
      }
    }
  }

  fn commit(&mut self, hash: &Hash, blob_ref: &~[u8]) {
    // Update persistent reference for ready hash
    let queue_entry = self.locate(hash).expect("hash was committed");
    self.queue.update_value(&hash.bytes,
                            |old_qe| QueueEntry{persistent_ref: Some(blob_ref.clone()),
                                                ..old_qe.clone()});
    self.queue.set_ready(queue_entry.id);

    self.insert_completed_in_order();

    self.maybeFlush();
  }

  fn maybeFlush(&mut self) {
    if self.flush_timer.did_fire() {
      self.flush();
    }
  }

  fn flush(&mut self) {
    // Callbacks assume their data is safe, so commit before calling them
    self.execOrDie("COMMIT; BEGIN");

    // Run ready callbacks
    self.callbacks.flush();
  }
}

impl <'db> Drop for HashIndex<'db> {
  fn drop(&mut self) {
    self.flush();

    assert_eq!(self.callbacks.len(), 0);

    assert_eq!(self.queue.len(), 0);
    self.execOrDie("COMMIT");
  }
}


impl <'db> MsgHandler<Msg, Reply> for HashIndex<'db> {
  fn handle(&mut self, msg: Msg, reply: |Reply|) {
    match msg {

      HashExists(hash) => {
        assert!(hash.bytes.len() > 0);
        return reply(match self.locate(&hash) {
          Some(_) => HashKnown,
          None => HashNotKnown,
        });
      },

      FetchPayload(hash) => {
        assert!(hash.bytes.len() > 0);
        return reply(match self.locate(&hash) {
          Some(ref queue_entry) => Payload(queue_entry.payload.clone()),
          None => HashNotKnown,
        });
      },

      FetchPersistentRef(hash) => {
        assert!(hash.bytes.len() > 0);
        return reply(match self.locate(&hash) {
          Some(ref queue_entry) if queue_entry.persistent_ref.is_none() => Retry,
          Some(queue_entry) => PersistentRef(queue_entry.persistent_ref.unwrap()),
          None => HashNotKnown,
        });
      },

      Reserve(hash_entry) => {
        assert!(hash_entry.hash.bytes.len() > 0);
        // To avoid unused IO, we store entries in-memory until committed to persistent storage.
        // This allows us to continue after a crash without needing to scan through and delete
        // uncommitted entries.
        return reply(match self.locate(&hash_entry.hash) {
          Some(_) => HashKnown,
          None => { self.reserve(hash_entry); ReserveOK },
        });
      },

      UpdateReserved(hash_entry) => {
        assert!(hash_entry.hash.bytes.len() > 0);
        self.update_reserved(hash_entry);
        return reply(ReserveOK);
      }

      Commit(hash, persistent_ref) => {
        assert!(hash.bytes.len() > 0);
        self.commit(&hash, &persistent_ref);
        return reply(CommitOK);
      },

      CallAfterHashIsComitted(hash, callback) => {
        assert!(hash.bytes.len() > 0);
        if self.register_hash_callback(&hash, callback) {
          return reply(CallbackRegistered);
        } else {
          return reply(HashNotKnown);
        }
      },

      Flush => {
        self.flush();
        return reply(CommitOK);
      }
    }
  }
}
