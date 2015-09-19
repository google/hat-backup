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

use std::boxed::FnBox;
use std::sync::mpsc;
use time::Duration;
use rustc_serialize::hex::{ToHex};

use cumulative_counter::{CumulativeCounter};
use unique_priority_queue::{UniquePriorityQueue};
use process::{Process, MsgHandler};
use tags;

use sqlite3::database::{Database};
use sqlite3::cursor::{Cursor};
use sqlite3::types::ResultCode::{SQLITE_DONE, SQLITE_OK, SQLITE_ROW};
use sqlite3::BindArg::{Integer64, Blob};
use sqlite3::{open};

use periodic_timer::{PeriodicTimer};

use sodiumoxide::crypto::hash::{sha512};


pub static HASHBYTES:usize = sha512::HASHBYTES;

pub type HashIndexProcess = Process<Msg, Reply>;


/// A wrapper around Hash digests.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct Hash{
  pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcData{
  pub num: i64,
  pub bytes: Vec<u8>,
}

impl Hash {
  /// Computes `hash(text)` and stores this digest as the `bytes` field in a new `Hash` structure.
  pub fn new(text: &[u8]) -> Hash {
    let sha512::Digest(digest_bytes) = sha512::hash(text);
    Hash{bytes: digest_bytes[0 .. HASHBYTES].to_vec()}
  }
}


/// An entry that can be inserted into the hash index.
#[derive(Clone)]
pub struct HashEntry{

  /// The hash of this entry (unique among all entries in the index).
  pub hash: Hash,

  /// The level in a hash tree that this entry is from. Level `0` represents `leaf`s, i.e. entries
  /// that represent user-data, where levels `1` and up represents `branches` of the tree,
  /// i.e. internal meta-data.
  pub level: i64,

  /// A local payload to store inside the index, along with this entry.
  pub payload: Option<Vec<u8>>,

  /// A reference to a location in the external persistent storage (a blob reference) that contains
  /// the data for this entry (e.g. an object-name and a byte range).
  pub persistent_ref: Option<Vec<u8>>,
}

pub enum Msg {
  /// Check whether this `Hash` already exists in the system.
  /// Returns `HashKnown` or `HashNotKnown`.
  HashExists(Hash),

  /// Locate the local payload of the `Hash`.
  /// Returns `Payload` or `HashNotKnown`.
  FetchPayload(Hash),

  /// Locate the local ID of this hash.
  GetID(Hash),

  /// Locate hash entry from its ID.
  GetHash(i64),

  /// Locate the persistent reference (external blob reference) for this `Hash`.
  /// Returns `PersistentRef` or `HashNotKnown`.
  FetchPersistentRef(Hash),

  /// Reserve a `Hash` in the index, while sending its content to external storage.
  /// This is used to ensure that each `Hash` is stored only once.
  /// Returns `ReserveOk` or `HashKnown`.
  Reserve(HashEntry),

  /// Update the info for a reserved `Hash`. The `Hash` remains reserved. This is used to update
  /// the persistent reference (external blob reference) as soon as it is available (to allow new
  /// references to the `Hash` to be created before it is committed).
  /// Returns ReserveOk.
  UpdateReserved(HashEntry),

  /// A `Hash` is committed when it has been `finalized` in the external storage. `Commit` includes
  /// the persistent reference that the content is available at.
  /// Returns CommitOk.
  Commit(Hash, Vec<u8>),

  /// List all hash entries.
  List,

  /// APIs related to tagging, which is useful to indicate state during operation stages.
  /// These operate directly on the underlying IDs.
  SetTag(i64, tags::Tag),
  SetAllTags(tags::Tag),
  GetTag(i64),
  GetIDsByTag(i64),

  /// Permanently delete hash by its ID.
  Delete(i64),

  /// APIs related to garbage collector metadata tied to (hash id, family id) pairs.
  ReadGcData(i64, i64),
  UpdateGcData(i64, i64, Box<FnBox(GcData) -> Option<GcData> + Send>),
  UpdateFamilyGcData(i64, mpsc::Receiver<Box<FnBox(GcData) -> Option<GcData> + Send>>),

  /// Manual commit. This also disables automatic periodic commit.
  ManualCommit,

  /// Flush the hash index to clear internal buffers and commit the underlying database.
  Flush,
}

pub enum Reply {
  HashID(i64),
  HashKnown,
  HashNotKnown,
  Entry(HashEntry),

  Payload(Option<Vec<u8>>),
  PersistentRef(Vec<u8>),

  ReserveOk,
  CommitOk,
  CallbackRegistered,

  Listing(mpsc::Receiver<HashEntry>),

  HashTag(Option<tags::Tag>),
  HashIDs(Vec<i64>),

  CurrentGcData(GcData),

  Ok,
  Retry,
}


#[derive(Clone)]
struct QueueEntry {
  id: i64,
  level: i64,
  payload: Option<Vec<u8>>,
  persistent_ref: Option<Vec<u8>>,
}

pub struct HashIndex {
  dbh: Database,

  id_counter: CumulativeCounter,

  queue: UniquePriorityQueue<i64, Vec<u8>, QueueEntry>,

  flush_timer: PeriodicTimer,
  flush_periodically: bool,
}

impl HashIndex {

  pub fn new(path: String) -> HashIndex {
    let mut hi = match open(&path) {
      Ok(dbh) => {
        HashIndex{dbh: dbh,
                  id_counter: CumulativeCounter::new(0),
                  queue: UniquePriorityQueue::new(),
                  flush_timer: PeriodicTimer::new(Duration::seconds(10)),
                  flush_periodically: true,
        }
      },
      Err(err) => panic!("{:?}", err),
    };
    hi.exec_or_die("CREATE TABLE IF NOT EXISTS
                    hash_index (id        INTEGER PRIMARY KEY,
                                hash      BLOB,
                                tag       INTEGER,
                                height    INTEGER,
                                payload   BLOB,
                                blob_ref  BLOB)");

    hi.exec_or_die("CREATE UNIQUE INDEX IF NOT EXISTS
                    HashIndex_UniqueHash
                    ON hash_index(hash)");

    hi.exec_or_die("CREATE TABLE IF NOT EXISTS
                    gc_metadata (hash_id    INTEGER,
                                 family_id  INTEGER,
                                 gc_int     INTEGER,
                                 gc_vec     BLOB)");

    hi.exec_or_die("CREATE UNIQUE INDEX IF NOT EXISTS
                    gc_metadata_UniqueHashFamily
                    ON gc_metadata(hash_id, family_id)");

    hi.exec_or_die("BEGIN");

    hi.refresh_id_counter();
    hi
  }

  #[cfg(test)]
  pub fn new_for_testing() -> HashIndex {
    HashIndex::new(":memory:".to_string())
  }

  fn exec_or_die(&mut self, sql: &str) {
    match self.dbh.exec(sql) {
      Ok(true) => (),
      Ok(false) => panic!("exec: {}", self.dbh.get_errmsg()),
      Err(msg) => panic!("exec: {:?}, {:?}\nIn sql: '{}'\n",
                         msg, self.dbh.get_errmsg(), sql)
    }
  }

  fn prepare_or_die<'a>(&'a self, sql: &str) -> Cursor<'a> {
    match self.dbh.prepare(sql, &None) {
      Ok(s)  => s,
      Err(x) => panic!("sqlite error: {} ({:?})",
                       self.dbh.get_errmsg(), x),
    }
  }

  fn select1<'a>(&'a mut self, sql: &str) -> Option<Cursor<'a>> {
    let mut cursor = self.prepare_or_die(sql);
    if cursor.step() == SQLITE_ROW { Some(cursor) } else { None }
  }

  fn index_locate(&mut self, hash: &Hash) -> Option<QueueEntry> {
    assert!(hash.bytes.len() > 0);

    let result_opt = self.select1(&format!(
      "SELECT id, height, payload, blob_ref FROM hash_index WHERE hash=x'{}'",
      hash.bytes.to_hex()
    ));
    result_opt.map(|mut result| {
      let id = result.get_i64(0);
      let level = result.get_int(1) as i64;
      let payload: Vec<u8> = result.get_blob(2).unwrap_or(&[]).to_vec();
      let persistent_ref: Vec<u8> = result.get_blob(3).unwrap_or(&[]).to_vec();
      QueueEntry{id: id, level: level,
                 payload: if payload.len() == 0 { None }
                          else {Some(payload) },
                 persistent_ref: Some(persistent_ref)
      } })
  }

  fn locate(&mut self, hash: &Hash) -> Option<QueueEntry> {
    let result_opt = self.queue.find_value_of_key(&hash.bytes);
    result_opt.map(|x| x).or_else(|| self.index_locate(hash))
  }

  fn locate_by_id(&mut self, id: i64) -> Option<HashEntry> {
    let result_opt = self.select1(&format!(
        "SELECT hash, height, payload, blob_ref FROM hash_index WHERE id='{:?}'", id));
    return result_opt.map(|mut result|
               HashEntry{hash: Hash{bytes: result.get_blob(0).unwrap_or(&[]).to_vec()},
                         level: result.get_int(1) as i64,
                         payload: result.get_blob(2).and_then(|p| if p.len() == 0 { None } else { Some(p.to_vec()) }),
                         persistent_ref: result.get_blob(3).map(|p| p.to_vec())
        });
  }

  fn refresh_id_counter(&mut self) {
    let id = self.select1("SELECT MAX(id) FROM hash_index").expect("id").get_i64(0);
    self.id_counter = CumulativeCounter::new(id);
  }

  fn next_id(&mut self) -> i64 {
    self.id_counter.next()
  }

  fn reserve(&mut self, hash_entry: HashEntry) -> i64 {
    self.maybe_flush();

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

  fn insert_completed_in_order(&mut self) {
    let mut insert_stm = self.dbh.prepare(
      "INSERT INTO hash_index (id, hash, height, payload, blob_ref) VALUES (?, ?, ?, ?, ?)",
      &None).unwrap();

    loop {
      match self.queue.pop_min_if_complete() {
        None => break,
        Some((id, hash_bytes, queue_entry)) => {
          assert_eq!(id, queue_entry.id);

          let child_refs_opt = queue_entry.payload;
          let payload = child_refs_opt.unwrap_or_else(|| vec!());
          let level = queue_entry.level;
          let persistent_ref = queue_entry.persistent_ref.expect("hash was comitted");

          assert_eq!(SQLITE_OK, insert_stm.bind_param(1, &Integer64(id)));
          assert_eq!(SQLITE_OK, insert_stm.bind_param(2, &Blob(hash_bytes.clone())));
          assert_eq!(SQLITE_OK, insert_stm.bind_param(3, &Integer64(level)));
          assert_eq!(SQLITE_OK, insert_stm.bind_param(4, &Blob(payload)));
          assert_eq!(SQLITE_OK, insert_stm.bind_param(5, &Blob(persistent_ref)));

          assert_eq!(SQLITE_DONE, insert_stm.step());

          assert_eq!(SQLITE_OK, insert_stm.clear_bindings());
          assert_eq!(SQLITE_OK, insert_stm.reset());
        },
      }
    }
  }

  fn set_tag(&mut self, id_opt: Option<i64>, tag: tags::Tag) {
    let condition = id_opt.map(|id| format!("WHERE id={:?}", id)).unwrap_or("".to_string());
    self.exec_or_die(&format!("UPDATE hash_index SET tag={:?} {}",
                              tag as i64, condition)[..]);
  }

  fn get_tag(&mut self, id: i64) -> Option<tags::Tag> {
    let tag_opt = self.select1(&format!("SELECT tag FROM hash_index WHERE id={:?}", id)[..])
                      .as_mut().map(|row| row.get_i64(0));
    return tag_opt.and_then(|i| tags::tag_from_num(i));
  }

  fn list_ids_by_tag(&mut self, tag: i64) -> Vec<i64> {
    // We list hashes top-down.
    // This is required for safe deletion.
    // TODO(jos): consider moving this requirement closer to the code that needs it.
    let mut cursor = self.prepare_or_die(
      &format!("SELECT id FROM hash_index WHERE tag={:?} ORDER BY height DESC", tag)[..]);

    let mut out = vec![];
    while cursor.step() == SQLITE_ROW {
      out.push(cursor.get_i64(0));
    }
    return out;
  }

  fn read_gc_data(&mut self, hash_id: i64, family_id: i64) -> GcData {
    match self.select1(&format!("SELECT gc_int, gc_vec FROM gc_metadata
                                 WHERE hash_id={:?} AND family_id={:?}", hash_id, family_id)[..])
              .as_mut() {
      None => GcData{num: 0, bytes: vec![]},
      Some(row) => GcData{num: row.get_i64(0), bytes: row.get_blob(1).unwrap_or(&[]).to_vec()},
    }
  }

  fn set_gc_data(&mut self, hash_id: i64, family_id: i64, data: GcData) {
    let mut insert_stm = self.prepare_or_die("INSERT OR REPLACE INTO gc_metadata
                                              (hash_id, family_id, gc_int, gc_vec)
                                              VALUES (?, ?, ?, ?)");
    assert_eq!(SQLITE_OK, insert_stm.bind_param(1, &Integer64(hash_id)));
    assert_eq!(SQLITE_OK, insert_stm.bind_param(2, &Integer64(family_id)));
    assert_eq!(SQLITE_OK, insert_stm.bind_param(3, &Integer64(data.num)));
    assert_eq!(SQLITE_OK, insert_stm.bind_param(4, &Blob(data.bytes)));
    assert_eq!(SQLITE_DONE, insert_stm.step());
  }

  fn update_gc_data(&mut self, hash_id: i64, family_id: i64,
                    f: Box<FnBox(GcData) -> Option<GcData> + Send>) -> GcData
  {
    let data = self.read_gc_data(hash_id, family_id);
    match f(data.clone()) {
      None => {
        self.delete_gc_data(hash_id, family_id);
        data
      },
      Some(new) => {
        self.set_gc_data(hash_id, family_id, new.clone());
        new
      },
    }
  }

  fn update_family_gc_data(&mut self, family_id: i64,
                           fs: mpsc::Receiver<Box<FnBox(GcData) -> Option<GcData> + Send>>)
  {
    let mut hash_ids = Vec::new();

    {
      let mut select_stm = self.prepare_or_die(
        "SELECT hash_id FROM gc_metadata WHERE family_id=?");

      assert_eq!(SQLITE_OK, select_stm.bind_param(1, &Integer64(family_id)));

      while SQLITE_ROW == select_stm.step() {
        hash_ids.push(select_stm.get_i64(0));
      }
    }

    for hash_id in hash_ids {
      let f = fs.recv().unwrap();
      self.update_gc_data(hash_id, family_id, f);
    }
  }

  fn delete_gc_data(&mut self, hash_id: i64, family_id: i64) {
    self.exec_or_die(&format!("DELETE FROM gc_metadata WHERE hash_id={:?} AND family_id={:?}",
                              hash_id, family_id)[..]);
  }

  fn commit(&mut self, hash: &Hash, blob_ref: &Vec<u8>) {
    // Update persistent reference for ready hash
    let queue_entry = self.locate(hash).expect("hash was committed");
    self.queue.update_value(&hash.bytes,
                            |old_qe| QueueEntry{persistent_ref: Some(blob_ref.clone()),
                                                ..old_qe.clone()});
    self.queue.set_ready(queue_entry.id);

    self.insert_completed_in_order();

    self.maybe_flush();
  }

  fn list(&mut self) -> mpsc::Receiver<HashEntry> {
    let (sender, receiver) = mpsc::channel();
    let mut cursor = self.prepare_or_die("SELECT hash, height, payload, blob_ref FROM hash_index");
    loop {
      let status = cursor.step();
      if status == SQLITE_DONE {
        break;
      }
      assert_eq!(status, SQLITE_ROW);
      if let Err(_) = sender.send(
        HashEntry{hash: Hash{bytes: cursor.get_blob(0).unwrap_or(&[]).to_vec()},
                  level: cursor.get_int(1) as i64,
                  payload: cursor.get_blob(2).and_then(|p| if p.len() == 0 { None } else { Some(p.to_vec()) }),
                  persistent_ref: cursor.get_blob(3).map(|p| p.to_vec())
      }) {
          break;
      }
    }
    return receiver;
  }

  fn delete(&mut self, id: i64) {
    let mut cursor = self.prepare_or_die(&format!("DELETE FROM hash_index WHERE id={:?} LIMIT 1", id));
    assert_eq!(cursor.step(), SQLITE_DONE);
    let mut cursor2 = self.prepare_or_die(&format!("DELETE FROM gc_metadata WHERE hash_id={:?}", id));
    assert_eq!(cursor2.step(), SQLITE_DONE);
   }

  fn maybe_flush(&mut self) {
    if self.flush_periodically && self.flush_timer.did_fire() {
      self.flush();
    }
  }

  fn flush(&mut self) {
    // Callbacks assume their data is safe, so commit before calling them
    self.exec_or_die("COMMIT; BEGIN");
  }
}

// #[unsafe_desctructor]
// impl  Drop for HashIndex {
//   fn drop(&mut self) {
//     self.flush();
//     assert_eq!(self.queue.len(), 0);
//     self.exec_or_die("COMMIT");
//   }
// }


impl MsgHandler<Msg, Reply> for HashIndex {
  fn handle(&mut self, msg: Msg, reply: Box<Fn(Reply)>) {
    match msg {

      Msg::GetID(hash) => {
        assert!(hash.bytes.len() > 0);
        return reply(match self.locate(&hash) {
          Some(entry) => Reply::HashID(entry.id),
          None => Reply::HashNotKnown,
        });
      },

      Msg::GetHash(id) => {
        return reply(match self.locate_by_id(id) {
         Some(hash) => Reply::Entry(hash),
         None => Reply::HashNotKnown,
        });
      },

      Msg::HashExists(hash) => {
        assert!(hash.bytes.len() > 0);
        return reply(match self.locate(&hash) {
          Some(_) => Reply::HashKnown,
          None => Reply::HashNotKnown,
        });
      },

      Msg::FetchPayload(hash) => {
        assert!(hash.bytes.len() > 0);
        return reply(match self.locate(&hash) {
          Some(ref queue_entry) => Reply::Payload(queue_entry.payload.clone()),
          None => Reply::HashNotKnown,
        });
      },

      Msg::FetchPersistentRef(hash) => {
        assert!(hash.bytes.len() > 0);
        return reply(match self.locate(&hash) {
          Some(ref queue_entry) if queue_entry.persistent_ref.is_none() => Reply::Retry,
          Some(queue_entry) =>
            Reply::PersistentRef(queue_entry.persistent_ref.expect("persistent_ref")),
          None => Reply::HashNotKnown,
        });
      },

      Msg::Reserve(hash_entry) => {
        assert!(hash_entry.hash.bytes.len() > 0);
        // To avoid unused IO, we store entries in-memory until committed to persistent storage.
        // This allows us to continue after a crash without needing to scan through and delete
        // uncommitted entries.
        return reply(match self.locate(&hash_entry.hash) {
          Some(_) => Reply::HashKnown,
          None => { self.reserve(hash_entry); Reply::ReserveOk },
        });
      },

      Msg::UpdateReserved(hash_entry) => {
        assert!(hash_entry.hash.bytes.len() > 0);
        self.update_reserved(hash_entry);
        return reply(Reply::ReserveOk);
      }

      Msg::Commit(hash, persistent_ref) => {
        assert!(hash.bytes.len() > 0);
        self.commit(&hash, &persistent_ref);
        return reply(Reply::CommitOk);
      },

      Msg::List => {
        return reply(Reply::Listing(self.list()));
      },

      Msg::Delete(id) => {
        self.delete(id);
        return reply(Reply::Ok);
      },

      Msg::SetTag(id, tag) => {
        self.set_tag(Some(id), tag);
        return reply(Reply::Ok);
      },

      Msg::SetAllTags(tag) => {
        self.set_tag(None, tag);
        return reply(Reply::Ok);
      },

      Msg::GetTag(id) => {
        return reply(Reply::HashTag(self.get_tag(id)));
      }

      Msg::GetIDsByTag(tag) => {
        return reply(Reply::HashIDs(self.list_ids_by_tag(tag)));
      },

      Msg::ReadGcData(hash_id, family_id) => {
        reply(Reply::CurrentGcData(self.read_gc_data(hash_id, family_id)));
      },

      Msg::UpdateGcData(hash_id, family_id, update_fn) => {
        reply(Reply::CurrentGcData(self.update_gc_data(hash_id, family_id, update_fn)));
      },

      Msg::UpdateFamilyGcData(family_id, update_fs) => {
        self.update_family_gc_data(family_id, update_fs);
        return reply(Reply::Ok);
      },

      Msg::ManualCommit => {
        self.flush();
        self.flush_periodically = false;
        return reply(Reply::CommitOk);
      },

      Msg::Flush => {
        self.flush();
        return reply(Reply::CommitOk);
      }
    }
  }
}
