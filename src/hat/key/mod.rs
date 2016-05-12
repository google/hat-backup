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

//! External API for creating and manipulating snapshots.

use std::borrow::Cow;
use std::sync::mpsc;

use blob;
use hash;
use hash::tree::{HashTreeBackend, SimpleHashTreeWriter, SimpleHashTreeReader, ReaderResult};

use process::{Process, MsgHandler};
use util::FnBox;


mod schema;
mod index;


pub use self::index::{Index, IndexError, IndexProcess, Entry};


error_type! {
    #[derive(Debug)]
    pub enum MsgError {
        Recv(mpsc::RecvError) {
            cause;
        },
        Message(Cow<'static, str>) {
            desc (e) &**e;
            from (s: &'static str) s.into();
            from (s: String) s.into();
        },
        Index(IndexError) {
            cause;
        },
        Hashes(hash::MsgError) {
            cause;
        },
        Blobs(blob::MsgError) {
            cause;
        },
     }
}


pub type StoreProcess<IT> = Process<Msg<IT>, Result<Reply, MsgError>>;

pub type DirElem = (Entry, Option<blob::ChunkRef>, Option<HashTreeReaderInitializer>);

pub struct HashTreeReaderInitializer {
    hash: hash::Hash,
    persistent_ref: Option<blob::ChunkRef>,
    hash_index: hash::HashIndex,
    blob_store: blob::StoreProcess,
}

impl HashTreeReaderInitializer {
    pub fn init(self) -> Option<ReaderResult<HashStoreBackend>> {
        let backend = HashStoreBackend::new(self.hash_index, self.blob_store);
        SimpleHashTreeReader::open(backend, &self.hash, self.persistent_ref)
    }
}

// Public structs
pub enum Msg<IT> {
    /// Insert a key into the index. If this key has associated data a "chunk-iterator creator"
    /// can be passed along with it. If the data turns out to be unreadable, this iterator proc
    /// can return `None`. Returns `Id` with the new entry ID.
    Insert(Entry, Option<Box<FnBox<(), Option<IT>>>>),

    /// List a "directory" (aka. a `level`) in the index.
    /// Returns `ListResult` with all the entries under the given parent.
    ListDir(Option<u64>),

    /// Flush this key store and its dependencies.
    /// Returns `FlushOk`.
    Flush,
}

pub enum Reply {
    Id(u64),
    ListResult(Vec<DirElem>),
    FlushOk,
}

#[derive(Clone)]
pub struct Store {
    index: index::IndexProcess,
    hash_index: hash::HashIndex,
    blob_store: blob::StoreProcess,
}

// Implementations
impl Store {
    pub fn new(index: index::IndexProcess,
               hash_index: hash::HashIndex,
               blob_store: blob::StoreProcess)
               -> Result<Store, MsgError> {
        Ok(Store {
            index: index,
            hash_index: hash_index,
            blob_store: blob_store,
        })
    }

    #[cfg(test)]
    pub fn new_for_testing<B: 'static + blob::StoreBackend + Send + Clone>
        (backend: B)
         -> Result<Store, MsgError> {
        let ki_p = index::IndexProcess::new(try!(index::Index::new_for_testing()));
        let hi_p = try!(hash::HashIndex::new_for_testing(None));
        let bs_p = try!(Process::new(move || blob::Store::new_for_testing(backend, 1024)));
        Ok(Store {
            index: ki_p,
            hash_index: hi_p,
            blob_store: bs_p,
        })
    }

    pub fn flush(&mut self) -> Result<(), MsgError> {
        self.blob_store.send_reply(blob::Msg::Flush);
        self.hash_index.flush();
        try!(self.index.flush());

        Ok(())
    }

    pub fn hash_tree_writer(&mut self) -> SimpleHashTreeWriter<HashStoreBackend> {
        let backend = HashStoreBackend::new(self.hash_index.clone(), self.blob_store.clone());
        SimpleHashTreeWriter::new(8, backend)
    }
}

#[derive(Clone)]
pub struct HashStoreBackend {
    hash_index: hash::HashIndex,
    blob_store: blob::StoreProcess,
}

impl HashStoreBackend {
    pub fn new(hash_index: hash::HashIndex, blob_store: blob::StoreProcess) -> HashStoreBackend {
        HashStoreBackend {
            hash_index: hash_index,
            blob_store: blob_store,
        }
    }

    fn fetch_chunk_from_hash(&mut self, hash: hash::Hash) -> Option<Vec<u8>> {
        assert!(!hash.bytes.is_empty());
        match self.hash_index.fetch_persistent_ref(&hash) {
            Ok(Some(chunk_ref)) => self.fetch_chunk_from_persistent_ref(chunk_ref),
            _ => None,  // TODO: Do we need to distinguish `missing` from `unknown ref`?
        }
    }

    fn fetch_chunk_from_persistent_ref(&mut self, chunk_ref: blob::ChunkRef) -> Option<Vec<u8>> {
        match self.blob_store.send_reply(blob::Msg::Retrieve(chunk_ref)) {
            blob::Reply::RetrieveOk(chunk) => Some(chunk),
            _ => None,
        }
    }
}

impl HashTreeBackend for HashStoreBackend {
    fn fetch_chunk(&mut self,
                   hash: &hash::Hash,
                   persistent_ref: Option<blob::ChunkRef>)
                   -> Option<Vec<u8>> {
        assert!(!hash.bytes.is_empty());

        let data_opt = if let Some(r) = persistent_ref {
            self.fetch_chunk_from_persistent_ref(r)
        } else {
            self.fetch_chunk_from_hash(hash.clone())
        };

        data_opt.and_then(|data| {
            let actual_hash = hash::Hash::new(&data[..]);
            if *hash == actual_hash {
                Some(data)
            } else {
                error!("Data hash does not match expectation: {:?} instead of {:?}",
                       actual_hash,
                       hash);
                None
            }
        })
    }

    fn fetch_persistent_ref(&mut self, hash: &hash::Hash) -> Option<blob::ChunkRef> {
        assert!(!hash.bytes.is_empty());
        loop {
            match self.hash_index.fetch_persistent_ref(hash) {
                Ok(Some(r)) => return Some(r), // done
                Ok(None) => return None, // done
                Err(hash::RetryError) => (),  // continue loop
            }
        }
    }

    fn fetch_payload(&mut self, hash: &hash::Hash) -> Option<Vec<u8>> {
        match self.hash_index.fetch_payload(hash) {
            Some(p) => p, // done
            None => None, // done
        }
    }

    fn insert_chunk(&mut self,
                    hash: hash::Hash,
                    level: i64,
                    payload: Option<Vec<u8>>,
                    chunk: Vec<u8>)
                    -> blob::ChunkRef {
        assert!(!hash.bytes.is_empty());

        let mut hash_entry = hash::Entry {
            hash: hash.clone(),
            level: level,
            payload: payload,
            persistent_ref: None,
        };

        match self.hash_index.reserve(hash_entry.clone()) {
            hash::ReserveResult::HashKnown => {
                // Someone came before us: piggyback on their result.
                return self.fetch_persistent_ref(&hash)
                           .expect("Could not find persistent_ref for known chunk.");
            }
            hash::ReserveResult::ReserveOk => {
                // We came first: this data-chunk is ours to process.
                let local_hash_index = self.hash_index.clone();

                let callback = Box::new(move |chunk_ref: blob::ChunkRef| {
                    local_hash_index.commit(&hash, chunk_ref);
                });
                let kind = if level == 0 {
                    blob::Kind::TreeLeaf
                } else {
                    blob::Kind::TreeBranch
                };
                match self.blob_store.send_reply(blob::Msg::Store(chunk, kind, callback)) {
                    blob::Reply::StoreOk(chunk_ref) => {
                        hash_entry.persistent_ref = Some(chunk_ref.clone());
                        self.hash_index.update_reserved(hash_entry);
                        return chunk_ref;
                    }
                    _ => panic!("Unexpected reply from BlobStore."),
                };
            }
        };
    }
}

fn file_size_warning(name: &[u8], wanted: u64, got: u64) {
    if wanted < got {
        println!("Warning: File grew while reading it: {:?} (wanted {}, got {})",
                 name,
                 wanted,
                 got)
    } else if wanted > got {
        println!("Warning: Could not read whole file (or it shrank): {:?} (wanted {}, got {})",
                 name,
                 wanted,
                 got)
    }
}

impl<IT: Iterator<Item = Vec<u8>>> MsgHandler<Msg<IT>, Result<Reply, MsgError>> for Store {
    type Err = MsgError;

    fn handle(&mut self,
              msg: Msg<IT>,
              reply: Box<Fn(Result<Reply, MsgError>)>)
              -> Result<(), MsgError> {
        let reply_ok = |x| {
            reply(Ok(x));
            Ok(())
        };
        let reply_err = |e| {
            reply(Err(e));
            Ok(())
        };

        match msg {
            Msg::Flush => {
                try!(self.flush());
                reply_ok(Reply::FlushOk)
            }

            Msg::ListDir(parent) => {
                match self.index.list_dir(parent) {
                    Ok(index::Reply::ListResult(entries)) => {
                        let mut my_entries: Vec<DirElem> = Vec::with_capacity(entries.len());
                        for (entry, persistent_ref) in entries.into_iter() {
                            let open_fn = entry.data_hash.as_ref().map(|bytes| {
                                HashTreeReaderInitializer {
                                    hash: hash::Hash { bytes: bytes.clone() },
                                    persistent_ref: persistent_ref.clone(),
                                    hash_index: self.hash_index.clone(),
                                    blob_store: self.blob_store.clone(),
                                }
                            });

                            my_entries.push((entry, persistent_ref, open_fn));
                        }
                        reply_ok(Reply::ListResult(my_entries))
                    }
                    Err(e) => reply_err(From::from(e)),
                    _ => reply_err(From::from("Unexpected result from key index")),
                }
            }

            Msg::Insert(org_entry, chunk_it_opt) => {
                let entry = match try!(self.index.lookup(org_entry.parent_id,
                                                         org_entry.name.clone())) {
                    index::Reply::Entry(ref entry) if org_entry.accessed == entry.accessed &&
                                                      org_entry.modified == entry.modified &&
                                                      org_entry.created == entry.created => {
                        if chunk_it_opt.is_some() && entry.data_hash.is_some() {
                            let hash = hash::Hash { bytes: entry.data_hash.clone().unwrap() };
                            if self.hash_index.hash_exists(&hash) {
                                // Short-circuit: We have the data.
                                return reply_ok(Reply::Id(entry.id.unwrap()));
                            }
                        } else if chunk_it_opt.is_none() && entry.data_hash.is_none() {
                            // Short-circuit: No data needed.
                            return reply_ok(Reply::Id(entry.id.unwrap()));
                        }
                        // Our stored entry is incomplete.
                        Entry { id: entry.id, ..org_entry }
                    }
                    index::Reply::Entry(entry) => Entry { id: entry.id, ..org_entry },
                    index::Reply::NotFound => org_entry,
                    _ => return reply_err(From::from("Unexpected reply from key index")),
                };

                let entry = match try!(self.index.insert(entry)) {
                    index::Reply::Entry(entry) => entry,
                    _ => return reply_err(From::from("Could not insert entry into key index")),
                };

                // Send out the ID early to allow the client to continue its key discovery routine.
                // The bounded input-channel will prevent the client from overflowing us.
                assert!(entry.id.is_some());
                reply(Ok(Reply::Id(entry.id.unwrap())));


                // Setup hash tree structure
                let mut tree = self.hash_tree_writer();

                // Check if we have an data source:
                let it_opt = chunk_it_opt.and_then(|open| open.call(()));
                if it_opt.is_none() {
                    // No data is associated with this entry.
                    try!(self.index.update_data_hash(entry, None, None));
                    // Bail out before storing data that does not exist:
                    return Ok(());
                }

                // Read and insert all file chunks:
                // (see HashStoreBackend::insert_chunk above)
                let mut bytes_read = 0u64;
                for chunk in it_opt.unwrap() {
                    bytes_read += chunk.len() as u64;
                    tree.append(chunk);
                }

                // Warn the user if we did not read the expected size:
                entry.data_length.map(|s| {
                    file_size_warning(&entry.name, s, bytes_read);
                });

                // Get top tree hash:
                let (hash, persistent_ref) = tree.hash();

                // Update hash in key index.
                // It is OK that this has is not yet valid, as we check hashes at snapshot time.
                match try!(self.index.update_data_hash(entry,
                                                       Some(hash),
                                                       Some(persistent_ref))) {
                    index::Reply::UpdateOk => (),
                    _ => {
                        // We lost the hash index before we completed the data transfer.
                        // Returning an error here will terminate our process.
                        return Err(From::from("Unexpected reply from key index"));
                    }
                };

                Ok(())
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    use blob::tests::MemoryBackend;
    use process::Process;

    use rand::Rng;
    use rand::thread_rng;

    use quickcheck;

    fn random_ascii_bytes() -> Vec<u8> {
        let ascii: String = thread_rng().gen_ascii_chars().take(32).collect();
        ascii.into_bytes()
    }

    #[derive(Clone, Debug)]
    pub struct EntryStub {
        pub key_entry: Entry,
        pub data: Option<Vec<Vec<u8>>>,
    }

    impl Iterator for EntryStub {
        type Item = Vec<u8>;

        fn next(&mut self) -> Option<Vec<u8>> {
            match self.data.as_mut() {
                Some(x) => {
                    if !x.is_empty() {
                        Some(x.remove(0))
                    } else {
                        None
                    }
                }
                None => None,
            }
        }
    }

    #[derive(Clone, Debug)]
    struct FileSystem {
        file: EntryStub,
        filelist: Vec<FileSystem>,
    }

    fn rng_filesystem(size: usize) -> FileSystem {
        fn create_files(size: usize) -> Vec<FileSystem> {
            let children = size as f32 / 10.0;

            // dist_factor * i for i in range(children) + children == size
            let dist_factor: f32 = (size as f32 - children) / ((children * (children - 1.0)) / 2.0);

            let mut child_size = 0.0 as f32;

            let mut files = Vec::new();
            for _ in 0..children as usize {

                let data_opt = if thread_rng().gen() {
                    None
                } else {
                    let mut v = vec![];
                    for _ in 0..8 {
                        v.push(random_ascii_bytes())
                    }
                    Some(v)
                };

                let new_root = EntryStub {
                    data: data_opt,
                    key_entry: Entry {
                        id: None,
                        parent_id: None, // updated by insert_and_update_fs()

                        name: random_ascii_bytes(),
                        data_hash: None,
                        data_length: None,

                        created: thread_rng().gen(),
                        modified: thread_rng().gen(),
                        accessed: thread_rng().gen(),

                        permissions: None,
                        user_id: None,
                        group_id: None,
                    },
                };

                files.push(FileSystem {
                    file: new_root,
                    filelist: create_files(child_size as usize),
                });
                child_size += dist_factor;
            }
            files
        }

        let root = EntryStub {
            data: None,
            key_entry: Entry {
                parent_id: None,
                id: None, // updated by insert_and_update_fs()
                name: b"root".to_vec(),
                data_hash: None,
                data_length: None,
                created: thread_rng().gen(),
                modified: thread_rng().gen(),
                accessed: thread_rng().gen(),
                permissions: None,
                user_id: None,
                group_id: None,
            },
        };

        FileSystem {
            file: root,
            filelist: create_files(size),
        }
    }

    fn insert_and_update_fs(fs: &mut FileSystem, ks_p: StoreProcess<EntryStub>) {
        let local_file = fs.file.clone();
        let id = match ks_p.send_reply(Msg::Insert(fs.file.key_entry.clone(),
                                                   if fs.file.data.is_some() {
                                                       Some(Box::new(move |()| Some(local_file)))
                                                   } else {
                                                       None
                                                   }))
                           .unwrap() {
            Reply::Id(id) => id,
            _ => panic!("unexpected reply from key store"),
        };

        fs.file.key_entry.id = Some(id);

        for f in fs.filelist.iter_mut() {
            f.file.key_entry.parent_id = Some(id);
            insert_and_update_fs(f, ks_p.clone());
        }
    }

    fn verify_filesystem(fs: &FileSystem, ks_p: StoreProcess<EntryStub>) -> usize {
        let listing = match ks_p.send_reply(Msg::ListDir(fs.file.key_entry.id)).unwrap() {
            Reply::ListResult(ls) => ls,
            _ => panic!("Unexpected result from key store."),
        };

        assert_eq!(fs.filelist.len(), listing.len());

        for (entry, persistent_ref, tree_data) in listing {
            let mut found = false;

            for dir in fs.filelist.iter() {
                if dir.file.key_entry.name == entry.name {
                    found = true;

                    assert_eq!(dir.file.key_entry.id, entry.id);
                    assert_eq!(dir.file.key_entry.created, entry.created);
                    assert_eq!(dir.file.key_entry.accessed, entry.accessed);
                    assert_eq!(dir.file.key_entry.modified, entry.modified);

                    match dir.file.data {
                        Some(ref original) => {
                            let it = match tree_data.expect("has data").init() {
                                None => panic!("No data."),
                                Some(it) => it,
                            };
                            let mut chunk_count = 0;
                            for (i, chunk) in it.enumerate() {
                                assert_eq!(original.get(i), Some(&chunk));
                                chunk_count += 1;
                            }
                            assert_eq!(original.len(), chunk_count);
                        }
                        None => {
                            assert_eq!(entry.data_hash, None);
                            assert_eq!(persistent_ref, None);
                        }
                    }

                    break;  // Proceed to check next file
                }
            }
            assert_eq!(true, found);
        }

        let mut count = fs.filelist.len();
        for dir in fs.filelist.iter() {
            count += verify_filesystem(dir, ks_p.clone());
        }

        count
    }

    #[test]
    fn identity() {
        fn prop(size: u8) -> bool {
            let backend = MemoryBackend::new();
            let ks_p = Process::new(move || Store::new_for_testing(backend)).unwrap();

            let mut fs = rng_filesystem(size as usize);
            insert_and_update_fs(&mut fs, ks_p.clone());
            let fs = fs;

            match ks_p.send_reply(Msg::Flush).unwrap() {
                Reply::FlushOk => (),
                _ => panic!("Unexpected result from key store."),
            }

            verify_filesystem(&fs, ks_p.clone());
            true
        }
        quickcheck::quickcheck(prop as fn(u8) -> bool);
    }
}

#[cfg(all(test, feature = "benchmarks"))]
mod bench {
    use super::*;
    use super::tests::*;

    use blob::tests::DevNullBackend;
    use process::Process;
    use test::Bencher;

    #[bench]
    fn insert_1_key_x_128000_zeros(bench: &mut Bencher) {
        let backend = DevNullBackend;
        let ks_p: StoreProcess<EntryStub> = Process::new(move || Store::new_for_testing(backend))
                                                .unwrap();

        let bytes = vec![0u8; 128*1024];

        let mut i = 0i32;
        bench.iter(|| {
            i += 1;

            let entry = EntryStub {
                data: Some(vec![bytes.clone()]),
                key_entry: Entry {
                    parent_id: None,
                    id: None,
                    name: format!("{}", i).as_bytes().to_vec(),
                    created: None,
                    modified: None,
                    accessed: None,
                    group_id: None,
                    user_id: None,
                    permissions: None,
                    data_hash: None,
                    data_length: None,
                },
            };

            ks_p.send_reply(Msg::Insert(entry.key_entry.clone(),
                                        Some(Box::new(move |()| Some(entry)))))
                .unwrap();
        });

        bench.bytes = 128 * 1024;

    }

    #[bench]
    fn insert_1_key_x_128000_unique(bench: &mut Bencher) {
        let backend = DevNullBackend;
        let ks_p: StoreProcess<EntryStub> = Process::new(move || Store::new_for_testing(backend))
                                                .unwrap();

        let bytes = vec![0u8; 128*1024];

        let mut i = 0i32;
        bench.iter(|| {
            i += 1;

            let mut my_bytes = bytes.clone();
            my_bytes[0] = i as u8;
            my_bytes[1] = (i / 256) as u8;
            my_bytes[2] = (i / 65536) as u8;

            let entry = EntryStub {
                data: Some(vec![my_bytes]),
                key_entry: Entry {
                    parent_id: None,
                    id: None,
                    name: format!("{}", i).as_bytes().to_vec(),
                    created: None,
                    modified: None,
                    accessed: None,
                    group_id: None,
                    user_id: None,
                    permissions: None,
                    data_hash: None,
                    data_length: None,
                },
            };

            ks_p.send_reply(Msg::Insert(entry.key_entry.clone(),
                                        Some(Box::new(move |()| Some(entry)))))
                .unwrap();
        });

        bench.bytes = 128 * 1024;
    }

    #[bench]
    fn insert_1_key_x_16_x_128000_zeros(bench: &mut Bencher) {
        let backend = DevNullBackend;
        let ks_p: StoreProcess<EntryStub> = Process::new(move || Store::new_for_testing(backend))
                                                .unwrap();

        bench.iter(|| {
            let bytes = vec![0u8; 128*1024];

            let entry = EntryStub {
                data: Some(vec![bytes; 16]),
                key_entry: Entry {
                    parent_id: None,
                    id: None,
                    name: vec![1u8, 2, 3].to_vec(),
                    created: None,
                    modified: None,
                    accessed: None,
                    group_id: None,
                    user_id: None,
                    permissions: None,
                    data_hash: None,
                    data_length: None,
                },
            };
            ks_p.send_reply(Msg::Insert(entry.key_entry.clone(),
                                        Some(Box::new(move |()| Some(entry)))))
                .unwrap();

            match ks_p.send_reply(Msg::Flush).unwrap() {
                Reply::FlushOk => (),
                _ => panic!("Unexpected result from key store."),
            }
        });

        bench.bytes = 16 * (128 * 1024);
    }

    #[bench]
    fn insert_1_key_x_16_x_128000_unique(bench: &mut Bencher) {
        let backend = DevNullBackend;
        let ks_p: StoreProcess<EntryStub> = Process::new(move || Store::new_for_testing(backend))
                                                .unwrap();

        let bytes = vec![0u8; 128*1024];
        let mut i = 0i32;

        bench.iter(|| {
            i += 1;

            let mut my_bytes = bytes.clone();
            my_bytes[0] = i as u8;
            my_bytes[1] = (i / 256) as u8;
            my_bytes[2] = (i / 65536) as u8;

            let mut chunks = vec![];
            for i in 0..16 {
                let mut local_bytes = my_bytes.clone();
                local_bytes[3] = i as u8;
                chunks.push(local_bytes);
            }

            let entry = EntryStub {
                data: Some(chunks),
                key_entry: Entry {
                    parent_id: None,
                    id: None,
                    name: vec![1u8, 2, 3],
                    created: None,
                    modified: None,
                    accessed: None,
                    group_id: None,
                    user_id: None,
                    permissions: None,
                    data_hash: None,
                    data_length: None,
                },
            };

            ks_p.send_reply(Msg::Insert(entry.key_entry.clone(),
                                        Some(Box::new(move |()| Some(entry)))))
                .unwrap();

            match ks_p.send_reply(Msg::Flush).unwrap() {
                Reply::FlushOk => (),
                _ => panic!("Unexpected result from key store."),
            }
        });

        bench.bytes = 16 * (128 * 1024);
    }

    #[bench]
    fn insert_1_key_unchanged_empty(bench: &mut Bencher) {
        let backend = DevNullBackend;
        let ks_p: StoreProcess<EntryStub> = Process::new(move || Store::new_for_testing(backend))
                                                .unwrap();

        bench.iter(|| {
            let entry = EntryStub {
                data: None,
                key_entry: Entry {
                    parent_id: None,
                    id: None,
                    name: vec![1u8, 2, 3].to_vec(),
                    created: Some(0),
                    modified: Some(0),
                    accessed: Some(0),
                    group_id: None,
                    user_id: None,
                    permissions: None,
                    data_hash: None,
                    data_length: None,
                },
            };
            ks_p.send_reply(Msg::Insert(entry.key_entry.clone(), None)).unwrap();
        });
    }

    #[bench]
    fn insert_1_key_updated_empty(bench: &mut Bencher) {
        let backend = DevNullBackend;
        let ks_p: StoreProcess<EntryStub> = Process::new(move || Store::new_for_testing(backend))
                                                .unwrap();

        let mut i = 0;
        bench.iter(|| {
            i += 1;
            let entry = EntryStub {
                data: None,
                key_entry: Entry {
                    parent_id: None,
                    id: None,
                    name: vec![1u8, 2, 3].to_vec(),
                    created: Some(i),
                    modified: Some(i),
                    accessed: Some(i),
                    group_id: None,
                    user_id: None,
                    permissions: None,
                    data_hash: None,
                    data_length: None,
                },
            };
            ks_p.send_reply(Msg::Insert(entry.key_entry.clone(), None)).unwrap();
        });
    }

    #[bench]
    fn insert_1_key_unique_empty(bench: &mut Bencher) {
        let backend = DevNullBackend;
        let ks_p: StoreProcess<EntryStub> = Process::new(move || Store::new_for_testing(backend))
                                                .unwrap();

        let mut i = 0;
        bench.iter(|| {
            i += 1;
            let entry = EntryStub {
                data: None,
                key_entry: Entry {
                    parent_id: None,
                    id: None,
                    name: format!("{}", i).as_bytes().to_vec(),
                    created: Some(i),
                    modified: Some(i),
                    accessed: Some(i),
                    group_id: None,
                    user_id: None,
                    permissions: None,
                    data_hash: None,
                    data_length: None,
                },
            };
            ks_p.send_reply(Msg::Insert(entry.key_entry.clone(), None)).unwrap();
        });
    }
}
