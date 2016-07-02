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

use key::*;
use std::sync::Arc;

use backend::{MemoryBackend, StoreBackend};
use util::Process;

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

fn insert_and_update_fs<B: StoreBackend>(fs: &mut FileSystem, ks_p: &StoreProcess<EntryStub, B>) {
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
        insert_and_update_fs(f, ks_p);
    }
}

fn verify_filesystem<B: StoreBackend>(fs: &FileSystem, ks_p: &StoreProcess<EntryStub, B>) -> usize {
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
                        let it = match tree_data.expect("has data").init().unwrap() {
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
        count += verify_filesystem(dir, ks_p);
    }

    count
}

#[test]
fn identity() {
    fn prop(size: u8) -> bool {
        let backend = Arc::new(MemoryBackend::new());
        let ks_p = Process::new(move || Store::new_for_testing(backend)).unwrap();

        let mut fs = rng_filesystem(size as usize);
        insert_and_update_fs(&mut fs, &ks_p);
        let fs = fs;

        match ks_p.send_reply(Msg::Flush).unwrap() {
            Reply::FlushOk => (),
            _ => panic!("Unexpected result from key store."),
        }

        verify_filesystem(&fs, &ks_p);
        true
    }
    quickcheck::quickcheck(prop as fn(u8) -> bool);
}
