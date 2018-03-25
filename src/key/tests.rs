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

use backend::{MemoryBackend, StoreBackend};
use key::*;

use quickcheck;

use rand::Rng;
use rand::thread_rng;
use std::io;
use std::sync::Arc;
use util::Process;

fn random_ascii_bytes() -> Vec<u8> {
    let ascii: String = thread_rng().gen_ascii_chars().take(32).collect();
    ascii.into_bytes()
}

#[derive(Clone, Debug)]
pub struct EntryStub {
    pub key_entry: Entry,
    pub data: Option<Vec<Vec<u8>>>,
}

impl io::Read for EntryStub {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.data.as_mut() {
            Some(x) => {
                if !x.is_empty() {
                    let c = x.remove(0);
                    buf[..c.len()].copy_from_slice(&c[..]);
                    Ok(c.len())
                } else {
                    Ok(0)
                }
            }
            None => Ok(0),
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

            let data = if data_opt.is_some() {
                Data::FilePlaceholder
            } else {
                Data::DirPlaceholder
            };
            let new_root = EntryStub {
                data: data_opt,
                key_entry: Entry {
                    node_id: None,
                    parent_id: None, // updated by insert_and_update_fs()
                    data: data,

                    info: Info {
                        name: random_ascii_bytes(),
                        byte_length: None,

                        created_ts_secs: thread_rng().gen(),
                        modified_ts_secs: thread_rng().gen(),
                        accessed_ts_secs: thread_rng().gen(),

                        permissions: None,
                        user_id: None,
                        group_id: None,

                        snapshot_ts_utc: 0,
                    },
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
            node_id: None, // updated by insert_and_update_fs()
            data: Data::DirPlaceholder,
            info: Info {
                name: b"root".to_vec(),
                created_ts_secs: thread_rng().gen(),
                modified_ts_secs: thread_rng().gen(),
                accessed_ts_secs: thread_rng().gen(),
                permissions: None,
                user_id: None,
                group_id: None,
                byte_length: None,
                snapshot_ts_utc: 0,
            },
        },
    };

    FileSystem {
        file: root,
        filelist: create_files(size),
    }
}

fn insert_and_update_fs<B: StoreBackend>(fs: &mut FileSystem, ks_p: &StoreProcess<EntryStub, B>) {
    let local_file = fs.file.clone();
    fs.file.key_entry.node_id = match ks_p.send_reply(Msg::Insert(
        fs.file.key_entry.clone(),
        if fs.file.data.is_some() {
            Some(Box::new(move |()| Some(local_file)))
        } else {
            None
        },
    )).unwrap()
    {
        Reply::Id(id) => Some(id),
        _ => panic!("unexpected reply from key store"),
    };

    for f in fs.filelist.iter_mut() {
        f.file.key_entry.parent_id = fs.file.key_entry.node_id;
        insert_and_update_fs(f, ks_p);
    }
}

fn verify_filesystem<B: StoreBackend>(fs: &FileSystem, ks_p: &StoreProcess<EntryStub, B>) -> usize {
    let listing = match ks_p.send_reply(Msg::ListDir(fs.file.key_entry.node_id))
        .unwrap()
    {
        Reply::ListResult(ls) => ls,
        _ => panic!("Unexpected result from key store."),
    };

    assert_eq!(fs.filelist.len(), listing.len());

    for (entry, persistent_ref, tree_data) in listing {
        let mut found = false;

        for dir in fs.filelist.iter() {
            if dir.file.key_entry.info.name == entry.info.name {
                found = true;

                assert_eq!(dir.file.key_entry.node_id, entry.node_id);
                assert_eq!(
                    dir.file.key_entry.info.created_ts_secs,
                    entry.info.created_ts_secs
                );
                assert_eq!(
                    dir.file.key_entry.info.accessed_ts_secs,
                    entry.info.accessed_ts_secs
                );
                assert_eq!(
                    dir.file.key_entry.info.modified_ts_secs,
                    entry.info.modified_ts_secs
                );

                match dir.file.data {
                    Some(ref original) => {
                        let it = match tree_data.expect("has data").init().unwrap() {
                            None => panic!("No data."),
                            Some(it) => it,
                        };
                        let mut original_all = vec![];
                        for chunk in original {
                            original_all.extend_from_slice(&chunk[..]);
                        }
                        let mut recovered_all = vec![];
                        for chunk in it {
                            recovered_all.extend_from_slice(&chunk[..]);
                        }
                        assert_eq!(original_all.len(), recovered_all.len());
                        assert_eq!(original_all, recovered_all);
                    }
                    None => {
                        assert_eq!(Data::DirPlaceholder, entry.data);
                        assert!(persistent_ref.is_none());
                    }
                }

                break; // Proceed to check next file
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
        let ks_p = Process::new(Store::new_for_testing(backend, 4096).unwrap());

        let mut fs = rng_filesystem(size as usize);
        insert_and_update_fs(&mut fs, &ks_p);
        let fs = fs;
        match ks_p.send_reply(Msg::CommitReservedNodes(None)).unwrap() {
            Reply::Ok => (),
            _ => panic!("Unexpected result from key store."),
        }

        match ks_p.send_reply(Msg::Flush).unwrap() {
            Reply::FlushOk => (),
            _ => panic!("Unexpected result from key store."),
        }

        verify_filesystem(&fs, &ks_p);
        true
    }
    quickcheck::quickcheck(prop as fn(u8) -> bool);
}
