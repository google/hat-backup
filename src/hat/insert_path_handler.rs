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

use backend::StoreBackend;
use key;
use std::error::Error;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::{atomic, Mutex};
use time;
use util::{FileIterator, PathHandler, SyncPool};

struct FileEntry {
    key_entry: key::Entry,
    metadata: fs::Metadata,
    full_path: PathBuf,
}

impl FileEntry {
    fn new(full_path: PathBuf, parent: Option<u64>) -> Result<FileEntry, Box<Error>> {
        debug!("FileEntry::new({:?})", full_path);

        let filename_opt = full_path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|s| s.as_bytes().to_vec());

        if let Some(filename) = filename_opt {
            let meta = fs::symlink_metadata(&full_path)?;
            let data = if meta.is_file() {
                key::Data::FilePlaceholder
            } else if meta.is_dir() {
                key::Data::DirPlaceholder
            } else if meta.file_type().is_symlink() {
                let path = fs::read_link(&full_path)?;
                key::Data::Symlink(path)
            } else {
                // Unsupported file type. Skipping.
                return Err(From::from(format!("unknown file kind")));
            };
            Ok(FileEntry {
                key_entry: key::Entry::new(parent, filename, data, Some(&meta)),
                metadata: meta,
                full_path: full_path,
            })
        } else {
            Err(From::from("Could not parse filename."[..].to_owned()))
        }
    }

    fn is_file(&self) -> bool {
        self.metadata.is_file()
    }
    fn is_directory(&self) -> bool {
        self.metadata.is_dir()
    }
}

pub struct InsertPathHandler<B: StoreBackend> {
    count: atomic::AtomicIsize,
    last_print: Mutex<time::Timespec>,
    key_store: SyncPool<key::StoreProcess<FileIterator, B>>,
}

impl<B: StoreBackend> InsertPathHandler<B> {
    pub fn new(key_stores: Vec<key::StoreProcess<FileIterator, B>>) -> InsertPathHandler<B> {
        InsertPathHandler {
            count: atomic::AtomicIsize::new(0),
            last_print: Mutex::new(time::now().to_timespec()),
            key_store: SyncPool::new(key_stores),
        }
    }
}

impl<B: StoreBackend> PathHandler<Option<u64>> for InsertPathHandler<B> {
    type DirItem = fs::DirEntry;
    type DirIter = fs::ReadDir;

    fn read_dir(&self, path: &PathBuf) -> io::Result<Self::DirIter> {
        fs::read_dir(path)
    }

    fn handle_path(&self, parent: &Option<u64>, path: &PathBuf) -> Option<Option<u64>> {
        let count = self.count.fetch_add(1, atomic::Ordering::SeqCst) + 1;

        if count % 16 == 0 {
            // don't hammer the mutex
            let mut guarded_last_print = self.last_print.lock().unwrap();
            let now = time::now().to_timespec();
            if guarded_last_print.sec <= now.sec - 1 {
                println!("#{}: {}", count, path.display());
                *guarded_last_print = now;
            }
        }

        match FileEntry::new(path.clone(), *parent) {
            Err(e) => {
                println!("Skipping '{}': {}", path.display(), e);
            }
            Ok(file_entry) => {
                let is_file = file_entry.is_file();
                let is_directory = file_entry.is_directory();
                let local_root = path.clone();
                let full_path = file_entry.full_path.clone();

                let ks = self.key_store.lock().unwrap();
                match ks.send_reply(key::Msg::Insert(
                    file_entry.key_entry,
                    if is_file {
                        Some(Box::new(move |()| match FileIterator::new(&full_path) {
                            Err(e) => {
                                println!("Skipping '{}': {}", local_root.display(), e.to_string());
                                None
                            }
                            Ok(it) => Some(it),
                        }))
                    } else {
                        None
                    },
                )) {
                    Ok(key::Reply::Id(id)) => {
                        if is_directory {
                            return Some(Some(id));
                        }
                    }
                    Err(e) => panic!("Error from key store: {:?}", e),
                    _ => panic!("Unexpected reply from key store."),
                }
            }
        }

        None
    }
}
