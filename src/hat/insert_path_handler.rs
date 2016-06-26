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

use std::error::Error;
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::str;
use std::sync::{Mutex, atomic};
use time;

use backend::StoreBackend;
use key;
use util::{FileIterator, PathHandler};

struct FileEntry {
    key_entry: key::Entry,
    metadata: fs::Metadata,
    full_path: PathBuf,
    link_path: Option<PathBuf>,
}

impl FileEntry {
    fn new(full_path: PathBuf, parent: Option<u64>) -> Result<FileEntry, Box<Error>> {
        debug!("FileEntry::new({:?})", full_path);

        let filename_opt =
            full_path.file_name().and_then(|n| n.to_str()).map(|n| n.bytes().collect());

        if filename_opt.is_some() {
            let md = try!(fs::symlink_metadata(&full_path));
            let link_path = fs::read_link(&full_path).ok();
            Ok(FileEntry {
                key_entry: key::Entry {
                    name: filename_opt.unwrap(),
                    created: Some(md.ctime_nsec()),
                    modified: Some(md.mtime_nsec()),
                    accessed: Some(md.atime_nsec()),
                    parent_id: parent,
                    data_length: Some(md.len()),
                    data_hash: None,
                    id: None,
                    permissions: None,
                    user_id: None,
                    group_id: None,
                },
                metadata: md,
                full_path: full_path,
                link_path: link_path,
            })
        } else {
            Err(From::from("Could not parse filename."[..].to_owned()))
        }
    }

    fn is_directory(&self) -> bool {
        self.metadata.is_dir()
    }
    fn is_symlink(&self) -> bool {
        self.link_path.is_some()
    }
}

pub struct InsertPathHandler<B: StoreBackend> {
    count: atomic::AtomicIsize,
    last_print: Mutex<time::Timespec>,
    key_store: Mutex<key::StoreProcess<FileIterator, B>>,
}

impl<B: StoreBackend> InsertPathHandler<B> {
    pub fn new(key_store: key::StoreProcess<FileIterator, B>) -> InsertPathHandler<B> {
        InsertPathHandler {
            count: atomic::AtomicIsize::new(0),
            last_print: Mutex::new(time::now().to_timespec()),
            key_store: Mutex::new(key_store),
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

        match FileEntry::new(path.clone(), parent.clone()) {
            Err(e) => {
                println!("Skipping '{}': {}", path.display(), e);
            }
            Ok(file_entry) => {
                if file_entry.is_symlink() {
                    return None;
                }
                let is_directory = file_entry.is_directory();
                let local_root = path.clone();
                let full_path = file_entry.full_path.clone();

                match self.key_store
                    .lock()
                    .unwrap()
                    .send_reply(key::Msg::Insert(file_entry.key_entry,
                                                 if is_directory {
                                                     None
                                                 } else {
                                                     Some(Box::new(move |()| {
                            match FileIterator::new(&full_path) {
                                Err(e) => {
                                    println!("Skipping '{}': {}",
                                             local_root.display(),
                                             e.to_string());
                                    None
                                }
                                Ok(it) => Some(it),
                            }
                        }))
                                                 })) {
                    Ok(key::Reply::Id(id)) => {
                        if is_directory {
                            return Some(Some(id));
                        }
                    }
                    _ => panic!("Unexpected reply from key store."),
                }
            }
        }

        None
    }
}
