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

//! Helpers for reading directory structures from the local filesystem.

use std::fs;
use std::io;
use std::iter;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use threadpool;


pub trait HasPath {
    fn path(&self) -> PathBuf;
}

impl HasPath for fs::DirEntry {
    fn path(&self) -> PathBuf {
        fs::DirEntry::path(self)
    }
}

pub trait PathHandler<D> {
    type DirIter;

    fn read_dir(&self, &PathBuf) -> io::Result<Self::DirIter>;
    fn handle_path(&self, D, PathBuf) -> Option<D>;
}


pub fn iterate_recursively<P: 'static + Send + Clone,
                           W: 'static + PathHandler<P> + Send + Clone,
                           I: 'static + HasPath>
    (first: (PathBuf, P),
     worker: &mut W)
    where W::DirIter: iter::Iterator<Item = io::Result<I>>
{
    let threads = 10;
    let (push_ch, work_ch) = mpsc::sync_channel(threads);
    let pool = threadpool::ThreadPool::new(threads);

    enum Work<P> {
        Done,
        More(PathBuf, P),
    }

    // Insert the first task into the queue:
    let (root, payload) = first;
    push_ch.send(Work::More(root, payload)).unwrap();
    let idle_workers = Arc::new(Mutex::new(vec![worker.clone(); threads]));
    let mut active_workers = 0;

    // Master thread:
    loop {
        match work_ch.recv() {
            Err(_) => unreachable!(),
            Ok(Work::Done) => {
                // A worker has completed a task.
                // We are done when no more workers are active (i.e. all tasks are done):
                active_workers -= 1;
                if active_workers == 0 {
                    break;
                }
            }
            Ok(Work::More(root, payload)) => {
                // Execute the task in a pool thread:
                active_workers += 1;

                let idle_workers_ = idle_workers.clone();
                let push_ch_ = push_ch.clone();
                pool.execute(move || {
                    let worker = {
                        let mut lock = idle_workers_.lock().unwrap();
                        lock.pop().expect("not enough workers")
                    };
                    match worker.read_dir(&root) {
                        Ok(dir) => {
                            for entry_res in dir {
                                match entry_res {
                                    Ok(entry) => {
                                        let file = entry.path();
                                        let path = PathBuf::from(file.to_str().unwrap());
                                        let dir_opt = worker.handle_path(payload.clone(), path);
                                        if let Some(dir) = dir_opt {
                                            push_ch_.send(Work::More(file, dir)).unwrap();
                                        }
                                    }
                                    Err(err) => {
                                        // For some reason, we failed to read this entry.
                                        // Just skip it and continue with the next.
                                        warn!("Could not read directory entry: {}", err);
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            // Cannot read this directory.
                            warn!("Skipping unreadable directory {:?}: {}", root, err);
                        }
                    }
                    // Return our worker.
                    idle_workers_.lock().unwrap().push(worker);
                    // Count this pool thread as idle:
                    push_ch_.send(Work::Done).unwrap();
                });
            }
        }
    }
}

struct PrintPathHandler;

impl Clone for PrintPathHandler {
    fn clone(&self) -> PrintPathHandler {
        PrintPathHandler
    }
}

impl PathHandler<()> for PrintPathHandler {
    type DirIter = fs::ReadDir;

    fn read_dir(&self, path: &PathBuf) -> io::Result<Self::DirIter> {
        fs::read_dir(path)
    }

    fn handle_path(&self, _: (), path: PathBuf) -> Option<()> {
        println!("{}", path.display());
        match fs::metadata(&path) {
            Ok(ref m) if m.is_dir() => Some(()),
            _ => None,
        }
    }
}
