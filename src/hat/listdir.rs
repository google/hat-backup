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
use std::sync::Arc;

use threadpool;


pub trait HasPath {
    fn path(&self) -> PathBuf;
}

impl HasPath for fs::DirEntry {
    fn path(&self) -> PathBuf {
        fs::DirEntry::path(self)
    }
}

pub trait PathHandler<D: Send + 'static>: Send + Clone + 'static {
    type DirItem: HasPath;
    type DirIter: iter::Iterator<Item = io::Result<Self::DirItem>>;

    fn read_dir(&self, &PathBuf) -> io::Result<Self::DirIter>;
    fn handle_path(&self, &D, PathBuf) -> Option<D>;
}


pub fn iterate_recursively<P: 'static + Send, W: PathHandler<P>>(first: (PathBuf, P), worker: &W) {
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
    let worker = Arc::new(worker);
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

                let worker = worker.clone();
                let push_ch_ = push_ch.clone();
                pool.execute(move || {
                    match worker.read_dir(&root) {
                        Ok(dir) => {
                            for entry_res in dir {
                                match entry_res {
                                    Ok(entry) => {
                                        let file = entry.path();
                                        let path = PathBuf::from(file.to_str().unwrap());
                                        let dir_opt = worker.handle_path(&payload, path);
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
    type DirItem = fs::DirEntry;
    type DirIter = fs::ReadDir;

    fn read_dir(&self, path: &PathBuf) -> io::Result<Self::DirIter> {
        fs::read_dir(path)
    }

    fn handle_path(&self, _: &(), path: PathBuf) -> Option<()> {
        println!("{}", path.display());
        match fs::metadata(&path) {
            Ok(ref m) if m.is_dir() => Some(()),
            _ => None,
        }
    }
}


#[cfg(test)]
mod tests {

    use std::collections::btree_map;
    use std::io;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::vec;

    use super::*;

    type ParentOpt = Option<PathBuf>;
    type VisitedPaths = btree_map::BTreeMap<PathBuf, bool>;

    impl HasPath for PathBuf {
        fn path(&self) -> PathBuf {
            self.to_owned()
        }
    }

    #[derive(Clone)]
    struct StubPathHandler {
        paths: Arc<Mutex<VisitedPaths>>,
    }

    impl StubPathHandler {
        fn new(paths: Vec<PathBuf>) -> StubPathHandler {
            let mut tree = btree_map::BTreeMap::new();
            for path in paths {
                tree.insert(path, false);
            }
            StubPathHandler { paths: Arc::new(Mutex::new(tree)) }
        }

        fn visit(&self, path: PathBuf) -> Option<bool> {
            let mut paths = self.paths.lock().unwrap();
            paths.insert(path, true)
        }

        fn list(&self, dir: &PathBuf) -> vec::IntoIter<io::Result<PathBuf>> {
            let paths = self.paths.lock().unwrap();
            let mut contents = vec![];
            for k in paths.keys() {
                if k.parent() == Some(dir) {
                    contents.push(Ok(k.clone()));
                }
            }
            contents.into_iter()
        }

        fn not_visited(&self) -> Vec<PathBuf> {
            let paths = self.paths.lock().unwrap();
            let mut contents = vec![];
            for (path, visited) in paths.iter() {
                if !visited {
                    contents.push(path.clone());
                }
            }
            contents
        }
    }

    impl PathHandler<ParentOpt> for StubPathHandler {
        type DirItem = PathBuf;
        type DirIter = vec::IntoIter<io::Result<Self::DirItem>>;

        fn read_dir(&self, path: &PathBuf) -> io::Result<Self::DirIter> {
            Ok(self.list(path))
        }

        fn handle_path(&self, p_opt: ParentOpt, path: PathBuf) -> Option<ParentOpt> {
            assert_eq!(self.visit(path.clone()), Some(false));

            if let Some(p) = p_opt {
                assert!(path.parent() == Some(&p));
            }

            self.list(&path).next().map(|_| Some(path))
        }
    }

    #[test]
    fn can_visit_all() {
        let paths: [&str; 20] = ["/",
                                 "/foo",
                                 "/bar/",
                                 "/bar/baz/",
                                 "/bar/baz/qux",
                                 "/bar/baz/foo",
                                 "/bar/baz/bar/",
                                 "/bar/baz/bar/foo",
                                 "/bar/baz/bar/bar/",
                                 "/bar/baz/bar/bar/bar",
                                 "/empty/",
                                 "/empty/1/",
                                 "/empty/2/",
                                 "/empty/3/",
                                 "/empty/4/",
                                 "/empty/5/",
                                 "/empty/6/",
                                 "/empty/7/",
                                 "/empty/8/",
                                 "/empty/9/",
                                 ];

        let mut handler = StubPathHandler::new(paths.iter().map(PathBuf::from).collect());
        iterate_recursively((PathBuf::from("/"), None), &mut handler);

        assert_eq!(handler.not_visited(), vec![PathBuf::from("/")]);
    }

}
