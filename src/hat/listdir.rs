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
use std::path::PathBuf;
use std::sync::mpsc;

use threadpool;


pub trait PathHandler<D> {
  fn handle_path(&self, D, PathBuf) -> Option<D>;
}


pub fn iterate_recursively<P: 'static + Send + Clone, W: 'static + PathHandler<P> + Send + Clone>
  (root: (PathBuf, P), worker: &mut W)
{
  let threads = 10;
  let (push_ch, work_ch) = mpsc::sync_channel(threads);
  let pool = threadpool::ThreadPool::new(threads);

  // Insert the first task into the queue:
  push_ch.send(Some(root)).unwrap();
  let mut running_workers = 0 as i32;

  // Master thread:
  loop {
    match work_ch.recv() {
      Err(_) => unreachable!(),
      Ok(None) => {
        // A worker has completed a task.
        // We are done when no more workers are active (i.e. all tasks are done):
        running_workers -= 1;
        if running_workers == 0 {
          break
        }
      },
      Ok(Some((root, payload))) => {
        // Execute the task in a pool thread:
        running_workers += 1;
        let _worker = worker.clone();
        let _push_ch = push_ch.clone();
        pool.execute(move|| {
          let res = fs::read_dir(&root);
          if res.is_ok() {
            for entry in res.unwrap() {
              if entry.is_ok() {
                let entry = entry.unwrap();
                let file = entry.path();
                let path = PathBuf::from(file.to_str().unwrap());
                let dir_opt = _worker.handle_path(payload.clone(), path);
                if dir_opt.is_some() {
                  _push_ch.send(Some((file.clone(), dir_opt.unwrap()))).unwrap();
                }
              }
            }
          }
          // Count this pool thread as idle:
          _push_ch.send(None).unwrap();
        });
      }
    }
  }
}

struct PrintPathHandler;

impl Clone for PrintPathHandler {
  fn clone(&self) -> PrintPathHandler { PrintPathHandler }
}

impl PathHandler<()> for PrintPathHandler {
  fn handle_path(&self, _: (), path: PathBuf) -> Option<()> {
    println!("{}", path.display());
    match fs::metadata(&path) {
      Ok(ref m) if m.is_dir() => Some(()),
      _ => None,
    }
  }
}
