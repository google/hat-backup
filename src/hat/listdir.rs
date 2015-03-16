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

use std::path::PathBuf;

use std::old_io::FileType::{Directory};
use std::old_io::fs::{lstat};

use std::fs::{read_dir};

use std::sync;
use std::sync::mpsc;


pub trait PathHandler<D> {
  fn handle_path(&self, D, Path) -> Option<D>;
}


pub fn iterate_recursively<P: 'static + Send + Clone, W: 'static + PathHandler<P> + Send + Clone>
  (root: (Path, P), worker: &mut W)
{
  let root = (PathBuf::new(&root.0), root.1);

  let threads = 10;
  let (push_ch, work_ch) = mpsc::sync_channel(threads);
  let mut pool = sync::TaskPool::new(threads);

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
          let res = read_dir(&root);
          if res.is_ok() {
            let mut it = res.unwrap();
            for entry in it {
              if entry.is_ok() {
                let entry = entry.unwrap();
                let file = entry.path();
                let old_path = Path::new(file.to_str().unwrap());
                let dir_opt = _worker.handle_path(payload.clone(), old_path);
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
  fn handle_path(&self, _: (), path: Path) -> Option<()> {
    let filename_opt = path.filename_str();
    println!("{}", path.display());
    match filename_opt {
      Some(".") | Some("..") | None => None,
      Some(_) => {
        match lstat(&path) {
          Ok(ref st) if st.kind == Directory => Some(()),
          _ => None,
        }
      }
    }
  }
}
