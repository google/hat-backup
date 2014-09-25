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

use std::sync;
use std::os::{last_os_error};
use std::io::{TypeDirectory};
use std::io::fs::{lstat};

use std::c_str::CString;
use libc::funcs::posix88::dirent;
use libc::types::common::posix88::{DIR,dirent_t};
use libc::types::os::arch::c95::c_char;
use libc::{c_int};


pub struct DirIterator {
  fd: *DIR,
}

impl DirIterator {
  pub fn new(path: Path) -> Result<DirIterator, String> {
    let fd = path.with_c_str(|c_str| unsafe { dirent::opendir(c_str) });

    if fd as int > 0 { Ok(DirIterator{fd:fd}) }
    else { Err(last_os_error()) }
  }

  fn read(&mut self) -> String {
    extern {
      fn rust_dirent_t_size() -> c_int;
      fn rust_list_dir_val(ptr: *mut dirent_t) -> *c_char;
    }

    let mut entry_ptr = 0 as *mut dirent_t;

    let size = unsafe { rust_dirent_t_size() };
    let mut buf = Vec::<u8>::with_capacity(size as uint);
    let buf_ptr = buf.as_mut_ptr() as *mut dirent_t;

    let retval = unsafe { dirent::readdir_r(self.fd, buf_ptr, &mut entry_ptr) };

    if retval == 0 && !entry_ptr.is_null() {
      let cstr = unsafe { CString::new(rust_list_dir_val(entry_ptr), false) };
      cstr.as_str().expect("Path not UTF8.").clone().into_string()
    } else { "".to_owned() }

  }

}

impl Drop for DirIterator {
  fn drop(&mut self) {
    unsafe {
      dirent::closedir(self.fd)
    };
  }
}

impl Iterator<String> for DirIterator {
  fn next(&mut self) -> Option<String> {
    let name = self.read();
    if name.len() == 0 { None }
    else { Some(name) }
  }
}


pub trait PathHandler<D> {
  fn handle_path(&mut self, D, Path) -> Option<D>;
}


pub fn iterate_recursively<P: Send + Clone, W: PathHandler<P> + Send + Clone>
  (root: (Path, P), worker: &mut W)
{
  let threads = 10;

  let queue = sync::Arc::new(sync::Mutex::new(vec!(root)));
  let in_progress = sync::Arc::new(sync::Mutex::new(0));
  let done = sync::Arc::new(sync::Mutex::new(0));

  for _ in range(0u, threads) {
    let t_in_progress = in_progress.clone();
    let t_queue = queue.clone();
    let t_done = done.clone();
    let t_worker_imm = worker.clone();

    spawn(proc() {
      let mut t_worker = t_worker_imm.clone();

      loop {
        let mut next = None;
        let mut task_count = 0;
        {
          let mut guarded_tasks = t_in_progress.lock();
          let mut guarded_queue = t_queue.lock();
          if guarded_queue.len() > 0 {
            next = guarded_queue.pop();
            *guarded_tasks += 1;
          }
          task_count = *guarded_tasks;
        }

        match next {
          None => {
            if task_count == 0 {
              break;  // Stop when all workers are idle
            }
          },
          Some((root_, payload)) => {
            let mut root = root_;
            let res = DirIterator::new(root.clone());
            if res.is_ok() {
              let mut it = res.unwrap();
              for file in it {
                if file != ".".into_string() && file != "..".into_string() {
                  let rel_path = Path::new(file);
                  root.push(rel_path);
                  let dir_opt = t_worker.handle_path(payload.clone(), root.clone());
                  if dir_opt.is_some() {
                    let mut guarded_queue = t_queue.lock();
                    guarded_queue.push((root.clone(), dir_opt.unwrap()));
                  }
                  root.pop();
                }
              }
            }

            // Tell waiting threads that will not produce more
            {
              let mut guarded_tasks = t_in_progress.lock();
              *guarded_tasks -= 1;
            }
          }
        }
      }

      // Loop is over, thread is stopping
      {
        let mut guarded_tasks_done = t_done.lock();
        *guarded_tasks_done += 1;
        guarded_tasks_done.cond.signal();
      }
    });
  }

  // Wait for threads to complete
  {
    let guarded_done = done.lock();
    while *guarded_done < threads {
      guarded_done.cond.wait();
    }
  }
}

struct PrintPathHandler;

impl Clone for PrintPathHandler {
  fn clone(&self) -> PrintPathHandler { PrintPathHandler }
}

impl PathHandler<()> for PrintPathHandler {
  fn handle_path(&mut self, _: (), path: Path) -> Option<()> {
    let filename_opt = path.filename_str();
    println!("{}", path.display());
    match filename_opt {
      Some(".") | Some("..") | None => None,
      Some(_) =>
      match lstat(&path) {
        Ok(ref st) if st.kind == TypeDirectory => Some(()),
        _ => None,
      }
    }
  }
}
