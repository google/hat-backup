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

#![crate_id="hat#0.1"]
#![crate_type="bin"]
#![license = "ALv2"]

#![warn(non_uppercase_statics)]
#![warn(non_camel_case_types)]
#![warn(managed_heap_memory)]
#![warn(unnecessary_qualification)]

#![feature(globs)]

// Standard Rust imports
extern crate debug;
extern crate libc;
extern crate serialize;
extern crate test;
extern crate time;

// Rust bindings
extern crate sodiumoxide;
extern crate sqlite3;

// Testing
#[cfg(test)]
extern crate quickcheck;

use std::os;

mod callback_container;
mod cumulative_counter;
mod ordered_collection;
mod periodic_timer;
mod unique_priority_queue;

mod hat;
mod listdir;
mod process;

mod hash_index;
mod hash_tree;
mod hash_store;

mod blob_index;
mod blob_store;

mod key_index;
mod key_store;


static MAX_BLOB_SIZE: uint = 4 * 1024 * 1024;

fn blob_dir() -> Path { Path::new("blobs") }


fn usage() {
  println!("Usage: {} [snapshot|checkout] name path", os::args().get(0));
}

fn license() {
  println!(include_str!("../../LICENSE"));
}


#[main]
fn main() {
  // Initialize sodium (must only be called once)
  sodiumoxide::init();

  let args = os::args();
  if args.len() == 2 {
    let flag = args.get(1);
    if flag == &"--license".to_string() {
        license();
    }
    else if flag == &"--help".to_string() {
      usage();
      license();
    }
    return;
  }

  if args.len() != 4 {
    return usage();
  }

  let cmd = args.get(1);

  if cmd == &"snapshot".to_string() {
    let name = args.get(2);  // used for naming the key index
    let path = args.get(3);

    {
      let backend = blob_store::FileBackend::new(blob_dir());
      let hat_opt = hat::Hat::open_repository(
        &Path::new("repo"), backend, MAX_BLOB_SIZE);
      let hat = hat_opt.expect(format!("Could not open repository in {}.", path).as_slice());

      let family_opt = hat.open_family(name.clone());
      let family = family_opt.expect(format!("Could not open family '{}'", name).as_slice());

      family.snapshot_dir(Path::new(path.clone()));
      family.flush();
    }

    println!("Waiting for final flush...");
    return;
  }
  else if cmd == &"checkout".to_string() {
    let name = args.get(2);  // used for naming the key index
    let path = args.get(3);

    let backend = blob_store::FileBackend::new(blob_dir());
    let hat_opt = hat::Hat::open_repository(
      &Path::new("repo"), backend, MAX_BLOB_SIZE);
    let hat = hat_opt.expect(format!("Could not open repository in {}.", path).as_slice());

    let family_opt = hat.open_family(name.clone());
    let family = family_opt.expect(format!("Could not open family '{}'", name).as_slice());

    family.checkout_in_dir(&mut Path::new(path.clone()), None);
    return;
  }

  usage();

}
