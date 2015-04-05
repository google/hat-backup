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

#![crate_type="bin"]

#![allow(dead_code)]

#![warn(non_upper_case_globals)]
#![warn(non_camel_case_types)]
#![warn(unused_qualifications)]

#![feature(test)]
#![feature(std_misc)]
#![feature(collections)]
#![feature(core)]
#![feature(io)]
#![feature(path)]

#![feature(unboxed_closures)]

#![feature(custom_attribute)]
#![feature(plugin)]
#![plugin(quickcheck_macros)]

// Standard Rust imports
// extern crate serialize;
extern crate rand;
extern crate test;
extern crate time;

// Rust bindings
extern crate sodiumoxide;
extern crate sqlite3;

extern crate "rustc-serialize" as rustc_serialize;
extern crate threadpool;

// Testing
#[cfg(test)]
extern crate quickcheck;


use std::env;
use std::path::PathBuf;

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

mod blob_index;
mod blob_store;

mod key_index;
mod key_store;

mod snapshot_index;


static MAX_BLOB_SIZE: usize = 4 * 1024 * 1024;

fn blob_dir() -> PathBuf { PathBuf::new("blobs") }


#[cfg(not(test))]
fn usage() {
  println!("Usage: {} [snapshot|commit|checkout] name path", env::args().next().unwrap());
}


#[cfg(not(test))]
fn license() {
  println!(include_str!("../../LICENSE"));
}


#[cfg(not(test))]
fn main() {
  // Initialize sodium (must only be called once)
  sodiumoxide::init();

  let mut args = env::args();

  if args.len() < 2 {
    return usage(); // There's not even a command here.
  }
  args.next(); // strip called name

  if args.len() == 1 {
    let ref flag = args.next().unwrap();
    if flag == &"--license".to_string() {
        license();
    }
    else if flag == &"--help".to_string() {
      usage();
      license();
    }
    return;
  }

  let ref cmd = args.next().unwrap();

  if cmd == &"snapshot".to_string() && args.len() == 2 {
    let ref name = args.next().unwrap();  // used for naming the key index
    let ref path = args.next().unwrap();

    {
      let backend = blob_store::FileBackend::new(blob_dir());
      let hat = hat::Hat::open_repository(&PathBuf::new("repo"), backend, MAX_BLOB_SIZE);

      let family_opt = hat.open_family(name.clone());
      let family = family_opt.expect(format!("Could not open family '{}'", name).as_slice());

      family.snapshot_dir(PathBuf::new(path));
      family.flush();
    }

    println!("Waiting for final flush...");
    return;
  }
  else if cmd == &"checkout".to_string() && args.len() == 2 {
    let ref name = args.next().unwrap();  // used for naming the key index
    let ref path = args.next().unwrap();

    let backend = blob_store::FileBackend::new(blob_dir());
    let hat = hat::Hat::open_repository(&PathBuf::new("repo"), backend, MAX_BLOB_SIZE);

    hat.checkout_in_dir(name.clone(), PathBuf::new(path));
    return;
  }
  else if cmd == &"commit".to_string() && args.len() == 1 {
    let ref name = args.next().unwrap();

    let backend = blob_store::FileBackend::new(blob_dir());
    let hat = hat::Hat::open_repository(&PathBuf::new("repo"), backend, MAX_BLOB_SIZE);

    hat.commit(name.clone());
    return;
  }

  usage();

}
