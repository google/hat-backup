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

#![crate_type="lib"]
#![license = "ALv2"]

#![warn(non_uppercase_statics)]
#![warn(non_camel_case_types)]
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

pub use blob_index::{BlobIndex};
pub use blob_store::{BlobStore};
pub use hash_index::{HashIndex};
pub use hash_store::{HashStore};
pub use key_index::{KeyIndex};
pub use key_store::{KeyStore};
pub use process::{Process};


mod callback_container;
mod cumulative_counter;
mod ordered_collection;
mod periodic_timer;
mod unique_priority_queue;

pub mod listdir;
pub mod process;

pub mod hash_index;
pub mod hash_tree;
pub mod hash_store;

pub mod blob_index;
pub mod blob_store;

pub mod key_index;
pub mod key_store;
