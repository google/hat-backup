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

// Unstable APIs:
#![feature(test)]

#![feature(custom_attribute)]
#![feature(custom_derive)]
#![feature(plugin)]

#![feature(fnbox)]

#![feature(custom_derive, custom_attribute, plugin)]
#![plugin(diesel_codegen)]


// Standard Rust imports
extern crate rand;
extern crate test;
extern crate time;

// Rust bindings
extern crate capnp;
extern crate sodiumoxide;

// SQLite
extern crate sqlite3;
#[macro_use]
extern crate diesel;

extern crate rustc_serialize;
extern crate threadpool;

// Argument parsing
#[macro_use]
extern crate clap;

// Testing
#[cfg(test)]
extern crate quickcheck;


use std::convert::From;
use std::path::PathBuf;
use std::borrow::ToOwned;

use clap::{App, SubCommand};


mod cumulative_counter;
mod ordered_collection;
mod periodic_timer;
mod unique_priority_queue;

mod hat;
mod listdir;
mod process;

mod hash;
mod blob;

mod key;

mod gc;
mod gc_noop;
mod gc_rc;
mod tags;

mod snapshot;


pub mod root_capnp {
    include!(concat!(env!("OUT_DIR"), "/root_capnp.rs"));
}


static MAX_BLOB_SIZE: usize = 4 * 1024 * 1024;

fn blob_dir() -> PathBuf {
    PathBuf::from("blobs")
}

#[cfg(not(test))]
fn license() {
    println!(include_str!("../../LICENSE"));
    println!("clap (Command Line Argument Parser) License:");
    println!(include_str!("../../LICENSE-CLAP"));
}


#[cfg(not(test))]
fn main() {
    // Because "snapshot" and "checkout" use the exact same type of arguments, we can make a
    // template. This template defines two positional arguments, both are required
    let arg_template = "<NAME> 'Name of the snapshot'
                        <PATH> 'The path of \
                        the snapshot'";

    // Create valid arguments
    let matches = App::new("hat")
                      .version(&format!("v{}", crate_version!())[..])
                      .about("Create backup snapshots")
                      .arg_from_usage("--license 'Display the license'")
                      .subcommand(SubCommand::with_name("snapshot")
                                      .about("Create a snapshot")
                                      .args_from_usage(arg_template))
                      .subcommand(SubCommand::with_name("checkout")
                                      .about("Checkout a snapshot")
                                      .args_from_usage(arg_template))
                      .subcommand(SubCommand::with_name("commit")
                                      .about("Commit a snapshot")
                                      .arg_from_usage("<NAME> 'Name of the snapshot'"))
                      .subcommand(SubCommand::with_name("meta-commit")
                                      .about("Commit snapshot metadata (required for recover \
                                              command"))
                      .subcommand(SubCommand::with_name("recover")
                                      .about("Recover list of commit'ed snapshots"))
                      .subcommand(SubCommand::with_name("delete")
                                      .about("Delete a snapshot")
                                      .args_from_usage("<NAME> 'Name of the snapshot family'
                                                        \
                                                        <ID> 'The snapshot id to delete'"))
                      .subcommand(SubCommand::with_name("gc")
                                      .about("Garbage collect: identify and remove unused data \
                                              blocks.")
                                      .args_from_usage("-p --pretend 'Do not modify any data'"))
                      .subcommand(SubCommand::with_name("resume")
                                      .about("Resume previous failed command."))
                      .get_matches();

    // Check for license flag
    if matches.is_present("license") {
        license();
        std::process::exit(0);
    }

    // Initialize sodium (must only be called once)
    sodiumoxide::init();

    match matches.subcommand() {
        ("resume", Some(_matches)) => {
            // Setting up the repository triggers automatic resume.
            let backend = blob::FileBackend::new(blob_dir());
            hat::Hat::open_repository(&PathBuf::from("repo"), backend, MAX_BLOB_SIZE);
        }
        ("snapshot", Some(matches)) => {
            let name = matches.value_of("NAME").unwrap().to_owned();
            let path = matches.value_of("PATH").unwrap();

            let backend = blob::FileBackend::new(blob_dir());
            let hat = hat::Hat::open_repository(&PathBuf::from("repo"), backend, MAX_BLOB_SIZE);

            let family_opt = hat.open_family(name.clone());
            let family = family_opt.expect(&format!("Could not open family '{}'", name));

            family.snapshot_dir(PathBuf::from(path));
            family.flush();

            println!("Waiting for final flush...");
        }
        ("checkout", Some(matches)) => {
            let name = matches.value_of("NAME").unwrap().to_owned();
            let path = matches.value_of("PATH").unwrap();

            let backend = blob::FileBackend::new(blob_dir());
            let hat = hat::Hat::open_repository(&PathBuf::from("repo"), backend, MAX_BLOB_SIZE);

            hat.checkout_in_dir(name.clone(), PathBuf::from(path));
        }
        ("meta-commit", Some(_)) => {
            let backend = blob::FileBackend::new(blob_dir());
            let mut hat = hat::Hat::open_repository(&PathBuf::from("repo"), backend, MAX_BLOB_SIZE);

            hat.meta_commit();
        }
        ("recover", Some(_)) => {
            let backend = blob::FileBackend::new(blob_dir());
            let mut hat = hat::Hat::open_repository(&PathBuf::from("repo"), backend, MAX_BLOB_SIZE);

            hat.recover();
        }
        ("commit", Some(matches)) => {
            let name = matches.value_of("NAME").unwrap().to_owned();

            let backend = blob::FileBackend::new(blob_dir());
            let mut hat = hat::Hat::open_repository(&PathBuf::from("repo"), backend, MAX_BLOB_SIZE);

            hat.commit(name, None);
        }
        ("delete", Some(matches)) => {
            let name = matches.value_of("NAME").unwrap().to_owned();
            let id = matches.value_of("ID").unwrap().to_owned();

            let backend = blob::FileBackend::new(blob_dir());
            let mut hat = hat::Hat::open_repository(&PathBuf::from("repo"), backend, MAX_BLOB_SIZE);

            hat.deregister(name, id.parse::<i64>().unwrap());
        }
        ("gc", Some(_matches)) => {
            let backend = blob::FileBackend::new(blob_dir());
            let mut hat = hat::Hat::open_repository(&PathBuf::from("repo"), backend, MAX_BLOB_SIZE);
            hat.gc();
        }
        _ => {
            println!("No subcommand specified\n{}\nFor more information re-run with --help",
                     matches.usage());
            std::process::exit(1);
        }
    }
}
