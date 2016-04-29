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

#![feature(custom_derive)]
#![feature(plugin)]

#![feature(fnbox)]

#![plugin(diesel_codegen)]


// Standard Rust imports.
#[macro_use]
extern crate log;
extern crate rand;
extern crate test;
extern crate time;

// Rust crates.
extern crate capnp;
extern crate env_logger;
extern crate sodiumoxide;
extern crate libsodium_sys;
extern crate rustc_serialize;
extern crate threadpool;

// Error definition macros.
#[macro_use]
extern crate error_type;

// Diesel supplies our SQLite wrapper.
#[macro_use]
extern crate diesel;

// We use Clap for argument parsing.
#[macro_use]
extern crate clap;

// Testing utilities.
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
mod util;


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
    env_logger::init().unwrap();

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
        ("resume", Some(_cmd)) => {
            // Setting up the repository triggers automatic resume.
            let backend = blob::FileBackend::new(blob_dir()).unwrap();
            hat::Hat::open_repository(&PathBuf::from("repo"), backend, MAX_BLOB_SIZE).unwrap();
        }
        ("snapshot", Some(cmd)) => {
            let name = cmd.value_of("NAME").unwrap().to_owned();
            let path = cmd.value_of("PATH").unwrap();

            let backend = blob::FileBackend::new(blob_dir()).unwrap();
            let hat = hat::Hat::open_repository(&PathBuf::from("repo"), backend, MAX_BLOB_SIZE)
                          .unwrap();

            let family = hat.open_family(name.clone())
                            .expect(&format!("Could not open family '{}'", name));

            family.snapshot_dir(PathBuf::from(path));
            family.flush().unwrap();

            println!("Waiting for final flush...");
        }
        ("checkout", Some(cmd)) => {
            let name = cmd.value_of("NAME").unwrap().to_owned();
            let path = cmd.value_of("PATH").unwrap();

            let backend = blob::FileBackend::new(blob_dir()).unwrap();
            let hat = hat::Hat::open_repository(&PathBuf::from("repo"), backend, MAX_BLOB_SIZE)
                          .unwrap();

            hat.checkout_in_dir(name.clone(), PathBuf::from(path));
        }
        ("meta-commit", Some(_cmd)) => {
            let backend = blob::FileBackend::new(blob_dir()).unwrap();
            let mut hat = hat::Hat::open_repository(&PathBuf::from("repo"), backend, MAX_BLOB_SIZE)
                              .unwrap();

            hat.meta_commit();
        }
        ("recover", Some(_cmd)) => {
            let backend = blob::FileBackend::new(blob_dir()).unwrap();
            let mut hat = hat::Hat::open_repository(&PathBuf::from("repo"), backend, MAX_BLOB_SIZE)
                              .unwrap();

            hat.recover().unwrap();
        }
        ("commit", Some(cmd)) => {
            let name = cmd.value_of("NAME").unwrap().to_owned();

            let backend = blob::FileBackend::new(blob_dir()).unwrap();
            let mut hat = hat::Hat::open_repository(&PathBuf::from("repo"), backend, MAX_BLOB_SIZE)
                              .unwrap();

            hat.commit_by_name(name, None).unwrap();
        }
        ("delete", Some(cmd)) => {
            let name = cmd.value_of("NAME").unwrap().to_owned();
            let id = cmd.value_of("ID").unwrap().to_owned();

            let backend = blob::FileBackend::new(blob_dir()).unwrap();
            let mut hat = hat::Hat::open_repository(&PathBuf::from("repo"), backend, MAX_BLOB_SIZE)
                              .unwrap();

            hat.deregister_by_name(name, id.parse::<i64>().unwrap()).unwrap();
        }
        ("gc", Some(_cmd)) => {
            let backend = blob::FileBackend::new(blob_dir()).unwrap();
            let mut hat = hat::Hat::open_repository(&PathBuf::from("repo"), backend, MAX_BLOB_SIZE)
                              .unwrap();
            let (deleted_hashes, live_blobs) = hat.gc().unwrap();
            println!("Deleted hashes: {:?}", deleted_hashes);
            println!("Live data blobs after deletion: {:?}", live_blobs);

        }
        _ => {
            println!("No subcommand specified\n{}\nFor more information re-run with --help",
                     matches.usage());
            std::process::exit(1);
        }
    }
}
