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

// Unstable APIs:
#![feature(convert)]
#![feature(fs_time)]
#![feature(std_misc)]
#![feature(test)]

#![feature(custom_attribute)]
#![feature(plugin)]
#![plugin(quickcheck_macros)]

// Standard Rust imports
extern crate rand;
extern crate test;
extern crate time;

// Rust bindings
extern crate sodiumoxide;
extern crate sqlite3;

extern crate rustc_serialize;
extern crate threadpool;

// Argument parsing
extern crate clap;

// Testing
#[cfg(test)]
extern crate quickcheck;


// use std::env;
use std::path::PathBuf;
use std::borrow::ToOwned;

use clap::{App, Arg, SubCommand};

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

fn blob_dir() -> PathBuf { PathBuf::from("blobs") }


// #[cfg(not(test))]
// fn usage() {
//   println!("Usage: {} [snapshot|commit|checkout] name path", env::args().next().unwrap());
// }


#[cfg(not(test))]
fn license() {
  println!(include_str!("../../LICENSE"));
}


#[cfg(not(test))]
fn main() {
    // get version from Cargo.toml
    let version = format!("{}.{}.{}{}",
                          env!("CARGO_PKG_VERSION_MAJOR"),
                          env!("CARGO_PKG_VERSION_MINOR"),
                          env!("CARGO_PKG_VERSION_PATCH"),
                          option_env!("CARGO_PKG_VERSION_PRE").unwrap_or(""));
                          
    // Create valid arguments
    let matches = App::new("hat-backup")
                        .version(&version[..])
                        .about("Create backup snapshots")
                        // If custom usage statement desired (instead of the auto-generated one), uncomment:
                        //.usage("hat-backup [snapshot|commit|checkout] <name> <path>")
                        .arg(Arg::new("lic")
                            .long("license")
                            .help("Display the license"))
                        .subcommand(SubCommand::new("snapshot")
                            .about("Create a snapshot")
                            .arg(Arg::new("NAME")
                                .index(1)
                                .help("Name of the snapshot")
                                .required(true))
                            .arg(Arg::new("PATH")
                                .index(2)
                                .required(true)
                                .help("The path of the snapshot")))
                        .subcommand(SubCommand::new("checkout")
                            .about("Checkout a snapshot")
                            .arg(Arg::new("NAME")
                                .index(1)
                                .help("Name of the snapshot")
                                .required(true))
                            .arg(Arg::new("PATH")
                                .index(2)
                                .required(true)
                                .help("The path of the snapshot")))
                        .subcommand(SubCommand::new("commit")
                            .about("Commit a snapshot")
                            .arg(Arg::new("NAME")
                                .index(1)
                                .help("Name of the snapshot")
                                .required(true)))
                        .get_matches();
 
    // Check for license flag
    if matches.is_present("lic") { license(); std::process::exit(0); }
    
    // Initialize sodium (must only be called once)
    sodiumoxide::init();
    
    match matches.subcommand() {
        ("snapshot", Some(matches)) => {
            let name = matches.value_of("NAME").unwrap().to_owned();
            let path = matches.value_of("PATH").unwrap();
            
            let backend = blob_store::FileBackend::new(blob_dir());
            let hat = hat::Hat::open_repository(&PathBuf::from("repo"), backend, MAX_BLOB_SIZE);
        
            let family_opt = hat.open_family(name.clone());
            let family = family_opt.expect(&format!("Could not open family '{}'", name));
    
            family.snapshot_dir(PathBuf::from(path));
            family.flush();
    
            println!("Waiting for final flush...");
        },
        ("checkout", Some(matches)) => {
            let name = matches.value_of("NAME").unwrap().to_owned();
            let path = matches.value_of("PATH").unwrap();
            
            let backend = blob_store::FileBackend::new(blob_dir());
            let hat = hat::Hat::open_repository(&PathBuf::from("repo"), backend, MAX_BLOB_SIZE);
    
            hat.checkout_in_dir(name.clone(), PathBuf::from(path));
        },
        ("commit", Some(matches)) => {
            let name = matches.value_of("NAME").unwrap().to_owned();
            
            let backend = blob_store::FileBackend::new(blob_dir());
            let hat = hat::Hat::open_repository(&PathBuf::from("repo"), backend, MAX_BLOB_SIZE);
    
            hat.commit(name);
        },
        _       => { 
            println!("{}\nFor more information re-run with --help", matches.usage());
            license();
            std::process::exit(1);
        }
    }
}
