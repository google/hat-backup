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

// Import the hat library
extern crate hat;

// Rust crates.
extern crate env_logger;
extern crate libsodium_sys;

// We use Clap for argument parsing.
#[macro_use]
extern crate clap;

use std::env;
use clap::{App, SubCommand};

use hat::backend;
use std::borrow::ToOwned;
use std::convert::From;
use std::path::{Path, PathBuf};
use std::sync::Arc;

static MAX_BLOB_SIZE: usize = 4 * 1024 * 1024;

fn blob_dir() -> PathBuf {
    PathBuf::from("blobs")
}

fn license() {
    println!(include_str!("../LICENSE"));
    println!("clap (Command Line Argument Parser) License:");
    println!(include_str!("../LICENSE-CLAP"));
}


fn main() {
    env_logger::init().unwrap();

    // Because "snapshot" and "checkout" use the exact same type of arguments, we can make a
    // template. This template defines two positional arguments, both are required
    let arg_template = "<NAME> 'Name of the snapshot'
                        <PATH> 'The path of the snapshot'";

    // Create valid arguments
    let matches = App::new("hat")
        .version(&format!("v{}", crate_version!())[..])
        .about("Create backup snapshots")
        .args_from_usage("-l, --license 'Display the license'
                          --hat_migrations_dir=[DIR] 'Location of Hat SQL migrations'
                          --hat_cache_dir=[DIR] 'Location of Hat local state'")
        .subcommand(SubCommand::with_name("commit")
                        .about("Commit a new snapshot")
                        .args_from_usage(arg_template))
        .subcommand(SubCommand::with_name("checkout")
                        .about("Checkout a snapshot")
                        .args_from_usage(arg_template))
        .subcommand(SubCommand::with_name("recover").about("Recover list of commit'ed snapshots"))
        .subcommand(SubCommand::with_name("delete")
                        .about("Delete a snapshot")
                        .args_from_usage("<NAME> 'Name of the snapshot family'
                                                        \
                              <ID> 'The snapshot id to delete'"))
        .subcommand(SubCommand::with_name("gc")
                        .about("Garbage collect: identify and remove unused data blocks.")
                        .args_from_usage("-p --pretend 'Do not modify any data'"))
        .subcommand(SubCommand::with_name("resume").about("Resume previous failed command."))
        .get_matches();

    // Check for license flag
    if matches.is_present("license") {
        license();
        std::process::exit(0);
    }

    let flag_or_env = |name: &str| {
        matches
            .value_of(name)
            .map(|x| x.to_string())
            .or_else(|| env::var_os(name.to_uppercase()).map(|s| s.into_string().unwrap()))
            .expect(&format!("{} required", name))
    };

    // Setup config variables that can take their value from either flag or environment.
    let migrations_dir_str = flag_or_env("hat_migrations_dir");
    let migrations_dir = Path::new(&migrations_dir_str);
    let cache_dir = PathBuf::from(flag_or_env("hat_cache_dir"));

    // Initialize sodium (must only be called once)
    unsafe { libsodium_sys::sodium_init() };

    match matches.subcommand() {
        ("resume", Some(_cmd)) => {
            // Setting up the repository triggers automatic resume.
            let backend = Arc::new(backend::FileBackend::new(blob_dir()));
            hat::Hat::open_repository(migrations_dir, cache_dir, backend, MAX_BLOB_SIZE).unwrap();
        }
        ("commit", Some(cmd)) => {
            let name = cmd.value_of("NAME").unwrap().to_owned();
            let path = cmd.value_of("PATH").unwrap();

            let backend = Arc::new(backend::FileBackend::new(blob_dir()));
            let mut hat =
                hat::Hat::open_repository(migrations_dir, cache_dir, backend, MAX_BLOB_SIZE)
                    .unwrap();

            // Update the family index.
            let mut family = hat.open_family(name.clone())
                .expect(&format!("Could not open family '{}'", name));
            family.snapshot_dir(PathBuf::from(path));

            // Commit the updated index.
            hat.commit(&mut family, None).unwrap();

            // Meta commit.
            hat.meta_commit().unwrap();

            // Flush any remaining blobs.
            hat.data_flush().unwrap();
        }
        ("checkout", Some(cmd)) => {
            let name = cmd.value_of("NAME").unwrap().to_owned();
            let path = cmd.value_of("PATH").unwrap();

            let backend = Arc::new(backend::FileBackend::new(blob_dir()));
            let mut hat =
                hat::Hat::open_repository(migrations_dir, cache_dir, backend, MAX_BLOB_SIZE)
                    .unwrap();

            hat.checkout_in_dir(name, PathBuf::from(path)).unwrap();
        }
        ("recover", Some(_cmd)) => {
            let backend = Arc::new(backend::FileBackend::new(blob_dir()));
            let mut hat =
                hat::Hat::open_repository(migrations_dir, cache_dir, backend, MAX_BLOB_SIZE)
                    .unwrap();

            hat.recover().unwrap();
        }
        ("delete", Some(cmd)) => {
            let name = cmd.value_of("NAME").unwrap().to_owned();
            let id = cmd.value_of("ID").unwrap().to_owned();

            let backend = Arc::new(backend::FileBackend::new(blob_dir()));
            let mut hat =
                hat::Hat::open_repository(migrations_dir, cache_dir, backend, MAX_BLOB_SIZE)
                    .unwrap();

            hat.deregister_by_name(name, id.parse::<u64>().unwrap())
                .unwrap();
        }
        ("gc", Some(_cmd)) => {
            let backend = Arc::new(backend::FileBackend::new(blob_dir()));
            let mut hat =
                hat::Hat::open_repository(migrations_dir, cache_dir, backend, MAX_BLOB_SIZE)
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
