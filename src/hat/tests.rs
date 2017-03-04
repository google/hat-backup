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


use backend::{MemoryBackend, StoreBackend};
use errors::HatError;
use hat::HatRc;
use hat::family::Family;
use key;
use std::collections::HashMap;
use std::sync::Arc;
use util::FileIterator;


pub fn setup_hat<B: StoreBackend>(backend: Arc<B>) -> HatRc<B> {
    let max_blob_size = 4 * 1024 * 1024;
    HatRc::new_for_testing(backend, max_blob_size).unwrap()
}

fn setup_family() -> (Arc<MemoryBackend>, HatRc<MemoryBackend>, Family<MemoryBackend>) {
    let backend = Arc::new(MemoryBackend::new());
    let hat = setup_hat(backend.clone());

    let family = "familyname".to_string();
    let fam = hat.open_family(family).unwrap();

    (backend, hat, fam)
}

pub fn entry(name: Vec<u8>) -> key::Entry {
    key::Entry {
        id: None,
        parent_id: None,
        data_hash: None,
        info: key::Info {
            name: name,
            created: None,
            modified: None,
            accessed: None,
            permissions: None,
            user_id: None,
            group_id: None,
            byte_length: None,
        },
    }
}

fn snapshot_files<B: StoreBackend>(family: &Family<B>,
                                   files: Vec<(&str, Vec<u8>)>)
                                   -> Result<(), HatError> {
    let mut dirs = HashMap::new();
    for (name, contents) in files {
        let mut parent = None;
        let mut parts = name.split("/").peekable();
        let mut current = parts.next().unwrap();
        loop {
            if parts.peek().is_none() {
                // Reached the filename part of the string.
                break;
            }
            let mut e = entry(current.bytes().collect());
            e.parent_id = parent.clone();

            parent = dirs.entry((parent, current))
                .or_insert_with(|| {
                    Some(family.snapshot_direct(e, true, None)
                        .unwrap())
                })
                .clone();
            current = parts.next().unwrap();
        }
        if current.len() > 0 {
            // We have a file to insert.
            let mut e = entry(current.bytes().collect());
            e.parent_id = parent.clone();
            family.snapshot_direct(e, false, Some(FileIterator::from_bytes(contents)))?;
        }
    }
    Ok(())
}

fn basic_snapshot<B: StoreBackend>(fam: &Family<B>) {
    snapshot_files(&fam,
                   vec![("zeros", vec![0; 1000000]),
                        ("ones", vec![1; 1000000]),
                        ("twos", vec![2; 1000000]),
                        ("zeros2", vec![0; 1000000]),
                        ("block1", "block1".into()),
                        ("block2", "block2".into()),
                        ("block3", "block3".into()),
                        ("block12", "block1".into()),
                        ("some/number/of/empty/dirs/with/an/empty/file/deep/down", vec![]),
                        ("dir1/zeros", vec![0; 10]),
                        ("dir1/block1", "block1".into()),
                        ("dir1/unique", "abcdefg".into()),
                        ("dir2/zeros", vec![0; 10]),
                        ("dir2/dir3/twos", vec![2; 10]),
                        ("dir2/dir3/dir4/ones", vec![1; 10]),
                        ("x/y/z/a", vec![]),
                        ("x/y/z/b/", vec![]),
                        ("z/y/x/a", vec![]),
                        ("y/x/z/b/", vec![]),
                        ("a/", vec![]),
                        ("b/", vec![]),
                        ("c/", vec![])])
        .unwrap();
}

#[test]
fn snapshot_commit() {
    let (_, mut hat, fam) = setup_family();

    basic_snapshot(&fam);

    fam.flush().unwrap();
    hat.commit(&fam, None).unwrap();
    hat.meta_commit().unwrap();

    let (deleted, live) = hat.gc().unwrap();
    assert_eq!(deleted, 0);
    assert!(live > 0);
}

#[test]
fn snapshot_commit_many_empty_files() {
    let (_, mut hat, fam) = setup_family();

    let names: Vec<String> = (0..3000).map(|i| format!("name-{}", i)).collect();
    snapshot_files(&fam, names.iter().map(|n| (n.as_str(), vec![])).collect()).unwrap();

    fam.flush().unwrap();
    hat.commit(&fam, None).unwrap();
    hat.meta_commit().unwrap();

    let (deleted, live) = hat.gc().unwrap();
    assert_eq!(deleted, 0);
    assert!(live > 0);

    hat.deregister(&fam, 1).unwrap();
    let (deleted, live) = hat.gc().unwrap();
    assert!(deleted > 0);
    assert_eq!(live, 0);
}

#[test]
fn snapshot_commit_many_empty_directories() {
    let (_, mut hat, fam) = setup_family();

    for i in 0..3000 {
        fam.snapshot_direct(entry(format!("name-{}", i).bytes().collect()), true, None)
            .unwrap();
    }

    fam.flush().unwrap();
    hat.commit(&fam, None).unwrap();
    hat.meta_commit().unwrap();

    let (deleted, live) = hat.gc().unwrap();
    assert_eq!(deleted, 0);
    assert!(live > 0);

    hat.deregister(&fam, 1).unwrap();
    let (deleted, live) = hat.gc().unwrap();
    assert!(deleted > 0);
    assert_eq!(live, 0);
}

#[test]
fn snapshot_reuse_index() {
    let (_, mut hat, fam) = setup_family();

    // Insert hashes.
    basic_snapshot(&fam);
    fam.flush().unwrap();

    // Reuse hashes.
    basic_snapshot(&fam);
    basic_snapshot(&fam);
    fam.flush().unwrap();

    // No commit, so GC removes all the new hashes.
    let (deleted, live) = hat.gc().unwrap();
    assert!(deleted > 0);
    assert_eq!(live, 0);

    // Update index and reinsert hashes.
    basic_snapshot(&fam);
    fam.flush().unwrap();

    // Commit.
    hat.commit(&fam, None).unwrap();
    let (deleted, live) = hat.gc().unwrap();
    assert_eq!(deleted, 0);
    assert!(live > 0);

    // Inserting again does not increase number of hashes.
    basic_snapshot(&fam);
    fam.flush().unwrap();
    let (deleted2, live2) = hat.gc().unwrap();
    assert_eq!(live2, live);
    assert_eq!(deleted2, 0);

    // Cleanup: only 1 snapshot was committed.
    hat.deregister(&fam, 1).unwrap();
    let (deleted, live) = hat.gc().unwrap();
    assert!(deleted > 0);
    assert_eq!(live, 0);
}

#[test]
fn snapshot_gc() {
    let (_, mut hat, fam) = setup_family();

    basic_snapshot(&fam);
    fam.flush().unwrap();

    // No commit so everything is deleted.
    let (deleted, live) = hat.gc().unwrap();
    assert!(deleted > 0);
    assert_eq!(live, 0);
}

#[test]
fn recover() {
    // Prepare a snapshot.
    let (backend, mut hat, fam) = setup_family();
    basic_snapshot(&fam);
    fam.flush().unwrap();

    hat.commit(&fam, None).unwrap();
    hat.meta_commit().unwrap();

    let (deleted, live1) = hat.gc().unwrap();
    assert_eq!(deleted, 0);
    assert!(live1 > 0);

    // Create a new hat to wipe the index states.
    let mut hat2 = setup_hat(backend);

    // Recover index states.
    hat2.recover().unwrap();

    // Check that we now reference all the blobs.
    let (deleted, live2) = hat2.gc().unwrap();
    assert_eq!(deleted, 0);
    assert_eq!(live1, live2);

    // Check that we can delete the snapshot.
    hat2.deregister(&fam, 1).unwrap();

    let (deleted, live3) = hat2.gc().unwrap();
    assert!(deleted > 0);
    assert_eq!(live3, 0);
}
