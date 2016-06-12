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

use key::*;
use key::tests::*;
use std::sync::Arc;

use backend::DevNullBackend;
use util::Process;
use test::Bencher;

#[bench]
fn insert_1_key_x_128000_zeros(bench: &mut Bencher) {
    let backend = Arc::new(DevNullBackend);
    let ks_p: StoreProcess<EntryStub, _> = Process::new(move || Store::new_for_testing(backend))
        .unwrap();

    let bytes = vec![0u8; 128*1024];

    let mut i = 0i32;
    bench.iter(|| {
        i += 1;

        let entry = EntryStub {
            data: Some(vec![bytes.clone()]),
            key_entry: Entry {
                parent_id: None,
                id: None,
                name: format!("{}", i).as_bytes().to_vec(),
                created: None,
                modified: None,
                accessed: None,
                group_id: None,
                user_id: None,
                permissions: None,
                data_hash: None,
                data_length: None,
            },
        };

        ks_p.send_reply(Msg::Insert(entry.key_entry.clone(),
                                    Some(Box::new(move |()| Some(entry)))))
            .unwrap();
    });

    bench.bytes = 128 * 1024;

}

#[bench]
fn insert_1_key_x_128000_unique(bench: &mut Bencher) {
    let backend = Arc::new(DevNullBackend);
    let ks_p: StoreProcess<EntryStub, _> = Process::new(move || Store::new_for_testing(backend))
        .unwrap();

    let bytes = vec![0u8; 128*1024];

    let mut i = 0i32;
    bench.iter(|| {
        i += 1;

        let mut my_bytes = bytes.clone();
        my_bytes[0] = i as u8;
        my_bytes[1] = (i / 256) as u8;
        my_bytes[2] = (i / 65536) as u8;

        let entry = EntryStub {
            data: Some(vec![my_bytes]),
            key_entry: Entry {
                parent_id: None,
                id: None,
                name: format!("{}", i).as_bytes().to_vec(),
                created: None,
                modified: None,
                accessed: None,
                group_id: None,
                user_id: None,
                permissions: None,
                data_hash: None,
                data_length: None,
            },
        };

        ks_p.send_reply(Msg::Insert(entry.key_entry.clone(),
                                    Some(Box::new(move |()| Some(entry)))))
            .unwrap();
    });

    bench.bytes = 128 * 1024;
}

#[bench]
fn insert_1_key_x_16_x_128000_zeros(bench: &mut Bencher) {
    let backend = Arc::new(DevNullBackend);
    let ks_p: StoreProcess<EntryStub, _> = Process::new(move || Store::new_for_testing(backend))
        .unwrap();

    bench.iter(|| {
        let bytes = vec![0u8; 128*1024];

        let entry = EntryStub {
            data: Some(vec![bytes; 16]),
            key_entry: Entry {
                parent_id: None,
                id: None,
                name: vec![1u8, 2, 3].to_vec(),
                created: None,
                modified: None,
                accessed: None,
                group_id: None,
                user_id: None,
                permissions: None,
                data_hash: None,
                data_length: None,
            },
        };
        ks_p.send_reply(Msg::Insert(entry.key_entry.clone(),
                                    Some(Box::new(move |()| Some(entry)))))
            .unwrap();

        match ks_p.send_reply(Msg::Flush).unwrap() {
            Reply::FlushOk => (),
            _ => panic!("Unexpected result from key store."),
        }
    });

    bench.bytes = 16 * (128 * 1024);
}

#[bench]
fn insert_1_key_x_16_x_128000_unique(bench: &mut Bencher) {
    let backend = Arc::new(DevNullBackend);
    let ks_p: StoreProcess<EntryStub, _> = Process::new(move || Store::new_for_testing(backend))
        .unwrap();

    let bytes = vec![0u8; 128*1024];
    let mut i = 0i32;

    bench.iter(|| {
        i += 1;

        let mut my_bytes = bytes.clone();
        my_bytes[0] = i as u8;
        my_bytes[1] = (i / 256) as u8;
        my_bytes[2] = (i / 65536) as u8;

        let mut chunks = vec![];
        for i in 0..16 {
            let mut local_bytes = my_bytes.clone();
            local_bytes[3] = i as u8;
            chunks.push(local_bytes);
        }

        let entry = EntryStub {
            data: Some(chunks),
            key_entry: Entry {
                parent_id: None,
                id: None,
                name: vec![1u8, 2, 3],
                created: None,
                modified: None,
                accessed: None,
                group_id: None,
                user_id: None,
                permissions: None,
                data_hash: None,
                data_length: None,
            },
        };

        ks_p.send_reply(Msg::Insert(entry.key_entry.clone(),
                                    Some(Box::new(move |()| Some(entry)))))
            .unwrap();

        match ks_p.send_reply(Msg::Flush).unwrap() {
            Reply::FlushOk => (),
            _ => panic!("Unexpected result from key store."),
        }
    });

    bench.bytes = 16 * (128 * 1024);
}

#[bench]
fn insert_1_key_unchanged_empty(bench: &mut Bencher) {
    let backend = Arc::new(DevNullBackend);
    let ks_p: StoreProcess<EntryStub, _> = Process::new(move || Store::new_for_testing(backend))
        .unwrap();

    bench.iter(|| {
        let entry = EntryStub {
            data: None,
            key_entry: Entry {
                parent_id: None,
                id: None,
                name: vec![1u8, 2, 3].to_vec(),
                created: Some(0),
                modified: Some(0),
                accessed: Some(0),
                group_id: None,
                user_id: None,
                permissions: None,
                data_hash: None,
                data_length: None,
            },
        };
        ks_p.send_reply(Msg::Insert(entry.key_entry.clone(), None)).unwrap();
    });
}

#[bench]
fn insert_1_key_updated_empty(bench: &mut Bencher) {
    let backend = Arc::new(DevNullBackend);
    let ks_p: StoreProcess<EntryStub, _> = Process::new(move || Store::new_for_testing(backend))
        .unwrap();

    let mut i = 0;
    bench.iter(|| {
        i += 1;
        let entry = EntryStub {
            data: None,
            key_entry: Entry {
                parent_id: None,
                id: None,
                name: vec![1u8, 2, 3].to_vec(),
                created: Some(i),
                modified: Some(i),
                accessed: Some(i),
                group_id: None,
                user_id: None,
                permissions: None,
                data_hash: None,
                data_length: None,
            },
        };
        ks_p.send_reply(Msg::Insert(entry.key_entry.clone(), None)).unwrap();
    });
}

#[bench]
fn insert_1_key_unique_empty(bench: &mut Bencher) {
    let backend = Arc::new(DevNullBackend);
    let ks_p: StoreProcess<EntryStub, _> = Process::new(move || Store::new_for_testing(backend))
        .unwrap();

    let mut i = 0;
    bench.iter(|| {
        i += 1;
        let entry = EntryStub {
            data: None,
            key_entry: Entry {
                parent_id: None,
                id: None,
                name: format!("{}", i).as_bytes().to_vec(),
                created: Some(i),
                modified: Some(i),
                accessed: Some(i),
                group_id: None,
                user_id: None,
                permissions: None,
                data_hash: None,
                data_length: None,
            },
        };
        ks_p.send_reply(Msg::Insert(entry.key_entry.clone(), None)).unwrap();
    });
}
