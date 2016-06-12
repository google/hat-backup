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

use rustc_serialize::hex::ToHex;
use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Mutex;


use backend::StoreBackend;

pub struct FileBackend {
    root: PathBuf,
    read_cache: Mutex<BTreeMap<Vec<u8>, Result<Vec<u8>, String>>>,
    max_cache_size: usize,
}

impl FileBackend {
    pub fn new(root: PathBuf) -> FileBackend {
        FileBackend {
            root: root,
            read_cache: Mutex::new(BTreeMap::new()),
            max_cache_size: 10,
        }
    }

    fn guarded_cache_get(&self, name: &[u8]) -> Option<Result<Vec<u8>, String>> {
        self.read_cache.lock().unwrap().get(name).cloned()
    }

    fn guarded_cache_delete(&self, name: &[u8]) {
        self.read_cache.lock().unwrap().remove(name);
    }

    fn guarded_cache_put(&self, name: Vec<u8>, result: Result<Vec<u8>, String>) {
        let mut cache = self.read_cache.lock().unwrap();
        if cache.len() >= self.max_cache_size {
            cache.clear();
        }
        cache.insert(name, result);
    }
}

impl StoreBackend for FileBackend {
    fn store(&self, name: &[u8], data: &[u8]) -> Result<(), String> {
        let mut path = self.root.clone();
        path.push(&name.to_hex());

        let mut file = match fs::File::create(&path) {
            Err(e) => return Err(e.to_string()),
            Ok(f) => f,
        };

        match file.write_all(data) {
            Err(e) => Err(e.to_string()),
            Ok(()) => Ok(()),
        }
    }

    fn retrieve(&self, name: &[u8]) -> Result<Vec<u8>, String> {
        // Check for key in cache:
        let value_opt = self.guarded_cache_get(name);
        if let Some(r) = value_opt {
            return r;
        }

        // Read key:
        let path = {
            let mut p = self.root.clone();
            p.push(&name.to_hex());
            p
        };

        let mut fd = fs::File::open(&path).unwrap();
        let mut buf = Vec::new();
        let res = match fd.read_to_end(&mut buf) {
            Ok(_) => Ok(buf),
            Err(e) => Err(e.to_string()),
        };

        // Update cache to contain key:
        self.guarded_cache_put(name.to_vec(), res.clone());

        res
    }

    fn delete(&self, name: &[u8]) -> Result<(), String> {
        let name = name.to_vec();
        self.guarded_cache_delete(&name);

        let path = {
            let mut p = self.root.clone();
            p.push(&name.to_hex());
            p
        };

        match fs::remove_file(&path) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}
