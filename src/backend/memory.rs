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

use backend::StoreBackend;
use crypto::CipherText;
use std::collections::BTreeMap;
use std::sync::Mutex;

pub struct MemoryBackend {
    files: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl MemoryBackend {
    pub fn new() -> MemoryBackend {
        MemoryBackend {
            files: Mutex::new(BTreeMap::new()),
        }
    }

    fn guarded_insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String> {
        let mut guarded_files = self.files.lock().unwrap();
        if guarded_files.contains_key(&key) {
            return Err(format!("Key already exists: '{:?}'", key));
        }
        guarded_files.insert(key, value);
        Ok(())
    }

    fn guarded_retrieve(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        match self.files.lock() {
            Err(e) => Err(e.to_string()),
            Ok(map) => Ok(map.get(key).cloned()),
        }
    }

    fn guarded_delete(&self, key: &[u8]) -> Result<(), String> {
        let mut guarded_files = self.files.lock().unwrap();
        guarded_files.remove(key);
        Ok(())
    }

    fn guarded_list(&self) -> Result<Vec<Box<[u8]>>, String> {
        let guarded_files = self.files.lock().unwrap();
        Ok(guarded_files
            .keys()
            .cloned()
            .map(|x| x.into_boxed_slice())
            .collect())
    }
}

impl StoreBackend for MemoryBackend {
    fn store(&self, name: &[u8], data: &CipherText) -> Result<(), String> {
        self.guarded_insert(name.to_vec(), data.to_vec())
    }

    fn retrieve(&self, name: &[u8]) -> Result<Option<Vec<u8>>, String> {
        self.guarded_retrieve(name)
    }

    fn delete(&self, name: &[u8]) -> Result<(), String> {
        self.guarded_delete(name)
    }

    fn list(&self) -> Result<Vec<Box<[u8]>>, String> {
        self.guarded_list()
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}
