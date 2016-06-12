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

use std::collections::BTreeMap;
use std::sync::Mutex;

use backend::StoreBackend;

pub struct MemoryBackend {
    files: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl MemoryBackend {
    pub fn new() -> MemoryBackend {
        MemoryBackend { files: Mutex::new(BTreeMap::new()) }
    }

    fn guarded_insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String> {
        let mut guarded_files = self.files.lock().unwrap();
        if guarded_files.contains_key(&key) {
            return Err(format!("Key already exists: '{:?}'", key));
        }
        guarded_files.insert(key, value);
        Ok(())
    }

    fn guarded_retrieve(&self, key: &[u8]) -> Result<Vec<u8>, String> {
        let value_opt = self.files.lock().unwrap().get(&key.to_vec()).map(|v| v.clone());
        value_opt.map(|v| Ok(v)).unwrap_or_else(|| Err(format!("Unknown key: '{:?}'", key)))
    }

    fn guarded_delete(&self, key: &[u8]) -> Result<(), String> {
        let mut guarded_files = self.files.lock().unwrap();
        guarded_files.remove(key);
        Ok(())
    }
}

impl StoreBackend for MemoryBackend {
    fn store(&self, name: &[u8], data: &[u8]) -> Result<(), String> {
        self.guarded_insert(name.to_vec(), data.to_vec())
    }

    fn retrieve(&self, name: &[u8]) -> Result<Vec<u8>, String> {
        self.guarded_retrieve(name)
    }

    fn delete(&self, name: &[u8]) -> Result<(), String> {
        self.guarded_delete(name)
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}
