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

pub struct DevNullBackend;

impl StoreBackend for DevNullBackend {
    fn store(&self, _name: &[u8], _data: &CipherText) -> Result<(), String> {
        Ok(())
    }

    fn retrieve(&self, _name: &[u8]) -> Result<Option<Vec<u8>>, String> {
        Ok(None)
    }

    fn delete(&self, _name: &[u8]) -> Result<(), String> {
        Ok(())
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}
