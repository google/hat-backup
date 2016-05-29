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


use std::sync::mpsc;

use gc;
use hat;
use snapshot;


pub struct GcNoop<B> {
    backend: B,
}

impl<B: gc::GcBackend> gc::Gc for GcNoop<B> {
    type Err = hat::HatError;
    type Backend = B;

    fn new(backend: B) -> GcNoop<B>
        where B: gc::GcBackend
    {
        GcNoop { backend: backend }
    }

    fn register(&mut self,
                _snapshot: snapshot::Info,
                refs: mpsc::Receiver<gc::Id>)
                -> Result<(), Self::Err> {
        // It is an error to ignore the provided refereces, so we consume them here.
        refs.iter().last();
        Ok(())
    }

    fn register_final(&mut self,
                      _snapshot: snapshot::Info,
                      _ref_final: gc::Id)
                      -> Result<(), Self::Err> {
        Ok(())
    }

    fn register_cleanup(&mut self,
                        _snapshot: snapshot::Info,
                        _ref_final: gc::Id)
                        -> Result<(), Self::Err> {
        Ok(())
    }

    fn deregister<F>(&mut self,
                     _snapshot: snapshot::Info,
                     _final_ref: gc::Id,
                     _refs: F)
                     -> Result<(), Self::Err>
        where F: FnOnce() -> mpsc::Receiver<gc::Id>
    {
        Ok(())
    }

    fn list_unused_ids(&mut self, _refs: mpsc::Sender<gc::Id>) -> Result<(), Self::Err> {
        Ok(())
    }

    fn status(&mut self, _final_ref: gc::Id) -> Result<Option<gc::Status>, Self::Err> {
        Ok(Some(gc::Status::Complete))
    }
}

#[test]
fn gc_noop_test() {
    gc::gc_test::<GcNoop<_>, _>(vec![vec![1], vec![2], vec![1, 2, 3]],
                                move |backend| gc::Gc::new(backend),
                                gc::GcType::InExact);
}