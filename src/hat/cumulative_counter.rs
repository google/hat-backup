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

use std::num::{One, one};


pub struct CumulativeCounter<N> {
  previous: N,
}


impl <N: Clone + Num + One> CumulativeCounter<N> {

  pub fn new(previous: N) -> CumulativeCounter<N> {
    CumulativeCounter{previous: previous}
  }

  pub fn next(&mut self) -> N {
    self.previous = self.previous.add(&one());
    self.previous.clone()
  }

}