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

use std::old_io::{Timer};
use std::sync::mpsc::{Receiver};
use std::time::duration::{Duration};

pub struct PeriodicTimer {
  timer: Timer,
  periodic: Receiver<()>,
}


impl PeriodicTimer {

  pub fn new(interval: Duration) -> PeriodicTimer {
    let mut pt = PeriodicTimer{timer: Timer::new().unwrap(),
                               periodic: Timer::new().unwrap().periodic(interval)};
    pt.periodic = pt.timer.periodic(interval);
    pt
  }

  pub fn did_fire(&mut self) -> bool {
    let mut fired = false;
    while self.periodic.try_recv().is_ok() {
      fired = true;
    }
    return fired;
  }

}
