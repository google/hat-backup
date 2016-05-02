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

use std::io;
use std::str;


pub struct InfoWriter;

impl io::Write for InfoWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        info!("{}", str::from_utf8(&buf[..]).unwrap());
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// TODO(idolf): This is a hack until FnBox gets stabilized, see issue
// rust-lang/rust#28796.  When this issue gets stabilized, remove
// this and use the the library implementation. Then FnOnce gets a
// proper implementation, use that.
//
pub trait FnBox<A, B>: Send {
    fn call(self: Box<Self>, args: A) -> B;
}

impl<A, B, F> FnBox<A, B> for F
    where F: FnOnce(A) -> B + Send
{
    fn call(self: Box<F>, args: A) -> B {
        self(args)
    }
}
