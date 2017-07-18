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

//! Standardized isolation of logic as a `process` wrapper around a `thread`.
//!
//! Basic wrapper around threads to provide some standard API. All long-living Hat threads run
//! as a `process`. The main difference between a normal Rust `thread` and a Hat `process` is
//! currently that the `process` has a bounded input-channel and a standard implementation of a
//! synchronous `send_reply()`.

/// A long-living `thread` is promoted to a standard `process`.
///
/// To create a new `process`, simply define a `Msg` type, a `Reply` type and a state struct
/// that implements the `MsgHandler` trait. You can then create a new process with:
///
/// ```rust,ignore
/// enum Msg {Ping, Foo}
/// enum Reply {Pong, Bar}
///
/// struct MyHandler {
///   fn handle(&mut self, msg: Msg, reply: |Reply|) {
///     match Msg {
///       Ping => return reply(Pong),
///       Foo => return reply(Bar),
///     }
///   }
/// }
///
/// fn main() {
///   let p = Process::new(MyHandler::new());
///   let pong = p.send_reply(Ping);
/// }
/// ```
///
/// The handler must always call `reply()` to release any call to `send_reply()`. It is allowed to
/// call `reply()` "early" - before all work has been done - but calling `reply()` is usually the
/// last thing a handler does. The idiom `return reply(...)` is a simple way of guaranteeing this.
///
/// A handler is allowed to call `reply()` **exactly** once. Not calling it or calling it multiple
/// times will likely cause runtime panicure when using `send_reply()`.

use std::fmt;
use std::sync::mpsc;
use std::thread;


pub struct Process<Msg, Reply, E> {
    sender: mpsc::SyncSender<(Msg, mpsc::Sender<Result<Reply, E>>)>,
}

/// When cloning a `process` we clone the input-channel, allowing multiple threads to share the same
/// `process`.
impl<Msg: Send, Reply: Send, E> Clone for Process<Msg, Reply, E> {
    fn clone(&self) -> Process<Msg, Reply, E> {
        Process { sender: self.sender.clone() }
    }
}

pub trait MsgHandler<Msg, Reply>: 'static + Send {
    type Err;

    fn handle<F: FnOnce(Result<Reply, Self::Err>)>(
        &mut self,
        msg: Msg,
        callback: F,
    ) -> Result<(), Self::Err>;
}

impl<Msg, Reply, E> Process<Msg, Reply, E>
where
    Msg: 'static + Send,
    Reply: 'static + Send,
    E: 'static + Send + fmt::Debug,
{
    /// Create and start a new process using `handler`.
    pub fn new<H>(mut handler: H) -> Process<Msg, Reply, E>
    where
        H: MsgHandler<Msg, Reply, Err = E>,
    {
        let (sender, receiver) = mpsc::sync_channel::<(Msg, mpsc::Sender<Result<Reply, E>>)>(10);

        thread::spawn(move || while let Ok((msg, rep)) = receiver.recv() {
            let mut did_reply = false;
            let ret = handler.handle(msg, |r| {
                did_reply = true;
                rep.send(r).expect("Message reply ignored");
            });
            match (ret, did_reply) {
                (Ok(()), true) => (),
                (Ok(()), false) => panic!("Handler returned without replying"),
                (Err(e), true) => panic!("Encountered unrecoverable error: {:?}", e),
                (Err(e), false) => rep.send(Err(e)).expect("Message reply ignored"),
            }
        });

        Process { sender: sender }
    }

    /// Synchronous send.
    ///
    /// Will always wait for a reply from the receiving `process`.
    pub fn send_reply(&self, msg: Msg) -> Result<Reply, E> {
        let (sender, receiver) = mpsc::channel();

        self.sender.send((msg, sender)).expect(
            "Could not send message; process looks dead",
        );
        receiver.recv().expect("Could not read reply")
    }
}
