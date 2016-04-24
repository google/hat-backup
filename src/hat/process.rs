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

use std::boxed::FnBox;
use std::fmt;
use std::thread;
use std::sync::mpsc;


pub struct Process<Msg, Reply> {
    sender: mpsc::SyncSender<(Msg, mpsc::Sender<Reply>)>,
    shutdown_after: Option<i64>,
}

/// When cloning a `process` we clone the input-channel, allowing multiple threads to share the same
/// `process`.
impl<Msg: Send, Reply: Send> Clone for Process<Msg, Reply> {
    fn clone(&self) -> Process<Msg, Reply> {
        Process {
            sender: self.sender.clone(),
            shutdown_after: None,
        }
    }
}

pub trait MsgHandler<Msg, Reply> {
    type Err;

    fn handle(&mut self, msg: Msg, callback: Box<Fn(Reply)>) -> Result<(), Self::Err>;
}

impl<Msg: 'static + Send, Reply: 'static + Send> Process<Msg, Reply> {
    /// Create and start a new process using `handler`.
    pub fn new<H>(handler_proc: Box<FnBox() -> Result<H, H::Err>>)
                  -> Result<Process<Msg, Reply>, H::Err>
        where H: 'static + MsgHandler<Msg, Reply> + Send,
              H::Err: From<mpsc::RecvError> + fmt::Debug
    {
        Process::new_with_shutdown(handler_proc, None)
    }

    pub fn new_with_shutdown<H>(handler_proc: Box<FnBox() -> Result<H, H::Err>>,
                                shutdown_after: Option<i64>)
                                -> Result<Process<Msg, Reply>, H::Err>
        where H: 'static + MsgHandler<Msg, Reply> + Send,
              H::Err: From<mpsc::RecvError> + fmt::Debug
    {
        let (sender, receiver) = mpsc::sync_channel(10);
        let p = Process {
            sender: sender,
            shutdown_after: shutdown_after,
        };

        try!(p.start(receiver, handler_proc));

        Ok(p)
    }


    fn start<H>(&self,
                receiver: mpsc::Receiver<(Msg, mpsc::Sender<Reply>)>,
                handler_proc: Box<FnBox() -> Result<H, H::Err>>)
                -> Result<(), H::Err>
        where H: 'static + MsgHandler<Msg, Reply> + Send,
              H::Err: From<mpsc::RecvError> + fmt::Debug
    {
        let shutdown_after = self.shutdown_after;
        let mut my_handler = try!(handler_proc());
        thread::spawn(move || {
            // fork handler
            let mut handle_msg = |msg, rep: mpsc::Sender<Reply>| {
                my_handler.handle(msg,
                                  Box::new(move |r| {
                                      rep.send(r).expect("Message reply was ignored");
                                  }))
                          .expect("Process encountered unrecoverable error");
            };
            match shutdown_after {
                Some(msg_count) => {
                    for _ in 0..msg_count {
                        if let Ok((msg, rep)) = receiver.recv() {
                            handle_msg(msg, rep)
                        } else {
                            break;  // Shutting down.
                        }
                    }
                }
                None => {
                    while let Ok((msg, rep)) = receiver.recv() {
                        handle_msg(msg, rep)
                    }
                }
            };
        });

        Ok(())
    }

    /// Synchronous send.
    ///
    /// Will always wait for a reply from the receiving `process`.
    pub fn send_reply(&self, msg: Msg) -> Reply {
        let (sender, receiver) = mpsc::channel();
        self.sender.send((msg, sender)).ok();

        receiver.recv().expect("Failed to get reply for msg")
    }
}
