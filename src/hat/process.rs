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

use std::sync::{Arc, Mutex};

/// A long-living `thread` is promoted to a standard `process`.
///
/// To create a new `process`, simply define a `Msg` type, a `Reply` type and a state struct
/// that implements the `MsgHandler` trait. You can then create a new process with:
///
/// ```rust
/// enum Msg (Ping, Foo);
/// enum Reply (Pong, Bar);
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
/// times will likely cause runtime failure when using `send_reply()`.

pub struct Process<Msg, Reply, Handler>
{
  sender: Sender<(Msg, Option<Sender<Reply>>)>,
  ticket_count: Arc<Mutex<i32>>,
}

/// When cloning a `process` we clone the input-channel, allowing multiple threads to share the same
/// `process`.
impl <Msg: Send, Reply: Send, Handler> Clone
  for Process<Msg, Reply, Handler> {
  fn clone(&self) -> Process<Msg, Reply, Handler> {
    Process{sender: self.sender.clone(),
            ticket_count: self.ticket_count.clone()}
  }
}

pub trait MsgHandler<Msg, Reply> {
  fn handle(&mut self, msg: Msg, callback: |Reply|);
}

impl <Msg:Send, Reply:Send, Handler: MsgHandler<Msg, Reply>> Process<Msg, Reply, Handler>
{

  /// Create and start a new process using `handler`.
  pub fn new(handler_proc: proc():Send -> Handler) -> Process<Msg, Reply, Handler> {

    let (sender, receiver) = channel();
    let p = Process{
      sender: sender,
      ticket_count: Arc::new(Mutex::new(10)),
    };

    p.start(receiver, handler_proc);

    p
  }


  fn start(&self, receiver: Receiver<(Msg, Option<Sender<Reply>>)>,
           handler_proc: proc():Send -> Handler) {
    let local_ticket_count = self.ticket_count.clone();
    spawn(proc() {
      // fork handler
      let mut my_handler = handler_proc();
      loop {
        { // increment available queueing tickets by one
          let mut guarded_tickets = local_ticket_count.lock();
          *guarded_tickets += 1;
          guarded_tickets.cond.signal();
        }
        match receiver.recv_opt() {
          Ok((msg, None)) => {
            my_handler.handle(msg, |_r: Reply| {});
          },
          Ok((msg, Some(rep))) => {
            my_handler.handle(msg, |r| { rep.send(r) });
          },
          Err(()) => break,
        };
      };
    });
  }

  fn take_ticket(&self) {
    let mut guarded_tickets = self.ticket_count.lock();
    while *guarded_tickets <= 0 {
      // release lock while waiting for change
      guarded_tickets.cond.wait();
    }
    *guarded_tickets -= 1;
  }

  /// Asynchronous send.
  ///
  /// Will block if the bounded input-channel is full, but otherwise return immediately.
  pub fn send(&self, msg: Msg) {
    self.take_ticket();
    self.sender.send((msg, None));
  }

  /// Synchronous send.
  ///
  /// Will always wait for a reply from the receiving `process`.
  pub fn send_reply(&self, msg: Msg) -> Reply {
    self.take_ticket();
    let (sender, receiver) = channel();
    self.sender.send((msg, Some(sender)));
    return receiver.recv();
  }
}
