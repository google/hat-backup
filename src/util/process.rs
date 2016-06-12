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
use std::thread;
use std::sync::{mpsc, Arc, Mutex};


enum InternalMsg<M> {
    Msg(M),
    Reset,
}

enum InternalReply<R, E> {
    Reply(Result<R, E>),
    Reset(Result<(), E>),
}

pub struct Process<Msg, Reply, E> {
    sender: mpsc::SyncSender<(InternalMsg<Msg>, mpsc::Sender<InternalReply<Reply, E>>)>,
    poison_after: Option<i64>,
    poisoned: Arc<Mutex<bool>>,
}

/// When cloning a `process` we clone the input-channel, allowing multiple threads to share the same
/// `process`.
impl<Msg: Send, Reply: Send, E> Clone for Process<Msg, Reply, E> {
    fn clone(&self) -> Process<Msg, Reply, E> {
        Process {
            sender: self.sender.clone(),
            poison_after: None,
            poisoned: self.poisoned.clone(),
        }
    }
}

pub trait MsgHandler<Msg, Reply> {
    type Err;

    fn handle<F: FnOnce(Result<Reply, Self::Err>)>(&mut self,
                                                   msg: Msg,
                                                   callback: F)
                                                   -> Result<(), Self::Err>;

    fn reset(&mut self) -> Result<(), Self::Err>;
}

impl<Msg: 'static + Send, Reply: 'static + Send, E> Process<Msg, Reply, E> {
    /// Create and start a new process using `handler`.
    pub fn new<H, F>(handler_proc: F) -> Result<Process<Msg, Reply, E>, E>
        where H: 'static + MsgHandler<Msg, Reply, Err = E> + Send,
              E: 'static + From<mpsc::RecvError> + fmt::Debug + Send,
              F: FnOnce() -> Result<H, E>
    {
        Process::new_with_poison(handler_proc, None)
    }

    pub fn new_with_poison<H, F>(handler_proc: F,
                                 poison_after: Option<i64>)
                                 -> Result<Process<Msg, Reply, E>, E>
        where H: 'static + MsgHandler<Msg, Reply, Err = E> + Send,
              E: 'static + From<mpsc::RecvError> + fmt::Debug + Send,
              F: FnOnce() -> Result<H, E>
    {
        let (sender, receiver) = mpsc::sync_channel(10);
        let p = Process {
            sender: sender,
            poison_after: poison_after,
            poisoned: Arc::new(Mutex::new(false)),
        };

        try!(p.start(receiver, handler_proc));

        Ok(p)
    }

    fn start<H, F>(&self,
                   receiver: mpsc::Receiver<(InternalMsg<Msg>,
                                             mpsc::Sender<InternalReply<Reply, E>>)>,
                   handler_proc: F)
                   -> Result<(), E>
        where H: 'static + MsgHandler<Msg, Reply, Err = E> + Send,
              E: 'static + From<mpsc::RecvError> + fmt::Debug + Send,
              F: FnOnce() -> Result<H, E>
    {
        let poison_after = self.poison_after;
        let mut my_handler = try!(handler_proc());

        let poisoned = self.poisoned.clone();

        thread::spawn(move || {
            // fork handler
            let mut handle_msg = |m| {
                match m {
                    (InternalMsg::Reset, rep) => {
                        let rep = rep as mpsc::Sender<InternalReply<Reply, E>>;
                        let result = my_handler.reset();
                        rep.send(InternalReply::Reset(result)).unwrap();
                    }
                    (InternalMsg::Msg(msg), rep) => {
                        let mut did_reply = false;
                        let rep = rep as mpsc::Sender<InternalReply<Reply, E>>;
                        let ret = my_handler.handle(msg, |r| {
                            did_reply = true;
                            rep.send(InternalReply::Reply(r))
                                .expect("Message reply ignored");
                        });
                        match ret {
                            Ok(()) => assert!(did_reply),
                            Err(e) => {
                                if did_reply {
                                    return Err(format!("Encountered unrecoverable error: {:?}", e));
                                } else {
                                    rep.send(InternalReply::Reply(Err(e)))
                                        .expect("Message reply ignored");
                                }
                            }
                        }
                    }
                }
                Ok(())
            };
            let mut do_loop = || {
                match poison_after {
                    Some(msg_count) => {
                        for _ in 0..msg_count {
                            if let Ok(m) = receiver.recv() {
                                try!(handle_msg(m))
                            } else {
                                return Ok(());  // Clean shutdown.
                            }
                        }
                        return Err("Process has reached message limit".to_string());
                    }
                    None => {
                        while let Ok(m) = receiver.recv() {
                            try!(handle_msg(m))
                        }
                    }
                };
                Ok(())
            };
            while let Err(e) = do_loop() {
                *poisoned.lock().unwrap() = true;  // This process is in a polluted state.
                warn!("Poisoned process: {:?}", e);
            }
        });

        Ok(())
    }

    /// Reset in-memory state of a poisoned process, making it available again.
    pub fn reset(&mut self) -> Result<(), E>
        where E: From<String>
    {
        let (sender, receiver) = mpsc::channel();

        if !*self.poisoned.lock().unwrap() {
            return Err(From::from("This process has not been poisoned".to_string()));
        }

        if let Err(e) = self.sender.send((InternalMsg::Reset, sender)) {
            return Err(From::from(format!("Could not send Reset message; process looks dead: {}",
                                          e)));
        }

        match receiver.recv() {
            Err(e) => Err(From::from(format!("Could not read reply: {}", e))),
            Ok(InternalReply::Reset(result)) => result,
            _ => Err(From::from("Unexpected internal reply from process".to_string())),
        }
    }

    /// Synchronous send.
    ///
    /// Will always wait for a reply from the receiving `process`.
    pub fn send_reply(&self, msg: Msg) -> Result<Reply, E>
        where E: From<String>
    {
        let (sender, receiver) = mpsc::channel();

        if *self.poisoned.lock().unwrap() {
            return Err(From::from("Refusing to forward message to poisoned process".to_string()));
        }

        if let Err(e) = self.sender.send((InternalMsg::Msg(msg), sender)) {
            return Err(From::from(format!("Could not send message; process looks dead: {}", e)));
        }

        match receiver.recv() {
            Err(e) => Err(From::from(format!("Could not read reply: {}", e))),
            Ok(InternalReply::Reply(reply)) => reply,
            _ => Err(From::from("Unexpected internal reply from process".to_string())),
        }
    }
}
