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

use capnp;
use root_capnp;
use sodiumoxide::crypto::secretbox::xsalsa20poly1305;


#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Packing {
    GZip,
    Snappy,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Key {
    XSalsa20Poly1305(xsalsa20poly1305::Key),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Kind {
    TreeBranch(i64),
    TreeLeaf,
}

pub fn node_from_height(height: i64) -> Kind {
    assert!(height >= 0);
    match height {
        0 => Kind::TreeLeaf,
        h => Kind::TreeBranch(h),
    }
}

pub fn node_height(kind: &Kind) -> i64 {
    match kind {
        &Kind::TreeBranch(height) => {
            assert!(height >= 1);
            height
        }
        &Kind::TreeLeaf => 0,
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ChunkRef {
    pub blob_id: Option<i64>,
    pub blob_name: Vec<u8>,
    pub offset: usize,
    pub length: usize,
    pub packing: Option<Packing>,
    pub key: Option<Key>,
}

impl ChunkRef {
    pub fn from_bytes(bytes: &mut &[u8]) -> Result<ChunkRef, capnp::Error> {
        let reader = try!(capnp::serialize_packed::read_message(bytes,
                                                       capnp::message::ReaderOptions::new()));
        let root = try!(reader.get_root::<root_capnp::chunk_ref::Reader>());

        Ok(try!(ChunkRef::read_msg(&root)))
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut message = ::capnp::message::Builder::new_default();
        {
            let mut root = message.init_root::<root_capnp::chunk_ref::Builder>();
            self.populate_msg(root.borrow());
        }
        let mut out = Vec::new();
        capnp::serialize_packed::write_message(&mut out, &message).unwrap();
        out
    }

    pub fn as_bytes_no_name(&self) -> Vec<u8> {
        let mut message = ::capnp::message::Builder::new_default();
        {
            let mut root = message.init_root::<root_capnp::chunk_ref::Builder>();
            self.populate_msg_no_name(root.borrow());
        }
        let mut out = Vec::new();
        capnp::serialize_packed::write_message(&mut out, &message).unwrap();
        out
    }

    pub fn populate_msg(&self, mut msg: root_capnp::chunk_ref::Builder) {
        self.populate_msg_name(msg.borrow());
        self.populate_msg_no_name(msg);
    }

    pub fn populate_msg_name(&self, mut msg: root_capnp::chunk_ref::Builder) {
        msg.set_blob_name(&self.blob_name[..]);
    }

    pub fn populate_msg_no_name(&self, mut msg: root_capnp::chunk_ref::Builder) {
        msg.set_offset(self.offset as i64);
        msg.set_length(self.length as i64);

        if let Some(Key::XSalsa20Poly1305(ref salsa)) = self.key {
            msg.borrow().init_key().set_xsalsa20_poly1305(salsa.0.as_ref());
        } else {
            msg.borrow().init_key().set_none(());
        }

        match self.packing {
            None => msg.borrow().init_packing().set_none(()),
            Some(Packing::GZip) => msg.borrow().init_packing().set_gzip(()),
            Some(Packing::Snappy) => msg.borrow().init_packing().set_snappy(()),
        }
    }

    pub fn read_msg(msg: &root_capnp::chunk_ref::Reader) -> Result<ChunkRef, capnp::Error> {
        Ok(ChunkRef {
            blob_id: None,
            blob_name: try!(msg.get_blob_name()).to_owned(),
            offset: msg.get_offset() as usize,
            length: msg.get_length() as usize,
            packing: match try!(msg.get_packing().which()) {
                root_capnp::chunk_ref::packing::None(()) => None,
                root_capnp::chunk_ref::packing::Gzip(()) => Some(Packing::GZip),
                root_capnp::chunk_ref::packing::Snappy(()) => Some(Packing::Snappy),
            },
            key: match try!(msg.get_key().which()) {
                root_capnp::chunk_ref::key::None(()) => None,
                root_capnp::chunk_ref::key::Xsalsa20Poly1305(res) => {
                    Some(Key::XSalsa20Poly1305(xsalsa20poly1305::Key::from_slice(try!(res))
                        .expect("Incorrect key-size")))
                }
            },
        })
    }
}
