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
use secstr;


#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Packing {
    GZip,
    Snappy,
}

#[derive(Debug, Clone)]
pub enum Key {
    AeadChacha20Poly1305(secstr::SecStr),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum NodeType {
    Branch(u64),
    Leaf,
}

impl From<u64> for NodeType {
    fn from(n: u64) -> NodeType {
        match n {
            0 => NodeType::Leaf,
            _ => NodeType::Branch(n),
        }
    }
}

impl From<NodeType> for u64 {
    fn from(t: NodeType) -> u64 {
        match t {
            NodeType::Branch(height) => height,
            NodeType::Leaf => 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum LeafType {
    FileChunk = 1,
    TreeList = 2,
    SnapshotList = 3,
}

impl LeafType {
    pub fn read_msg(which: root_capnp::hash_ref::leaf_type::Which) -> LeafType {
        match which {
            root_capnp::hash_ref::leaf_type::Chunk(()) => LeafType::FileChunk,
            root_capnp::hash_ref::leaf_type::TreeList(()) => LeafType::TreeList,
            root_capnp::hash_ref::leaf_type::SnapshotList(()) => LeafType::SnapshotList,
        }
    }
    pub fn populate_msg(self, mut msg: root_capnp::hash_ref::leaf_type::Builder) {
        match self {
            LeafType::FileChunk => msg.set_chunk(()),
            LeafType::TreeList => msg.set_tree_list(()),
            LeafType::SnapshotList => msg.set_snapshot_list(()),
        }
    }
}

impl From<u64> for LeafType {
    fn from(n: u64) -> LeafType {
        match n {
            1 => LeafType::FileChunk,
            2 => LeafType::TreeList,
            3 => LeafType::SnapshotList,
            _ => unreachable!("Corrupt LeafType tag: {}", n),
        }
    }
}

impl From<LeafType> for u64 {
    fn from(t: LeafType) -> u64 {
        match t {
            LeafType::FileChunk => 1,
            LeafType::TreeList => 2,
            LeafType::SnapshotList => 3,
        }
    }
}

#[derive(Debug, Clone)]
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
        let reader = capnp::serialize_packed::read_message(bytes,
                                                           capnp::message::ReaderOptions::new())?;
        let root = reader.get_root::<root_capnp::chunk_ref::Reader>()?;

        Ok(ChunkRef::read_msg(&root)?)
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
        msg.set_offset(self.offset as u64);
        msg.set_length(self.length as u64);

        if let Some(Key::AeadChacha20Poly1305(ref chacha)) = self.key {
            msg.borrow()
                .init_key()
                .set_aead_chacha20_poly1305(chacha.unsecure());
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
               blob_name: msg.get_blob_name()?.to_owned(),
               offset: msg.get_offset() as usize,
               length: msg.get_length() as usize,
               packing: match msg.get_packing().which()? {
                   root_capnp::chunk_ref::packing::None(()) => None,
                   root_capnp::chunk_ref::packing::Gzip(()) => Some(Packing::GZip),
                   root_capnp::chunk_ref::packing::Snappy(()) => Some(Packing::Snappy),
               },
               key: match msg.get_key().which()? {
                   root_capnp::chunk_ref::key::None(()) => None,
                   root_capnp::chunk_ref::key::AeadChacha20Poly1305(res) => {
                       Some(Key::AeadChacha20Poly1305(secstr::SecStr::from(res?)))
                   }
               },
           })
    }
}
