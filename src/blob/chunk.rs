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

use models;
pub use models::LeafType;

use serde_cbor;
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

impl From<models::ChunkRef> for ChunkRef {
    fn from(chunk_ref: models::ChunkRef) -> ChunkRef {
        ChunkRef {
            blob_id: None,
            blob_name: chunk_ref.blob_name,
            offset: chunk_ref.offset as usize,
            length: chunk_ref.length as usize,
            packing: match chunk_ref.packing {
                models::Packing::Raw => None,
                models::Packing::GZip => Some(Packing::GZip),
                models::Packing::Snappy => Some(Packing::Snappy),
            },
            key: match chunk_ref.key {
                models::Key::None => None,
                models::Key::AeadChacha20Poly1305(key) => {
                    Some(Key::AeadChacha20Poly1305(secstr::SecVec::new(key)))
                }
            },
        }
    }
}

impl ChunkRef {
    pub fn to_model(&self) -> models::ChunkRef {
        models::ChunkRef {
            blob_name: self.blob_name.clone(),
            offset: self.offset as u64,
            length: self.length as u64,
            packing: match self.packing {
                None => models::Packing::Raw,
                Some(Packing::GZip) => models::Packing::GZip,
                Some(Packing::Snappy) => models::Packing::Snappy,
            },
            key: match self.key {
                None => models::Key::None,
                Some(Key::AeadChacha20Poly1305(ref key)) => {
                    models::Key::AeadChacha20Poly1305(key.unsecure().to_owned())
                }
            },
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<ChunkRef, serde_cbor::error::Error> {
        let model: models::ChunkRef = serde_cbor::from_slice(bytes)?;
        Ok(From::from(model))
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        serde_cbor::to_vec(&self.to_model()).unwrap()
    }

    pub fn as_bytes_no_name(&self) -> Vec<u8> {
        let mut model = self.to_model();
        model.blob_name = "".into();
        serde_cbor::to_vec(&model).unwrap()
    }
}
