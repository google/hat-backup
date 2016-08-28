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

use crypto;
use crypto::{CipherText, CipherTextRef, PlainTextRef};
use hash::Hash;
use hash::tree::HashRef;

use super::BlobError;
use super::ChunkRef;


pub struct Blob {
    master_key: crypto::FixedKey,
    chunks: CipherText,
    footer: Vec<u8>,
    overhead: usize,
    max_size: usize,
}

impl Blob {
    pub fn new(max_size: usize) -> Blob {
        // TODO(jos): Plug an actual crypto key through somehow.
        let pubkey = crypto::sealed::desc::PublicKey::from_slice(&[215, 136, 80, 128, 158, 109,
                                                                   227, 141, 219, 63, 118, 91,
                                                                   123, 97, 1, 97, 65, 237, 62,
                                                                   171, 83, 159, 200, 11, 68,
                                                                   138, 40, 82, 24, 47, 187, 29])
            .unwrap();
        let seckey = crypto::sealed::desc::SecretKey::from_slice(&[94, 13, 181, 81, 97, 87, 76,
                                                                   37, 53, 92, 120, 232, 17, 126,
                                                                   234, 78, 12, 23, 141, 61, 40,
                                                                   10, 136, 127, 103, 192, 255,
                                                                   193, 142, 154, 101, 35])
            .unwrap();

        let master_key = crypto::FixedKey::new(pubkey, Some(seckey));

        Blob {
            master_key: master_key,
            chunks: CipherText::new(Vec::with_capacity(max_size)),
            footer: Vec::with_capacity(max_size / 2),
            overhead: crypto::sealed::desc::overhead(),
            max_size: max_size,
        }
    }

    pub fn read_chunk(blob: &[u8], hash: &Hash, cref: &ChunkRef) -> Result<Vec<u8>, BlobError> {
        let ct = crypto::CipherTextRef::new(blob);
        Ok(try!(crypto::RefKey::unseal(hash, cref, ct)).into_vec())
    }

    pub fn upperbound_len(&self) -> usize {
        if self.chunks.len() == 0 {
            0
        } else {
            self.chunks.len() + self.footer.len() + self.overhead
        }
    }

    pub fn try_append(&mut self, chunk: &[u8], mut href: &mut HashRef) -> Result<(), ()> {
        let mut ct = crypto::RefKey::seal(&mut href, PlainTextRef::new(&chunk));

        href.persistent_ref.offset = self.chunks.len();
        let mut href_bytes = href.as_bytes();
        assert!(href_bytes.len() < 255);

        if self.upperbound_len() + 1 + href_bytes.len() + ct.len() >= self.max_size {
            if self.chunks.len() == 0 {
                panic!("Can never fit chunk of size {} in blob of size {}",
                       chunk.len(),
                       self.max_size);
            }
            return Err(());
        }

        ct.empty_into(&mut self.chunks);

        // Generate footer entry.
        self.footer.push(href_bytes.len() as u8);
        self.footer.append(&mut href_bytes);

        Ok(())
    }

    pub fn into_bytes(&mut self, mut out: &mut CipherText) {
        if self.chunks.len() == 0 {
            return;
        }

        let mut footer = self.master_key.seal(PlainTextRef::new(&self.footer[..]));
        self.footer.truncate(0);

        assert!(self.chunks.len() + footer.len() <= self.max_size);
        out.append(&mut self.chunks);
        out.random_pad_upto(self.max_size - footer.len());
        out.append(&mut footer);

        // Everything has been reset. We are ready to go again.
        assert_eq!(0, self.chunks.len());
        assert_eq!(0, self.footer.len());
    }

    pub fn refs_from_bytes(&self, bytes: &[u8]) -> Result<Vec<HashRef>, BlobError> {
        if bytes.len() == 0 {
            return Ok(Vec::new());
        }

        let (_rest, footer_vec) = try!(self.master_key.unseal(CipherTextRef::new(bytes)));
        let mut footer_pos = footer_vec.as_bytes();

        let mut hrefs = Vec::new();
        while footer_pos.len() > 0 {
            let len = footer_pos[0] as usize;
            assert!(footer_pos.len() > len);

            hrefs.push(try!(HashRef::from_bytes(&mut &footer_pos[1..1 + len])));
            footer_pos = &footer_pos[len + 1..];
        }

        Ok(hrefs)
    }
}
