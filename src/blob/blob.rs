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

use std::mem;
use std::sync::Arc;

use super::BlobError;
use super::ChunkRef;


pub struct Blob {
    keys: Arc<crypto::keys::Keeper>,
    access_key: crypto::authed::desc::Key,
    chunks: CipherText,
    footer: Vec<u8>,
    overhead: usize,
    max_len: usize,
}

impl Blob {
    pub fn new(keys: Arc<crypto::keys::Keeper>, max_len: usize) -> Blob {
        Blob {
            keys: keys,
            access_key: crypto::FixedKey::new_access_partial_key(),
            chunks: CipherText::empty(),
            footer: Vec::with_capacity(max_len / 2),
            overhead: crypto::sealed::desc::overhead() + crypto::authed::hash::DIGESTBYTES,
            max_len: max_len,
        }
    }

    pub fn upperbound_len(&self) -> usize {
        if self.chunks.len() == 0 {
            0
        } else {
            self.chunks.len() + self.footer.len() + self.overhead
        }
    }

    pub fn try_append(&mut self, chunk: &[u8], mut href: &mut HashRef) -> Result<(), ()> {
        let ct = crypto::RefKey::seal(&mut href, &self.access_key, PlainTextRef::new(chunk));

        href.persistent_ref.offset = self.chunks.len();
        let mut href_bytes = href.as_bytes();
        assert!(href_bytes.len() < 65535);

        if self.upperbound_len() + 1 + href_bytes.len() + ct.len() >= self.max_len {
            if self.chunks.len() == 0 {
                panic!("Can never fit chunk of size {} in blob of size {}",
                       chunk.len(),
                       self.max_len);
            }
            return Err(());
        }

        self.chunks.append(ct);

        // Generate footer entry.
        self.footer.push((href_bytes.len() % 256) as u8);
        self.footer.push((href_bytes.len() / 256) as u8);
        self.footer.append(&mut href_bytes);

        Ok(())
    }

    pub fn to_ciphertext(&mut self) -> Option<CipherText> {
        if self.chunks.len() == 0 {
            return None;
        }

        // Generate a new access key for the next blob to use.
        let access_key = mem::replace(&mut self.access_key,
                                      crypto::FixedKey::new_access_partial_key());

        let footer_overhead = self.footer.len() + self.overhead;
        let footer =
            crypto::FixedKey::new(&self.keys).seal(&access_key,
                                                   PlainTextRef::new(&self.footer[..]));
        self.footer.truncate(0);

        assert!(self.chunks.len() + footer_overhead <= self.max_len);

        let mut out = mem::replace(&mut self.chunks, CipherText::empty());
        out.random_pad_upto(self.max_len - footer_overhead);
        out.append(footer);
        out.append_authentication();

        assert_eq!(out.len(), self.max_len);

        // Everything has been reset. We are ready to go again.
        assert_eq!(0, self.chunks.len());
        assert_eq!(0, self.footer.len());

        Some(out)
    }
}

pub struct BlobReader<'b> {
    keys: Arc<crypto::keys::Keeper>,
    access_key: crypto::authed::desc::Key,
    footer_ct: Vec<u8>,
    blob: CipherTextRef<'b>,
}

impl<'b> BlobReader<'b> {
    pub fn new(keys: Arc<crypto::keys::Keeper>,
               blob: CipherTextRef<'b>)
               -> Result<BlobReader<'b>, crypto::CryptoError> {
        let rest = blob.strip_authentication()?;
        let (access_key, footer_ct, rest) = crypto::FixedKey::new(&keys).unseal_access_ctx(rest)?;

        // TODO(jos): Figure out how to make the borrow checker happy without this.
        let rest_of_blob = blob.slice(0, rest.len());

        Ok(BlobReader {
               keys: keys,
               access_key: access_key,
               footer_ct: footer_ct.to_vec(),
               blob: rest_of_blob,
           })
    }

    pub fn refs(&self) -> Result<Vec<HashRef>, BlobError> {
        let (_rest, footer_vec) =
            crypto::FixedKey::new(&self.keys)
                .unseal(CipherTextRef::new(&self.footer_ct[..]), self.blob.as_ref())?;
        let mut footer_pos = footer_vec.as_bytes();

        let mut hrefs = Vec::new();
        while footer_pos.len() > 0 {
            let len = footer_pos[0] as usize + 256 * (footer_pos[1] as usize);
            assert!(footer_pos.len() > len);

            hrefs.push(HashRef::from_bytes(&mut &footer_pos[2..2 + len])?);
            footer_pos = &footer_pos[len + 2..];
        }

        Ok(hrefs)
    }

    pub fn read_chunk(&self, hash: &Hash, cref: &ChunkRef) -> Result<Vec<u8>, BlobError> {
        Ok(crypto::RefKey::unseal(&self.access_key, hash, cref, self.blob.as_ref())?
               .into_vec())
    }
}
