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

use blob::{ChunkRef, Key};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use errors::CryptoError;
use hash::Hash;
use hash::tree::HashRef;
use sodiumoxide::crypto::stream;
use std::io;

pub mod keys;

pub struct PlainText(Vec<u8>);
pub struct PlainTextRef<'a>(&'a [u8]);

pub struct CipherText {
    chunks: Vec<Vec<u8>>,
    len: usize,
}
pub struct CipherTextRef<'a>(&'a [u8]);


pub mod authed {
    pub mod desc {
        pub use sodiumoxide::crypto::secretbox::xsalsa20poly1305::{KEYBYTES, Key, MACBYTES,
                                                                   NONCEBYTES, Nonce};
    }

    pub mod imp {
        pub use sodiumoxide::crypto::secretbox::xsalsa20poly1305::{gen_key, gen_nonce, open, seal};
    }

    pub mod hash {
        use libsodium_sys::crypto_generichash_blake2b;
        pub use libsodium_sys::crypto_generichash_blake2b_BYTES_MAX as DIGESTBYTES;

        pub fn new(text: &[u8]) -> Vec<u8> {
            let mut digest = vec![0; DIGESTBYTES];
            let key = vec![];
            unsafe {
                crypto_generichash_blake2b(digest.as_mut_ptr(),
                                           digest.len(),
                                           text.as_ptr(),
                                           text.len() as u64,
                                           key.as_ptr(),
                                           key.len());
            }
            digest
        }
    }
}

pub mod sealed {
    pub mod desc {
        pub use sodiumoxide::crypto::box_::curve25519xsalsa20poly1305::{MACBYTES, PUBLICKEYBYTES,
                                                                        PublicKey, SECRETKEYBYTES,
                                                                        SecretKey};

        pub const SEALBYTES: usize = PUBLICKEYBYTES + MACBYTES;

        pub fn footer_plain_bytes() -> usize {
            // Footer encodes message length in LittleEndian i64 + a symmetric inner key.
            8 + ::crypto::authed::desc::KEYBYTES
        }

        pub fn footer_cipher_bytes() -> usize {
            // Footer plaintext + seal.
            footer_plain_bytes() + SEALBYTES
        }

        pub fn overhead() -> usize {
            // plaintext MAC + random inner nonce + the footer.
            ::crypto::authed::desc::MACBYTES + ::crypto::authed::desc::NONCEBYTES +
            footer_cipher_bytes()
        }
    }

    pub mod imp {
        pub use sodiumoxide::crypto::box_::curve25519xsalsa20poly1305::gen_keypair;
        pub use sodiumoxide::crypto::sealedbox::curve25519blake2bxsalsa20poly1305::{open, seal};
    }
}

fn wrap_key(key: authed::desc::Key) -> Key {
    Key::XSalsa20Poly1305(key)
}

impl<'a> PlainTextRef<'a> {
    pub fn new(bytes: &[u8]) -> PlainTextRef {
        PlainTextRef(bytes)
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn read_i64(&self) -> Result<i64, io::Error> {
        return (&self.0[..]).read_i64::<LittleEndian>();
    }
    pub fn slice(&self, from: usize, to: usize) -> PlainTextRef<'a> {
        PlainTextRef(&self.0[from..to])
    }
    pub fn split_from_right(&self,
                            len: usize)
                            -> Result<(PlainTextRef<'a>, PlainTextRef<'a>), CryptoError> {
        if self.len() < len {
            Err("crypto read failed: split_from_right".into())
        } else {
            Ok((self.slice(0, self.len() - len), self.slice(self.len() - len, self.len())))
        }
    }
    pub fn to_ciphertext(&self,
                         nonce: &authed::desc::Nonce,
                         key: &authed::desc::Key)
                         -> CipherText {
        CipherText::new(authed::imp::seal(self.0, nonce, key))
    }
}

impl PlainText {
    pub fn new(bytes: Vec<u8>) -> PlainText {
        PlainText(bytes)
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn as_ref(&self) -> PlainTextRef {
        PlainTextRef::new(&self.0[..])
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
    pub fn into_vec(self) -> Vec<u8> {
        self.0
    }
    pub fn from_i64(n: i64) -> Self {
        let mut buf = PlainText::new(Vec::with_capacity(8));
        buf.0.write_i64::<LittleEndian>(n).unwrap();
        return buf;
    }
}

impl CipherText {
    pub fn empty() -> CipherText {
        CipherText {
            chunks: vec![],
            len: 0,
        }
    }
    pub fn new(ct: Vec<u8>) -> CipherText {
        let len = ct.len();
        CipherText {
            chunks: vec![ct],
            len: len,
        }
    }
    pub fn append(&mut self, mut other: CipherText) {
        self.len += other.len();
        self.chunks.append(&mut other.chunks);
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn random_pad_upto(&mut self, final_len: usize) {
        let len = self.len;
        if final_len > len {
            self.append(CipherText::random_pad(final_len - len));
        }
    }
    pub fn random_pad(size: usize) -> CipherText {
        let key = stream::salsa20::gen_key();
        let nonce = stream::salsa20::gen_nonce();
        CipherText::new(stream::salsa20::stream(size, &nonce, &key))
    }
    pub fn to_vec(&self) -> Vec<u8> {
        let mut out = vec![];
        for c in &self.chunks {
            out.extend_from_slice(&c[..]);
        }
        out
    }

    pub fn append_authentication(&mut self) {
        let v = self.to_vec();
        let h = authed::hash::new(&v[..]);
        self.append(CipherText::new(h));
    }

    pub fn slices(&self) -> Vec<&[u8]> {
        self.chunks.iter().map(|x| &x[..]).collect()
    }
}

impl<'a> CipherTextRef<'a> {
    pub fn new(bytes: &'a [u8]) -> CipherTextRef<'a> {
        CipherTextRef(bytes)
    }
    pub fn slice(&self, from: usize, to: usize) -> CipherTextRef<'a> {
        CipherTextRef(&self.0[from..to])
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn split_from_right(&self,
                            len: usize)
                            -> Result<(CipherTextRef<'a>, CipherTextRef<'a>), CryptoError> {
        if self.len() < len {
            Err("crypto read failed: split_from_right".into())
        } else {
            Ok((self.slice(0, self.len() - len), self.slice(self.len() - len, self.len())))
        }
    }
    pub fn to_plaintext(&self,
                        nonce: &authed::desc::Nonce,
                        key: &authed::desc::Key)
                        -> Result<PlainText, CryptoError> {
        Ok(PlainText::new(try!(authed::imp::open(&self.0, &nonce, &key)
           .map_err(|()| "crypto read failed: to_plaintext"))))
    }

    pub fn strip_authentication(&self) -> Result<CipherTextRef, CryptoError> {
        let (rest, want) = self.split_from_right(authed::hash::DIGESTBYTES)?;
        let got = authed::hash::new(rest.0);
        if want.0 == &got[..] {
            Ok(rest)
        } else {
            Err(From::from("crypto read failed: strip_authentication"))
        }
    }
}


pub struct RefKey {}


impl RefKey {
    pub fn seal(href: &mut HashRef, pt: PlainTextRef) -> CipherText {
        let key = authed::imp::gen_key();
        href.persistent_ref.key = Some(wrap_key(key.clone()));

        let nonce = authed::desc::Nonce::from_slice(&href.hash.bytes[..authed::desc::NONCEBYTES])
            .unwrap();
        let ct = pt.to_ciphertext(&nonce, &key);
        href.persistent_ref.length = ct.len();

        ct
    }

    pub fn unseal(hash: &Hash,
                  cref: &ChunkRef,
                  ct: CipherTextRef)
                  -> Result<PlainText, CryptoError> {
        assert!(ct.len() > cref.offset + cref.length);
        let ct = ct.slice(cref.offset, cref.offset + cref.length);
        match cref.key {
            Some(Key::XSalsa20Poly1305(ref key)) if hash.bytes.len() >=
                                                    authed::desc::NONCEBYTES => {
                match authed::desc::Nonce::from_slice(&hash.bytes[..authed::desc::NONCEBYTES]) {
                    None => Err("crypto read failed: unseal".into()),
                    Some(nonce) => Ok(ct.to_plaintext(&nonce, &key)?),
                }
            }
            _ => Err("crypto read failed: unseal".into()),
        }
    }
}

pub struct FixedKey<'k> {
    keeper: &'k keys::Keeper,
}

impl<'k> FixedKey<'k> {
    pub fn new(keeper: &'k keys::Keeper) -> FixedKey<'k> {
        FixedKey { keeper: keeper }
    }

    pub fn seal_blob_name(&self, pt: PlainTextRef) -> CipherText {
        CipherText::new(self.keeper.naming_lock(pt.0))
    }

    pub fn unseal_blob_name(&self, ct: CipherTextRef) -> PlainText {
        PlainText::new(self.keeper.naming_unlock(ct.0))
    }

    pub fn seal_blob_data(&self, pt: PlainTextRef) -> CipherText {
        CipherText::new(self.keeper.data_lock(pt.0))
    }

    pub fn unseal_blob_data(&self, ct: CipherTextRef) -> PlainText {
        PlainText::new(self.keeper.data_unlock(ct.0))
    }

    pub fn seal(&self, pt: PlainTextRef) -> CipherText {
        // Generate inner symmetric key.
        let inner_key = ::crypto::authed::imp::gen_key();
        let nonce = ::crypto::authed::imp::gen_nonce();

        // Encrypt plaintext with inner key.
        let mut ct = pt.to_ciphertext(&nonce, &inner_key);
        ct.append(CipherText::new(nonce.0.to_vec()));

        // Construct footer of length as LittleEndian and inner key.
        let mut foot_pt = PlainText::from_i64(ct.len() as i64);
        foot_pt.0.extend_from_slice(&inner_key.0);
        assert_eq!(foot_pt.len(), sealed::desc::footer_plain_bytes());

        // Seal footer.
        let foot_ct = self.seal_blob_data(foot_pt.as_ref());
        assert_eq!(foot_ct.len(), sealed::desc::footer_cipher_bytes());

        // Append sealed footer to symmetric cipher text.
        ct.append(foot_ct);
        assert_eq!(ct.len(), pt.len() + sealed::desc::overhead());
        ct
    }

    pub fn unseal<'a>(&self,
                      ct: CipherTextRef<'a>)
                      -> Result<(CipherTextRef<'a>, PlainText), CryptoError> {
        // Read sealed ciphertext length and unseal it.
        let (rest, foot_ct) = ct.split_from_right(sealed::desc::footer_cipher_bytes())?;
        let foot_pt = self.unseal_blob_data(foot_ct);
        assert_eq!(foot_pt.len(), sealed::desc::footer_plain_bytes());

        // Read length as LittleEndian and inner key.
        let (foot_len, inner_key) = foot_pt
            .as_ref()
            .split_from_right(::crypto::authed::desc::KEYBYTES)?;

        // Parse length of inner symmetric cipher text.
        let ct_len = foot_len
            .read_i64()
            .map_err(|_| "crypto read failed: unseal")?;
        assert!(ct_len > 0);

        // Read and unseal inner symmetric cipher text.
        let (rest, ct_and_nonce) = rest.split_from_right(ct_len as usize)?;
        let (ct, nonce) = ct_and_nonce
            .split_from_right(::crypto::authed::desc::NONCEBYTES)?;
        Ok((rest,
            ct.to_plaintext(&::crypto::authed::desc::Nonce::from_slice(nonce.0).unwrap(),
                            &::crypto::authed::desc::Key::from_slice(inner_key.0).unwrap())?))
    }
}
