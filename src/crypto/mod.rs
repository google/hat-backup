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

use errors::CryptoError;
use sodiumoxide::crypto::stream;
use hash::Hash;
use hash::tree::HashRef;
use blob::{ChunkRef, Key};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

pub struct PlainText(Vec<u8>);
pub struct PlainTextRef<'a>(&'a [u8]);
pub struct CipherText(Vec<u8>);
pub struct CipherTextRef<'a>(&'a [u8]);

pub mod authed {
    pub mod desc {
        pub use sodiumoxide::crypto::secretbox::xsalsa20poly1305::{KEYBYTES, Key, MACBYTES,
                                                                   NONCEBYTES, Nonce};
    }

    pub mod imp {
        pub use sodiumoxide::crypto::secretbox::xsalsa20poly1305::{gen_key, gen_nonce, open, seal};
    }
}

pub mod sealed {
    pub mod desc {
        pub use sodiumoxide::crypto::box_::curve25519xsalsa20poly1305::{MACBYTES, PUBLICKEYBYTES,
                                                                        PublicKey, SECRETKEYBYTES,
                                                                        SecretKey};

        pub const SEALBYTES: usize = PUBLICKEYBYTES + MACBYTES;

        pub fn footer_plain_bytes() -> usize {
            // Footer contains a LittleEndian u64.
            8
        }

        pub fn footer_cipher_bytes() -> usize {
            footer_plain_bytes() + SEALBYTES
        }

        pub fn overhead() -> usize {
            // MAC for the plaintext being sealed + the footer.
            SEALBYTES + footer_cipher_bytes()
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
    pub fn to_ciphertext(&self,
                         nonce: &authed::desc::Nonce,
                         key: &authed::desc::Key)
                         -> CipherText {
        CipherText(authed::imp::seal(&self.0, &nonce, &key))
    }
    pub fn to_sealed_ciphertext(&self, pubkey: &sealed::desc::PublicKey) -> CipherText {
        CipherText(sealed::imp::seal(&self.0, &pubkey))
    }
}

impl PlainText {
    pub fn new(bytes: Vec<u8>) -> PlainText {
        PlainText(bytes)
    }
    pub fn from_vec(mut bytes: &mut Vec<u8>) -> PlainText {
        let mut pt = PlainText(Vec::with_capacity(bytes.len()));
        pt.0.append(&mut bytes);
        pt
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
}

impl CipherText {
    pub fn new(ct: Vec<u8>) -> CipherText {
        CipherText(ct)
    }
    pub fn from(mut other_ct: &mut CipherText) -> CipherText {
        let mut ct = CipherText::new(Vec::with_capacity(other_ct.len()));
        ct.append(&mut other_ct);
        ct
    }
    pub fn append(&mut self, other: &mut CipherText) {
        self.0.append(&mut other.0);
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn empty_into(&mut self, out: &mut CipherText) {
        out.0.append(&mut self.0)
    }
    pub fn random_pad_upto(&mut self, final_size: usize) {
        let size = self.len();
        if final_size > size {
            self.0.resize(final_size, 0);
            let key = stream::salsa20::gen_key();
            let nonce = stream::salsa20::gen_nonce();
            stream::salsa20::stream_xor_inplace(&mut self.0[size..], &nonce, &key);
        }
    }
    pub fn into_vec(self) -> Vec<u8> {
        self.0
    }
    pub fn as_ref(&self) -> CipherTextRef {
        CipherTextRef(&self.0[..])
    }
}

impl<'a> CipherTextRef<'a> {
    pub fn new(bytes: &'a [u8]) -> CipherTextRef<'a> {
        CipherTextRef(bytes)
    }
    pub fn bytes(&self) -> &[u8] {
        &self.0[..]
    }
    pub fn slice(&self, from: usize, to: usize) -> CipherTextRef<'a> {
        CipherTextRef(&self.0[from..to])
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn split_from_right(&self, len: usize) -> (CipherTextRef<'a>, CipherTextRef<'a>) {
        assert!(self.len() >= len);
        (self.slice(0, self.len() - len), self.slice(self.len() - len, self.len()))
    }
    pub fn to_plaintext(&self,
                        nonce: &authed::desc::Nonce,
                        key: &authed::desc::Key)
                        -> Result<PlainText, CryptoError> {
        Ok((PlainText(try!(authed::imp::open(&self.0, &nonce, &key)
            .map_err(|()| "Crypto failed to open authenticated message")))))
    }
    pub fn to_sealed_plaintext(&self,
                               pubkey: &sealed::desc::PublicKey,
                               seckey: &sealed::desc::SecretKey)
                               -> Result<PlainText, CryptoError> {
        Ok(PlainText(try!(sealed::imp::open(&self.0, &pubkey, &seckey)
            .map_err(|()| "Crypto failed to open sealed message"))))
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
        assert!(ct.len() >= cref.offset + cref.length);
        let ct = ct.slice(cref.offset, cref.offset + cref.length);
        match cref.key {
            Some(Key::XSalsa20Poly1305(ref key)) => {
                let nonce =
                    authed::desc::Nonce::from_slice(&hash.bytes[..authed::desc::NONCEBYTES])
                        .unwrap();
                Ok(try!(ct.to_plaintext(&nonce, &key)))
            }
            _ => panic!("Unknown blob key type"),
        }
    }
}

pub struct FixedKey {
    pubkey: sealed::desc::PublicKey,
    seckey: Option<sealed::desc::SecretKey>,
}

impl FixedKey {
    pub fn new(pubkey: sealed::desc::PublicKey,
               seckey_opt: Option<sealed::desc::SecretKey>)
               -> FixedKey {
        FixedKey {
            pubkey: pubkey,
            seckey: seckey_opt,
        }
    }

    pub fn seal(&self, pt: PlainTextRef) -> CipherText {
        let mut ct = pt.to_sealed_ciphertext(&self.pubkey);

        // Add ciphertext length as LittleEndian.
        let mut foot_pt = PlainText(Vec::with_capacity(8));
        foot_pt.0.write_u64::<LittleEndian>(ct.len() as u64).unwrap();
        assert_eq!(foot_pt.len(), sealed::desc::footer_plain_bytes());

        // Append sealed ciphertext length to full ciphertext.
        ct.append(&mut foot_pt.as_ref().to_sealed_ciphertext(&self.pubkey));
        assert_eq!(ct.len(), pt.len() + sealed::desc::overhead());

        // Return complete ciphertext.
        ct
    }

    pub fn unseal<'a>(&self,
                      ct: CipherTextRef<'a>)
                      -> Result<(CipherTextRef<'a>, PlainText), CryptoError> {
        let seckey = self.seckey.as_ref().expect("unseal requires access to read-key");

        // Read sealed ciphertext length and unseal it.
        let (rest, foot_ct) = ct.split_from_right(sealed::desc::footer_cipher_bytes());
        let foot_pt = try!(foot_ct.to_sealed_plaintext(&self.pubkey, &seckey));
        assert_eq!(foot_pt.len(), sealed::desc::footer_plain_bytes());

        // Parse back from encoded LittleEndian.
        let ct_len = foot_pt.as_bytes().read_u64::<LittleEndian>().unwrap();

        // Read and unseal original ciphertext.
        let (rest, ct) = rest.split_from_right(ct_len as usize);
        Ok((rest, try!(ct.to_sealed_plaintext(&self.pubkey, &seckey))))
    }
}
