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

use blob::Key;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
pub use errors::CryptoError;
use hash::tree::HashRef;
use std::io;
use std::mem;

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
        use libsodium_sys;
        use secstr;

        pub const KEYBYTES: usize = libsodium_sys::crypto_aead_chacha20poly1305_KEYBYTES;
        pub const NONCEBYTES: usize = libsodium_sys::crypto_aead_chacha20poly1305_NPUBBYTES;
        pub const MACBYTES: usize = libsodium_sys::crypto_aead_chacha20poly1305_ABYTES;
        pub type Key = secstr::SecStr;
        pub type Nonce = secstr::SecStr;
    }

    pub mod imp {
        use secstr;

        pub fn gen_key() -> secstr::SecStr {
            ::crypto::keys::random_bytes(super::desc::KEYBYTES)
        }
        pub fn gen_nonce() -> secstr::SecStr {
            ::crypto::keys::random_bytes(super::desc::NONCEBYTES)
        }

        pub fn mix_keys(
            access_key: &super::desc::Key,
            other_key: &super::desc::Key,
        ) -> super::desc::Key {
            let mut mixed_key = [0u8; super::hash::DIGESTBYTES];
            let salt: &[u8; 16] = b"mixkey~~mixkey~~";
            ::crypto::keys::keyed_fingerprint(
                &access_key.unsecure()[..],
                &other_key.unsecure()[..],
                salt,
                &mut mixed_key[..],
            );
            super::desc::Key::from(&mixed_key[..super::desc::KEYBYTES])
        }
    }

    pub mod hash {
        pub use libsodium_sys::crypto_generichash_blake2b_BYTES_MAX as DIGESTBYTES;
    }
}

pub mod sealed {
    pub mod desc {
        use libsodium_sys;

        pub const SEALBYTES: usize = libsodium_sys::crypto_box_SEALBYTES;

        pub fn symmetric_seal_bytes() -> usize {
            // Mac and nonce from inner symmetric seal
            // Stored as plaintext outside the asymmetric seal.
            ::crypto::authed::desc::MACBYTES + ::crypto::authed::desc::NONCEBYTES
        }

        pub fn footer_plain_bytes() -> usize {
            // Footer encodes message length in LittleEndian i64 + the symmetric inner key.
            8 + ::crypto::authed::desc::KEYBYTES
        }

        pub fn footer_cipher_bytes() -> usize {
            // Footer plaintext + seal.
            footer_plain_bytes() + SEALBYTES
        }

        pub fn access_plain_bytes() -> usize {
            // Data reference footer + access key.
            footer_cipher_bytes() + ::crypto::authed::desc::KEYBYTES
        }

        pub fn access_cipher_bytes() -> usize {
            // Access footer + seal.
            access_plain_bytes() + SEALBYTES
        }

        pub fn overhead() -> usize {
            // plaintext MAC + the access footer.
            symmetric_seal_bytes() + access_cipher_bytes()
        }
    }
}

fn wrap_key(key: authed::desc::Key) -> Key {
    Key::AeadChacha20Poly1305(key)
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
    pub fn split_from_right(
        &self,
        len: usize,
    ) -> Result<(PlainTextRef<'a>, PlainTextRef<'a>), CryptoError> {
        if self.len() < len {
            Err("crypto read failed: split_from_right".into())
        } else {
            Ok((
                self.slice(0, self.len() - len),
                self.slice(self.len() - len, self.len()),
            ))
        }
    }
    pub fn to_ciphertext(
        &self,
        additional_data: &[u8],
        nonce: &authed::desc::Nonce,
        key: &authed::desc::Key,
    ) -> CipherText {
        CipherText::new(keys::Keeper::symmetric_lock(
            self.0,
            additional_data,
            nonce.unsecure(),
            key.unsecure(),
        ))
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
    fn collapse(&mut self) {
        if self.chunks.len() > 1 {
            let v = self.chunks.concat();
            mem::replace(&mut self.chunks, vec![v]);
            assert_eq!(self.chunks[0].len(), self.len);
        }
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
        use libsodium_sys::{crypto_stream_chacha20, crypto_stream_chacha20_KEYBYTES,
                            crypto_stream_chacha20_NONCEBYTES};

        let key = keys::random_bytes(crypto_stream_chacha20_KEYBYTES);
        let nonce = keys::random_bytes(crypto_stream_chacha20_NONCEBYTES);
        let mut stream = vec![0u8; size];

        let ret = unsafe {
            crypto_stream_chacha20(
                stream.as_mut_ptr(),
                stream.len() as u64,
                nonce.unsecure().as_ptr() as *const [u8; crypto_stream_chacha20_NONCEBYTES],
                key.unsecure().as_ptr() as *const [u8; crypto_stream_chacha20_KEYBYTES],
            )
        };
        assert_eq!(0, ret);

        CipherText::new(stream)
    }
    pub fn to_vec(&self) -> Vec<u8> {
        let mut out = vec![];
        for c in &self.chunks {
            out.extend_from_slice(&c[..]);
        }
        out
    }

    pub fn append_authentication(&mut self, keys: &keys::Keeper) {
        self.collapse();
        assert_eq!(self.chunks.len(), 1);

        let blob = &mut self.chunks[0];
        let blob_len = blob.len();
        blob.resize(blob_len + authed::hash::DIGESTBYTES, 0u8);
        {
            let (data, hash) = blob.split_at_mut(blob_len);
            keys.blob_authentication(&data[..], &mut hash[..]);
        }
        self.len = blob.len();
    }

    pub fn slices(&self) -> Vec<&[u8]> {
        self.chunks.iter().map(|x| &x[..]).collect()
    }
}

impl<'a> CipherTextRef<'a> {
    pub fn new(bytes: &'a [u8]) -> CipherTextRef<'a> {
        CipherTextRef(bytes)
    }
    pub fn as_ref(&self) -> CipherTextRef {
        CipherTextRef(&self.0[..])
    }
    pub fn slice(&self, from: usize, to: usize) -> CipherTextRef<'a> {
        CipherTextRef(&self.0[from..to])
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn split_from_right(
        &self,
        len: usize,
    ) -> Result<(CipherTextRef<'a>, CipherTextRef<'a>), CryptoError> {
        if self.len() < len {
            Err("crypto read failed: split_from_right".into())
        } else {
            Ok((
                self.slice(0, self.len() - len),
                self.slice(self.len() - len, self.len()),
            ))
        }
    }
    pub fn to_plaintext(
        &self,
        additional_data: &[u8],
        nonce: &authed::desc::Nonce,
        key: &authed::desc::Key,
    ) -> Result<PlainText, CryptoError> {
        Ok(PlainText::new(keys::Keeper::symmetric_unlock(
            key.unsecure(),
            &self.0,
            additional_data,
            nonce.unsecure(),
        )))
    }

    pub fn strip_authentication(&self, keys: &keys::Keeper) -> Result<CipherTextRef, CryptoError> {
        let (rest, want) = self.split_from_right(authed::hash::DIGESTBYTES)?;

        let mut got = vec![0u8; authed::hash::DIGESTBYTES];
        keys.blob_authentication(&rest.0[..], &mut got[..]);

        if want.0 == &got[..] {
            Ok(rest)
        } else {
            Err(From::from("crypto read failed: strip_authentication"))
        }
    }
}

pub struct RefKey {}

impl RefKey {
    pub fn seal(
        href: &mut HashRef,
        access_key: &::crypto::authed::desc::Key,
        pt: PlainTextRef,
    ) -> CipherText {
        let partial_key = authed::imp::gen_key();
        href.persistent_ref.key = Some(wrap_key(partial_key.clone()));

        let nonce = authed::desc::Nonce::from(&href.hash.bytes[..authed::desc::NONCEBYTES]);
        let key = ::crypto::authed::imp::mix_keys(&access_key, &partial_key);

        let additional_data = keys::compute_salt(href.node, href.leaf);
        let ct = pt.to_ciphertext(&additional_data, &nonce, &key);
        href.persistent_ref.length = ct.len();

        ct
    }

    pub fn unseal(
        access_key: &::crypto::authed::desc::Key,
        href: &HashRef,
        ct: CipherTextRef,
    ) -> Result<PlainText, CryptoError> {
        assert!(ct.len() > href.persistent_ref.offset + href.persistent_ref.length);
        let ct = ct.slice(
            href.persistent_ref.offset,
            href.persistent_ref.offset + href.persistent_ref.length,
        );
        match href.persistent_ref.key {
            Some(Key::AeadChacha20Poly1305(ref key))
                if href.hash.bytes.len() >= authed::desc::NONCEBYTES =>
            {
                let nonce = authed::desc::Nonce::from(&href.hash.bytes[..authed::desc::NONCEBYTES]);

                let additional_data = keys::compute_salt(href.node, href.leaf);
                let real_key = ::crypto::authed::imp::mix_keys(access_key, &key);
                Ok(ct.to_plaintext(&additional_data, &nonce, &real_key)?)
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

    pub fn seal_blob_access(&self, pt: PlainTextRef) -> CipherText {
        CipherText::new(self.keeper.access_lock(pt.0))
    }

    pub fn unseal_blob_access(&self, ct: CipherTextRef) -> PlainText {
        PlainText::new(self.keeper.access_unlock(ct.0))
    }

    pub fn new_access_partial_key() -> ::crypto::authed::desc::Key {
        // Generate new random symmetric key needed for reading blob data through a reference.
        // This key will later be stored as part of the seal footer, accessible only with the
        // asymmetric access private key.
        ::crypto::authed::imp::gen_key()
    }

    pub fn seal(&self, access_key: &::crypto::authed::desc::Key, pt: PlainTextRef) -> CipherText {
        // Generate inner symmetric key.
        let inner_key = ::crypto::authed::imp::gen_key();
        let nonce = ::crypto::authed::imp::gen_nonce();

        // Encrypt plaintext with inner key.
        let additional_data: &[u8] = b"hat_blob_seal~";
        let mut ct = pt.to_ciphertext(additional_data, &nonce, &inner_key);
        ct.append(CipherText::new(nonce.unsecure().to_vec()));

        // Construct footer of length as LittleEndian and inner key.
        let mut foot_pt = PlainText::from_i64(ct.len() as i64);
        foot_pt.0.extend_from_slice(inner_key.unsecure());
        assert_eq!(foot_pt.len(), sealed::desc::footer_plain_bytes());

        // Seal footer with blob data key as it contains all the reference keys.
        let foot_ct = self.seal_blob_data(foot_pt.as_ref());
        assert_eq!(foot_ct.len(), sealed::desc::footer_cipher_bytes());

        let mut access_pt = PlainText::new(foot_ct.to_vec());
        access_pt.0.extend_from_slice(access_key.unsecure());
        assert_eq!(access_pt.len(), sealed::desc::access_plain_bytes());

        let access_ct = self.seal_blob_access(access_pt.as_ref());
        assert_eq!(access_ct.len(), sealed::desc::access_cipher_bytes());

        // Append sealed footer to symmetric cipher text.
        ct.append(access_ct);
        assert_eq!(ct.len(), pt.len() + sealed::desc::overhead());
        ct
    }

    pub fn unseal_access_ctx<'a>(
        &self,
        ct: CipherTextRef<'a>,
    ) -> Result<(::crypto::authed::desc::Key, CipherText, CipherTextRef<'a>), CryptoError> {
        // Read sealed ciphertext length and unseal it.
        let (rest, access_ct) = ct.split_from_right(sealed::desc::access_cipher_bytes())?;
        let mut access_pt = self.unseal_blob_access(access_ct).into_vec();
        assert_eq!(access_pt.len(), sealed::desc::access_plain_bytes());

        let access_key = access_pt
            .split_off(sealed::desc::access_plain_bytes() - ::crypto::authed::desc::KEYBYTES);
        Ok((
            ::crypto::authed::desc::Key::from(&access_key[..]),
            CipherText::new(access_pt),
            rest,
        ))
    }

    pub fn unseal<'a>(
        &self,
        footer_ct: CipherTextRef,
        ct: CipherTextRef<'a>,
    ) -> Result<(CipherTextRef<'a>, PlainText), CryptoError> {
        assert_eq!(footer_ct.len(), sealed::desc::footer_cipher_bytes());
        let foot_pt = self.unseal_blob_data(footer_ct);
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
        let additional_data: &[u8] = b"hat_blob_seal~";
        let (rest, ct_and_nonce) = ct.split_from_right(ct_len as usize)?;
        let (ct, nonce) = ct_and_nonce.split_from_right(::crypto::authed::desc::NONCEBYTES)?;
        Ok((
            rest,
            ct.to_plaintext(
                additional_data,
                &::crypto::authed::desc::Nonce::from(nonce.0),
                &::crypto::authed::desc::Key::from(inner_key.0),
            )?,
        ))
    }
}
