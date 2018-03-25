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

use blob;
use libsodium_sys;
use secstr;
use argon2rs;

struct PublicKey(secstr::SecStr);
struct SecretKey(secstr::SecStr);

pub fn compute_salt(node_type: blob::NodeType, leaf_type: blob::LeafType) -> Box<[u8]> {
    use byteorder::{LittleEndian, WriteBytesExt};

    let mut salt = [0u8; 16];
    {
        let (mut left, mut right) = salt.split_at_mut(8);
        left.write_u64::<LittleEndian>(From::from(node_type))
            .unwrap();
        right
            .write_u64::<LittleEndian>(From::from(leaf_type))
            .unwrap();
    }

    Box::new(salt)
}

pub fn random_bytes(size: usize) -> secstr::SecStr {
    let mut r = vec![0u8; size];
    unsafe { libsodium_sys::randombytes_buf(r.as_mut_ptr(), r.len()) };
    secstr::SecStr::new(r)
}

pub fn keyed_fingerprint(sk: &[u8], msg: &[u8], salt: &[u8], out: &mut [u8]) {
    use libsodium_sys::{crypto_generichash_blake2b_PERSONALBYTES,
                        crypto_generichash_blake2b_SALTBYTES};
    assert_eq!(crypto_generichash_blake2b_SALTBYTES, salt.len());

    let outlen = out.len();
    let personal: &[u8; libsodium_sys::crypto_generichash_blake2b_PERSONALBYTES] =
        b"hat-backup~~~~~a";

    let ret = unsafe {
        libsodium_sys::crypto_generichash_blake2b_salt_personal(
            out.as_mut_ptr(),
            outlen,
            msg.as_ptr(),
            msg.len() as u64,
            sk.as_ptr(),
            sk.len(),
            salt.as_ptr() as *const [u8; crypto_generichash_blake2b_SALTBYTES],
            personal.as_ptr() as *const [u8; crypto_generichash_blake2b_PERSONALBYTES],
        )
    };
    assert_eq!(ret, 0);
}

pub struct Keeper {
    universal_key: secstr::SecStr,
    fingerprint_key: Option<secstr::SecStr>,
    blob_authentication_key: Option<secstr::SecStr>,

    data_key_pk: Option<PublicKey>,
    data_key_sk: Option<SecretKey>,

    naming_key_pk: Option<PublicKey>,
    naming_key_sk: Option<SecretKey>,

    access_key_pk: Option<PublicKey>,
    access_key_sk: Option<SecretKey>,
}

impl Keeper {
    pub fn new(universal: &str) -> Keeper {
        let app: &str = "hat-backup:universal-key";
        let mut keeper = Keeper {
            universal_key: Keeper::strengthen(universal, app),
            fingerprint_key: None,
            blob_authentication_key: None,
            data_key_pk: None,
            data_key_sk: None,
            access_key_pk: None,
            access_key_sk: None,
            naming_key_pk: None,
            naming_key_sk: None,
        };
        keeper.init();
        keeper
    }

    #[cfg(test)]
    pub fn new_for_testing() -> Keeper {
        let mut keeper = Keeper {
            universal_key: secstr::SecStr::new(vec![0; 32]),
            fingerprint_key: None,
            blob_authentication_key: None,
            data_key_pk: None,
            data_key_sk: None,
            access_key_pk: None,
            access_key_sk: None,
            naming_key_pk: None,
            naming_key_sk: None,
        };
        keeper.init();
        keeper
    }

    fn init(&mut self) {
        // Generate key used for fingerprinting.
        self.fingerprint_key = Some(self.from_nonce("hat:FINGERPRINT-key".as_bytes(), 64));

        // Generate key for authenticating blob data.
        self.blob_authentication_key =
            Some(self.from_nonce("hat:BLOB-AUTHENTICATION-key".as_bytes(), 64));

        // Generate data key.
        // Required for reading blob data without a direct reference.
        let (pk, sk) = self.x25519_key_pair_from_nonce("hat:DATA-key-x25519".as_bytes());
        self.data_key_pk = Some(pk);
        self.data_key_sk = Some(sk);

        // Generate access key.
        // Required for reading any blob data (with direct reference or with data key).
        let (pk, sk) = self.x25519_key_pair_from_nonce("hat:ACCESS-key-x25519".as_bytes());
        self.access_key_pk = Some(pk);
        self.access_key_sk = Some(sk);

        // Generate naming key.
        // Required for reading blob names.
        let (pk, sk) = self.x25519_key_pair_from_nonce("hat:NAMING-key-x25519".as_bytes());
        self.naming_key_pk = Some(pk);
        self.naming_key_sk = Some(sk);
    }

    fn strengthen(phrase: &str, salt: &str) -> secstr::SecStr {
        let passes = 5;
        let threads = 2;
        let kib = 16 * 1024;

        let argon2 =
            argon2rs::Argon2::new(passes, threads, kib, argon2rs::Variant::Argon2d).unwrap();

        let mut out = vec![0; 64];
        argon2.hash(&mut out[..], phrase.as_bytes(), salt.as_bytes(), &[], &[]);
        secstr::SecStr::new(out)
    }

    pub fn from_nonce(&self, nonce: &[u8], outlen: usize) -> secstr::SecStr {
        let mut out = secstr::SecStr::new(vec![0; outlen]);
        let salt: &[u8; 16] = b"nonce~~~nonce~~~";
        keyed_fingerprint(
            &self.universal_key.unsecure(),
            &nonce[..],
            salt,
            out.unsecure_mut(),
        );
        out
    }

    fn x25519_key_pair_from_nonce(&self, nonce: &[u8]) -> (PublicKey, SecretKey) {
        let mut pk = secstr::SecStr::new(vec![0; 32]);
        let mut sk = secstr::SecStr::new(vec![0; 32]);

        let seed = self.from_nonce(nonce, 32);

        let ret = unsafe {
            libsodium_sys::crypto_box_seed_keypair(
                pk.unsecure_mut().as_mut_ptr() as *mut [u8; 32],
                sk.unsecure_mut().as_mut_ptr() as *mut [u8; 32],
                seed.unsecure().as_ptr() as *const [u8; 32],
            )
        };
        assert_eq!(ret, 0);

        (PublicKey(pk), SecretKey(sk))
    }

    fn asymmetric_lock(pk: &PublicKey, msg: &[u8]) -> Vec<u8> {
        let mut out = vec![0; msg.len() + libsodium_sys::crypto_box_SEALBYTES];
        let ret = unsafe {
            libsodium_sys::crypto_box_seal(
                out.as_mut_ptr(),
                msg.as_ptr(),
                msg.len() as u64,
                pk.0.unsecure().as_ptr() as *const [u8; 32],
            )
        };
        assert_eq!(0, ret);

        out
    }

    fn asymmetric_unlock(pk: &PublicKey, sk: &SecretKey, ciphertext: &[u8]) -> Vec<u8> {
        let mut out = vec![0; ciphertext.len() - libsodium_sys::crypto_box_SEALBYTES];
        let ret = unsafe {
            libsodium_sys::crypto_box_seal_open(
                out.as_mut_ptr(),
                ciphertext.as_ptr(),
                ciphertext.len() as u64,
                pk.0.unsecure().as_ptr() as *const [u8; 32],
                sk.0.unsecure().as_ptr() as *const [u8; 32],
            )
        };
        assert_eq!(0, ret);

        out
    }

    pub fn data_lock(&self, msg: &[u8]) -> Vec<u8> {
        Keeper::asymmetric_lock(
            self.data_key_pk.as_ref().expect("need data public key"),
            msg,
        )
    }

    pub fn data_unlock(&self, ciphertext: &[u8]) -> Vec<u8> {
        Keeper::asymmetric_unlock(
            self.data_key_pk.as_ref().expect("need data public key"),
            self.data_key_sk.as_ref().expect("need data private key"),
            ciphertext,
        )
    }

    pub fn access_lock(&self, msg: &[u8]) -> Vec<u8> {
        Keeper::asymmetric_lock(
            self.access_key_pk
                .as_ref()
                .expect("need access publick key"),
            msg,
        )
    }

    pub fn access_unlock(&self, ciphertext: &[u8]) -> Vec<u8> {
        Keeper::asymmetric_unlock(
            self.access_key_pk.as_ref().expect("need access public key"),
            self.access_key_sk
                .as_ref()
                .expect("need access private key"),
            ciphertext,
        )
    }

    pub fn naming_lock(&self, msg: &[u8]) -> Vec<u8> {
        Keeper::asymmetric_lock(
            self.naming_key_pk.as_ref().expect("need naming public key"),
            msg,
        )
    }

    pub fn naming_unlock(&self, ciphertext: &[u8]) -> Vec<u8> {
        Keeper::asymmetric_unlock(
            self.naming_key_pk.as_ref().expect("need naming public key"),
            self.naming_key_sk
                .as_ref()
                .expect("need naming private key"),
            ciphertext,
        )
    }

    pub fn fingerprint(&self, msg: &[u8], salt: &[u8], out: &mut [u8]) {
        let key = self.fingerprint_key.as_ref().expect("need fingerprint key");
        keyed_fingerprint(key.unsecure(), msg, salt, out);
    }

    pub fn blob_authentication(&self, blob: &[u8], out: &mut [u8]) {
        let key = self.blob_authentication_key
            .as_ref()
            .expect("need blob authentication key");
        let salt: &[u8; 16] = b"blob~~~~blob~~~~";
        keyed_fingerprint(key.unsecure(), blob, salt, &mut out[..])
    }

    pub fn symmetric_lock(msg: &[u8], ad: &[u8], nonce: &[u8], key: &[u8]) -> Vec<u8> {
        let mut out = vec![0u8; msg.len() + libsodium_sys::crypto_aead_chacha20poly1305_ABYTES];
        let mut out_len = 0;

        let ret = unsafe {
            libsodium_sys::crypto_aead_chacha20poly1305_encrypt(
                out.as_mut_ptr(),
                &mut out_len,
                msg.as_ptr(),
                msg.len() as u64,
                ad.as_ptr(),
                ad.len() as u64,
                &[0u8; 0],
                nonce.as_ptr() as *const [u8; 8],
                key.as_ptr() as *const [u8; 32],
            )
        };
        assert_eq!(0, ret);
        assert_eq!(out_len, out.len() as u64);

        out
    }

    pub fn symmetric_unlock(key: &[u8], ciphertext: &[u8], ad: &[u8], nonce: &[u8]) -> Vec<u8> {
        let mut out =
            vec![0u8; ciphertext.len() - libsodium_sys::crypto_aead_chacha20poly1305_ABYTES];
        let mut out_len = 0;

        let ret = unsafe {
            libsodium_sys::crypto_aead_chacha20poly1305_decrypt(
                out.as_mut_ptr(),
                &mut out_len,
                &mut [0u8; 0],
                ciphertext.as_ptr(),
                ciphertext.len() as u64,
                ad.as_ptr(),
                ad.len() as u64,
                nonce.as_ptr() as *const [u8; 8],
                key.as_ptr() as *const [u8; 32],
            )
        };
        assert_eq!(0, ret);
        assert_eq!(out_len, out.len() as u64);

        out
    }
}
