use sodiumoxide::crypto::stream;
use blob::{ChunkRef, Kind};


pub struct PlainText(Vec<u8>);
pub struct CipherText(Vec<u8>);
pub struct CipherTextRef<'a>(&'a [u8]);


impl PlainText {
    pub fn new(bytes: Vec<u8>) -> PlainText {
        PlainText(bytes)
    }
    pub fn from_vec(mut bytes: &mut Vec<u8>) -> PlainText {
        let mut pt = PlainText(Vec::with_capacity(bytes.len()));
        pt.0.append(&mut bytes);
        pt
    }
    pub fn append(&mut self, mut other: PlainText) {
        self.0.append(&mut other.0);
    }
    pub fn len(&self) -> usize {
        self.0.len()
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
    pub fn into_vec(self) -> Vec<u8> {
        self.0
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
    pub fn as_ref(&self) -> CipherTextRef {
        CipherTextRef(self.0.as_ref())
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
    pub fn split_from_right(&self, len: usize) -> (CipherTextRef<'a>, CipherTextRef<'a>) {
        assert!(self.len() >= len);
        (self.slice(0, self.len() - len), self.slice(self.len() - len, self.len()))
    }
}


pub struct RefKey {}

impl RefKey {
    pub fn add_sealed_size(size: usize) -> usize {
        size + 0  // TODO: should be len of seal(.., vec[0; size])
    }
    pub fn seal(cref: &mut ChunkRef, pt: PlainText) -> CipherText {
        // TODO(jos): WIP: Plug in encryption/crypto here.
        // Update cref with key.
        cref.length = pt.len();
        CipherText(pt.0)
    }

    pub fn unseal(cref: &ChunkRef, ct: CipherTextRef) -> PlainText {
        PlainText(ct.slice(cref.offset, cref.offset + cref.length).0.to_vec())
    }
}

pub struct FixedKey {
    key: Vec<u8>,
}

impl FixedKey {
    pub fn new(key: Vec<u8>) -> FixedKey {
        FixedKey { key: key }
    }

    fn fixed_key_seal(&self, pt: PlainText) -> CipherText {
        // TODO(jos): WIP: Plug in encryption/crypto here.
        // Seal with our fixed key.
        let nonce = stream::xsalsa20::gen_nonce();
        let mut version = vec![pt.len() as u8, 0, 0, 1];
        let mut ct = CipherText(pt.0);

        // Add nonce.
        ct.0.extend_from_slice(nonce.as_ref());

        // Add version with size of ciphertext.
        assert!(ct.len() < 255);
        version[0] = ct.len() as u8;
        ct.0.extend_from_slice(&version[..]);
        ct
    }

    fn ciphertext_and_version<'a>(ct: CipherTextRef<'a>) -> (CipherTextRef<'a>, CipherTextRef<'a>) {
        let (rest, version) = ct.split_from_right(4);

        assert_eq!(1, version.0[3]);
        assert_eq!(0, version.0[2]);
        assert_eq!(0, version.0[1]);
        assert!(rest.len() >= version.0[0] as usize);

        (rest, version)
    }

    fn fixed_key_unseal<'a, 'b>(&'a self, ct: CipherTextRef<'b>) -> (CipherTextRef<'b>, PlainText) {
        // TODO(jos): WIP: Plug in encryption/crypto here.
        // Unseal with our fixed key.

        // Recover version with size of ciphertext.
        let (ct, version) = Self::ciphertext_and_version(ct);
        let ct_len = version.0[0] as usize;
        let (rest, ct) = ct.split_from_right(ct_len);

        // Recover nonce.
        let (ct, nonce) = ct.split_from_right(stream::xsalsa20::NONCEBYTES);
        let nonce = stream::xsalsa20::Nonce::from_slice(nonce.0);
        // TODO(jos): WIP: Use nonce to decrypt ct.
        drop(nonce);

        (rest, PlainText(ct.0.to_vec()))
    }

    pub fn seal(&self, pt: PlainText) -> CipherText {
        // Seal plaintext with random key stored in reference.
        let mut cref = ChunkRef {
            blob_id: vec![],
            offset: 0,
            length: pt.0.len(),
            kind: Kind::TreeLeaf,
        };
        let mut ct = RefKey::seal(&mut cref, pt);

        // Seal reference with our fixed master key.
        ct.append(&mut self.fixed_key_seal(PlainText(cref.as_bytes())));
        ct
    }

    pub fn unseal(&self, ct: CipherTextRef) -> PlainText {
        assert!(ct.len() >= 4);

        // Recover ChunkRef which contains the random key.
        let (ct, pt_cref) = self.fixed_key_unseal(ct);
        let cref = ChunkRef::from_bytes(&mut &pt_cref.0[..]).unwrap();
        let (_, ct) = ct.split_from_right(cref.length);

        // Use ChunkRef to decrypt remaining CipherText.
        RefKey::unseal(&cref, ct)
    }
}
