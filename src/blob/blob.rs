use crypto;
use crypto::{CipherText, CipherTextRef, PlainText};

use super::{ChunkRef, Kind};
use capnp;
use root_capnp;


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
        let master_key = crypto::FixedKey::new(Vec::new());

        let mut len = 0;
        let mut i = max_size;
        while i > 0 {
            len = len | i;
            i = i >> 1;
        }

        let overhead = master_key.seal(PlainText::new(vec![0; len])).len() - len + 2;

        Blob {
            master_key: master_key,
            chunks: CipherText::new(Vec::with_capacity(max_size)),
            footer: Vec::with_capacity(max_size / 2),
            overhead: overhead,
            max_size: max_size,
        }
    }

    pub fn upperbound_len(&self) -> usize {
        if self.chunks.len() == 0 {
            0
        } else {
            self.chunks.len() + self.footer.len() + self.overhead
        }
    }

    pub fn chunk_len(&self) -> usize {
        self.chunks.len()
    }

    pub fn try_append(&mut self, chunk: Vec<u8>, mut cref: &mut ChunkRef) -> Result<(), Vec<u8>> {
        assert!(self.max_size > chunk.len());

        let sealed_size = crypto::RefKey::add_sealed_size(chunk.len());
        let mut entry = cref.as_bytes();

        if self.upperbound_len() + entry.len() + sealed_size >= self.max_size {
            return Err(chunk);
        }

        self.chunks.append(&mut crypto::RefKey::seal(&mut cref, PlainText::new(chunk)));

        // Generate footer entry.
        let size = entry.len();
        assert!(size < 255);
        self.footer.push(size as u8);
        self.footer.append(&mut entry);

        Ok(())
    }

    pub fn into_bytes(&mut self, mut out: &mut Vec<u8>) {
        if self.chunks.len() == 0 {
            return;
        }

        let mut ct = CipherText::from(&mut self.chunks);
        ct.append(&mut self.chunks);

        let mut footer = self.master_key.seal(PlainText::from_vec(&mut self.footer));

        assert!(ct.len() + footer.len() <= self.max_size);
        ct.random_pad_upto(self.max_size - footer.len());
        ct.append(&mut footer);

        // Everything has been reset. We are ready to go again.
        assert_eq!(0, self.chunks.len());
        assert_eq!(0, self.footer.len());

        out.append(&mut ct.into_vec());
    }

    pub fn chunk_refs_from_bytes(&self, bytes: &[u8]) -> Result<Vec<ChunkRef>, capnp::Error> {
        if bytes.len() == 0 {
            return Ok(Vec::new());
        }

        let (_rest, footer_vec) = self.master_key.unseal(CipherTextRef::new(bytes));
        let mut footer_pos = footer_vec.as_ref();

        let mut crefs = Vec::new();
        while footer_pos.len() > 0 {
            let len = footer_pos[0] as usize;
            assert!(footer_pos.len() > len);

            crefs.push(try!(ChunkRef::from_bytes(&mut &footer_pos[1..1 + len])));
            footer_pos = &footer_pos[len + 1..];
        }

        Ok(crefs)
    }
}
