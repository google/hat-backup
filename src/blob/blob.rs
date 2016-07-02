use sodiumoxide::randombytes::randombytes;

use capnp;
use root_capnp;


#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Kind {
    TreeBranch = 1,
    TreeLeaf = 2,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ChunkRef {
    pub blob_id: Vec<u8>,
    pub offset: usize,
    pub length: usize,
    pub kind: Kind,
}

impl ChunkRef {
    pub fn from_bytes(bytes: &mut &[u8]) -> Result<ChunkRef, capnp::Error> {
        let reader = try!(capnp::serialize_packed::read_message(bytes,
                                                       capnp::message::ReaderOptions::new()));
        let root = try!(reader.get_root::<root_capnp::chunk_ref::Reader>());

        Ok(try!(ChunkRef::read_msg(&root)))
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

    pub fn populate_msg(&self, msg: root_capnp::chunk_ref::Builder) {
        let mut msg = msg;
        msg.set_blob_id(&self.blob_id[..]);
        msg.set_offset(self.offset as i64);
        msg.set_length(self.length as i64);
        match self.kind {
            Kind::TreeLeaf => msg.init_kind().set_tree_leaf(()),
            Kind::TreeBranch => msg.init_kind().set_tree_branch(()),
        }
    }

    pub fn read_msg(msg: &root_capnp::chunk_ref::Reader) -> Result<ChunkRef, capnp::Error> {
        Ok(ChunkRef {
            blob_id: try!(msg.get_blob_id()).to_owned(),
            offset: msg.get_offset() as usize,
            length: msg.get_length() as usize,
            kind: match try!(msg.get_kind().which()) {
                root_capnp::chunk_ref::kind::TreeBranch(()) => Kind::TreeBranch,
                root_capnp::chunk_ref::kind::TreeLeaf(()) => Kind::TreeLeaf,
            },
        })
    }
}

pub struct Blob {
    chunks: Vec<u8>,
    footer: Vec<u8>,
    overhead: usize,
    max_size: usize,
}

impl Blob {
    pub fn new(max_size: usize) -> Blob {
        Blob {
            chunks: Vec::with_capacity(max_size),
            footer: Vec::with_capacity(max_size / 2),
            overhead: 3 * Blob::len_into(max_size - 1, &mut Vec::new()),
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

    pub fn try_append(&mut self, mut chunk: Vec<u8>, cref: &ChunkRef) -> Result<(), Vec<u8>> {
        assert!(self.max_size > chunk.len());

        let mut entry = cref.as_bytes();

        if self.upperbound_len() + entry.len() + chunk.len() >= self.max_size {
            return Err(chunk);
        }

        self.chunks.append(&mut chunk);

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

        out.append(&mut self.chunks);

        let mut footer = Vec::new();
        footer.append(&mut self.footer);

        let meta_len = Self::len_into(footer.len(), &mut footer);
        let meta_meta_len = Self::len_into(meta_len, &mut footer);

        assert!(meta_meta_len < 255);
        footer.push(meta_meta_len as u8);

        assert!(out.len() + footer.len() <= self.max_size);
        let left = self.max_size - out.len() - footer.len();
        if left > 0 {
            out.append(&mut randombytes(left));
        }
        out.append(&mut footer);

        // Everything has been reset. We are ready to go again.
        assert_eq!(0, self.chunks.len());
        assert_eq!(0, self.footer.len());
    }

    fn len_into(len: usize, mut out: &mut Vec<u8>) -> usize {
        let mut message = ::capnp::message::Builder::new_default();
        {
            let mut root = message.init_root::<::root_capnp::meta_footer_entry::Builder>();
            root.set_length(len as i64);
        }

        let before_len = out.len();
        capnp::serialize_packed::write_message(&mut out, &message).unwrap();

        out.len() - before_len
    }

    fn len_from(bytes: &mut &[u8]) -> Result<i64, capnp::Error> {
        let reader = try!(
            capnp::serialize_packed::read_message(bytes, capnp::message::ReaderOptions::new()));
        let root = try!(reader.get_root::<root_capnp::meta_footer_entry::Reader>());
        Ok(root.get_length())
    }

    pub fn chunk_refs_from_bytes(bytes: &[u8]) -> Result<Vec<ChunkRef>, capnp::Error> {
        if bytes.len() == 0 {
            return Ok(Vec::new());
        }

        let meta_meta_len = *bytes.last().unwrap() as usize;
        let mut pos = bytes.len() - 1;
        assert!(pos >= meta_meta_len);
        pos -= meta_meta_len;

        let meta_len = try!(Blob::len_from(&mut &bytes[pos..pos + meta_meta_len])) as usize;
        assert!(pos >= meta_len);
        pos -= meta_len;

        let footer_len = try!(Blob::len_from(&mut &bytes[pos..pos + meta_len])) as usize;
        assert!(pos >= footer_len);
        pos -= footer_len;

        let mut footer = &bytes[pos..pos + footer_len];
        let mut crefs = Vec::new();
        while footer.len() > 0 {
            let len = footer[0] as usize;
            assert!(footer.len() > len);

            crefs.push(try!(ChunkRef::from_bytes(&mut &footer[1..1 + len])));
            footer = &footer[len + 1..];
        }

        Ok(crefs)
    }
}
