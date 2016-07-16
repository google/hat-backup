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
