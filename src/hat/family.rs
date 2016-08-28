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

use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::str;
use std::sync::mpsc;
use capnp;

use backend::StoreBackend;
use blob;
use hash;
use key;
use root_capnp;
use util::{FileIterator, FnBox, PathHandler};
use errors::HatError;
use hat::insert_path_handler::InsertPathHandler;

fn try_a_few_times_then_panic<F>(mut f: F, msg: &str)
    where F: FnMut() -> bool
{
    for _ in 1 as i32..5 {
        if f() {
            return;
        }
    }
    panic!(msg.to_owned());
}

pub struct Family<B> {
    pub name: String,
    pub key_store: key::Store<B>,
    pub key_store_process: Vec<key::StoreProcess<FileIterator, B>>,
}
impl<B: StoreBackend> Clone for Family<B> {
    fn clone(&self) -> Family<B> {
        Family {
            name: self.name.clone(),
            key_store: self.key_store.clone(),
            key_store_process: self.key_store_process.clone(),
        }
    }
}

impl<B: StoreBackend> Family<B> {
    pub fn snapshot_dir(&self, dir: PathBuf) {
        let handler = InsertPathHandler::new(self.key_store_process.clone());
        handler.recurse(PathBuf::from(&dir), None);
    }

    pub fn snapshot_direct(&self,
                           file: key::Entry,
                           is_directory: bool,
                           contents: Option<FileIterator>)
                           -> Result<(), HatError> {
        let f = if is_directory {
            None
        } else {
            Some(Box::new(move |()| contents) as Box<FnBox<(), _>>)
        };
        match try!(self.key_store_process[0].send_reply(key::Msg::Insert(file, f))) {
            key::Reply::Id(..) => return Ok(()),
            _ => Err(From::from("Unexpected reply from key store")),
        }
    }

    pub fn flush(&self) -> Result<(), HatError> {
        for ks in &self.key_store_process {
            if let key::Reply::FlushOk = try!(ks.send_reply(key::Msg::Flush)) {
                continue;
            }
            return Err(From::from("Unexpected reply from key store"));
        }
        Ok(())
    }

  pub fn write_file_chunks<HTB: hash::tree::HashTreeBackend<Err=key::MsgError>>(
    &self, fd: &mut fs::File, tree: hash::tree::ReaderResult<HTB>)
  {
        for chunk in tree {
            try_a_few_times_then_panic(|| fd.write_all(&chunk[..]).is_ok(),
                                       "Could not write chunk.");
        }
        try_a_few_times_then_panic(|| fd.flush().is_ok(), "Could not flush file.");
    }

    pub fn checkout_in_dir(&self,
                           output_dir: PathBuf,
                           dir_id: Option<u64>)
                           -> Result<(), HatError> {
        let mut path = output_dir;
        for (entry, _ref, read_fn_opt) in try!(self.list_from_key_store(dir_id)).into_iter() {
            // Extend directory with filename:
            path.push(str::from_utf8(&entry.name[..]).unwrap());

            match read_fn_opt {
                None => {
                    // This is a directory, recurse!
                    fs::create_dir_all(&path).unwrap();
                    try!(self.checkout_in_dir(path.clone(), entry.id));
                }
                Some(read_fn) => {
                    // This is a file, write it
                    let mut fd = fs::File::create(&path).unwrap();
                    if let Some(tree) = try!(read_fn.init()) {
                        self.write_file_chunks(&mut fd, tree);
                    }
                }
            }

            // Prepare for next filename:
            path.pop();
        }

        Ok(())
    }

    pub fn list_from_key_store(&self,
                               dir_id: Option<u64>)
                               -> Result<Vec<key::DirElem<B>>, HatError> {
        match try!(self.key_store_process[0].send_reply(key::Msg::ListDir(dir_id))) {
            key::Reply::ListResult(ls) => Ok(ls),
            _ => Err(From::from("Unexpected result from key store")),
        }
    }

    pub fn fetch_dir_data<HTB: hash::tree::HashTreeBackend<Err = key::MsgError>>
        (&self,
         dir_hash: &hash::Hash,
         dir_ref: blob::ChunkRef,
         backend: HTB)
         -> Result<Vec<(key::Entry, hash::Hash, blob::ChunkRef)>, HatError> {
        let mut out = Vec::new();
        let it = try!(hash::tree::SimpleHashTreeReader::open(backend, dir_hash, Some(dir_ref)))
            .expect("unable to open dir");

        for chunk in it {
            if chunk.is_empty() {
                continue;
            }
            let reader =
                capnp::serialize_packed::read_message(&mut &chunk[..],
                                                      capnp::message::ReaderOptions::new())
                    .unwrap();

            let list = reader.get_root::<root_capnp::file_list::Reader>().unwrap();
            for f in list.get_files().unwrap().iter() {
                if f.get_name().unwrap().len() == 0 {
                    // Empty entry at end.
                    // TODO(jos): Can we get rid of these?
                    break;
                }
                let entry = key::Entry {
                    id: Some(f.get_id()),
                    name: f.get_name().unwrap().to_owned(),
                    created: match f.get_created().which().unwrap() {
                        root_capnp::file::created::Unknown(()) => None,
                        root_capnp::file::created::Timestamp(ts) => Some(ts),
                    },
                    modified: match f.get_modified().which().unwrap() {
                        root_capnp::file::modified::Unknown(()) => None,
                        root_capnp::file::modified::Timestamp(ts) => Some(ts),
                    },
                    accessed: match f.get_accessed().which().unwrap() {
                        root_capnp::file::accessed::Unknown(()) => None,
                        root_capnp::file::accessed::Timestamp(ts) => Some(ts),
                    },
                    data_hash: match f.get_content().which().unwrap() {
                        root_capnp::file::content::Data(r) => {
                            Some(r.unwrap().get_hash().unwrap().to_owned())
                        }
                        root_capnp::file::content::Directory(_) => None,
                    },
                    // TODO(jos): Implement support for these remaining fields.
                    user_id: None,
                    group_id: None,
                    permissions: None,
                    data_length: None,
                    parent_id: None,
                };
                let hash = match f.get_content().which().unwrap() {
                    root_capnp::file::content::Data(r) => r.unwrap().get_hash().unwrap().to_owned(),
                    root_capnp::file::content::Directory(d) => {
                        d.unwrap().get_hash().unwrap().to_owned()
                    }
                };
                let pref = match f.get_content().which().unwrap() {
                    root_capnp::file::content::Data(r) => {
                        blob::ChunkRef::read_msg(&r.unwrap().get_chunk_ref().unwrap()).unwrap()
                    }
                    root_capnp::file::content::Directory(d) => {
                        blob::ChunkRef::read_msg(&d.unwrap().get_chunk_ref().unwrap()).unwrap()
                    }
                };

                out.push((entry, hash::Hash { bytes: hash }, pref));
            }
        }

        Ok(out)
    }

    pub fn commit(&mut self,
                  hash_ch: &mpsc::Sender<hash::Hash>)
                  -> Result<(hash::Hash, blob::ChunkRef), HatError> {
        let mut top_tree = self.key_store.hash_tree_writer();
        try!(self.commit_to_tree(&mut top_tree, None, hash_ch));

        Ok(try!(top_tree.hash()))
    }

    pub fn commit_to_tree(&mut self,
                          tree: &mut hash::tree::SimpleHashTreeWriter<key::HashStoreBackend<B>>,
                          dir_id: Option<u64>,
                          hash_ch: &mpsc::Sender<hash::Hash>)
                          -> Result<(), HatError> {

        let files_at_a_time = 1024;
        let mut it = try!(self.list_from_key_store(dir_id)).into_iter();

        loop {
            let mut current_msg_is_empty = true;
            let mut file_block_msg = capnp::message::Builder::new_default();

            {
                let files_root = file_block_msg.init_root::<root_capnp::file_list::Builder>();
                let mut files = files_root.init_files(files_at_a_time as u32);

                for (idx, (entry, data_ref, _data_res_open)) in it.by_ref()
                    .take(files_at_a_time)
                    .enumerate() {
                    assert!(idx < files_at_a_time);

                    current_msg_is_empty = false;
                    let mut file_msg = files.borrow().get(idx as u32);

                    file_msg.set_id(entry.id.unwrap_or(0));
                    file_msg.set_name(&entry.name);

                    match entry.created {
                        None => file_msg.borrow().init_created().set_unknown(()),
                        Some(ts) => file_msg.borrow().init_created().set_timestamp(ts),
                    }

                    match entry.modified {
                        None => file_msg.borrow().init_modified().set_unknown(()),
                        Some(ts) => file_msg.borrow().init_modified().set_timestamp(ts),
                    }

                    match entry.accessed {
                        None => file_msg.borrow().init_accessed().set_unknown(()),
                        Some(ts) => file_msg.borrow().init_accessed().set_timestamp(ts),
                    }

                    if let Some(hash_bytes) = entry.data_hash {
                        // This is a file, store its data hash:
                        let mut hash_ref_msg = capnp::message::Builder::new_default();
                        let mut hash_ref_root =
                            hash_ref_msg.init_root::<root_capnp::hash_ref::Builder>();

                        // Populate data hash and ChunkRef.
                        hash_ref_root.set_hash(&hash_bytes);
                        data_ref.expect("has data")
                            .populate_msg(hash_ref_root.borrow().init_chunk_ref());
                        // Set as file content.
                        try!(file_msg.borrow()
                            .init_content()
                            .set_data(hash_ref_root.as_reader()));
                        hash_ch.send(hash::Hash { bytes: hash_bytes }).unwrap();
                    } else {
                        drop(data_ref);  // May not use data reference without hash.

                        // This is a directory, recurse!
                        let mut inner_tree = self.key_store.hash_tree_writer();
                        try!(self.commit_to_tree(&mut inner_tree, entry.id, hash_ch));
                        // Store a reference for the sub-tree in our tree:
                        let (dir_hash, dir_ref) = try!(inner_tree.hash());

                        let mut hash_ref_msg = capnp::message::Builder::new_default();
                        let mut hash_ref_root =
                            hash_ref_msg.init_root::<root_capnp::hash_ref::Builder>();

                        // Populate directory hash and ChunkRef.
                        hash_ref_root.set_hash(&dir_hash.bytes);
                        dir_ref.populate_msg(hash_ref_root.borrow().init_chunk_ref());
                        // Set as directory content.
                        try!(file_msg.borrow()
                            .init_content()
                            .set_directory(hash_ref_root.as_reader()));

                        hash_ch.send(dir_hash).unwrap();
                    }
                }
            }

            // Flush to our own tree when we have a decent amount.
            // The tree prevents large directories from clogging ram.
            if current_msg_is_empty {
                break;
            } else {
                let mut buf = vec![];
                try!(capnp::serialize_packed::write_message(&mut buf, &file_block_msg));
                try!(tree.append(buf));
            }
        }

        Ok(())
    }
}
