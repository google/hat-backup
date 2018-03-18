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

use backend::StoreBackend;
use blob;
use capnp;
use errors::HatError;
use hash;
use hat::insert_path_handler::InsertPathHandler;
use hat::walker;
use key;
use root_capnp;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::str;
use util::{FileIterator, FnBox, PathHandler};
use filetime;

fn try_a_few_times_then_panic<F>(mut f: F, msg: &str)
where
    F: FnMut() -> bool,
{
    for _ in 1 as i32..5 {
        if f() {
            return;
        }
    }
    panic!(msg.to_owned());
}

pub mod recover {
    use blob;
    use hash;
    use hash::tree;
    use hat::walker;
    use std::collections::VecDeque;
    use std::mem;
    use super::parse_dir_data;

    #[derive(Clone)]
    pub struct Node {
        pub href: tree::HashRef,
        pub childs: Option<Vec<tree::HashRef>>,
    }

    pub struct FileVisitor {
        tops: Vec<hash::Hash>,
        nodes: VecDeque<Node>,
    }

    impl FileVisitor {
        pub fn new() -> FileVisitor {
            FileVisitor {
                tops: vec![],
                nodes: VecDeque::new(),
            }
        }
        pub fn tops(&mut self) -> Vec<hash::Hash> {
            mem::replace(&mut self.tops, vec![])
        }
        pub fn nodes(&mut self) -> VecDeque<Node> {
            mem::replace(&mut self.nodes, VecDeque::new())
        }
    }

    impl tree::Visitor for FileVisitor {
        fn branch_enter(&mut self, href: &tree::HashRef, childs: &Vec<tree::HashRef>) -> bool {
            self.nodes.push_back(Node {
                href: href.clone(),
                childs: Some(childs.clone()),
            });
            true
        }
        fn leaf_enter(&mut self, href: &tree::HashRef) -> bool {
            self.nodes.push_back(Node {
                href: href.clone(),
                childs: None,
            });

            // Do not proceed to read the actual file data.
            false
        }
        fn leaf_leave(&mut self, _chunk: Vec<u8>, _href: &tree::HashRef) -> bool {
            unreachable!();
        }
    }

    impl walker::LikesFiles for FileVisitor {
        fn include_file(&mut self, file: &walker::FileEntry) -> bool {
            if let walker::Content::Data(ref hash_ref) = file.hash_ref {
                self.tops.push(hash_ref.hash.clone());
                true
            } else {
                false
            }
        }

        fn include_dir(&mut self, file: &walker::FileEntry) -> bool {
            if let walker::Content::Dir(ref hash_ref) = file.hash_ref {
                self.tops.push(hash_ref.hash.clone());
                true
            } else {
                false
            }
        }
    }

    pub struct DirVisitor {
        nodes: VecDeque<Node>,
        files: Vec<walker::FileEntry>,
    }

    impl DirVisitor {
        pub fn new() -> DirVisitor {
            DirVisitor {
                nodes: VecDeque::new(),
                files: vec![],
            }
        }
        pub fn nodes(&mut self) -> VecDeque<Node> {
            mem::replace(&mut self.nodes, VecDeque::new())
        }
    }

    impl walker::HasFiles for DirVisitor {
        fn files(&mut self) -> Vec<walker::FileEntry> {
            mem::replace(&mut self.files, vec![])
        }
    }

    impl tree::Visitor for DirVisitor {
        fn branch_enter(&mut self, href: &tree::HashRef, childs: &Vec<tree::HashRef>) -> bool {
            self.nodes.push_back(Node {
                href: href.clone(),
                childs: Some(childs.clone()),
            });
            true
        }
        fn leaf_enter(&mut self, href: &tree::HashRef) -> bool {
            self.nodes.push_back(Node {
                href: href.clone(),
                childs: None,
            });
            // Only proceed to the leaf if it contains a tree list.
            match href.leaf {
                blob::LeafType::TreeList => true,
                blob::LeafType::SnapshotList => false,
                blob::LeafType::FileChunk => unreachable!("Opened a file with DirVisitor"),
            }
        }
        fn leaf_leave(&mut self, chunk: Vec<u8>, _href: &tree::HashRef) -> bool {
            parse_dir_data(&chunk[..], &mut self.files).unwrap();
            true
        }
    }
}

fn parse_dir_data(chunk: &[u8], out: &mut Vec<walker::FileEntry>) -> Result<(), HatError> {
    if chunk.is_empty() {
        return Ok(());
    }

    let reader = capnp::serialize_packed::read_message(
        &mut &chunk[..],
        capnp::message::ReaderOptions::new(),
    ).unwrap();

    let list = reader.get_root::<root_capnp::file_list::Reader>().unwrap();
    for f in list.get_files().unwrap().iter() {
        if f.get_info()?.get_name().unwrap().len() == 0 {
            // Empty entry at end.
            // TODO(jos): Can we get rid of these?
            break;
        }

        let (data, hash_ref) = match f.get_content().which().unwrap() {
            root_capnp::file::content::Data(r) => (
                key::Data::FilePlaceholder,
                walker::Content::Data(
                    hash::tree::HashRef::read_msg(&r.expect("File has no data reference")).unwrap(),
                ),
            ),
            root_capnp::file::content::Directory(d) => (
                key::Data::DirPlaceholder,
                walker::Content::Dir(
                    hash::tree::HashRef::read_msg(&d.expect("Directory has no listing reference"))
                        .unwrap(),
                ),
            ),
            root_capnp::file::content::SymbolicLink(path) => {
                let link = PathBuf::from(String::from_utf8(path?.to_owned()).unwrap());
                (
                    key::Data::Symlink(link.clone()),
                    walker::Content::Link(link),
                )
            }
        };

        let entry = key::Entry {
            info: key::Info::read(f.get_info()?.borrow())?,
            data: data,
            parent_id: None,
            node_id: Some(f.get_id()),
        };

        out.push(walker::FileEntry {
            hash_ref: hash_ref,
            meta: entry,
        });
    }
    Ok(())
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

        let mut parent_path = PathBuf::from("/");

        let dir = fs::canonicalize(dir).unwrap();
        info!("Committing: {}", dir.display());
        assert!(dir.is_absolute());

        let mut bailout = false;
        let mut parent = None;
        let mut inside_non_dir = false;
        for name in dir.iter().map(PathBuf::from).filter(|p| !p.has_root()) {
            if inside_non_dir {
                // The remaining part of the path is inside a link or similar.
                // This should not happen, as the path was canonical.
                warn!(
                    "Ignoring components after non-dir path: {}",
                    parent_path.display()
                );
                bailout = true;
                break;
            }
            parent_path.push(name);
            if let Some(new_parent) = handler.handle_path(&parent, &parent_path) {
                parent = new_parent;
            } else {
                // Trigger warning if this is not the final component.
                // If this is the final component, we just commit'ed a file or link, which is OK.
                inside_non_dir = true;
            }
        }

        if !bailout && dir.is_dir() {
            handler.recurse(PathBuf::from(&dir), parent);

            match self.key_store_process[0].send_reply(key::Msg::CommitReservedNodes(Some(parent)))
            {
                Ok(key::Reply::Ok) => (),
                _ => panic!("Unexpected reply from keystore"),
            }
        } else {
            match self.key_store_process[0].send_reply(key::Msg::CommitReservedNodes(None)) {
                Ok(key::Reply::Ok) => (),
                _ => panic!("Unexpected reply from keystore"),
            }
        }
    }

    pub fn snapshot_direct(
        &self,
        file: key::Entry,
        is_directory: bool,
        contents: Option<FileIterator>,
    ) -> Result<u64, HatError> {
        let f = if is_directory {
            None
        } else {
            Some(Box::new(move |()| contents) as Box<FnBox<(), _>>)
        };
        let ks = self.key_store_process.iter().last().unwrap();
        let id = match ks.send_reply(key::Msg::Insert(file, f))? {
            key::Reply::Id(id) => id,
            _ => return Err(From::from("Unexpected reply from key store")),
        };
        match ks.send_reply(key::Msg::CommitReservedNodes(None)) {
            Ok(key::Reply::Ok) => (),
            _ => return Err(From::from("Unexpected reply from keystore")),
        }
        Ok(id)
    }

    pub fn flush(&self) -> Result<(), HatError> {
        for ks in &self.key_store_process {
            if let key::Reply::FlushOk = ks.send_reply(key::Msg::Flush)? {
                continue;
            }
            return Err(From::from("Unexpected reply from key store"));
        }
        Ok(())
    }

    pub fn write_file_chunks<HTB: hash::tree::HashTreeBackend<Err = key::MsgError>>(
        &self,
        fd: &mut fs::File,
        tree: hash::tree::LeafIterator<HTB>,
    ) {
        for chunk in tree {
            try_a_few_times_then_panic(
                || fd.write_all(&chunk[..]).is_ok(),
                "Could not write chunk.",
            );
        }
        try_a_few_times_then_panic(|| fd.flush().is_ok(), "Could not flush file.");
    }

    // FIXME(jos): Merge with hat's checkout_in_dir which checks out snapshots.
    // (this checkout_in_dir checks out the family index)
    pub fn checkout_in_dir(
        &self,
        output_dir: PathBuf,
        dir_id: Option<u64>,
    ) -> Result<(), HatError> {
        let mut path = output_dir;
        for (entry, _ref, read_fn_opt) in self.list_from_key_store(dir_id)? {
            // Extend directory with filename:
            path.push(str::from_utf8(&entry.info.name[..]).unwrap());

            match entry.data {
                key::Data::DirPlaceholder => {
                    // This is a directory, recurse!
                    fs::create_dir_all(&path).unwrap();
                    self.checkout_in_dir(path.clone(), entry.node_id)?;
                }
                key::Data::FilePlaceholder => {
                    // This is a file, write it
                    let mut fd = fs::File::create(&path).unwrap();
                    if let Some(tree) = read_fn_opt.expect("File has data").init()? {
                        self.write_file_chunks(&mut fd, tree);
                    }
                }
                key::Data::Symlink(link_path) => {
                    use std::os::unix::fs::symlink;
                    symlink(link_path, &path).unwrap()
                }
                _ => unreachable!("Unexpected data entry"),
            }

            if let Some(perms) = entry.info.permissions {
                fs::set_permissions(&path, perms)?;
            }

            if let (Some(m), Some(a)) = (entry.info.modified_ts_secs, entry.info.accessed_ts_secs) {
                let atime = filetime::FileTime::from_seconds_since_1970(a, 0 /* nanos */);
                let mtime = filetime::FileTime::from_seconds_since_1970(m, 0 /* nanos */);
                filetime::set_file_times(&path, atime, mtime).unwrap();
            }

            // Prepare for next filename:
            path.pop();
        }

        Ok(())
    }

    pub fn list_from_key_store(
        &self,
        dir_id: Option<u64>,
    ) -> Result<Vec<key::DirElem<B>>, HatError> {
        match self.key_store_process
            .iter()
            .last()
            .unwrap()
            .send_reply(key::Msg::ListDir(dir_id))?
        {
            key::Reply::ListResult(ls) => Ok(ls),
            _ => Err(From::from("Unexpected result from key store")),
        }
    }

    pub fn fetch_dir_data<HTB: hash::tree::HashTreeBackend<Err = key::MsgError>>(
        &self,
        dir_hash: hash::tree::HashRef,
        backend: HTB,
    ) -> Result<Vec<(key::Entry, walker::Content)>, HatError> {
        let it = hash::tree::LeafIterator::new(backend, dir_hash)?.expect("unable to open dir");

        let mut out = Vec::new();
        for chunk in it {
            if !chunk.is_empty() {
                parse_dir_data(&chunk[..], &mut out)?;
            }
        }

        Ok(out.into_iter().map(|f| (f.meta, f.hash_ref)).collect())
    }

    pub fn commit<F>(&mut self, top_hash_fn: &F) -> Result<hash::tree::HashRef, HatError>
    where
        F: Fn(&hash::Hash),
    {
        let mut top_tree = self.key_store.hash_tree_writer(blob::LeafType::TreeList);
        self.commit_to_tree(&mut top_tree, None, top_hash_fn)?;

        let info = key::Info::new(self.name.clone().into_bytes(), None);
        Ok(top_tree.hash(Some(&info))?)
    }

    pub fn commit_to_tree<F>(
        &mut self,
        tree: &mut hash::tree::SimpleHashTreeWriter<key::HashStoreBackend<B>>,
        dir_id: Option<u64>,
        top_hash_fn: &F,
    ) -> Result<(), HatError>
    where
        F: Fn(&hash::Hash),
    {
        let files_at_a_time = 1024;
        let mut it = self.list_from_key_store(dir_id)?.into_iter();

        loop {
            let mut current_msg_is_empty = true;
            let mut file_block_msg = capnp::message::Builder::new_default();

            {
                let files_root = file_block_msg.init_root::<root_capnp::file_list::Builder>();
                let mut files = files_root.init_files(files_at_a_time as u32);

                for (idx, (entry, data_ref, _data_res_open)) in
                    it.by_ref().take(files_at_a_time).enumerate()
                {
                    assert!(idx < files_at_a_time);

                    current_msg_is_empty = false;
                    let mut file_msg = files.borrow().get(idx as u32);

                    file_msg.set_id(entry.node_id.unwrap_or(0));

                    {
                        entry
                            .info
                            .populate_msg(file_msg.borrow().init_info().borrow());
                    }

                    match entry.data {
                        key::Data::FilePlaceholder => {
                            // This is a file, store its data hash:
                            let mut hash_ref_msg = capnp::message::Builder::new_default();
                            let mut hash_ref_root =
                                hash_ref_msg.init_root::<root_capnp::hash_ref::Builder>();

                            // Populate data hash and ChunkRef.
                            let href = data_ref.expect("Data::File");
                            href.populate_msg(hash_ref_root.borrow());
                            // Set as file content.
                            file_msg
                                .borrow()
                                .init_content()
                                .set_data(hash_ref_root.as_reader())?;

                            top_hash_fn(&hash::Hash {
                                bytes: href.hash.bytes,
                            });
                        }
                        key::Data::DirPlaceholder => {
                            // This is a directory, recurse!
                            let mut inner_tree =
                                self.key_store.hash_tree_writer(blob::LeafType::TreeList);
                            self.commit_to_tree(&mut inner_tree, entry.node_id, top_hash_fn)?;
                            // Store a reference for the sub-tree in our tree:
                            let dir_hash_ref = inner_tree.hash(Some(&entry.info))?;

                            let mut hash_ref_msg = capnp::message::Builder::new_default();
                            let mut hash_ref_root =
                                hash_ref_msg.init_root::<root_capnp::hash_ref::Builder>();

                            // Populate directory hash and ChunkRef.
                            dir_hash_ref.populate_msg(hash_ref_root.borrow());
                            // Set as directory content.
                            file_msg
                                .borrow()
                                .init_content()
                                .set_directory(hash_ref_root.as_reader())?;

                            top_hash_fn(&dir_hash_ref.hash);
                        }
                        key::Data::Symlink(path) => {
                            // Set symbolic link content.
                            file_msg
                                .borrow()
                                .init_content()
                                .set_symbolic_link(path.to_str().unwrap().as_ref());
                        }
                        _ => unreachable!("Unexpected key::Data"),
                    }
                }
            }

            // Flush to our own tree when we have a decent amount.
            // The tree prevents large directories from clogging ram.
            if current_msg_is_empty {
                break;
            } else {
                let mut buf = vec![];
                capnp::serialize_packed::write_message(&mut buf, &file_block_msg)?;
                tree.append(&buf[..])?;
            }
        }

        Ok(())
    }
}
