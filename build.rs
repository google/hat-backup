extern crate capnpc;
extern crate syntex;
extern crate diesel_codegen;

use std::env;
use std::path::Path;

fn build_diesel(paths: &[(&str, &str)]) {
    let out_dir = env::var_os("OUT_DIR").unwrap();

    for &(src, dst) in paths {
        let src = Path::new("src/hat").join(src);
        let dst = Path::new(&out_dir).join(dst);

        let mut registry = syntex::Registry::new();
        diesel_codegen::register(&mut registry);
        registry.expand("", &src, &dst).unwrap();
    }
}

fn main() {
    ::capnpc::compile("schema", &["schema/root.capnp"]).unwrap();
    build_diesel(&[
        ("blob/schema.in.rs", "blob-schema.rs"),
        ("hash/schema.in.rs", "hash-schema.rs"),
        ("key/schema.in.rs", "key-schema.rs"),
        ("snapshot/schema.in.rs", "snapshot-schema.rs"),
    ]);
}
