extern crate capnpc;

use std::env;
use std::path::Path;

fn build_diesel(paths: &[(&str, &str)]) {
    extern crate diesel_codegen_syntex as diesel_codegen;

    let out_dir = env::var_os("OUT_DIR").unwrap();

    for &(src, dst) in paths {
        let src = Path::new("src").join(src);
        let dst = Path::new(&out_dir).join(dst);
        diesel_codegen::expand(&src, &dst).unwrap();
    }
}

fn main() {
    ::capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("schema/root.capnp")
        .run().expect("capnp schema compiler failed");

    build_diesel(&[
        ("blob/schema.in.rs", "blob-schema.rs"),
        ("hash/schema.in.rs", "hash-schema.rs"),
        ("key/schema.in.rs", "key-schema.rs"),
        ("snapshot/schema.in.rs", "snapshot-schema.rs"),
    ]);
}
