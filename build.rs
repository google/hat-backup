extern crate capnpc;

fn main() {
    ::capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("schema/root.capnp")
        .run()
        .expect("capnp schema compiler failed");
}
