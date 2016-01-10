extern crate capnpc;

fn main() {
    ::capnpc::compile("schema", &["schema/root.capnp"]).unwrap();
}
