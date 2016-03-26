use diesel::prelude::*;


// Table schemas.

table! {
    blobs {
        id -> BigInt,
        name -> Binary,
        tag -> Integer,
    }
}


// Rust models.

#[derive(Queryable)]
pub struct Blob {
    pub id: i64,
    pub name: Vec<u8>,
    pub tag: i32,
}

#[insertable_into(blobs)]
pub struct NewBlob<'a> {
    pub id: i64,
    pub name: &'a [u8],
    pub tag: i32,
}
