// Table schemas.

table! {
    hashes {
        id -> BigInt,
        hash -> Binary,
        tag -> BigInt,
        height -> BigInt,
        payload -> Nullable<Binary>,
        blob_ref -> Nullable<Binary>,
    }
}

table!{
    gc_metadata {
        id -> BigInt,
        hash_id -> BigInt,
        family_id -> BigInt,
        gc_int -> BigInt,
        gc_vec -> Binary,
    }
}


// Rust models.

#[derive(Queryable)]
pub struct Hash {
    pub id: i64,
    pub hash: Vec<u8>,
    pub tag: i64,
    pub height: i64,
    pub payload: Option<Vec<u8>>,
    pub blob_ref: Option<Vec<u8>>,
}

#[insertable_into(hashes)]
pub struct NewHash<'a> {
    pub id: i64,
    pub hash: &'a [u8],
    pub tag: i64,
    pub height: i64,
    pub payload: Option<&'a [u8]>,
    pub blob_ref: Option<&'a [u8]>,
}

#[derive(Queryable)]
pub struct GcMetadata {
    pub id: i64,
    pub hash_id: i64,
    pub family_id: i64,
    pub gc_int: i64,
    pub gc_vec: Vec<u8>,
}

#[insertable_into(gc_metadata)]
pub struct NewGcMetadata<'a> {
    pub hash_id: i64,
    pub family_id: i64,
    pub gc_int: i64,
    pub gc_vec: &'a [u8],
}
