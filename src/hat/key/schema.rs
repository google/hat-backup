use diesel::prelude::*;


// Table schemas.

table! {
    keys {
        id -> BigInt,
        parent -> Nullable<BigInt>,
        name -> Binary,

        created -> Nullable<BigInt>,
        modified -> Nullable<BigInt>,
        accessed -> Nullable<BigInt>,

        permissions -> Nullable<BigInt>,
        user_id -> Nullable<BigInt>,
        group_id -> Nullable<BigInt>,

        hash -> Nullable<Binary>,
        persistent_ref -> Nullable<Binary>,
    }
}


// Rust models.

#[derive(Queryable)]
pub struct Key {
    pub id: i64,
    pub parent: Option<i64>,
    pub name: Vec<u8>,

    pub created: Option<i64>,
    pub modified: Option<i64>,
    pub accessed: Option<i64>,

    pub permissions: Option<i64>,
    pub user_id: Option<i64>,
    pub group_id: Option<i64>,

    pub hash: Option<Vec<u8>>,
    pub persistent_ref: Option<Vec<u8>>,
}

#[insertable_into(keys)]
pub struct NewKey<'a> {
    pub parent: Option<i64>,
    pub name: &'a [u8],

    pub created: Option<i64>,
    pub modified: Option<i64>,
    pub accessed: Option<i64>,

    pub permissions: Option<i64>,
    pub user_id: Option<i64>,
    pub group_id: Option<i64>,

    pub hash: Option<&'a [u8]>,
    pub persistent_ref: Option<&'a [u8]>,
}
