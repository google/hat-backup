use diesel::prelude::*;


// Table schemas.

table! {
    family {
        id -> BigInt,
        name -> VarChar,
    }
}

table! {
    snapshots {
        id -> BigInt,
        tag -> Integer,
        family_id -> BigInt,
        snapshot_id -> BigInt,
        msg -> Nullable<VarChar>,
        hash -> Nullable<Binary>,
        tree_ref -> Nullable<Binary>,
    }
}

joinable!(snapshots -> family (family_id));
select_column_workaround!(snapshots -> family (id, tag, family_id, snapshot_id, msg,
                                               hash, tree_ref));
select_column_workaround!(family -> snapshots (id, name));


// Rust models.

#[derive(Queryable)]
pub struct Family {
    pub id: i64,
    pub name: String,
}

#[insertable_into(family)]
pub struct NewFamily<'a> {
    pub name: &'a str,
}


#[derive(Queryable)]
pub struct Snapshot {
    pub id: i64,
    pub tag: i32,
    pub family_id: i64,
    pub snapshot_id: i64,
    pub msg: Option<String>,
    pub hash: Option<Vec<u8>>,
    pub tree_ref: Option<Vec<u8>>,
}

#[insertable_into(snapshots)]
pub struct NewSnapshot<'a> {
    pub tag: i32,
    pub family_id: i64,
    pub snapshot_id: i64,
    pub msg: Option<&'a str>,
    pub hash: Option<&'a [u8]>,
    pub tree_ref: Option<&'a [u8]>,
}
