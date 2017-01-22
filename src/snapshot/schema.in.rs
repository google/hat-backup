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
        hash_ref -> Nullable<Binary>,
    }
}

joinable!(snapshots -> family (family_id));
select_column_workaround!(snapshots -> family (id, tag, family_id, snapshot_id, msg,
                                               hash, hash_ref));
select_column_workaround!(family -> snapshots (id, name));


// Rust models.

#[derive(Queryable)]
pub struct Family {
    pub id: i64,
    pub name: String,
}

#[derive(Insertable)]
#[table_name="family"]
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
    pub hash_ref: Option<Vec<u8>>,
}

#[derive(Insertable)]
#[table_name="snapshots"]
pub struct NewSnapshot<'a> {
    pub tag: i32,
    pub family_id: i64,
    pub snapshot_id: i64,
    pub msg: Option<&'a str>,
    pub hash: Option<&'a [u8]>,
    pub hash_ref: Option<&'a [u8]>,
}
