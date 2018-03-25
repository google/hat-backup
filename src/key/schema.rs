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

use diesel::sql_types::BigInt;

// Table schemas.

table! {
    key_tree (node_id) {
        node_id -> Nullable<BigInt>,
        parent_id -> Nullable<BigInt>,
        name -> Binary,
    }
}

table! {
    key_data (node_id, committed) {
        node_id -> Nullable<BigInt>,
        committed -> Bool,
        tag -> BigInt,

        created -> Nullable<BigInt>,
        modified -> Nullable<BigInt>,
        accessed -> Nullable<BigInt>,

        permissions -> Nullable<BigInt>,
        user_id -> Nullable<BigInt>,
        group_id -> Nullable<BigInt>,

         symbolic_link_path -> Nullable<Binary>,

        hash -> Nullable<Binary>,
        hash_ref -> Nullable<Binary>,
    }
}

joinable!(key_data -> key_tree (node_id));

allow_tables_to_appear_in_same_query!(key_data, key_tree,);

// Rust models.
#[derive(Queryable, QueryableByName)]
pub struct RowId {
    #[sql_type = "BigInt"] pub row_id: i64,
}

#[derive(Queryable)]
pub struct KeyNode {
    pub node_id: Option<i64>,
    pub parent_id: Option<i64>,
    pub name: Vec<u8>,
}

#[derive(Insertable)]
#[table_name = "key_tree"]
pub struct NewKeyNode<'a> {
    pub node_id: Option<i64>,
    pub parent_id: Option<i64>,
    pub name: &'a [u8],
}

#[derive(Queryable)]
pub struct KeyData {
    pub node_id: Option<i64>,
    pub committed: bool,
    pub tag: i64,

    pub created: Option<i64>,
    pub modified: Option<i64>,
    pub accessed: Option<i64>,

    pub permissions: Option<i64>,
    pub user_id: Option<i64>,
    pub group_id: Option<i64>,

    pub symbolic_link_path: Option<Vec<u8>>,

    pub hash: Option<Vec<u8>>,
    pub hash_ref: Option<Vec<u8>>,
}

#[derive(Insertable)]
#[table_name = "key_data"]
pub struct NewKeyData<'a> {
    pub node_id: Option<i64>,
    pub committed: bool,
    pub tag: i64,

    pub created: Option<i64>,
    pub modified: Option<i64>,
    pub accessed: Option<i64>,

    pub permissions: Option<i64>,
    pub user_id: Option<i64>,
    pub group_id: Option<i64>,

    pub symbolic_link_path: Option<&'a [u8]>,

    pub hash: Option<&'a [u8]>,
    pub hash_ref: Option<&'a [u8]>,
}
