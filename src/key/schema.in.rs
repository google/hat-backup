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
        hash_ref -> Nullable<Binary>,
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
    pub hash_ref: Option<Vec<u8>>,
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
    pub hash_ref: Option<&'a [u8]>,
}
