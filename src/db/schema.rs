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

use chrono;
use diesel::prelude::*;

// Table schemas.

table! {
    hashes {
        id -> BigInt,
        hash -> Binary,
        tag -> BigInt,
        height -> BigInt,
        leaf_type -> BigInt,
        childs -> Nullable<Binary>,
        blob_id -> BigInt,
        blob_ref -> Nullable<Binary>,
        ready -> Bool,
    }
}

table! {
    gc_metadata {
        id -> BigInt,
        hash_id -> BigInt,
        family_id -> BigInt,
        gc_int -> BigInt,
        gc_vec -> Binary,
    }
}

table! {
    blobs {
        id -> BigInt,
        name -> Binary,
        tag -> Integer,
    }
}

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
        utc_datetime -> Timestamp,
        msg -> Nullable<VarChar>,
        hash -> Nullable<Binary>,
        hash_ref -> Nullable<Binary>,
    }
}

joinable!(snapshots -> family (family_id));
joinable!(hashes -> blobs (blob_id));

// Rust models.

#[derive(Queryable)]
pub struct Hash {
    pub id: i64,
    pub hash: Vec<u8>,
    pub tag: i64,
    pub height: i64,
    pub leaf_type: i64,
    pub childs: Option<Vec<u8>>,
    pub blob_id: i64,
    pub blob_ref: Option<Vec<u8>>,
    pub ready: bool,
}

#[derive(Insertable)]
#[table_name = "hashes"]
pub struct NewHash<'a> {
    pub id: i64,
    pub hash: &'a [u8],
    pub tag: i64,
    pub height: i64,
    pub leaf_type: i64,
    pub childs: Option<&'a [u8]>,
    pub blob_id: i64,
    pub blob_ref: Option<&'a [u8]>,
    pub ready: bool,
}

#[derive(Queryable)]
pub struct GcMetadata {
    pub id: i64,
    pub hash_id: i64,
    pub family_id: i64,
    pub gc_int: i64,
    pub gc_vec: Vec<u8>,
}

#[derive(Insertable)]
#[table_name = "gc_metadata"]
pub struct NewGcMetadata<'a> {
    pub hash_id: i64,
    pub family_id: i64,
    pub gc_int: i64,
    pub gc_vec: &'a [u8],
}

#[derive(Queryable)]
pub struct Blob {
    pub id: i64,
    pub name: Vec<u8>,
    pub tag: i32,
}

#[derive(Insertable)]
#[table_name = "blobs"]
pub struct NewBlob<'a> {
    pub id: i64,
    pub name: &'a [u8],
    pub tag: i32,
}

#[derive(Queryable)]
pub struct Family {
    pub id: i64,
    pub name: String,
}

#[derive(Insertable)]
#[table_name = "family"]
pub struct NewFamily<'a> {
    pub name: &'a str,
}


#[derive(Queryable)]
pub struct Snapshot {
    pub id: i64,
    pub tag: i32,
    pub family_id: i64,
    pub snapshot_id: i64,
    pub utc_datetime: chrono::NaiveDateTime,
    pub msg: Option<String>,
    pub hash: Option<Vec<u8>>,
    pub hash_ref: Option<Vec<u8>>,
}

#[derive(Insertable)]
#[table_name = "snapshots"]
pub struct NewSnapshot<'a> {
    pub tag: i32,
    pub family_id: i64,
    pub snapshot_id: i64,
    pub utc_datetime: chrono::NaiveDateTime,
    pub msg: Option<&'a str>,
    pub hash: Option<&'a [u8]>,
    pub hash_ref: Option<&'a [u8]>,
}
