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

#[derive(Serialize, Deserialize)]
pub struct Snapshot {
    pub id: u64,

    #[serde(rename = "r")]
    pub hash_ref: HashRef,
    #[serde(rename = "f")]
    pub family_name: String,
    #[serde(rename = "m")]
    pub msg: String,
    #[serde(rename = "c")]
    pub snapshot_ts_utc: i64,
}

#[derive(Serialize, Deserialize)]
pub struct Snapshots {
    #[serde(rename = "s")]
    pub snapshots: Vec<Snapshot>,
}

#[derive(Serialize, Deserialize)]
pub enum Packing {
    #[serde(rename = "r")]
    Raw,
    #[serde(rename = "g")]
    GZip,
    #[serde(rename = "s")]
    Snappy,
}

#[derive(Serialize, Deserialize)]
pub enum Key {
    #[serde(rename = "n")]
    None,
    #[serde(rename = "c")]
    AeadChacha20Poly1305(Vec<u8>),
}

#[derive(Serialize, Deserialize)]
pub struct ChunkRef {
    #[serde(rename = "b")]
    pub blob_name: Vec<u8>,
    #[serde(rename = "o")]
    pub offset: u64,
    #[serde(rename = "l")]
    pub length: u64,
    #[serde(rename = "p")]
    pub packing: Packing,
    #[serde(rename = "k")]
    pub key: Key,
}

#[derive(Clone, Eq, PartialEq, Copy, Debug, Serialize, Deserialize)]
pub enum LeafType {
    #[serde(rename = "f")]
    FileChunk,
    #[serde(rename = "t")]
    TreeList,
    #[serde(rename = "s")]
    SnapshotList,
}

#[derive(Serialize, Deserialize)]
pub enum ExtraInfo {
    #[serde(rename = "n")]
    None,
    #[serde(rename = "f")]
    FileInfo(FileInfo),
}

#[derive(Serialize, Deserialize)]
pub struct HashRef {
    #[serde(rename = "ha")]
    pub hash: Vec<u8>,
    #[serde(rename = "r")]
    pub chunk_ref: ChunkRef,
    #[serde(rename = "h")]
    pub height: u64,
    #[serde(rename = "l")]
    pub leaf_type: LeafType,
    #[serde(rename = "e")]
    pub extra: ExtraInfo,
}

#[derive(Serialize, Deserialize)]
pub struct HashRefs {
    #[serde(rename = "r")]
    pub refs: Vec<HashRef>,
}

#[derive(Serialize, Deserialize)]
pub struct HashIds {
    #[serde(rename = "i")]
    pub ids: Vec<u64>,
}

#[derive(Serialize, Deserialize)]
pub struct UserGroup {
    #[serde(rename = "u")]
    pub user_id: i64,
    #[serde(rename = "g")]
    pub group_id: i64,
}

#[derive(Serialize, Deserialize)]
pub enum Owner {
    #[serde(rename = "n")]
    None,
    #[serde(rename = "u")]
    UserGroup(UserGroup),
}

#[derive(Serialize, Deserialize)]
pub enum Permissions {
    #[serde(rename = "n")]
    None,
    #[serde(rename = "m")]
    Mode(u32),
}

#[derive(Serialize, Deserialize)]
pub struct FileInfo {
    #[serde(rename = "n")]
    pub name: Vec<u8>,
    #[serde(rename = "c")]
    pub created_ts: i64,
    #[serde(rename = "m")]
    pub modified_ts: i64,
    #[serde(rename = "a")]
    pub accessed_ts: i64,
    #[serde(rename = "l")]
    pub byte_length: i64,
    #[serde(rename = "o")]
    pub owner: Owner,
    #[serde(rename = "p")]
    pub permissions: Permissions,
    #[serde(rename = "s")]
    pub snapshot_ts_utc: i64,
}

#[derive(Serialize, Deserialize)]
pub enum Content {
    #[serde(rename = "f")]
    Data(HashRef),
    #[serde(rename = "d")]
    Directory(HashRef),
    #[serde(rename = "l")]
    SymbolicLink(Vec<u8>),
}

#[derive(Serialize, Deserialize)]
pub struct File {
    pub id: u64,
    #[serde(rename = "i")]
    pub info: FileInfo,
    #[serde(rename = "c")]
    pub content: Content,
}

#[derive(Serialize, Deserialize)]
pub struct Files {
    #[serde(rename = "f")]
    pub files: Vec<File>,
}
