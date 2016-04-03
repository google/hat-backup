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


#[derive(Clone, Debug, PartialEq)]
pub enum Tag {
    Done = 0,

    Reserved = 1,
    InProgress = 2,
    Complete = 3,

    WillDelete = 4,
    ReadyDelete = 5,
    DeleteComplete = 6,

    RecoverInProgress = 7,
}

pub fn tag_from_num(n: i64) -> Option<Tag> {
    match n {
        0 => Some(Tag::Done),
        1 => Some(Tag::Reserved),
        2 => Some(Tag::InProgress),
        3 => Some(Tag::Complete),

        4 => Some(Tag::WillDelete),
        5 => Some(Tag::ReadyDelete),
        6 => Some(Tag::DeleteComplete),

        7 => Some(Tag::RecoverInProgress),

        _ => None,
    }
}
