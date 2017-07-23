# Copyright 2014 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


@0x81f586f4d873f6ac;


struct Snapshot {
	id @0 :UInt64;
	hashRef @1 :HashRef;

	familyName @2: Text;
	msg @3 :Text;
	utcTimestamp @4 :Int64;
}

struct SnapshotList {
	snapshots @0 :List(Snapshot);
}

struct ChunkRef {
	blobName @0 :Data;

	offset @1: UInt64;
	length @2: UInt64;

	packing :union {
		none @3 :Void;
		gzip @4 :Void;
		snappy @5 :Void;
	}

	key :union {
		none @6 :Void;
		aeadChacha20Poly1305 @7 :Data;
	}
}

struct HashRef {
	hash @0 :Data;
	chunkRef @1 :ChunkRef;
	height @2 :UInt64;

	leafType :union {
	    chunk @3 :Void;
	    treeList @4 :Void;
	    snapshotList @5 :Void;
	}

	extra :union {
	    none @6 :Void;
	    info @7 :FileInfo;
	}
}

struct HashRefList {
	hashRefs @0 :List(HashRef);
}

struct HashIds {
	hashIds @0 :List(UInt64);
}

struct UserGroup {
	userId @0 :UInt64;
	groupId @1 :UInt64;
}

struct FileInfo {
	name @0 :Data;

	createdTimestampSecs @1 :UInt64;
	modifiedTimestampSecs @2 :UInt64;
	accessedTimestampSecs @3 :UInt64;

	byteLength @4 :UInt64;

	owner :union {
		none @5 :Void;
		userGroup @6 :UserGroup;
	}

	permissions :union {
	    none @7 :Void;
	    mode @8 :UInt32;
	}

	utcTimestamp @9 :Int64;
}

struct File {
	id @0 :UInt64;
	info @1 :FileInfo;

	content :union {
		data @2 :HashRef;
		directory @3 :HashRef;
		symbolicLink @4 :Data;
	}
}

struct FileList {
	files @0 :List(File);
}
