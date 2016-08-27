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
	id @0 :Int64;

	familyName @1: Text;
	msg @2 :Text;

	hash @3 :Data;
	treeReference @4 :Data;
}

struct SnapshotList {
	snapshots @0 :List(Snapshot);
}

struct ChunkRef {
	blobId @0 :Data;

	offset @1: Int64;
	length @2: Int64;

	kind :union {
		treeBranch @3 :Void;
		treeLeaf @4 :Void;
	}
}

struct HashRef {
	hash @0 :Data;
	chunkRef @1 :ChunkRef;
}

struct HashRefList {
	hashRefs @0 :List(HashRef);
}

struct HashIds {
	hashIds @0 :List(UInt64);
}

struct File {
	id @0 :UInt64;
	name @1 :Data;

	created :union {
		unknown @2 :Void;
		timestamp @3 :Int64;
	}
	modified :union {
		unknown @4 :Void;
		timestamp @5 :Int64;
	}
	accessed :union {
		unknown @6 :Void;
		timestamp @7 :Int64;
	}

	content :union {
		data @8 :HashRef;
		directory @9 :HashRef;
	}
}

struct FileList {
	files @0 :List(File);
}

struct MetaFooterEntry {
	length @0 :Int64;
}
