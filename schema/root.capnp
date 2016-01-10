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
