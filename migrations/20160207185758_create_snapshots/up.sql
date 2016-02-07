CREATE TABLE IF NOT EXISTS snapshots (
	id		INTEGER PRIMARY KEY,
	tag		INTEGER,
	family_id	INTEGER,
        snapshot_id	INTEGER,
        msg		BLOB,
	hash		BLOB,
	tree_ref	BLOB
);
