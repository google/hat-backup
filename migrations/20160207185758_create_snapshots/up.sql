CREATE TABLE IF NOT EXISTS snapshots (
	id		INTEGER PRIMARY KEY,
	tag		INTEGER,
	family_id	INTEGER,
	utc_datetime    TEXT,
	snapshot_id	INTEGER,
	msg		BLOB,
	hash		BLOB,
	hash_ref	BLOB
);
