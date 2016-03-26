CREATE TABLE IF NOT EXISTS blobs (
	id	INTEGER PRIMARY KEY,
	name	BLOB,
        tag	INT
);

CREATE UNIQUE INDEX IF NOT EXISTS Blobs_UniqueName ON blobs(name);
