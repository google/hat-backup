CREATE TABLE keys (
	id             INTEGER PRIMARY KEY,
        parent         INTEGER,
        name           BLOB,

	created        Integer,
        modified       Integer,
        accessed       Integer,

	permissions    Integer,
	user_id        Integer,
	group_id       Integer,

        hash           BLOB,
        persistent_ref BLOB
);

CREATE UNIQUE INDEX Keys_UniqueParentName ON keys(parent, name);
