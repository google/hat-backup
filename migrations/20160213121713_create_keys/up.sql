CREATE TABLE key_tree (
	node_id        INTEGER PRIMARY KEY,
	parent_id      INTEGER,
	name           BLOB,

	FOREIGN KEY(parent_id) REFERENCES key_tree(node_id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX key_tree_unique_parent_id_name ON key_tree(parent_id, name);


CREATE TABLE key_data (
	node_id        INTEGER,
	committed      BOOLEAN,
	tag            INTEGER,

	created        Integer,
	modified       Integer,
	accessed       Integer,

	permissions    Integer,
	user_id        Integer,
	group_id       Integer,

	hash           BLOB,
	hash_ref       BLOB,

	PRIMARY KEY (node_id, committed) ON CONFLICT REPLACE,
	FOREIGN KEY(node_id) REFERENCES key_tree(node_id) ON DELETE CASCADE
);
