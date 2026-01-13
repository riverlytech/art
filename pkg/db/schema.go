package db

import (
	"fmt"
)

const schema = `
-- Filesystem configuration
CREATE TABLE IF NOT EXISTS fs_config (
	key TEXT PRIMARY KEY,
	value TEXT NOT NULL
);

-- File/directory metadata (inodes)
CREATE TABLE IF NOT EXISTS fs_inode (
	ino INTEGER PRIMARY KEY AUTOINCREMENT,
	mode INTEGER NOT NULL,
	nlink INTEGER NOT NULL DEFAULT 1,
	uid INTEGER NOT NULL DEFAULT 0,
	gid INTEGER NOT NULL DEFAULT 0,
	size INTEGER NOT NULL DEFAULT 0,
	atime INTEGER NOT NULL,
	mtime INTEGER NOT NULL,
	ctime INTEGER NOT NULL
);

-- Directory entries (maps names to inodes)
CREATE TABLE IF NOT EXISTS fs_dentry (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	name TEXT NOT NULL,
	parent_ino INTEGER NOT NULL,
	ino INTEGER NOT NULL,
	UNIQUE(parent_ino, name),
	FOREIGN KEY (parent_ino) REFERENCES fs_inode(ino) ON DELETE CASCADE,
	FOREIGN KEY (ino) REFERENCES fs_inode(ino) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_fs_dentry_parent ON fs_dentry(parent_ino, name);
CREATE INDEX IF NOT EXISTS idx_fs_dentry_ino ON fs_dentry(ino);

-- File content in chunks
CREATE TABLE IF NOT EXISTS fs_data (
	ino INTEGER NOT NULL,
	chunk_index INTEGER NOT NULL,
	data BLOB NOT NULL,
	PRIMARY KEY (ino, chunk_index),
	FOREIGN KEY (ino) REFERENCES fs_inode(ino) ON DELETE CASCADE
);

-- Symbolic link targets
CREATE TABLE IF NOT EXISTS fs_symlink (
	ino INTEGER PRIMARY KEY,
	target TEXT NOT NULL,
	FOREIGN KEY (ino) REFERENCES fs_inode(ino) ON DELETE CASCADE
);

-- Whiteouts for overlay filesystem (tracks deleted files from base layer)
CREATE TABLE IF NOT EXISTS fs_whiteout (
	path TEXT PRIMARY KEY,
	parent_path TEXT NOT NULL,
	created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fs_whiteout_parent ON fs_whiteout(parent_path);

-- Origin mapping for copy-on-write (maps delta inode to original base inode)
CREATE TABLE IF NOT EXISTS fs_origin (
	delta_ino INTEGER PRIMARY KEY,
	base_ino INTEGER NOT NULL,
	FOREIGN KEY (delta_ino) REFERENCES fs_inode(ino) ON DELETE CASCADE
);
`

// initSchema initializes the database schema
func (s *Store) initSchema() error {
	// Create tables
	if _, err := s.db.Exec(schema); err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	// Initialize config if not exists
	_, err := s.db.Exec(`INSERT OR IGNORE INTO fs_config (key, value) VALUES ('chunk_size', ?)`,
		fmt.Sprintf("%d", s.chunkSize))
	if err != nil {
		return fmt.Errorf("failed to initialize config: %w", err)
	}

	// Create root inode if not exists (ino=1, mode=S_IFDIR|0755 = 16877)
	// S_IFDIR = 0o040000 = 16384, 0755 = 493, total = 16877
	now := nowUnix()
	_, err = s.db.Exec(`
		INSERT OR IGNORE INTO fs_inode (ino, mode, nlink, uid, gid, size, atime, mtime, ctime)
		VALUES (1, 16877, 2, 0, 0, 0, ?, ?, ?)
	`, now, now, now)
	if err != nil {
		return fmt.Errorf("failed to create root inode: %w", err)
	}

	return nil
}
