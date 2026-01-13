package db

import (
	"context"
	"database/sql"
)

// CreateSymlink stores a symlink target
func (s *Store) CreateSymlink(ctx context.Context, ino uint64, target string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO fs_symlink (ino, target) VALUES (?, ?)`,
		ino, target)
	return err
}

// CreateSymlinkTx stores a symlink target within a transaction
func (s *Store) CreateSymlinkTx(ctx context.Context, tx *sql.Tx, ino uint64, target string) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO fs_symlink (ino, target) VALUES (?, ?)`,
		ino, target)
	return err
}

// ReadSymlink retrieves a symlink target
func (s *Store) ReadSymlink(ctx context.Context, ino uint64) (string, error) {
	var target string
	err := s.db.QueryRowContext(ctx,
		`SELECT target FROM fs_symlink WHERE ino = ?`, ino).Scan(&target)
	if err == sql.ErrNoRows {
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}
	return target, nil
}

// DeleteSymlink removes a symlink target
func (s *Store) DeleteSymlink(ctx context.Context, ino uint64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM fs_symlink WHERE ino = ?`, ino)
	return err
}

// DeleteSymlinkTx removes a symlink target within a transaction
func (s *Store) DeleteSymlinkTx(ctx context.Context, tx *sql.Tx, ino uint64) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM fs_symlink WHERE ino = ?`, ino)
	return err
}
