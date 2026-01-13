package db

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
)

// CreateWhiteout creates a whiteout entry for the given path
func (s *Store) CreateWhiteout(ctx context.Context, path string) error {
	path = normalizePath(path)
	parentPath := filepath.Dir(path)
	if parentPath == "." {
		parentPath = "/"
	}
	now := nowUnix()

	_, err := s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO fs_whiteout (path, parent_path, created_at) VALUES (?, ?, ?)`,
		path, parentPath, now)
	return err
}

// CreateWhiteoutTx creates a whiteout entry within a transaction
func (s *Store) CreateWhiteoutTx(ctx context.Context, tx *sql.Tx, path string) error {
	path = normalizePath(path)
	parentPath := filepath.Dir(path)
	if parentPath == "." {
		parentPath = "/"
	}
	now := nowUnix()

	_, err := tx.ExecContext(ctx,
		`INSERT OR REPLACE INTO fs_whiteout (path, parent_path, created_at) VALUES (?, ?, ?)`,
		path, parentPath, now)
	return err
}

// DeleteWhiteout removes a whiteout entry for the given path
func (s *Store) DeleteWhiteout(ctx context.Context, path string) error {
	path = normalizePath(path)
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM fs_whiteout WHERE path = ?`, path)
	return err
}

// DeleteWhiteoutTx removes a whiteout entry within a transaction
func (s *Store) DeleteWhiteoutTx(ctx context.Context, tx *sql.Tx, path string) error {
	path = normalizePath(path)
	_, err := tx.ExecContext(ctx,
		`DELETE FROM fs_whiteout WHERE path = ?`, path)
	return err
}

// DeleteWhiteoutsUnder removes all whiteout entries under the given path (including the path itself)
func (s *Store) DeleteWhiteoutsUnder(ctx context.Context, path string) error {
	path = normalizePath(path)
	// Delete exact match and all children
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM fs_whiteout WHERE path = ? OR path LIKE ?`,
		path, path+"/%")
	return err
}

// DeleteWhiteoutsUnderTx removes all whiteout entries under the given path within a transaction
func (s *Store) DeleteWhiteoutsUnderTx(ctx context.Context, tx *sql.Tx, path string) error {
	path = normalizePath(path)
	_, err := tx.ExecContext(ctx,
		`DELETE FROM fs_whiteout WHERE path = ? OR path LIKE ?`,
		path, path+"/%")
	return err
}

// HasWhiteout checks if a whiteout exists for the exact path
func (s *Store) HasWhiteout(ctx context.Context, path string) (bool, error) {
	path = normalizePath(path)
	var count int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM fs_whiteout WHERE path = ?`, path).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// ListWhiteouts returns all whiteout paths
func (s *Store) ListWhiteouts(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT path FROM fs_whiteout ORDER BY path`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var paths []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, err
		}
		paths = append(paths, path)
	}
	return paths, rows.Err()
}

// GetChildWhiteouts returns all direct child whiteout names under the given directory path
func (s *Store) GetChildWhiteouts(ctx context.Context, parentPath string) ([]string, error) {
	parentPath = normalizePath(parentPath)
	rows, err := s.db.QueryContext(ctx,
		`SELECT path FROM fs_whiteout WHERE parent_path = ?`, parentPath)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, err
		}
		// Extract just the name from the full path
		name := filepath.Base(path)
		names = append(names, name)
	}
	return names, rows.Err()
}

// normalizePath ensures consistent path format (leading /, no trailing /, no double slashes)
func normalizePath(path string) string {
	// Ensure leading slash
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	// Remove trailing slash (except for root)
	if len(path) > 1 && strings.HasSuffix(path, "/") {
		path = strings.TrimSuffix(path, "/")
	}
	// Clean the path (removes .., ., double slashes)
	path = filepath.Clean(path)
	return path
}
