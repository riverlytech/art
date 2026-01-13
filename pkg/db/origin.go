package db

import (
	"context"
	"database/sql"
)

// AddOrigin records a mapping from a delta inode to its original base inode.
// This is used for copy-on-write to maintain inode consistency after a file
// is copied from the base layer to the delta layer.
func (s *Store) AddOrigin(ctx context.Context, deltaIno, baseIno uint64) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO fs_origin (delta_ino, base_ino) VALUES (?, ?)`,
		deltaIno, baseIno)
	return err
}

// AddOriginTx records an origin mapping within a transaction
func (s *Store) AddOriginTx(ctx context.Context, tx *sql.Tx, deltaIno, baseIno uint64) error {
	_, err := tx.ExecContext(ctx,
		`INSERT OR REPLACE INTO fs_origin (delta_ino, base_ino) VALUES (?, ?)`,
		deltaIno, baseIno)
	return err
}

// GetOrigin retrieves the original base inode for a delta inode.
// Returns 0 if no origin mapping exists (file was created in delta, not copied).
func (s *Store) GetOrigin(ctx context.Context, deltaIno uint64) (uint64, error) {
	var baseIno uint64
	err := s.db.QueryRowContext(ctx,
		`SELECT base_ino FROM fs_origin WHERE delta_ino = ?`, deltaIno).Scan(&baseIno)
	if err == sql.ErrNoRows {
		return 0, nil // No origin mapping, return 0
	}
	if err != nil {
		return 0, err
	}
	return baseIno, nil
}

// DeleteOrigin removes the origin mapping for a delta inode
func (s *Store) DeleteOrigin(ctx context.Context, deltaIno uint64) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM fs_origin WHERE delta_ino = ?`, deltaIno)
	return err
}

// DeleteOriginTx removes the origin mapping within a transaction
func (s *Store) DeleteOriginTx(ctx context.Context, tx *sql.Tx, deltaIno uint64) error {
	_, err := tx.ExecContext(ctx,
		`DELETE FROM fs_origin WHERE delta_ino = ?`, deltaIno)
	return err
}

// HasOrigin checks if a delta inode has an origin mapping
func (s *Store) HasOrigin(ctx context.Context, deltaIno uint64) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM fs_origin WHERE delta_ino = ?`, deltaIno).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// ListOrigins returns all origin mappings (for debugging/inspection)
func (s *Store) ListOrigins(ctx context.Context) (map[uint64]uint64, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT delta_ino, base_ino FROM fs_origin`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	origins := make(map[uint64]uint64)
	for rows.Next() {
		var deltaIno, baseIno uint64
		if err := rows.Scan(&deltaIno, &baseIno); err != nil {
			return nil, err
		}
		origins[deltaIno] = baseIno
	}
	return origins, rows.Err()
}
