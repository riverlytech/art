package db

import (
	"context"
	"database/sql"
	"strings"
)

// Dentry represents a directory entry
type Dentry struct {
	ParentIno uint64
	Name      string
	Ino       uint64
}

// isUniqueConstraintError checks if an error is a unique constraint violation
func isUniqueConstraintError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "UNIQUE constraint failed")
}

// Lookup finds a child inode by name in a directory
func (s *Store) Lookup(ctx context.Context, parentIno uint64, name string) (uint64, error) {
	var ino uint64
	err := s.db.QueryRowContext(ctx,
		`SELECT ino FROM fs_dentry WHERE parent_ino = ? AND name = ?`,
		parentIno, name).Scan(&ino)
	if err == sql.ErrNoRows {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	return ino, nil
}

// ListDir lists all entries in a directory
func (s *Store) ListDir(ctx context.Context, parentIno uint64) ([]Dentry, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT name, ino FROM fs_dentry WHERE parent_ino = ? ORDER BY name`,
		parentIno)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []Dentry
	for rows.Next() {
		var d Dentry
		d.ParentIno = parentIno
		if err := rows.Scan(&d.Name, &d.Ino); err != nil {
			return nil, err
		}
		entries = append(entries, d)
	}
	return entries, rows.Err()
}

// CreateDentry creates a new directory entry
func (s *Store) CreateDentry(ctx context.Context, parentIno uint64, name string, ino uint64) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO fs_dentry (parent_ino, name, ino) VALUES (?, ?, ?)`,
		parentIno, name, ino)
	if err != nil {
		if isUniqueConstraintError(err) {
			return ErrExists
		}
		return err
	}
	return nil
}

// CreateDentryTx creates a directory entry within a transaction
func (s *Store) CreateDentryTx(ctx context.Context, tx *sql.Tx, parentIno uint64, name string, ino uint64) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO fs_dentry (parent_ino, name, ino) VALUES (?, ?, ?)`,
		parentIno, name, ino)
	if err != nil {
		if isUniqueConstraintError(err) {
			return ErrExists
		}
		return err
	}
	return nil
}

// DeleteDentry removes a directory entry
func (s *Store) DeleteDentry(ctx context.Context, parentIno uint64, name string) error {
	result, err := s.db.ExecContext(ctx,
		`DELETE FROM fs_dentry WHERE parent_ino = ? AND name = ?`,
		parentIno, name)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// DeleteDentryTx removes a directory entry within a transaction
func (s *Store) DeleteDentryTx(ctx context.Context, tx *sql.Tx, parentIno uint64, name string) error {
	result, err := tx.ExecContext(ctx,
		`DELETE FROM fs_dentry WHERE parent_ino = ? AND name = ?`,
		parentIno, name)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// Rename moves/renames a directory entry
func (s *Store) Rename(ctx context.Context, oldParentIno, newParentIno uint64, oldName, newName string) error {
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		// Get the inode being moved
		var ino uint64
		err := tx.QueryRowContext(ctx,
			`SELECT ino FROM fs_dentry WHERE parent_ino = ? AND name = ?`,
			oldParentIno, oldName).Scan(&ino)
		if err == sql.ErrNoRows {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		// Check if target exists and delete if so
		var targetIno uint64
		err = tx.QueryRowContext(ctx,
			`SELECT ino FROM fs_dentry WHERE parent_ino = ? AND name = ?`,
			newParentIno, newName).Scan(&targetIno)
		if err == nil {
			// Target exists, check if it's a directory
			var targetMode uint32
			tx.QueryRowContext(ctx, `SELECT mode FROM fs_inode WHERE ino = ?`, targetIno).Scan(&targetMode)

			if targetMode&S_IFMT == S_IFDIR {
				// Check if directory is empty
				var count int
				tx.QueryRowContext(ctx,
					`SELECT COUNT(*) FROM fs_dentry WHERE parent_ino = ?`,
					targetIno).Scan(&count)
				if count > 0 {
					return ErrNotEmpty
				}
			}

			// Delete target dentry
			tx.ExecContext(ctx,
				`DELETE FROM fs_dentry WHERE parent_ino = ? AND name = ?`,
				newParentIno, newName)

			// Decrement target's link count
			tx.ExecContext(ctx,
				`UPDATE fs_inode SET nlink = nlink - 1 WHERE ino = ?`, targetIno)

			// Delete target inode if nlink = 0
			tx.ExecContext(ctx,
				`DELETE FROM fs_inode WHERE ino = ? AND nlink = 0`, targetIno)
		} else if err != sql.ErrNoRows {
			return err
		}

		// Delete old entry
		_, err = tx.ExecContext(ctx,
			`DELETE FROM fs_dentry WHERE parent_ino = ? AND name = ?`,
			oldParentIno, oldName)
		if err != nil {
			return err
		}

		// Create new entry
		_, err = tx.ExecContext(ctx,
			`INSERT INTO fs_dentry (parent_ino, name, ino) VALUES (?, ?, ?)`,
			newParentIno, newName, ino)
		return err
	})
}

// HasChildren returns true if the directory has any entries
func (s *Store) HasChildren(ctx context.Context, parentIno uint64) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM fs_dentry WHERE parent_ino = ?`,
		parentIno).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}
