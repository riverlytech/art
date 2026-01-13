package db

import (
	"context"
	"database/sql"
	"time"
)

// File type constants (matching syscall constants)
const (
	S_IFMT   = 0o170000 // File type mask
	S_IFDIR  = 0o040000 // Directory
	S_IFREG  = 0o100000 // Regular file
	S_IFLNK  = 0o120000 // Symbolic link
)

// Inode represents file/directory metadata
type Inode struct {
	Ino   uint64
	Mode  uint32
	Nlink uint32
	UID   uint32
	GID   uint32
	Size  uint64
	Atime int64 // Unix timestamp (seconds)
	Mtime int64
	Ctime int64
}

// IsDir returns true if the inode is a directory
func (i *Inode) IsDir() bool {
	return i.Mode&S_IFMT == S_IFDIR
}

// IsRegular returns true if the inode is a regular file
func (i *Inode) IsRegular() bool {
	return i.Mode&S_IFMT == S_IFREG
}

// IsSymlink returns true if the inode is a symbolic link
func (i *Inode) IsSymlink() bool {
	return i.Mode&S_IFMT == S_IFLNK
}

// nowUnix returns the current time as Unix timestamp
func nowUnix() int64 {
	return time.Now().Unix()
}

// CreateInode creates a new inode and returns its number
func (s *Store) CreateInode(ctx context.Context, mode, uid, gid uint32) (uint64, error) {
	now := nowUnix()
	result, err := s.db.ExecContext(ctx,
		`INSERT INTO fs_inode (mode, nlink, uid, gid, size, atime, mtime, ctime)
		 VALUES (?, 1, ?, ?, 0, ?, ?, ?)`,
		mode, uid, gid, now, now, now)
	if err != nil {
		return 0, err
	}

	ino, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}
	return uint64(ino), nil
}

// CreateInodeTx creates a new inode within a transaction
func (s *Store) CreateInodeTx(ctx context.Context, tx *sql.Tx, mode, uid, gid uint32) (uint64, error) {
	now := nowUnix()
	result, err := tx.ExecContext(ctx,
		`INSERT INTO fs_inode (mode, nlink, uid, gid, size, atime, mtime, ctime)
		 VALUES (?, 1, ?, ?, 0, ?, ?, ?)`,
		mode, uid, gid, now, now, now)
	if err != nil {
		return 0, err
	}

	ino, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}
	return uint64(ino), nil
}

// GetInode retrieves an inode by number
func (s *Store) GetInode(ctx context.Context, ino uint64) (*Inode, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT ino, mode, nlink, uid, gid, size, atime, mtime, ctime
		 FROM fs_inode WHERE ino = ?`, ino)

	inode := &Inode{}
	err := row.Scan(&inode.Ino, &inode.Mode, &inode.Nlink, &inode.UID, &inode.GID,
		&inode.Size, &inode.Atime, &inode.Mtime, &inode.Ctime)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return inode, nil
}

// UpdateInode updates inode metadata
func (s *Store) UpdateInode(ctx context.Context, inode *Inode) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE fs_inode SET mode=?, nlink=?, uid=?, gid=?, size=?, atime=?, mtime=?, ctime=?
		 WHERE ino=?`,
		inode.Mode, inode.Nlink, inode.UID, inode.GID, inode.Size,
		inode.Atime, inode.Mtime, inode.Ctime, inode.Ino)
	return err
}

// UpdateInodeTx updates inode metadata within a transaction
func (s *Store) UpdateInodeTx(ctx context.Context, tx *sql.Tx, inode *Inode) error {
	_, err := tx.ExecContext(ctx,
		`UPDATE fs_inode SET mode=?, nlink=?, uid=?, gid=?, size=?, atime=?, mtime=?, ctime=?
		 WHERE ino=?`,
		inode.Mode, inode.Nlink, inode.UID, inode.GID, inode.Size,
		inode.Atime, inode.Mtime, inode.Ctime, inode.Ino)
	return err
}

// UpdateSize updates the size of a file
func (s *Store) UpdateSize(ctx context.Context, ino uint64, size uint64) error {
	now := nowUnix()
	_, err := s.db.ExecContext(ctx,
		`UPDATE fs_inode SET size=?, mtime=?, ctime=? WHERE ino=?`,
		size, now, now, ino)
	return err
}

// UpdateSizeTx updates the size within a transaction
func (s *Store) UpdateSizeTx(ctx context.Context, tx *sql.Tx, ino uint64, size uint64) error {
	now := nowUnix()
	_, err := tx.ExecContext(ctx,
		`UPDATE fs_inode SET size=?, mtime=?, ctime=? WHERE ino=?`,
		size, now, now, ino)
	return err
}

// UpdateTimes updates access and modification times
func (s *Store) UpdateTimes(ctx context.Context, ino uint64, atime, mtime *int64) error {
	now := nowUnix()
	if atime == nil {
		atime = &now
	}
	if mtime == nil {
		mtime = &now
	}
	_, err := s.db.ExecContext(ctx,
		`UPDATE fs_inode SET atime=?, mtime=?, ctime=? WHERE ino=?`,
		*atime, *mtime, now, ino)
	return err
}

// DeleteInode deletes an inode (should only be called when nlink=0)
func (s *Store) DeleteInode(ctx context.Context, ino uint64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM fs_inode WHERE ino=?`, ino)
	return err
}

// DeleteInodeTx deletes an inode within a transaction
func (s *Store) DeleteInodeTx(ctx context.Context, tx *sql.Tx, ino uint64) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM fs_inode WHERE ino=?`, ino)
	return err
}

// IncrNlink increments the link count
func (s *Store) IncrNlink(ctx context.Context, ino uint64) error {
	now := nowUnix()
	_, err := s.db.ExecContext(ctx,
		`UPDATE fs_inode SET nlink = nlink + 1, ctime = ? WHERE ino = ?`,
		now, ino)
	return err
}

// IncrNlinkTx increments link count within a transaction
func (s *Store) IncrNlinkTx(ctx context.Context, tx *sql.Tx, ino uint64) error {
	now := nowUnix()
	_, err := tx.ExecContext(ctx,
		`UPDATE fs_inode SET nlink = nlink + 1, ctime = ? WHERE ino = ?`,
		now, ino)
	return err
}

// DecrNlink decrements the link count and returns the new count
func (s *Store) DecrNlink(ctx context.Context, ino uint64) (uint32, error) {
	now := nowUnix()
	_, err := s.db.ExecContext(ctx,
		`UPDATE fs_inode SET nlink = nlink - 1, ctime = ? WHERE ino = ?`,
		now, ino)
	if err != nil {
		return 0, err
	}

	var nlink uint32
	err = s.db.QueryRowContext(ctx, `SELECT nlink FROM fs_inode WHERE ino = ?`, ino).Scan(&nlink)
	if err != nil {
		return 0, err
	}
	return nlink, nil
}

// DecrNlinkTx decrements link count within a transaction
func (s *Store) DecrNlinkTx(ctx context.Context, tx *sql.Tx, ino uint64) (uint32, error) {
	now := nowUnix()
	_, err := tx.ExecContext(ctx,
		`UPDATE fs_inode SET nlink = nlink - 1, ctime = ? WHERE ino = ?`,
		now, ino)
	if err != nil {
		return 0, err
	}

	var nlink uint32
	err = tx.QueryRowContext(ctx, `SELECT nlink FROM fs_inode WHERE ino = ?`, ino).Scan(&nlink)
	if err != nil {
		return 0, err
	}
	return nlink, nil
}

// SetAttr sets inode attributes (for chmod, chown, utimens)
func (s *Store) SetAttr(ctx context.Context, ino uint64, mode *uint32, uid, gid *uint32, size *uint64, atime, mtime *int64) error {
	inode, err := s.GetInode(ctx, ino)
	if err != nil {
		return err
	}

	now := nowUnix()
	inode.Ctime = now

	if mode != nil {
		// Preserve file type, update permissions
		inode.Mode = (inode.Mode & S_IFMT) | (*mode & ^uint32(S_IFMT))
	}
	if uid != nil {
		inode.UID = *uid
	}
	if gid != nil {
		inode.GID = *gid
	}
	if size != nil {
		inode.Size = *size
	}
	if atime != nil {
		inode.Atime = *atime
	}
	if mtime != nil {
		inode.Mtime = *mtime
	}

	return s.UpdateInode(ctx, inode)
}

