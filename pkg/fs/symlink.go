package fs

import (
	"context"
	"database/sql"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"art/pkg/db"
)

// Symlink creates a symbolic link
func (n *Node) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	var newIno uint64
	err := n.store.WithTx(ctx, func(tx *sql.Tx) error {
		// Create inode with S_IFLNK flag
		// Symlink permissions are always 0777
		ino, err := n.store.CreateInodeTx(ctx, tx, db.S_IFLNK|0777, 0, 0)
		if err != nil {
			return err
		}
		newIno = ino

		// Store symlink target
		if err := n.store.CreateSymlinkTx(ctx, tx, ino, target); err != nil {
			return err
		}

		// Update size to target length
		if err := n.store.UpdateSizeTx(ctx, tx, ino, uint64(len(target))); err != nil {
			return err
		}

		// Create dentry
		return n.store.CreateDentryTx(ctx, tx, n.ino, name, ino)
	})
	if err != nil {
		return nil, toErrno(err)
	}

	inode, err := n.store.GetInode(ctx, newIno)
	if err != nil {
		return nil, toErrno(err)
	}

	child := n.NewInode(ctx, &Node{
		ino:   newIno,
		store: n.store,
	}, fs.StableAttr{
		Mode: inode.Mode,
		Ino:  newIno,
	})

	fillAttr(inode, &out.Attr)
	return child, 0
}

// Readlink reads the target of a symbolic link
func (n *Node) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	target, err := n.store.ReadSymlink(ctx, n.ino)
	if err != nil {
		return nil, toErrno(err)
	}
	return []byte(target), 0
}
