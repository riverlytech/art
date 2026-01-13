package fs

import (
	"context"
	"database/sql"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"art/pkg/db"
)

// Readdir returns directory entries
func (n *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := n.store.ListDir(ctx, n.ino)
	if err != nil {
		return nil, toErrno(err)
	}

	result := make([]fuse.DirEntry, 0, len(entries))
	for _, entry := range entries {
		inode, err := n.store.GetInode(ctx, entry.Ino)
		if err != nil {
			continue
		}
		result = append(result, fuse.DirEntry{
			Name: entry.Name,
			Mode: inode.Mode,
			Ino:  entry.Ino,
		})
	}

	return fs.NewListDirStream(result), 0
}

// Mkdir creates a new directory
func (n *Node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Create the directory inode and dentry in a transaction
	var newIno uint64
	err := n.store.WithTx(ctx, func(tx *sql.Tx) error {
		// Create inode with S_IFDIR flag
		ino, err := n.store.CreateInodeTx(ctx, tx, db.S_IFDIR|mode, 0, 0)
		if err != nil {
			return err
		}
		newIno = ino

		// Create dentry
		if err := n.store.CreateDentryTx(ctx, tx, n.ino, name, ino); err != nil {
			return err
		}

		// Increment parent's link count (for ..)
		return n.store.IncrNlinkTx(ctx, tx, n.ino)
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

// Rmdir removes an empty directory
func (n *Node) Rmdir(ctx context.Context, name string) syscall.Errno {
	// Look up the directory
	childIno, err := n.store.Lookup(ctx, n.ino, name)
	if err != nil {
		return toErrno(err)
	}

	// Check if it's a directory
	inode, err := n.store.GetInode(ctx, childIno)
	if err != nil {
		return toErrno(err)
	}
	if !inode.IsDir() {
		return syscall.ENOTDIR
	}

	// Check if empty
	hasChildren, err := n.store.HasChildren(ctx, childIno)
	if err != nil {
		return toErrno(err)
	}
	if hasChildren {
		return syscall.ENOTEMPTY
	}

	// Remove in transaction
	err = n.store.WithTx(ctx, func(tx *sql.Tx) error {
		// Delete dentry
		if err := n.store.DeleteDentryTx(ctx, tx, n.ino, name); err != nil {
			return err
		}

		// Decrement link count
		nlink, err := n.store.DecrNlinkTx(ctx, tx, childIno)
		if err != nil {
			return err
		}

		// Delete inode if no more links
		if nlink == 0 {
			if err := n.store.DeleteInodeTx(ctx, tx, childIno); err != nil {
				return err
			}
		}

		// Decrement parent's link count
		_, err = n.store.DecrNlinkTx(ctx, tx, n.ino)
		return err
	})

	return toErrno(err)
}

// Create creates a new file
func (n *Node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	var newIno uint64
	err := n.store.WithTx(ctx, func(tx *sql.Tx) error {
		// Create inode with S_IFREG flag
		ino, err := n.store.CreateInodeTx(ctx, tx, db.S_IFREG|mode, 0, 0)
		if err != nil {
			return err
		}
		newIno = ino

		// Create dentry
		return n.store.CreateDentryTx(ctx, tx, n.ino, name, ino)
	})
	if err != nil {
		return nil, nil, 0, toErrno(err)
	}

	dbInode, err := n.store.GetInode(ctx, newIno)
	if err != nil {
		return nil, nil, 0, toErrno(err)
	}

	child := n.NewInode(ctx, &Node{
		ino:   newIno,
		store: n.store,
	}, fs.StableAttr{
		Mode: dbInode.Mode,
		Ino:  newIno,
	})

	fillAttr(dbInode, &out.Attr)

	handle := &FileHandle{
		ino:   newIno,
		store: n.store,
		flags: flags,
	}

	return child, handle, 0, 0
}

// Unlink removes a file
func (n *Node) Unlink(ctx context.Context, name string) syscall.Errno {
	// Look up the file
	childIno, err := n.store.Lookup(ctx, n.ino, name)
	if err != nil {
		return toErrno(err)
	}

	// Check if it's a directory (use rmdir for directories)
	inode, err := n.store.GetInode(ctx, childIno)
	if err != nil {
		return toErrno(err)
	}
	if inode.IsDir() {
		return syscall.EISDIR
	}

	// Remove in transaction
	err = n.store.WithTx(ctx, func(tx *sql.Tx) error {
		// Delete dentry
		if err := n.store.DeleteDentryTx(ctx, tx, n.ino, name); err != nil {
			return err
		}

		// Decrement link count
		nlink, err := n.store.DecrNlinkTx(ctx, tx, childIno)
		if err != nil {
			return err
		}

		// Delete inode and data if no more links
		if nlink == 0 {
			if inode.IsSymlink() {
				if err := n.store.DeleteSymlinkTx(ctx, tx, childIno); err != nil {
					return err
				}
			} else {
				if err := n.store.DeleteDataTx(ctx, tx, childIno); err != nil {
					return err
				}
			}
			if err := n.store.DeleteInodeTx(ctx, tx, childIno); err != nil {
				return err
			}
		}

		return nil
	})

	return toErrno(err)
}

// Rename renames/moves a file or directory
func (n *Node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	newParentNode, ok := newParent.(*Node)
	if !ok {
		return syscall.EIO
	}

	err := n.store.Rename(ctx, n.ino, newParentNode.ino, name, newName)
	return toErrno(err)
}

// Link creates a hard link
func (n *Node) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	targetNode, ok := target.(*Node)
	if !ok {
		return nil, syscall.EIO
	}

	// Get target inode
	inode, err := n.store.GetInode(ctx, targetNode.ino)
	if err != nil {
		return nil, toErrno(err)
	}

	// Can't hard link directories
	if inode.IsDir() {
		return nil, syscall.EPERM
	}

	// Create link in transaction
	err = n.store.WithTx(ctx, func(tx *sql.Tx) error {
		// Create dentry
		if err := n.store.CreateDentryTx(ctx, tx, n.ino, name, targetNode.ino); err != nil {
			return err
		}

		// Increment link count
		return n.store.IncrNlinkTx(ctx, tx, targetNode.ino)
	})
	if err != nil {
		return nil, toErrno(err)
	}

	// Refresh inode data
	inode, err = n.store.GetInode(ctx, targetNode.ino)
	if err != nil {
		return nil, toErrno(err)
	}

	child := n.NewInode(ctx, &Node{
		ino:   targetNode.ino,
		store: n.store,
	}, fs.StableAttr{
		Mode: inode.Mode,
		Ino:  targetNode.ino,
	})

	fillAttr(inode, &out.Attr)
	return child, 0
}
