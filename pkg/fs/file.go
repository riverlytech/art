package fs

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"art/pkg/db"
)

// FileHandle represents an open file
type FileHandle struct {
	ino   uint64
	store *db.Store
	flags uint32
}

// Ensure interface compliance
var (
	_ fs.FileReader    = (*FileHandle)(nil)
	_ fs.FileWriter    = (*FileHandle)(nil)
	_ fs.FileFlusher   = (*FileHandle)(nil)
	_ fs.FileFsyncer   = (*FileHandle)(nil)
	_ fs.FileGetattrer = (*FileHandle)(nil)
)

// Open opens a file and returns a handle
func (n *Node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	// Verify the inode exists
	inode, err := n.store.GetInode(ctx, n.ino)
	if err != nil {
		return nil, 0, toErrno(err)
	}

	// Can't open directories with Open (use Opendir)
	if inode.IsDir() {
		return nil, 0, syscall.EISDIR
	}

	// Handle truncation if O_TRUNC is set
	if flags&syscall.O_TRUNC != 0 {
		if err := n.store.Truncate(ctx, n.ino, 0); err != nil {
			return nil, 0, toErrno(err)
		}
		if err := n.store.UpdateSize(ctx, n.ino, 0); err != nil {
			return nil, 0, toErrno(err)
		}
	}

	return &FileHandle{
		ino:   n.ino,
		store: n.store,
		flags: flags,
	}, 0, 0
}

// Read reads data from the file
func (fh *FileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	data, err := fh.store.ReadData(ctx, fh.ino, off, int64(len(dest)))
	if err != nil {
		return nil, toErrno(err)
	}
	return fuse.ReadResultData(data), 0
}

// Write writes data to the file
func (fh *FileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	if err := fh.store.WriteData(ctx, fh.ino, off, data); err != nil {
		return 0, toErrno(err)
	}

	// Update file size if we extended the file
	inode, err := fh.store.GetInode(ctx, fh.ino)
	if err != nil {
		return 0, toErrno(err)
	}

	newSize := uint64(off) + uint64(len(data))
	if newSize > inode.Size {
		if err := fh.store.UpdateSize(ctx, fh.ino, newSize); err != nil {
			return 0, toErrno(err)
		}
	} else {
		// Just update mtime
		if err := fh.store.UpdateTimes(ctx, fh.ino, nil, nil); err != nil {
			return 0, toErrno(err)
		}
	}

	return uint32(len(data)), 0
}

// Flush is called on close
func (fh *FileHandle) Flush(ctx context.Context) syscall.Errno {
	// SQLite handles durability, nothing to flush
	return 0
}

// Getattr returns file attributes from the handle
func (fh *FileHandle) Getattr(ctx context.Context, out *fuse.AttrOut) syscall.Errno {
	inode, err := fh.store.GetInode(ctx, fh.ino)
	if err != nil {
		return toErrno(err)
	}
	fillAttr(inode, &out.Attr)
	return 0
}

// Fsync syncs file data to disk
func (fh *FileHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	// SQLite with WAL mode handles durability
	// A checkpoint could be forced here if needed, but generally not necessary
	return 0
}
