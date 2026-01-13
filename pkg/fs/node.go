package fs

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"art/pkg/db"
)

// Node represents a filesystem node backed by SQLite
type Node struct {
	fs.Inode
	ino   uint64
	store *db.Store
}

// Ensure interface compliance at compile time
var (
	_ fs.InodeEmbedder  = (*Node)(nil)
	_ fs.NodeLookuper   = (*Node)(nil)
	_ fs.NodeGetattrer  = (*Node)(nil)
	_ fs.NodeSetattrer  = (*Node)(nil)
	_ fs.NodeReaddirer  = (*Node)(nil)
	_ fs.NodeMkdirer    = (*Node)(nil)
	_ fs.NodeRmdirer    = (*Node)(nil)
	_ fs.NodeCreater    = (*Node)(nil)
	_ fs.NodeUnlinker   = (*Node)(nil)
	_ fs.NodeRenamer    = (*Node)(nil)
	_ fs.NodeLinker     = (*Node)(nil)
	_ fs.NodeSymlinker  = (*Node)(nil)
	_ fs.NodeReadlinker = (*Node)(nil)
	_ fs.NodeOpener     = (*Node)(nil)
	_ fs.NodeStatfser   = (*Node)(nil)
	_ fs.NodeAccesser   = (*Node)(nil)
)

// Getattr returns file attributes
func (n *Node) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	inode, err := n.store.GetInode(ctx, n.ino)
	if err != nil {
		return toErrno(err)
	}
	fillAttr(inode, &out.Attr)
	return 0
}

// Setattr sets file attributes
func (n *Node) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	var mode *uint32
	var uid, gid *uint32
	var size *uint64
	var atime, mtime *int64

	if m, ok := in.GetMode(); ok {
		mode = &m
	}
	if u, ok := in.GetUID(); ok {
		uid = &u
	}
	if g, ok := in.GetGID(); ok {
		gid = &g
	}
	if sz, ok := in.GetSize(); ok {
		size = &sz
		// Also truncate the file data
		if err := n.store.Truncate(ctx, n.ino, sz); err != nil {
			return toErrno(err)
		}
	}
	if a, ok := in.GetATime(); ok {
		at := a.Unix()
		atime = &at
	}
	if m, ok := in.GetMTime(); ok {
		mt := m.Unix()
		mtime = &mt
	}

	if err := n.store.SetAttr(ctx, n.ino, mode, uid, gid, size, atime, mtime); err != nil {
		return toErrno(err)
	}

	return n.Getattr(ctx, fh, out)
}

// Lookup finds a child node by name
func (n *Node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childIno, err := n.store.Lookup(ctx, n.ino, name)
	if err != nil {
		return nil, toErrno(err)
	}

	inode, err := n.store.GetInode(ctx, childIno)
	if err != nil {
		return nil, toErrno(err)
	}

	child := n.NewInode(ctx, &Node{
		ino:   childIno,
		store: n.store,
	}, fs.StableAttr{
		Mode: inode.Mode,
		Ino:  childIno,
	})

	fillAttr(inode, &out.Attr)
	return child, 0
}

// fillAttr fills fuse.Attr from db.Inode
func fillAttr(inode *db.Inode, attr *fuse.Attr) {
	attr.Ino = inode.Ino
	attr.Mode = inode.Mode
	attr.Nlink = inode.Nlink
	attr.Uid = inode.UID
	attr.Gid = inode.GID
	attr.Size = inode.Size
	attr.Blksize = 4096
	attr.Blocks = (inode.Size + 511) / 512
	attr.Atime = uint64(inode.Atime)
	attr.Mtime = uint64(inode.Mtime)
	attr.Ctime = uint64(inode.Ctime)
}

// toErrno converts db errors to syscall.Errno
func toErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	switch err {
	case db.ErrNotFound:
		return syscall.ENOENT
	case db.ErrExists:
		return syscall.EEXIST
	case db.ErrNotEmpty:
		return syscall.ENOTEMPTY
	default:
		return syscall.EIO
	}
}

// Statfs returns filesystem statistics
func (n *Node) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	// Report virtual filesystem stats
	// These are approximate values for a SQLite-backed FS
	const blockSize = 4096
	const totalBlocks = 1 << 30 / blockSize  // 1GB virtual size
	const freeBlocks = 1 << 29 / blockSize   // 512MB free

	out.Bsize = blockSize
	out.Blocks = totalBlocks
	out.Bfree = freeBlocks
	out.Bavail = freeBlocks
	out.Files = 1 << 20       // Max inodes (1M)
	out.Ffree = 1<<20 - 1000  // Free inodes
	out.Frsize = blockSize
	out.NameLen = 255

	return 0
}

// Access checks if the calling process has access to the node
func (n *Node) Access(ctx context.Context, mask uint32) syscall.Errno {
	// For now, allow all access within the sandbox
	// The sandbox itself provides isolation
	return 0
}
