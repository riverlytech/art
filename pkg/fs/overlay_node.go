package fs

import (
	"context"
	"path/filepath"
	"syscall"
	"time"

	"art/pkg/overlay"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// Timeout constants for overlay FUSE
var (
	attrTimeout  = time.Second
	entryTimeout = time.Second
)

// OverlayNode is a FUSE node backed by an overlay.FileSystem
type OverlayNode struct {
	fs.Inode
	path string              // Path relative to root
	fsys overlay.FileSystem  // The underlying filesystem
}

// Ensure interface compliance
var (
	_ fs.InodeEmbedder = (*OverlayNode)(nil)
	_ fs.NodeLookuper  = (*OverlayNode)(nil)
	_ fs.NodeGetattrer = (*OverlayNode)(nil)
	_ fs.NodeSetattrer = (*OverlayNode)(nil)
	_ fs.NodeReaddirer = (*OverlayNode)(nil)
	_ fs.NodeMkdirer   = (*OverlayNode)(nil)
	_ fs.NodeRmdirer   = (*OverlayNode)(nil)
	_ fs.NodeCreater   = (*OverlayNode)(nil)
	_ fs.NodeUnlinker  = (*OverlayNode)(nil)
	_ fs.NodeRenamer   = (*OverlayNode)(nil)
	_ fs.NodeLinker    = (*OverlayNode)(nil)
	_ fs.NodeSymlinker = (*OverlayNode)(nil)
	_ fs.NodeReadlinker = (*OverlayNode)(nil)
	_ fs.NodeOpener    = (*OverlayNode)(nil)
	_ fs.NodeStatfser  = (*OverlayNode)(nil)
	_ fs.NodeAccesser  = (*OverlayNode)(nil)
)

// childPath returns the path for a child with the given name
func (n *OverlayNode) childPath(name string) string {
	if n.path == "/" {
		return "/" + name
	}
	return n.path + "/" + name
}

// Lookup finds a child by name
func (n *OverlayNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childPath := n.childPath(name)

	stats, err := n.fsys.Lstat(ctx, childPath)
	if err != nil {
		return nil, overlay.ToErrno(err)
	}

	fillOverlayAttr(stats, &out.Attr)
	out.SetAttrTimeout(attrTimeout)
	out.SetEntryTimeout(entryTimeout)

	child := &OverlayNode{
		path: childPath,
		fsys: n.fsys,
	}

	return n.NewInode(ctx, child, fs.StableAttr{
		Mode: stats.Mode,
		Ino:  stats.Ino,
	}), 0
}

// Getattr returns file attributes
func (n *OverlayNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	stats, err := n.fsys.Lstat(ctx, n.path)
	if err != nil {
		return overlay.ToErrno(err)
	}

	fillOverlayAttr(stats, &out.Attr)
	out.SetTimeout(attrTimeout)
	return 0
}

// Setattr sets file attributes
func (n *OverlayNode) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	// Handle truncate
	if sz, ok := in.GetSize(); ok {
		if err := n.fsys.Truncate(ctx, n.path, int64(sz)); err != nil {
			return overlay.ToErrno(err)
		}
	}

	// Handle chmod
	if mode, ok := in.GetMode(); ok {
		if err := n.fsys.Chmod(ctx, n.path, mode); err != nil {
			return overlay.ToErrno(err)
		}
	}

	// Handle chown
	if uid, ok := in.GetUID(); ok {
		gid := uint32(0)
		if g, ok := in.GetGID(); ok {
			gid = g
		}
		if err := n.fsys.Chown(ctx, n.path, uid, gid); err != nil {
			return overlay.ToErrno(err)
		}
	}

	// Handle utimens
	if atime, ok := in.GetATime(); ok {
		at := atime.Unix()
		var mt *int64
		if mtime, ok := in.GetMTime(); ok {
			m := mtime.Unix()
			mt = &m
		}
		if err := n.fsys.Utimens(ctx, n.path, &at, mt); err != nil {
			return overlay.ToErrno(err)
		}
	} else if mtime, ok := in.GetMTime(); ok {
		m := mtime.Unix()
		if err := n.fsys.Utimens(ctx, n.path, nil, &m); err != nil {
			return overlay.ToErrno(err)
		}
	}

	// Get updated attributes
	return n.Getattr(ctx, fh, out)
}

// Readdir returns directory entries
func (n *OverlayNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := n.fsys.Readdir(ctx, n.path)
	if err != nil {
		return nil, overlay.ToErrno(err)
	}

	result := make([]fuse.DirEntry, len(entries))
	for i, e := range entries {
		result[i] = fuse.DirEntry{
			Name: e.Name,
			Mode: e.Mode,
			Ino:  e.Ino,
		}
	}

	return fs.NewListDirStream(result), 0
}

// Mkdir creates a directory
func (n *OverlayNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childPath := n.childPath(name)

	if err := n.fsys.Mkdir(ctx, childPath, mode); err != nil {
		return nil, overlay.ToErrno(err)
	}

	stats, err := n.fsys.Lstat(ctx, childPath)
	if err != nil {
		return nil, overlay.ToErrno(err)
	}

	fillOverlayAttr(stats, &out.Attr)
	out.SetAttrTimeout(attrTimeout)
	out.SetEntryTimeout(entryTimeout)

	child := &OverlayNode{
		path: childPath,
		fsys: n.fsys,
	}

	return n.NewInode(ctx, child, fs.StableAttr{
		Mode: stats.Mode,
		Ino:  stats.Ino,
	}), 0
}

// Rmdir removes a directory
func (n *OverlayNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	childPath := n.childPath(name)
	return overlay.ToErrno(n.fsys.Rmdir(ctx, childPath))
}

// Create creates a new file
func (n *OverlayNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	childPath := n.childPath(name)

	file, stats, err := n.fsys.Create(ctx, childPath, mode)
	if err != nil {
		return nil, nil, 0, overlay.ToErrno(err)
	}

	fillOverlayAttr(stats, &out.Attr)
	out.SetAttrTimeout(attrTimeout)
	out.SetEntryTimeout(entryTimeout)

	child := &OverlayNode{
		path: childPath,
		fsys: n.fsys,
	}

	handle := &OverlayFileHandle{
		path: childPath,
		file: file,
		fsys: n.fsys,
	}

	return n.NewInode(ctx, child, fs.StableAttr{
		Mode: stats.Mode,
		Ino:  stats.Ino,
	}), handle, 0, 0
}

// Unlink removes a file
func (n *OverlayNode) Unlink(ctx context.Context, name string) syscall.Errno {
	childPath := n.childPath(name)
	return overlay.ToErrno(n.fsys.Remove(ctx, childPath))
}

// Rename renames a file or directory
func (n *OverlayNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	oldPath := n.childPath(name)

	// Get new parent's path
	newParentNode, ok := newParent.(*OverlayNode)
	if !ok {
		return syscall.EINVAL
	}
	newPath := newParentNode.childPath(newName)

	return overlay.ToErrno(n.fsys.Rename(ctx, oldPath, newPath))
}

// Link creates a hard link
func (n *OverlayNode) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	targetNode, ok := target.(*OverlayNode)
	if !ok {
		return nil, syscall.EINVAL
	}

	newPath := n.childPath(name)

	if err := n.fsys.Link(ctx, targetNode.path, newPath); err != nil {
		return nil, overlay.ToErrno(err)
	}

	stats, err := n.fsys.Lstat(ctx, newPath)
	if err != nil {
		return nil, overlay.ToErrno(err)
	}

	fillOverlayAttr(stats, &out.Attr)
	out.SetAttrTimeout(attrTimeout)
	out.SetEntryTimeout(entryTimeout)

	child := &OverlayNode{
		path: newPath,
		fsys: n.fsys,
	}

	return n.NewInode(ctx, child, fs.StableAttr{
		Mode: stats.Mode,
		Ino:  stats.Ino,
	}), 0
}

// Symlink creates a symbolic link
func (n *OverlayNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	linkPath := n.childPath(name)

	if err := n.fsys.Symlink(ctx, target, linkPath); err != nil {
		return nil, overlay.ToErrno(err)
	}

	stats, err := n.fsys.Lstat(ctx, linkPath)
	if err != nil {
		return nil, overlay.ToErrno(err)
	}

	fillOverlayAttr(stats, &out.Attr)
	out.SetAttrTimeout(attrTimeout)
	out.SetEntryTimeout(entryTimeout)

	child := &OverlayNode{
		path: linkPath,
		fsys: n.fsys,
	}

	return n.NewInode(ctx, child, fs.StableAttr{
		Mode: stats.Mode,
		Ino:  stats.Ino,
	}), 0
}

// Readlink reads a symbolic link
func (n *OverlayNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	target, err := n.fsys.Readlink(ctx, n.path)
	if err != nil {
		return nil, overlay.ToErrno(err)
	}
	return []byte(target), 0
}

// Open opens a file
func (n *OverlayNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	// Verify it's not a directory
	stats, err := n.fsys.Lstat(ctx, n.path)
	if err != nil {
		return nil, 0, overlay.ToErrno(err)
	}
	if stats.IsDir() {
		return nil, 0, syscall.EISDIR
	}

	file, err := n.fsys.Open(ctx, n.path, int(flags))
	if err != nil {
		return nil, 0, overlay.ToErrno(err)
	}

	return &OverlayFileHandle{
		path: n.path,
		file: file,
		fsys: n.fsys,
	}, 0, 0
}

// Statfs returns filesystem statistics
func (n *OverlayNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	stats, err := n.fsys.Statfs(ctx)
	if err != nil {
		return overlay.ToErrno(err)
	}

	out.Blocks = stats.Blocks
	out.Bfree = stats.Bfree
	out.Bavail = stats.Bavail
	out.Files = stats.Files
	out.Ffree = stats.Ffree
	out.Bsize = stats.Bsize
	out.NameLen = stats.Namelen
	out.Frsize = stats.Bsize

	return 0
}

// Access checks if the file is accessible
func (n *OverlayNode) Access(ctx context.Context, mask uint32) syscall.Errno {
	return overlay.ToErrno(n.fsys.Access(ctx, n.path, mask))
}

// fillOverlayAttr fills fuse.Attr from overlay.Stats
func fillOverlayAttr(stats *overlay.Stats, attr *fuse.Attr) {
	attr.Ino = stats.Ino
	attr.Mode = stats.Mode
	attr.Nlink = stats.Nlink
	attr.Uid = stats.Uid
	attr.Gid = stats.Gid
	attr.Size = uint64(stats.Size)
	attr.Atime = uint64(stats.Atime)
	attr.Mtime = uint64(stats.Mtime)
	attr.Ctime = uint64(stats.Ctime)
	attr.Blksize = 4096
	attr.Blocks = (uint64(stats.Size) + 511) / 512
}

// OverlayFileHandle implements fs.FileHandle for overlay filesystem
type OverlayFileHandle struct {
	path  string
	file  overlay.File
	fsys  overlay.FileSystem
	flags uint32
}

// Ensure interface compliance
var (
	_ fs.FileReader    = (*OverlayFileHandle)(nil)
	_ fs.FileWriter    = (*OverlayFileHandle)(nil)
	_ fs.FileFlusher   = (*OverlayFileHandle)(nil)
	_ fs.FileFsyncer   = (*OverlayFileHandle)(nil)
	_ fs.FileGetattrer = (*OverlayFileHandle)(nil)
)

// Read reads data from the file
func (fh *OverlayFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	n, err := fh.file.Read(ctx, dest, off)
	if err != nil {
		return nil, overlay.ToErrno(err)
	}
	return fuse.ReadResultData(dest[:n]), 0
}

// Write writes data to the file
func (fh *OverlayFileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	n, err := fh.file.Write(ctx, data, off)
	if err != nil {
		return 0, overlay.ToErrno(err)
	}
	return uint32(n), 0
}

// Flush is called on close
func (fh *OverlayFileHandle) Flush(ctx context.Context) syscall.Errno {
	return 0
}

// Fsync syncs file data to disk
func (fh *OverlayFileHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	return overlay.ToErrno(fh.file.Sync(ctx))
}

// Getattr returns file attributes
func (fh *OverlayFileHandle) Getattr(ctx context.Context, out *fuse.AttrOut) syscall.Errno {
	stats, err := fh.file.Stat(ctx)
	if err != nil {
		return overlay.ToErrno(err)
	}
	fillOverlayAttr(stats, &out.Attr)
	return 0
}

// Release is called when the file handle is released
func (fh *OverlayFileHandle) Release(ctx context.Context) syscall.Errno {
	if fh.file != nil {
		fh.file.Close()
	}
	return 0
}

// cleanPath normalizes a path for overlay filesystem
func cleanPath(path string) string {
	if path == "" {
		return "/"
	}
	path = filepath.Clean("/" + path)
	return path
}
