package overlay

import (
	"context"
	"errors"
	"os"
	"syscall"
)

// Common errors
var (
	ErrNotFound  = errors.New("file not found")
	ErrExists    = errors.New("file exists")
	ErrNotEmpty  = errors.New("directory not empty")
	ErrNotDir    = errors.New("not a directory")
	ErrIsDir     = errors.New("is a directory")
	ErrInvalid   = errors.New("invalid argument")
	ErrNoAccess  = errors.New("permission denied")
	ErrReadOnly  = errors.New("read-only filesystem")
	ErrCrossLink = errors.New("cross-device link")
)

// File type constants (matching Unix)
const (
	S_IFMT   = 0o170000 // File type mask
	S_IFDIR  = 0o040000 // Directory
	S_IFREG  = 0o100000 // Regular file
	S_IFLNK  = 0o120000 // Symbolic link
	S_IFBLK  = 0o060000 // Block device
	S_IFCHR  = 0o020000 // Character device
	S_IFIFO  = 0o010000 // FIFO
	S_IFSOCK = 0o140000 // Socket
)

// Open flags
const (
	O_RDONLY = syscall.O_RDONLY
	O_WRONLY = syscall.O_WRONLY
	O_RDWR   = syscall.O_RDWR
	O_APPEND = syscall.O_APPEND
	O_CREAT  = syscall.O_CREAT
	O_EXCL   = syscall.O_EXCL
	O_TRUNC  = syscall.O_TRUNC
)

// Stats holds file metadata
type Stats struct {
	Ino   uint64 // Inode number
	Mode  uint32 // File type and permissions
	Nlink uint32 // Number of hard links
	Uid   uint32 // Owner user ID
	Gid   uint32 // Owner group ID
	Size  int64  // Size in bytes
	Atime int64  // Access time (Unix timestamp)
	Mtime int64  // Modification time (Unix timestamp)
	Ctime int64  // Change time (Unix timestamp)
}

// IsDir returns true if the stats represent a directory
func (s *Stats) IsDir() bool {
	return s.Mode&S_IFMT == S_IFDIR
}

// IsRegular returns true if the stats represent a regular file
func (s *Stats) IsRegular() bool {
	return s.Mode&S_IFMT == S_IFREG
}

// IsSymlink returns true if the stats represent a symbolic link
func (s *Stats) IsSymlink() bool {
	return s.Mode&S_IFMT == S_IFLNK
}

// FileType returns just the file type bits
func (s *Stats) FileType() uint32 {
	return s.Mode & S_IFMT
}

// Perm returns just the permission bits
func (s *Stats) Perm() uint32 {
	return s.Mode & 0o777
}

// DirEntry represents a directory entry
type DirEntry struct {
	Name string // File name
	Mode uint32 // File type bits (for d_type)
	Ino  uint64 // Inode number
}

// IsDir returns true if the entry is a directory
func (d *DirEntry) IsDir() bool {
	return d.Mode&S_IFMT == S_IFDIR
}

// FilesystemStats holds filesystem statistics
type FilesystemStats struct {
	Blocks  uint64 // Total blocks
	Bfree   uint64 // Free blocks
	Bavail  uint64 // Available blocks
	Files   uint64 // Total inodes
	Ffree   uint64 // Free inodes
	Bsize   uint32 // Block size
	Namelen uint32 // Maximum filename length
}

// File represents an open file handle
type File interface {
	// Read reads up to len(dest) bytes at the given offset
	Read(ctx context.Context, dest []byte, offset int64) (int, error)

	// Write writes data at the given offset
	Write(ctx context.Context, data []byte, offset int64) (int, error)

	// Sync flushes any buffered data to storage
	Sync(ctx context.Context) error

	// Close closes the file handle
	Close() error

	// Stat returns the file's current metadata
	Stat(ctx context.Context) (*Stats, error)

	// Truncate changes the file size
	Truncate(ctx context.Context, size int64) error
}

// FileSystem is the abstraction for different storage backends
type FileSystem interface {
	// Stat returns file metadata, following symlinks
	Stat(ctx context.Context, path string) (*Stats, error)

	// Lstat returns file metadata without following symlinks
	Lstat(ctx context.Context, path string) (*Stats, error)

	// Readlink returns the target of a symbolic link
	Readlink(ctx context.Context, path string) (string, error)

	// Statfs returns filesystem statistics
	Statfs(ctx context.Context) (*FilesystemStats, error)

	// Readdir returns directory entries
	Readdir(ctx context.Context, path string) ([]DirEntry, error)

	// Mkdir creates a directory
	Mkdir(ctx context.Context, path string, mode uint32) error

	// Rmdir removes an empty directory
	Rmdir(ctx context.Context, path string) error

	// Create creates a new file and returns a handle
	Create(ctx context.Context, path string, mode uint32) (File, *Stats, error)

	// Open opens an existing file
	Open(ctx context.Context, path string, flags int) (File, error)

	// Remove removes a file (not a directory)
	Remove(ctx context.Context, path string) error

	// Rename renames/moves a file or directory
	Rename(ctx context.Context, oldpath, newpath string) error

	// Chmod changes file permissions
	Chmod(ctx context.Context, path string, mode uint32) error

	// Chown changes file ownership
	Chown(ctx context.Context, path string, uid, gid uint32) error

	// Truncate changes file size
	Truncate(ctx context.Context, path string, size int64) error

	// Utimens changes access and modification times
	Utimens(ctx context.Context, path string, atime, mtime *int64) error

	// Symlink creates a symbolic link
	Symlink(ctx context.Context, target, linkpath string) error

	// Link creates a hard link
	Link(ctx context.Context, oldpath, newpath string) error

	// Access checks if the path is accessible with the given mode
	Access(ctx context.Context, path string, mode uint32) error
}

// ToErrno converts filesystem errors to syscall.Errno
func ToErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	if errors.Is(err, ErrNotFound) || errors.Is(err, os.ErrNotExist) {
		return syscall.ENOENT
	}
	if errors.Is(err, ErrExists) || errors.Is(err, os.ErrExist) {
		return syscall.EEXIST
	}
	if errors.Is(err, ErrNotEmpty) {
		return syscall.ENOTEMPTY
	}
	if errors.Is(err, ErrNotDir) {
		return syscall.ENOTDIR
	}
	if errors.Is(err, ErrIsDir) {
		return syscall.EISDIR
	}
	if errors.Is(err, ErrInvalid) {
		return syscall.EINVAL
	}
	if errors.Is(err, ErrNoAccess) || errors.Is(err, os.ErrPermission) {
		return syscall.EACCES
	}
	if errors.Is(err, ErrReadOnly) {
		return syscall.EROFS
	}
	if errors.Is(err, ErrCrossLink) {
		return syscall.EXDEV
	}
	// Check for syscall.Errno
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno
	}
	return syscall.EIO
}
