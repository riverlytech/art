package overlay

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// HostFS implements FileSystem by passing through to the host filesystem
type HostFS struct {
	root string // Absolute path to the root directory
}

// NewHostFS creates a new HostFS rooted at the given directory
func NewHostFS(root string) (*HostFS, error) {
	// Resolve to absolute path
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve root path: %w", err)
	}

	// Verify the root exists and is a directory
	info, err := os.Stat(absRoot)
	if err != nil {
		return nil, fmt.Errorf("failed to stat root: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("root is not a directory: %s", absRoot)
	}

	return &HostFS{root: absRoot}, nil
}

// resolvePath converts a virtual path to a real host path, preventing escapes
func (h *HostFS) resolvePath(path string) (string, error) {
	// Clean and ensure it starts with /
	path = filepath.Clean("/" + path)

	// Join with root
	fullPath := filepath.Join(h.root, path)

	// Verify the resolved path is still under root (prevent ../ escapes)
	if !strings.HasPrefix(fullPath, h.root) {
		return "", ErrNoAccess
	}

	return fullPath, nil
}

// Root returns the root directory path
func (h *HostFS) Root() string {
	return h.root
}

// Stat implements FileSystem.Stat (follows symlinks)
func (h *HostFS) Stat(ctx context.Context, path string) (*Stats, error) {
	realPath, err := h.resolvePath(path)
	if err != nil {
		return nil, err
	}

	info, err := os.Stat(realPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return fileInfoToStats(info), nil
}

// Lstat implements FileSystem.Lstat (does not follow symlinks)
func (h *HostFS) Lstat(ctx context.Context, path string) (*Stats, error) {
	realPath, err := h.resolvePath(path)
	if err != nil {
		return nil, err
	}

	info, err := os.Lstat(realPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return fileInfoToStats(info), nil
}

// Readlink implements FileSystem.Readlink
func (h *HostFS) Readlink(ctx context.Context, path string) (string, error) {
	realPath, err := h.resolvePath(path)
	if err != nil {
		return "", err
	}

	target, err := os.Readlink(realPath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", ErrNotFound
		}
		return "", err
	}

	return target, nil
}

// Statfs implements FileSystem.Statfs
func (h *HostFS) Statfs(ctx context.Context) (*FilesystemStats, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(h.root, &stat); err != nil {
		return nil, err
	}

	return &FilesystemStats{
		Blocks:  stat.Blocks,
		Bfree:   stat.Bfree,
		Bavail:  stat.Bavail,
		Files:   stat.Files,
		Ffree:   stat.Ffree,
		Bsize:   uint32(stat.Bsize),
		Namelen: 255, // Typical Unix max
	}, nil
}

// Readdir implements FileSystem.Readdir
func (h *HostFS) Readdir(ctx context.Context, path string) ([]DirEntry, error) {
	realPath, err := h.resolvePath(path)
	if err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(realPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	result := make([]DirEntry, 0, len(entries))
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue // Skip entries we can't stat
		}
		stats := fileInfoToStats(info)
		result = append(result, DirEntry{
			Name: e.Name(),
			Mode: stats.Mode,
			Ino:  stats.Ino,
		})
	}

	return result, nil
}

// Mkdir implements FileSystem.Mkdir
func (h *HostFS) Mkdir(ctx context.Context, path string, mode uint32) error {
	realPath, err := h.resolvePath(path)
	if err != nil {
		return err
	}

	err = os.Mkdir(realPath, os.FileMode(mode&0o777))
	if err != nil {
		if os.IsExist(err) {
			return ErrExists
		}
		if os.IsNotExist(err) {
			return ErrNotFound
		}
		return err
	}
	return nil
}

// Rmdir implements FileSystem.Rmdir
func (h *HostFS) Rmdir(ctx context.Context, path string) error {
	realPath, err := h.resolvePath(path)
	if err != nil {
		return err
	}

	// Verify it's a directory
	info, err := os.Lstat(realPath)
	if err != nil {
		if os.IsNotExist(err) {
			return ErrNotFound
		}
		return err
	}
	if !info.IsDir() {
		return ErrNotDir
	}

	err = os.Remove(realPath)
	if err != nil {
		if os.IsNotExist(err) {
			return ErrNotFound
		}
		// Check for ENOTEMPTY
		if pe, ok := err.(*os.PathError); ok {
			if pe.Err == syscall.ENOTEMPTY {
				return ErrNotEmpty
			}
		}
		return err
	}
	return nil
}

// Create implements FileSystem.Create
func (h *HostFS) Create(ctx context.Context, path string, mode uint32) (File, *Stats, error) {
	realPath, err := h.resolvePath(path)
	if err != nil {
		return nil, nil, err
	}

	f, err := os.OpenFile(realPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, os.FileMode(mode&0o777))
	if err != nil {
		if os.IsExist(err) {
			return nil, nil, ErrExists
		}
		if os.IsNotExist(err) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}

	return NewOSFile(f, path), fileInfoToStats(info), nil
}

// Open implements FileSystem.Open
func (h *HostFS) Open(ctx context.Context, path string, flags int) (File, error) {
	realPath, err := h.resolvePath(path)
	if err != nil {
		return nil, err
	}

	// Convert flags
	osFlags := 0
	switch flags & (O_RDONLY | O_WRONLY | O_RDWR) {
	case O_RDONLY:
		osFlags = os.O_RDONLY
	case O_WRONLY:
		osFlags = os.O_WRONLY
	case O_RDWR:
		osFlags = os.O_RDWR
	}
	if flags&O_APPEND != 0 {
		osFlags |= os.O_APPEND
	}
	if flags&O_TRUNC != 0 {
		osFlags |= os.O_TRUNC
	}

	f, err := os.OpenFile(realPath, osFlags, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		if os.IsPermission(err) {
			return nil, ErrNoAccess
		}
		return nil, err
	}

	return NewOSFile(f, path), nil
}

// Remove implements FileSystem.Remove
func (h *HostFS) Remove(ctx context.Context, path string) error {
	realPath, err := h.resolvePath(path)
	if err != nil {
		return err
	}

	// Verify it's not a directory
	info, err := os.Lstat(realPath)
	if err != nil {
		if os.IsNotExist(err) {
			return ErrNotFound
		}
		return err
	}
	if info.IsDir() {
		return ErrIsDir
	}

	err = os.Remove(realPath)
	if err != nil {
		if os.IsNotExist(err) {
			return ErrNotFound
		}
		return err
	}
	return nil
}

// Rename implements FileSystem.Rename
func (h *HostFS) Rename(ctx context.Context, oldpath, newpath string) error {
	realOld, err := h.resolvePath(oldpath)
	if err != nil {
		return err
	}
	realNew, err := h.resolvePath(newpath)
	if err != nil {
		return err
	}

	return os.Rename(realOld, realNew)
}

// Chmod implements FileSystem.Chmod
func (h *HostFS) Chmod(ctx context.Context, path string, mode uint32) error {
	realPath, err := h.resolvePath(path)
	if err != nil {
		return err
	}

	return os.Chmod(realPath, os.FileMode(mode&0o777))
}

// Chown implements FileSystem.Chown
func (h *HostFS) Chown(ctx context.Context, path string, uid, gid uint32) error {
	realPath, err := h.resolvePath(path)
	if err != nil {
		return err
	}

	return os.Lchown(realPath, int(uid), int(gid))
}

// Truncate implements FileSystem.Truncate
func (h *HostFS) Truncate(ctx context.Context, path string, size int64) error {
	realPath, err := h.resolvePath(path)
	if err != nil {
		return err
	}

	return os.Truncate(realPath, size)
}

// Utimens implements FileSystem.Utimens
func (h *HostFS) Utimens(ctx context.Context, path string, atime, mtime *int64) error {
	realPath, err := h.resolvePath(path)
	if err != nil {
		return err
	}

	// Get current times
	info, err := os.Lstat(realPath)
	if err != nil {
		return err
	}

	var at, mt int64
	stats := fileInfoToStats(info)

	if atime != nil {
		at = *atime
	} else {
		at = stats.Atime
	}
	if mtime != nil {
		mt = *mtime
	} else {
		mt = stats.Mtime
	}

	return os.Chtimes(realPath, unixToTime(at), unixToTime(mt))
}

// Symlink implements FileSystem.Symlink
func (h *HostFS) Symlink(ctx context.Context, target, linkpath string) error {
	realLink, err := h.resolvePath(linkpath)
	if err != nil {
		return err
	}

	return os.Symlink(target, realLink)
}

// Link implements FileSystem.Link
func (h *HostFS) Link(ctx context.Context, oldpath, newpath string) error {
	realOld, err := h.resolvePath(oldpath)
	if err != nil {
		return err
	}
	realNew, err := h.resolvePath(newpath)
	if err != nil {
		return err
	}

	return os.Link(realOld, realNew)
}

// Access implements FileSystem.Access
func (h *HostFS) Access(ctx context.Context, path string, mode uint32) error {
	realPath, err := h.resolvePath(path)
	if err != nil {
		return err
	}

	// Check existence first
	_, err = os.Lstat(realPath)
	if err != nil {
		if os.IsNotExist(err) {
			return ErrNotFound
		}
		return err
	}

	// For now, just check existence (mode 0 = F_OK)
	// A full implementation would check R_OK, W_OK, X_OK
	return nil
}

// Ensure HostFS implements FileSystem
var _ FileSystem = (*HostFS)(nil)
