package overlay

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"art/pkg/db"

	lru "github.com/hashicorp/golang-lru/v2"
)

// AgentFS implements FileSystem backed by SQLite via db.Store
type AgentFS struct {
	store *db.Store
	cache *lru.Cache[dentryKey, uint64] // LRU cache for path resolution
	mu    sync.RWMutex
}

// dentryKey is the key for dentry cache
type dentryKey struct {
	parentIno uint64
	name      string
}

// NewAgentFS creates a new AgentFS wrapping the given store
func NewAgentFS(store *db.Store) (*AgentFS, error) {
	// Create LRU cache with 10000 entries
	cache, err := lru.New[dentryKey, uint64](10000)
	if err != nil {
		return nil, err
	}

	return &AgentFS{
		store: store,
		cache: cache,
	}, nil
}

// Store returns the underlying db.Store
func (a *AgentFS) Store() *db.Store {
	return a.store
}

// resolvePath converts a virtual path to an inode number
func (a *AgentFS) resolvePath(ctx context.Context, path string) (uint64, error) {
	parts := splitPath(path)
	if len(parts) == 0 {
		return 1, nil // Root inode
	}

	ino := uint64(1) // Start at root
	for _, part := range parts {
		key := dentryKey{parentIno: ino, name: part}

		// Check cache first
		if childIno, ok := a.cache.Get(key); ok {
			ino = childIno
			continue
		}

		// Cache miss - query database
		childIno, err := a.store.Lookup(ctx, ino, part)
		if err != nil {
			if err == db.ErrNotFound {
				return 0, ErrNotFound
			}
			return 0, err
		}

		// Cache the result
		a.cache.Add(key, childIno)
		ino = childIno
	}

	return ino, nil
}

// resolveParentAndName resolves the parent directory inode and returns the name component
func (a *AgentFS) resolveParentAndName(ctx context.Context, path string) (parentIno uint64, name string, err error) {
	parts := splitPath(path)
	if len(parts) == 0 {
		return 0, "", ErrInvalid // Can't get parent of root
	}

	name = parts[len(parts)-1]
	parentPath := joinPath(parts[:len(parts)-1])

	parentIno, err = a.resolvePath(ctx, parentPath)
	if err != nil {
		return 0, "", err
	}

	return parentIno, name, nil
}

// invalidateCache removes a dentry from the cache
func (a *AgentFS) invalidateCache(parentIno uint64, name string) {
	a.cache.Remove(dentryKey{parentIno: parentIno, name: name})
}

// Stat implements FileSystem.Stat (follows symlinks)
func (a *AgentFS) Stat(ctx context.Context, path string) (*Stats, error) {
	// For now, Stat and Lstat behave the same (we don't follow symlinks in paths)
	// A full implementation would resolve symlinks in the path
	return a.Lstat(ctx, path)
}

// Lstat implements FileSystem.Lstat (does not follow symlinks)
func (a *AgentFS) Lstat(ctx context.Context, path string) (*Stats, error) {
	ino, err := a.resolvePath(ctx, path)
	if err != nil {
		return nil, err
	}

	inode, err := a.store.GetInode(ctx, ino)
	if err != nil {
		if err == db.ErrNotFound {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return inodeToStats(inode), nil
}

// Readlink implements FileSystem.Readlink
func (a *AgentFS) Readlink(ctx context.Context, path string) (string, error) {
	ino, err := a.resolvePath(ctx, path)
	if err != nil {
		return "", err
	}

	target, err := a.store.ReadSymlink(ctx, ino)
	if err != nil {
		if err == db.ErrNotFound {
			return "", ErrNotFound
		}
		return "", err
	}

	return target, nil
}

// Statfs implements FileSystem.Statfs
func (a *AgentFS) Statfs(ctx context.Context) (*FilesystemStats, error) {
	// Return virtual filesystem stats
	return &FilesystemStats{
		Blocks:  1024 * 1024, // 1GB with 1KB blocks
		Bfree:   512 * 1024,  // 512MB free
		Bavail:  512 * 1024,
		Files:   1000000, // 1M inodes
		Ffree:   999000,
		Bsize:   1024,
		Namelen: 255,
	}, nil
}

// Readdir implements FileSystem.Readdir
func (a *AgentFS) Readdir(ctx context.Context, path string) ([]DirEntry, error) {
	ino, err := a.resolvePath(ctx, path)
	if err != nil {
		return nil, err
	}

	entries, err := a.store.ListDir(ctx, ino)
	if err != nil {
		return nil, err
	}

	result := make([]DirEntry, 0, len(entries))
	for _, e := range entries {
		inode, err := a.store.GetInode(ctx, e.Ino)
		if err != nil {
			continue // Skip entries we can't stat
		}
		result = append(result, DirEntry{
			Name: e.Name,
			Mode: inode.Mode,
			Ino:  e.Ino,
		})
	}

	return result, nil
}

// Mkdir implements FileSystem.Mkdir
func (a *AgentFS) Mkdir(ctx context.Context, path string, mode uint32) error {
	parentIno, name, err := a.resolveParentAndName(ctx, path)
	if err != nil {
		return err
	}

	return a.store.WithTx(ctx, func(tx *sql.Tx) error {
		// Create directory inode
		ino, err := a.store.CreateInodeTx(ctx, tx, db.S_IFDIR|mode, 0, 0)
		if err != nil {
			return err
		}

		// Create directory entry
		if err := a.store.CreateDentryTx(ctx, tx, parentIno, name, ino); err != nil {
			if err == db.ErrExists {
				return ErrExists
			}
			return err
		}

		// Increment parent's link count (for "..")
		return a.store.IncrNlinkTx(ctx, tx, parentIno)
	})
}

// Rmdir implements FileSystem.Rmdir
func (a *AgentFS) Rmdir(ctx context.Context, path string) error {
	parentIno, name, err := a.resolveParentAndName(ctx, path)
	if err != nil {
		return err
	}

	// Lookup the inode
	ino, err := a.store.Lookup(ctx, parentIno, name)
	if err != nil {
		if err == db.ErrNotFound {
			return ErrNotFound
		}
		return err
	}

	// Verify it's a directory
	inode, err := a.store.GetInode(ctx, ino)
	if err != nil {
		return err
	}
	if !inode.IsDir() {
		return ErrNotDir
	}

	// Check if empty
	hasChildren, err := a.store.HasChildren(ctx, ino)
	if err != nil {
		return err
	}
	if hasChildren {
		return ErrNotEmpty
	}

	err = a.store.WithTx(ctx, func(tx *sql.Tx) error {
		// Delete directory entry
		if err := a.store.DeleteDentryTx(ctx, tx, parentIno, name); err != nil {
			return err
		}

		// Delete inode
		if err := a.store.DeleteInodeTx(ctx, tx, ino); err != nil {
			return err
		}

		// Decrement parent's link count
		_, err := a.store.DecrNlinkTx(ctx, tx, parentIno)
		return err
	})

	if err == nil {
		a.invalidateCache(parentIno, name)
	}
	return err
}

// Create implements FileSystem.Create
func (a *AgentFS) Create(ctx context.Context, path string, mode uint32) (File, *Stats, error) {
	parentIno, name, err := a.resolveParentAndName(ctx, path)
	if err != nil {
		return nil, nil, err
	}

	var ino uint64
	err = a.store.WithTx(ctx, func(tx *sql.Tx) error {
		// Create file inode
		var err error
		ino, err = a.store.CreateInodeTx(ctx, tx, db.S_IFREG|mode, 0, 0)
		if err != nil {
			return err
		}

		// Create directory entry
		if err := a.store.CreateDentryTx(ctx, tx, parentIno, name, ino); err != nil {
			if err == db.ErrExists {
				return ErrExists
			}
			return err
		}

		return nil
	})

	if err != nil {
		return nil, nil, err
	}

	inode, err := a.store.GetInode(ctx, ino)
	if err != nil {
		return nil, nil, err
	}

	return &AgentFile{
		store: a.store,
		ino:   ino,
		path:  path,
	}, inodeToStats(inode), nil
}

// Open implements FileSystem.Open
func (a *AgentFS) Open(ctx context.Context, path string, flags int) (File, error) {
	ino, err := a.resolvePath(ctx, path)
	if err != nil {
		return nil, err
	}

	// Handle truncation
	if flags&O_TRUNC != 0 {
		if err := a.store.Truncate(ctx, ino, 0); err != nil {
			return nil, err
		}
		if err := a.store.UpdateSize(ctx, ino, 0); err != nil {
			return nil, err
		}
	}

	return &AgentFile{
		store: a.store,
		ino:   ino,
		path:  path,
	}, nil
}

// Remove implements FileSystem.Remove
func (a *AgentFS) Remove(ctx context.Context, path string) error {
	parentIno, name, err := a.resolveParentAndName(ctx, path)
	if err != nil {
		return err
	}

	// Lookup the inode
	ino, err := a.store.Lookup(ctx, parentIno, name)
	if err != nil {
		if err == db.ErrNotFound {
			return ErrNotFound
		}
		return err
	}

	// Verify it's not a directory
	inode, err := a.store.GetInode(ctx, ino)
	if err != nil {
		return err
	}
	if inode.IsDir() {
		return ErrIsDir
	}

	err = a.store.WithTx(ctx, func(tx *sql.Tx) error {
		// Delete directory entry
		if err := a.store.DeleteDentryTx(ctx, tx, parentIno, name); err != nil {
			return err
		}

		// Decrement link count
		remaining, err := a.store.DecrNlinkTx(ctx, tx, ino)
		if err != nil {
			return err
		}

		// If no more links, delete the file data and inode
		if remaining == 0 {
			if inode.IsSymlink() {
				if err := a.store.DeleteSymlinkTx(ctx, tx, ino); err != nil {
					return err
				}
			} else {
				if err := a.store.DeleteDataTx(ctx, tx, ino); err != nil {
					return err
				}
			}
			if err := a.store.DeleteInodeTx(ctx, tx, ino); err != nil {
				return err
			}
		}

		return nil
	})

	if err == nil {
		a.invalidateCache(parentIno, name)
	}
	return err
}

// Rename implements FileSystem.Rename
func (a *AgentFS) Rename(ctx context.Context, oldpath, newpath string) error {
	oldParentIno, oldName, err := a.resolveParentAndName(ctx, oldpath)
	if err != nil {
		return err
	}

	newParentIno, newName, err := a.resolveParentAndName(ctx, newpath)
	if err != nil {
		return err
	}

	err = a.store.Rename(ctx, oldParentIno, newParentIno, oldName, newName)
	if err == nil {
		a.invalidateCache(oldParentIno, oldName)
		a.invalidateCache(newParentIno, newName)
	}
	return err
}

// Chmod implements FileSystem.Chmod
func (a *AgentFS) Chmod(ctx context.Context, path string, mode uint32) error {
	ino, err := a.resolvePath(ctx, path)
	if err != nil {
		return err
	}

	inode, err := a.store.GetInode(ctx, ino)
	if err != nil {
		return err
	}

	// Keep file type, update permissions
	newMode := (inode.Mode & S_IFMT) | (mode & 0o777)
	return a.store.SetAttr(ctx, ino, &newMode, nil, nil, nil, nil, nil)
}

// Chown implements FileSystem.Chown
func (a *AgentFS) Chown(ctx context.Context, path string, uid, gid uint32) error {
	ino, err := a.resolvePath(ctx, path)
	if err != nil {
		return err
	}

	return a.store.SetAttr(ctx, ino, nil, &uid, &gid, nil, nil, nil)
}

// Truncate implements FileSystem.Truncate
func (a *AgentFS) Truncate(ctx context.Context, path string, size int64) error {
	ino, err := a.resolvePath(ctx, path)
	if err != nil {
		return err
	}

	if err := a.store.Truncate(ctx, ino, uint64(size)); err != nil {
		return err
	}
	return a.store.UpdateSize(ctx, ino, uint64(size))
}

// Utimens implements FileSystem.Utimens
func (a *AgentFS) Utimens(ctx context.Context, path string, atime, mtime *int64) error {
	ino, err := a.resolvePath(ctx, path)
	if err != nil {
		return err
	}

	return a.store.UpdateTimes(ctx, ino, atime, mtime)
}

// Symlink implements FileSystem.Symlink
func (a *AgentFS) Symlink(ctx context.Context, target, linkpath string) error {
	parentIno, name, err := a.resolveParentAndName(ctx, linkpath)
	if err != nil {
		return err
	}

	return a.store.WithTx(ctx, func(tx *sql.Tx) error {
		// Create symlink inode
		ino, err := a.store.CreateInodeTx(ctx, tx, db.S_IFLNK|0o777, 0, 0)
		if err != nil {
			return err
		}

		// Store symlink target
		if err := a.store.CreateSymlinkTx(ctx, tx, ino, target); err != nil {
			return err
		}

		// Update size to target length
		if err := a.store.UpdateSizeTx(ctx, tx, ino, uint64(len(target))); err != nil {
			return err
		}

		// Create directory entry
		return a.store.CreateDentryTx(ctx, tx, parentIno, name, ino)
	})
}

// Link implements FileSystem.Link
func (a *AgentFS) Link(ctx context.Context, oldpath, newpath string) error {
	// Get source inode
	srcIno, err := a.resolvePath(ctx, oldpath)
	if err != nil {
		return err
	}

	// Verify it's not a directory
	inode, err := a.store.GetInode(ctx, srcIno)
	if err != nil {
		return err
	}
	if inode.IsDir() {
		return ErrIsDir
	}

	// Get destination parent
	dstParentIno, dstName, err := a.resolveParentAndName(ctx, newpath)
	if err != nil {
		return err
	}

	return a.store.WithTx(ctx, func(tx *sql.Tx) error {
		// Create directory entry
		if err := a.store.CreateDentryTx(ctx, tx, dstParentIno, dstName, srcIno); err != nil {
			if err == db.ErrExists {
				return ErrExists
			}
			return err
		}

		// Increment link count
		return a.store.IncrNlinkTx(ctx, tx, srcIno)
	})
}

// Access implements FileSystem.Access
func (a *AgentFS) Access(ctx context.Context, path string, mode uint32) error {
	_, err := a.resolvePath(ctx, path)
	if err != nil {
		return err
	}
	// Just check existence for now
	return nil
}

// GetIno returns the inode number for a path (used by OverlayFS)
func (a *AgentFS) GetIno(ctx context.Context, path string) (uint64, error) {
	return a.resolvePath(ctx, path)
}

// Ensure AgentFS implements FileSystem
var _ FileSystem = (*AgentFS)(nil)

// AgentFile implements File for AgentFS
type AgentFile struct {
	store *db.Store
	ino   uint64
	path  string
}

// Read implements File.Read
func (f *AgentFile) Read(ctx context.Context, dest []byte, offset int64) (int, error) {
	data, err := f.store.ReadData(ctx, f.ino, offset, int64(len(dest)))
	if err != nil {
		return 0, err
	}
	return copy(dest, data), nil
}

// Write implements File.Write
func (f *AgentFile) Write(ctx context.Context, data []byte, offset int64) (int, error) {
	if err := f.store.WriteData(ctx, f.ino, offset, data); err != nil {
		return 0, err
	}

	// Update size if extended
	inode, err := f.store.GetInode(ctx, f.ino)
	if err != nil {
		return 0, err
	}

	newSize := uint64(offset) + uint64(len(data))
	if newSize > inode.Size {
		if err := f.store.UpdateSize(ctx, f.ino, newSize); err != nil {
			return 0, err
		}
	} else {
		// Just update mtime
		now := time.Now().Unix()
		if err := f.store.UpdateTimes(ctx, f.ino, nil, &now); err != nil {
			return 0, err
		}
	}

	return len(data), nil
}

// Sync implements File.Sync
func (f *AgentFile) Sync(ctx context.Context) error {
	return nil // SQLite handles durability
}

// Close implements File.Close
func (f *AgentFile) Close() error {
	return nil // Nothing to close
}

// Stat implements File.Stat
func (f *AgentFile) Stat(ctx context.Context) (*Stats, error) {
	inode, err := f.store.GetInode(ctx, f.ino)
	if err != nil {
		return nil, err
	}
	return inodeToStats(inode), nil
}

// Truncate implements File.Truncate
func (f *AgentFile) Truncate(ctx context.Context, size int64) error {
	if err := f.store.Truncate(ctx, f.ino, uint64(size)); err != nil {
		return err
	}
	return f.store.UpdateSize(ctx, f.ino, uint64(size))
}

// Ino returns the inode number (for OverlayFS)
func (f *AgentFile) Ino() uint64 {
	return f.ino
}

// inodeToStats converts db.Inode to overlay.Stats
func inodeToStats(inode *db.Inode) *Stats {
	return &Stats{
		Ino:   inode.Ino,
		Mode:  inode.Mode,
		Nlink: inode.Nlink,
		Uid:   inode.UID,
		Gid:   inode.GID,
		Size:  int64(inode.Size),
		Atime: inode.Atime,
		Mtime: inode.Mtime,
		Ctime: inode.Ctime,
	}
}

// EnsureParentDirs creates all parent directories for a path
func (a *AgentFS) EnsureParentDirs(ctx context.Context, path string) error {
	parts := splitPath(path)
	if len(parts) <= 1 {
		return nil // No parents to create
	}

	// Create each parent directory if it doesn't exist
	for i := 1; i < len(parts); i++ {
		parentPath := joinPath(parts[:i])
		_, err := a.resolvePath(ctx, parentPath)
		if err == ErrNotFound {
			// Create the directory
			if err := a.Mkdir(ctx, parentPath, 0o755); err != nil && err != ErrExists {
				return err
			}
		} else if err != nil {
			return err
		}
	}

	return nil
}

// WriteFile creates or overwrites a file with the given content
func (a *AgentFS) WriteFile(ctx context.Context, path string, data []byte, mode uint32) error {
	// Ensure parent directories exist
	if err := a.EnsureParentDirs(ctx, path); err != nil {
		return err
	}

	// Check if file exists
	ino, err := a.resolvePath(ctx, path)
	if err == ErrNotFound {
		// Create new file
		f, _, err := a.Create(ctx, path, mode)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = f.Write(ctx, data, 0)
		return err
	} else if err != nil {
		return err
	}

	// File exists - truncate and write
	if err := a.store.Truncate(ctx, ino, 0); err != nil {
		return err
	}
	if len(data) > 0 {
		if err := a.store.WriteData(ctx, ino, 0, data); err != nil {
			return err
		}
	}
	return a.store.UpdateSize(ctx, ino, uint64(len(data)))
}

// ReadFile reads the entire content of a file
func (a *AgentFS) ReadFile(ctx context.Context, path string) ([]byte, error) {
	ino, err := a.resolvePath(ctx, path)
	if err != nil {
		return nil, err
	}

	inode, err := a.store.GetInode(ctx, ino)
	if err != nil {
		return nil, err
	}

	return a.store.ReadData(ctx, ino, 0, int64(inode.Size))
}

// CopyFromBase copies a file from the base filesystem to AgentFS
func (a *AgentFS) CopyFromBase(ctx context.Context, path string, base FileSystem) (uint64, error) {
	// Get stats from base
	stats, err := base.Lstat(ctx, path)
	if err != nil {
		return 0, err
	}

	// Ensure parent directories exist
	if err := a.EnsureParentDirs(ctx, path); err != nil {
		return 0, err
	}

	parentIno, name, err := a.resolveParentAndName(ctx, path)
	if err != nil {
		return 0, err
	}

	var ino uint64
	if stats.IsRegular() {
		// Read content from base
		f, err := base.Open(ctx, path, O_RDONLY)
		if err != nil {
			return 0, err
		}
		data := make([]byte, stats.Size)
		_, err = f.Read(ctx, data, 0)
		f.Close()
		if err != nil {
			return 0, err
		}

		// Create file in delta
		err = a.store.WithTx(ctx, func(tx *sql.Tx) error {
			var err error
			ino, err = a.store.CreateInodeTx(ctx, tx, stats.Mode, stats.Uid, stats.Gid)
			if err != nil {
				return err
			}
			if len(data) > 0 {
				if err := a.store.WriteDataTx(ctx, tx, ino, 0, data); err != nil {
					return err
				}
			}
			if err := a.store.UpdateSizeTx(ctx, tx, ino, uint64(len(data))); err != nil {
				return err
			}
			return a.store.CreateDentryTx(ctx, tx, parentIno, name, ino)
		})
	} else if stats.IsSymlink() {
		// Copy symlink
		target, err := base.Readlink(ctx, path)
		if err != nil {
			return 0, err
		}

		err = a.store.WithTx(ctx, func(tx *sql.Tx) error {
			var err error
			ino, err = a.store.CreateInodeTx(ctx, tx, stats.Mode, stats.Uid, stats.Gid)
			if err != nil {
				return err
			}
			if err := a.store.CreateSymlinkTx(ctx, tx, ino, target); err != nil {
				return err
			}
			if err := a.store.UpdateSizeTx(ctx, tx, ino, uint64(len(target))); err != nil {
				return err
			}
			return a.store.CreateDentryTx(ctx, tx, parentIno, name, ino)
		})
	} else if stats.IsDir() {
		// Create directory
		err = a.store.WithTx(ctx, func(tx *sql.Tx) error {
			var err error
			ino, err = a.store.CreateInodeTx(ctx, tx, stats.Mode, stats.Uid, stats.Gid)
			if err != nil {
				return err
			}
			if err := a.store.CreateDentryTx(ctx, tx, parentIno, name, ino); err != nil {
				return err
			}
			return a.store.IncrNlinkTx(ctx, tx, parentIno)
		})
	}

	return ino, err
}
