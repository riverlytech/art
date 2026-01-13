package overlay

import (
	"context"
	"sync"
)

// OverlayFS implements a copy-on-write overlay filesystem.
// Reads come from the base layer (HostFS) and writes go to the delta layer (AgentFS).
// Deleted files from the base are tracked as "whiteouts" in the delta.
type OverlayFS struct {
	base     FileSystem      // Read-only base layer (HostFS)
	delta    *AgentFS        // Writable delta layer (SQLite)
	whiteout *WhiteoutCache  // In-memory cache of deleted paths
	mu       sync.RWMutex
}

// NewOverlayFS creates a new overlay filesystem
func NewOverlayFS(base FileSystem, delta *AgentFS) (*OverlayFS, error) {
	o := &OverlayFS{
		base:     base,
		delta:    delta,
		whiteout: NewWhiteoutCache(),
	}

	// Load existing whiteouts from database
	ctx := context.Background()
	paths, err := delta.Store().ListWhiteouts(ctx)
	if err != nil {
		return nil, err
	}
	o.whiteout.LoadFromPaths(paths)

	return o, nil
}

// Base returns the base filesystem
func (o *OverlayFS) Base() FileSystem {
	return o.base
}

// Delta returns the delta filesystem
func (o *OverlayFS) Delta() *AgentFS {
	return o.delta
}

// existsInDelta checks if a path exists in the delta layer
func (o *OverlayFS) existsInDelta(ctx context.Context, path string) bool {
	_, err := o.delta.Lstat(ctx, path)
	return err == nil
}

// existsInBase checks if a path exists in the base layer
func (o *OverlayFS) existsInBase(ctx context.Context, path string) bool {
	_, err := o.base.Lstat(ctx, path)
	return err == nil
}

// Stat implements FileSystem.Stat (follows symlinks)
func (o *OverlayFS) Stat(ctx context.Context, path string) (*Stats, error) {
	// Check if whited out
	if o.whiteout.HasWhiteoutAncestor(path) {
		return nil, ErrNotFound
	}

	// Try delta first
	if stats, err := o.delta.Stat(ctx, path); err == nil {
		// Check for origin mapping to return original inode
		return o.applyOrigin(ctx, stats)
	}

	// Fall back to base
	return o.base.Stat(ctx, path)
}

// Lstat implements FileSystem.Lstat (does not follow symlinks)
func (o *OverlayFS) Lstat(ctx context.Context, path string) (*Stats, error) {
	// Check if whited out
	if o.whiteout.HasWhiteoutAncestor(path) {
		return nil, ErrNotFound
	}

	// Try delta first
	if stats, err := o.delta.Lstat(ctx, path); err == nil {
		return o.applyOrigin(ctx, stats)
	}

	// Fall back to base
	return o.base.Lstat(ctx, path)
}

// applyOrigin checks for origin mapping and returns the original inode
func (o *OverlayFS) applyOrigin(ctx context.Context, stats *Stats) (*Stats, error) {
	baseIno, err := o.delta.Store().GetOrigin(ctx, stats.Ino)
	if err != nil {
		return nil, err
	}
	if baseIno != 0 {
		stats.Ino = baseIno
	}
	return stats, nil
}

// Readlink implements FileSystem.Readlink
func (o *OverlayFS) Readlink(ctx context.Context, path string) (string, error) {
	if o.whiteout.HasWhiteoutAncestor(path) {
		return "", ErrNotFound
	}

	// Try delta first
	if target, err := o.delta.Readlink(ctx, path); err == nil {
		return target, nil
	}

	return o.base.Readlink(ctx, path)
}

// Statfs implements FileSystem.Statfs
func (o *OverlayFS) Statfs(ctx context.Context) (*FilesystemStats, error) {
	// Return delta's stats (virtual)
	return o.delta.Statfs(ctx)
}

// Readdir implements FileSystem.Readdir - merges entries from both layers
func (o *OverlayFS) Readdir(ctx context.Context, path string) ([]DirEntry, error) {
	if o.whiteout.HasWhiteoutAncestor(path) {
		return nil, ErrNotFound
	}

	// Get whiteouts for this directory
	whiteouts := make(map[string]bool)
	for _, name := range o.whiteout.GetChildWhiteouts(path) {
		whiteouts[name] = true
	}

	// Collect entries from delta
	deltaEntries := make(map[string]DirEntry)
	if entries, err := o.delta.Readdir(ctx, path); err == nil {
		for _, e := range entries {
			deltaEntries[e.Name] = e
		}
	}

	// Collect entries from base (if not whited out or overridden)
	if entries, err := o.base.Readdir(ctx, path); err == nil {
		for _, e := range entries {
			// Skip if whited out
			if whiteouts[e.Name] {
				continue
			}
			// Skip if already in delta (delta overrides base)
			if _, exists := deltaEntries[e.Name]; exists {
				continue
			}
			deltaEntries[e.Name] = e
		}
	}

	// Convert map to slice
	result := make([]DirEntry, 0, len(deltaEntries))
	for _, e := range deltaEntries {
		result = append(result, e)
	}

	return result, nil
}

// Mkdir implements FileSystem.Mkdir
func (o *OverlayFS) Mkdir(ctx context.Context, path string, mode uint32) error {
	// Check if ancestor is whited out (can't create under deleted dir)
	parts := splitPath(path)
	if len(parts) > 1 {
		parentPath := joinPath(parts[:len(parts)-1])
		if o.whiteout.HasWhiteoutAncestor(parentPath) {
			return ErrNotFound
		}
	}

	// Remove whiteout if this path was deleted
	if o.whiteout.HasExactWhiteout(path) {
		if err := o.delta.Store().DeleteWhiteout(ctx, path); err != nil {
			return err
		}
		o.whiteout.Remove(path)
	}

	// Ensure parent directories exist in delta
	if err := o.ensureParentDirs(ctx, path); err != nil {
		return err
	}

	return o.delta.Mkdir(ctx, path, mode)
}

// ensureParentDirs ensures all parent directories exist in the delta layer
func (o *OverlayFS) ensureParentDirs(ctx context.Context, path string) error {
	parts := splitPath(path)
	if len(parts) <= 1 {
		return nil
	}

	for i := 1; i < len(parts); i++ {
		parentPath := joinPath(parts[:i])

		// Skip if already in delta
		if o.existsInDelta(ctx, parentPath) {
			continue
		}

		// Check if parent exists in base
		baseStats, baseErr := o.base.Lstat(ctx, parentPath)
		if baseErr == nil && baseStats.IsDir() {
			// Create matching directory in delta
			if err := o.delta.Mkdir(ctx, parentPath, baseStats.Perm()); err != nil && err != ErrExists {
				return err
			}
		} else {
			// Create directory in delta
			if err := o.delta.Mkdir(ctx, parentPath, 0o755); err != nil && err != ErrExists {
				return err
			}
		}
	}

	return nil
}

// Rmdir implements FileSystem.Rmdir
func (o *OverlayFS) Rmdir(ctx context.Context, path string) error {
	if o.whiteout.HasWhiteoutAncestor(path) {
		return ErrNotFound
	}

	inDelta := o.existsInDelta(ctx, path)
	inBase := o.existsInBase(ctx, path)

	if !inDelta && !inBase {
		return ErrNotFound
	}

	// If in delta, remove from delta
	if inDelta {
		if err := o.delta.Rmdir(ctx, path); err != nil {
			return err
		}
	}

	// If in base, create whiteout
	if inBase {
		if err := o.delta.Store().CreateWhiteout(ctx, path); err != nil {
			return err
		}
		o.whiteout.Insert(path)
	}

	return nil
}

// Create implements FileSystem.Create
func (o *OverlayFS) Create(ctx context.Context, path string, mode uint32) (File, *Stats, error) {
	// Check ancestors
	parts := splitPath(path)
	if len(parts) > 1 {
		parentPath := joinPath(parts[:len(parts)-1])
		if o.whiteout.HasWhiteoutAncestor(parentPath) {
			return nil, nil, ErrNotFound
		}
	}

	// Remove whiteout if recreating a deleted file
	if o.whiteout.HasExactWhiteout(path) {
		if err := o.delta.Store().DeleteWhiteout(ctx, path); err != nil {
			return nil, nil, err
		}
		o.whiteout.Remove(path)
	}

	// Ensure parent directories exist
	if err := o.ensureParentDirs(ctx, path); err != nil {
		return nil, nil, err
	}

	f, stats, err := o.delta.Create(ctx, path, mode)
	if err != nil {
		return nil, nil, err
	}

	return &OverlayFile{
		overlay: o,
		path:    path,
		delta:   f,
	}, stats, nil
}

// Open implements FileSystem.Open
func (o *OverlayFS) Open(ctx context.Context, path string, flags int) (File, error) {
	if o.whiteout.HasWhiteoutAncestor(path) {
		return nil, ErrNotFound
	}

	inDelta := o.existsInDelta(ctx, path)
	inBase := o.existsInBase(ctx, path)

	if !inDelta && !inBase {
		return nil, ErrNotFound
	}

	// Determine if write access is needed
	writeNeeded := flags&(O_WRONLY|O_RDWR|O_TRUNC|O_APPEND) != 0

	// If write needed and only in base, do copy-on-write
	if writeNeeded && !inDelta && inBase {
		if err := o.copyOnWrite(ctx, path); err != nil {
			return nil, err
		}
		inDelta = true
	}

	// Open from appropriate layer
	if inDelta {
		f, err := o.delta.Open(ctx, path, flags)
		if err != nil {
			return nil, err
		}
		return &OverlayFile{
			overlay: o,
			path:    path,
			delta:   f,
		}, nil
	}

	// Read-only from base
	f, err := o.base.Open(ctx, path, O_RDONLY)
	if err != nil {
		return nil, err
	}
	return &OverlayFile{
		overlay: o,
		path:    path,
		base:    f,
	}, nil
}

// copyOnWrite copies a file from base to delta
func (o *OverlayFS) copyOnWrite(ctx context.Context, path string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Double-check it's not already copied
	if o.existsInDelta(ctx, path) {
		return nil
	}

	// Get base stats for origin mapping
	baseStats, err := o.base.Lstat(ctx, path)
	if err != nil {
		return err
	}

	// Copy file to delta
	deltaIno, err := o.delta.CopyFromBase(ctx, path, o.base)
	if err != nil {
		return err
	}

	// Store origin mapping
	return o.delta.Store().AddOrigin(ctx, deltaIno, baseStats.Ino)
}

// Remove implements FileSystem.Remove
func (o *OverlayFS) Remove(ctx context.Context, path string) error {
	if o.whiteout.HasWhiteoutAncestor(path) {
		return ErrNotFound
	}

	inDelta := o.existsInDelta(ctx, path)
	inBase := o.existsInBase(ctx, path)

	if !inDelta && !inBase {
		return ErrNotFound
	}

	// If in delta, remove from delta
	if inDelta {
		// Also delete origin mapping if exists
		if ino, err := o.delta.GetIno(ctx, path); err == nil {
			_ = o.delta.Store().DeleteOrigin(ctx, ino)
		}
		if err := o.delta.Remove(ctx, path); err != nil {
			return err
		}
	}

	// If in base, create whiteout
	if inBase {
		if err := o.delta.Store().CreateWhiteout(ctx, path); err != nil {
			return err
		}
		o.whiteout.Insert(path)
	}

	return nil
}

// Rename implements FileSystem.Rename
func (o *OverlayFS) Rename(ctx context.Context, oldpath, newpath string) error {
	if o.whiteout.HasWhiteoutAncestor(oldpath) {
		return ErrNotFound
	}

	inDelta := o.existsInDelta(ctx, oldpath)
	inBase := o.existsInBase(ctx, oldpath)

	if !inDelta && !inBase {
		return ErrNotFound
	}

	// If only in base, copy to delta first
	if !inDelta && inBase {
		if err := o.copyOnWrite(ctx, oldpath); err != nil {
			return err
		}
	}

	// Ensure parent directories for destination
	if err := o.ensureParentDirs(ctx, newpath); err != nil {
		return err
	}

	// Remove whiteout at destination if exists
	if o.whiteout.HasExactWhiteout(newpath) {
		if err := o.delta.Store().DeleteWhiteout(ctx, newpath); err != nil {
			return err
		}
		o.whiteout.Remove(newpath)
	}

	// Rename in delta
	if err := o.delta.Rename(ctx, oldpath, newpath); err != nil {
		return err
	}

	// If source was in base, create whiteout
	if inBase {
		if err := o.delta.Store().CreateWhiteout(ctx, oldpath); err != nil {
			return err
		}
		o.whiteout.Insert(oldpath)
	}

	return nil
}

// Chmod implements FileSystem.Chmod
func (o *OverlayFS) Chmod(ctx context.Context, path string, mode uint32) error {
	if o.whiteout.HasWhiteoutAncestor(path) {
		return ErrNotFound
	}

	// If only in base, copy first
	if !o.existsInDelta(ctx, path) {
		if !o.existsInBase(ctx, path) {
			return ErrNotFound
		}
		if err := o.copyOnWrite(ctx, path); err != nil {
			return err
		}
	}

	return o.delta.Chmod(ctx, path, mode)
}

// Chown implements FileSystem.Chown
func (o *OverlayFS) Chown(ctx context.Context, path string, uid, gid uint32) error {
	if o.whiteout.HasWhiteoutAncestor(path) {
		return ErrNotFound
	}

	if !o.existsInDelta(ctx, path) {
		if !o.existsInBase(ctx, path) {
			return ErrNotFound
		}
		if err := o.copyOnWrite(ctx, path); err != nil {
			return err
		}
	}

	return o.delta.Chown(ctx, path, uid, gid)
}

// Truncate implements FileSystem.Truncate
func (o *OverlayFS) Truncate(ctx context.Context, path string, size int64) error {
	if o.whiteout.HasWhiteoutAncestor(path) {
		return ErrNotFound
	}

	if !o.existsInDelta(ctx, path) {
		if !o.existsInBase(ctx, path) {
			return ErrNotFound
		}
		if err := o.copyOnWrite(ctx, path); err != nil {
			return err
		}
	}

	return o.delta.Truncate(ctx, path, size)
}

// Utimens implements FileSystem.Utimens
func (o *OverlayFS) Utimens(ctx context.Context, path string, atime, mtime *int64) error {
	if o.whiteout.HasWhiteoutAncestor(path) {
		return ErrNotFound
	}

	if !o.existsInDelta(ctx, path) {
		if !o.existsInBase(ctx, path) {
			return ErrNotFound
		}
		if err := o.copyOnWrite(ctx, path); err != nil {
			return err
		}
	}

	return o.delta.Utimens(ctx, path, atime, mtime)
}

// Symlink implements FileSystem.Symlink
func (o *OverlayFS) Symlink(ctx context.Context, target, linkpath string) error {
	// Check ancestors
	parts := splitPath(linkpath)
	if len(parts) > 1 {
		parentPath := joinPath(parts[:len(parts)-1])
		if o.whiteout.HasWhiteoutAncestor(parentPath) {
			return ErrNotFound
		}
	}

	// Remove whiteout if recreating
	if o.whiteout.HasExactWhiteout(linkpath) {
		if err := o.delta.Store().DeleteWhiteout(ctx, linkpath); err != nil {
			return err
		}
		o.whiteout.Remove(linkpath)
	}

	if err := o.ensureParentDirs(ctx, linkpath); err != nil {
		return err
	}

	return o.delta.Symlink(ctx, target, linkpath)
}

// Link implements FileSystem.Link
func (o *OverlayFS) Link(ctx context.Context, oldpath, newpath string) error {
	if o.whiteout.HasWhiteoutAncestor(oldpath) {
		return ErrNotFound
	}

	// If only in base, copy first
	if !o.existsInDelta(ctx, oldpath) {
		if !o.existsInBase(ctx, oldpath) {
			return ErrNotFound
		}
		if err := o.copyOnWrite(ctx, oldpath); err != nil {
			return err
		}
	}

	// Remove whiteout at destination
	if o.whiteout.HasExactWhiteout(newpath) {
		if err := o.delta.Store().DeleteWhiteout(ctx, newpath); err != nil {
			return err
		}
		o.whiteout.Remove(newpath)
	}

	if err := o.ensureParentDirs(ctx, newpath); err != nil {
		return err
	}

	return o.delta.Link(ctx, oldpath, newpath)
}

// Access implements FileSystem.Access
func (o *OverlayFS) Access(ctx context.Context, path string, mode uint32) error {
	if o.whiteout.HasWhiteoutAncestor(path) {
		return ErrNotFound
	}

	if err := o.delta.Access(ctx, path, mode); err == nil {
		return nil
	}

	return o.base.Access(ctx, path, mode)
}

// Ensure OverlayFS implements FileSystem
var _ FileSystem = (*OverlayFS)(nil)

// OverlayFile implements File for OverlayFS with copy-on-write support
type OverlayFile struct {
	overlay     *OverlayFS
	path        string
	base        File // Open file handle from base (read-only)
	delta       File // Open file handle from delta (read-write)
	copiedToMu  sync.Mutex
	copiedToDelta bool
}

// Read implements File.Read
func (f *OverlayFile) Read(ctx context.Context, dest []byte, offset int64) (int, error) {
	if f.delta != nil {
		return f.delta.Read(ctx, dest, offset)
	}
	if f.base != nil {
		return f.base.Read(ctx, dest, offset)
	}
	return 0, ErrNotFound
}

// Write implements File.Write with copy-on-write
func (f *OverlayFile) Write(ctx context.Context, data []byte, offset int64) (int, error) {
	// Ensure file is in delta
	if f.delta == nil {
		if err := f.ensureDelta(ctx); err != nil {
			return 0, err
		}
	}

	return f.delta.Write(ctx, data, offset)
}

// ensureDelta copies the file to delta if needed
func (f *OverlayFile) ensureDelta(ctx context.Context) error {
	f.copiedToMu.Lock()
	defer f.copiedToMu.Unlock()

	if f.copiedToDelta {
		return nil
	}

	// Do copy-on-write
	if err := f.overlay.copyOnWrite(ctx, f.path); err != nil {
		return err
	}

	// Open delta file
	deltaFile, err := f.overlay.delta.Open(ctx, f.path, O_RDWR)
	if err != nil {
		return err
	}

	// Close base file if open
	if f.base != nil {
		f.base.Close()
		f.base = nil
	}

	f.delta = deltaFile
	f.copiedToDelta = true
	return nil
}

// Sync implements File.Sync
func (f *OverlayFile) Sync(ctx context.Context) error {
	if f.delta != nil {
		return f.delta.Sync(ctx)
	}
	if f.base != nil {
		return f.base.Sync(ctx)
	}
	return nil
}

// Close implements File.Close
func (f *OverlayFile) Close() error {
	var err error
	if f.delta != nil {
		err = f.delta.Close()
	}
	if f.base != nil {
		if baseErr := f.base.Close(); err == nil {
			err = baseErr
		}
	}
	return err
}

// Stat implements File.Stat
func (f *OverlayFile) Stat(ctx context.Context) (*Stats, error) {
	if f.delta != nil {
		stats, err := f.delta.Stat(ctx)
		if err != nil {
			return nil, err
		}
		return f.overlay.applyOrigin(ctx, stats)
	}
	if f.base != nil {
		return f.base.Stat(ctx)
	}
	return nil, ErrNotFound
}

// Truncate implements File.Truncate
func (f *OverlayFile) Truncate(ctx context.Context, size int64) error {
	// Ensure in delta
	if f.delta == nil {
		if err := f.ensureDelta(ctx); err != nil {
			return err
		}
	}

	return f.delta.Truncate(ctx, size)
}
