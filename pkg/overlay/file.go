package overlay

import (
	"context"
	"io"
	"os"
	"sync"
	"time"
)

// unixToTime converts a Unix timestamp to time.Time
func unixToTime(t int64) time.Time {
	return time.Unix(t, 0)
}

// timeToUnix converts a time.Time to Unix timestamp
func timeToUnix(t time.Time) int64 {
	return t.Unix()
}

// OSFile wraps an os.File to implement the File interface
type OSFile struct {
	f    *os.File
	path string
	mu   sync.Mutex
}

// NewOSFile creates a new OSFile wrapper
func NewOSFile(f *os.File, path string) *OSFile {
	return &OSFile{f: f, path: path}
}

// Read implements File.Read with pread semantics (reads at offset without changing position)
func (f *OSFile) Read(ctx context.Context, dest []byte, offset int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.f.ReadAt(dest, offset)
}

// Write implements File.Write with pwrite semantics (writes at offset without changing position)
func (f *OSFile) Write(ctx context.Context, data []byte, offset int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.f.WriteAt(data, offset)
}

// Sync implements File.Sync
func (f *OSFile) Sync(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.f.Sync()
}

// Close implements File.Close
func (f *OSFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.f.Close()
}

// Stat implements File.Stat
func (f *OSFile) Stat(ctx context.Context) (*Stats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	info, err := f.f.Stat()
	if err != nil {
		return nil, err
	}
	return fileInfoToStats(info), nil
}

// Truncate implements File.Truncate
func (f *OSFile) Truncate(ctx context.Context, size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.f.Truncate(size)
}

// fileInfoToStats converts os.FileInfo to overlay.Stats
func fileInfoToStats(info os.FileInfo) *Stats {
	mode := uint32(info.Mode().Perm())
	switch {
	case info.IsDir():
		mode |= S_IFDIR
	case info.Mode()&os.ModeSymlink != 0:
		mode |= S_IFLNK
	case info.Mode()&os.ModeDevice != 0:
		if info.Mode()&os.ModeCharDevice != 0 {
			mode |= S_IFCHR
		} else {
			mode |= S_IFBLK
		}
	case info.Mode()&os.ModeNamedPipe != 0:
		mode |= S_IFIFO
	case info.Mode()&os.ModeSocket != 0:
		mode |= S_IFSOCK
	default:
		mode |= S_IFREG
	}

	stats := &Stats{
		Mode:  mode,
		Size:  info.Size(),
		Mtime: info.ModTime().Unix(),
		Atime: info.ModTime().Unix(), // Use mtime as atime fallback
		Ctime: info.ModTime().Unix(), // Use mtime as ctime fallback
		Nlink: 1,
	}

	// Try to get inode and other Unix-specific info
	if sys := info.Sys(); sys != nil {
		fillUnixStats(stats, sys)
	}

	return stats
}

// MemFile is an in-memory file implementation for testing
type MemFile struct {
	data   []byte
	stats  Stats
	mu     sync.RWMutex
	closed bool
}

// NewMemFile creates a new in-memory file
func NewMemFile(data []byte, stats Stats) *MemFile {
	return &MemFile{
		data:  data,
		stats: stats,
	}
}

// Read implements File.Read
func (f *MemFile) Read(ctx context.Context, dest []byte, offset int64) (int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	if offset >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(dest, f.data[offset:])
	return n, nil
}

// Write implements File.Write
func (f *MemFile) Write(ctx context.Context, data []byte, offset int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	// Extend file if needed
	end := int(offset) + len(data)
	if end > len(f.data) {
		newData := make([]byte, end)
		copy(newData, f.data)
		f.data = newData
	}
	n := copy(f.data[offset:], data)
	f.stats.Size = int64(len(f.data))
	return n, nil
}

// Sync implements File.Sync
func (f *MemFile) Sync(ctx context.Context) error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.closed {
		return os.ErrClosed
	}
	return nil
}

// Close implements File.Close
func (f *MemFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

// Stat implements File.Stat
func (f *MemFile) Stat(ctx context.Context) (*Stats, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.closed {
		return nil, os.ErrClosed
	}
	stats := f.stats
	stats.Size = int64(len(f.data))
	return &stats, nil
}

// Truncate implements File.Truncate
func (f *MemFile) Truncate(ctx context.Context, size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return os.ErrClosed
	}
	if size < 0 {
		return ErrInvalid
	}
	if size > int64(len(f.data)) {
		// Extend with zeros
		newData := make([]byte, size)
		copy(newData, f.data)
		f.data = newData
	} else {
		f.data = f.data[:size]
	}
	f.stats.Size = size
	return nil
}

// Data returns a copy of the file's data (for testing)
func (f *MemFile) Data() []byte {
	f.mu.RLock()
	defer f.mu.RUnlock()
	data := make([]byte, len(f.data))
	copy(data, f.data)
	return data
}
