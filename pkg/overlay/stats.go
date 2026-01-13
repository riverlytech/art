package overlay

import (
	"syscall"
)

// fillUnixStats extracts Unix-specific fields from the sys interface (Linux version)
func fillUnixStats(stats *Stats, sys interface{}) {
	if stat, ok := sys.(*syscall.Stat_t); ok {
		stats.Ino = stat.Ino
		stats.Nlink = uint32(stat.Nlink)
		stats.Uid = stat.Uid
		stats.Gid = stat.Gid
		stats.Atime = stat.Atim.Sec
		stats.Mtime = stat.Mtim.Sec
		stats.Ctime = stat.Ctim.Sec
	}
}
