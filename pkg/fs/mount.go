package fs

import (
	"fmt"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"art/pkg/db"
	"art/pkg/overlay"
)

// Mounter manages the FUSE filesystem lifecycle
type Mounter struct {
	server *fuse.Server
	store  *db.Store
	path   string
}

// Mount creates and mounts the FUSE filesystem
func Mount(path string, store *db.Store) (*Mounter, error) {
	// Create root node (inode 1)
	root := &Node{
		ino:   1,
		store: store,
	}

	// Mount options
	timeout := time.Second
	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther: false,
			Debug:      false,
			FsName:     "artfs",
			Name:       "artfs",
		},
		AttrTimeout:  &timeout,
		EntryTimeout: &timeout,
		UID:          uint32(0),
		GID:          uint32(0),
	}

	server, err := fs.Mount(path, root, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to mount FUSE: %w", err)
	}

	return &Mounter{
		server: server,
		store:  store,
		path:   path,
	}, nil
}

// Unmount cleanly unmounts the filesystem
func (m *Mounter) Unmount() error {
	return m.server.Unmount()
}

// Wait blocks until the filesystem is unmounted
func (m *Mounter) Wait() {
	m.server.Wait()
}

// Path returns the mount path
func (m *Mounter) Path() string {
	return m.path
}

// Serve starts serving FUSE requests in the background
func (m *Mounter) Serve() {
	go m.server.Serve()
}

// OverlayMounter manages the overlay FUSE filesystem lifecycle
type OverlayMounter struct {
	server *fuse.Server
	path   string
}

// MountOverlay creates and mounts an overlay FUSE filesystem
func MountOverlay(mountPath string, fsys overlay.FileSystem) (*OverlayMounter, error) {
	// Create root node for overlay
	root := &OverlayNode{
		path: "/",
		fsys: fsys,
	}

	// Mount options
	timeout := time.Second
	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther: false,
			Debug:      false,
			FsName:     "artfs-overlay",
			Name:       "artfs",
		},
		AttrTimeout:  &timeout,
		EntryTimeout: &timeout,
		UID:          uint32(0),
		GID:          uint32(0),
	}

	server, err := fs.Mount(mountPath, root, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to mount overlay FUSE: %w", err)
	}

	return &OverlayMounter{
		server: server,
		path:   mountPath,
	}, nil
}

// Unmount cleanly unmounts the overlay filesystem
func (m *OverlayMounter) Unmount() error {
	return m.server.Unmount()
}

// Wait blocks until the overlay filesystem is unmounted
func (m *OverlayMounter) Wait() {
	m.server.Wait()
}

// Path returns the mount path
func (m *OverlayMounter) Path() string {
	return m.path
}

// Serve starts serving FUSE requests in the background
func (m *OverlayMounter) Serve() {
	go m.server.Serve()
}
