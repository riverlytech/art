package cmd

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"art/pkg/db"

	"github.com/spf13/cobra"
)

var pushCmd = &cobra.Command{
	Use:   "push",
	Short: "Push workspace files into SQLite database",
	Long: `Pushes all files from workspace directory into the SQLite database.
Files are stored under /<workspace-name>/ in the virtual filesystem,
where <workspace-name> is the basename of the mount directory.`,
	Run: func(cmd *cobra.Command, args []string) {
		if dbPath == "" {
			fmt.Println("Error: --db flag is required")
			os.Exit(1)
		}
		if mountDir == "" || mountDir == "." {
			fmt.Println("Error: --mount flag is required")
			os.Exit(1)
		}
		if err := runPush(dbPath, mountDir); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

func init() {
	RootCmd.AddCommand(pushCmd)
}

func runPush(dbPath, inputDir string) error {
	// Check input directory exists
	absInputDir, err := filepath.Abs(inputDir)
	if err != nil {
		return fmt.Errorf("cannot resolve input directory: %w", err)
	}

	info, err := os.Stat(absInputDir)
	if err != nil {
		return fmt.Errorf("cannot access input directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", absInputDir)
	}

	// Extract workspace name from input directory
	workspaceName := filepath.Base(absInputDir)
	fmt.Printf("Workspace name: %s\n", workspaceName)
	fmt.Printf("Files will be stored under: /%s/\n", workspaceName)

	// Open database
	store, err := db.Open(db.DefaultConfig(dbPath))
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer store.Close()

	ctx := context.Background()

	// First, ensure the workspace directory exists in the DB
	workspaceIno, err := ensureWorkspaceDir(ctx, store, workspaceName)
	if err != nil {
		return fmt.Errorf("failed to create workspace directory: %w", err)
	}
	fmt.Printf("Workspace directory inode: %d\n", workspaceIno)

	// Walk the input directory and import everything
	return filepath.WalkDir(absInputDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Get relative path from input directory
		relPath, err := filepath.Rel(absInputDir, path)
		if err != nil {
			return err
		}

		// Skip the root directory itself (it's the workspace dir)
		if relPath == "." {
			return nil
		}

		// Virtual path is /<workspace>/<relPath>
		virtualPath := "/" + workspaceName + "/" + filepath.ToSlash(relPath)

		// Get parent path and name
		parentPath := filepath.Dir(relPath)
		name := filepath.Base(relPath)

		// Resolve parent inode (relative to workspace)
		var parentIno uint64
		if parentPath == "." {
			parentIno = workspaceIno
		} else {
			parentIno, err = resolvePath(ctx, store, "/"+workspaceName+"/"+filepath.ToSlash(parentPath))
			if err != nil {
				return fmt.Errorf("cannot resolve parent %s: %w", parentPath, err)
			}
		}

		// Check if entry already exists
		existingIno, err := store.Lookup(ctx, parentIno, name)
		if err == nil {
			fmt.Printf("SKIP %s (exists as ino %d)\n", virtualPath, existingIno)
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		mode := uint32(info.Mode().Perm())

		if d.IsDir() {
			// Create directory
			ino, err := store.CreateInode(ctx, db.S_IFDIR|mode, 0, 0)
			if err != nil {
				return fmt.Errorf("failed to create directory inode: %w", err)
			}
			if err := store.CreateDentry(ctx, parentIno, name, ino); err != nil {
				return fmt.Errorf("failed to create directory dentry: %w", err)
			}
			fmt.Printf("DIR  %s (ino %d)\n", virtualPath, ino)

		} else if d.Type()&fs.ModeSymlink != 0 {
			// Create symlink
			target, err := os.Readlink(path)
			if err != nil {
				return fmt.Errorf("failed to read symlink: %w", err)
			}
			ino, err := store.CreateInode(ctx, db.S_IFLNK|0777, 0, 0)
			if err != nil {
				return fmt.Errorf("failed to create symlink inode: %w", err)
			}
			if err := store.CreateSymlink(ctx, ino, target); err != nil {
				return fmt.Errorf("failed to store symlink target: %w", err)
			}
			if err := store.CreateDentry(ctx, parentIno, name, ino); err != nil {
				return fmt.Errorf("failed to create symlink dentry: %w", err)
			}
			if err := store.UpdateSize(ctx, ino, uint64(len(target))); err != nil {
				return fmt.Errorf("failed to update symlink size: %w", err)
			}
			fmt.Printf("LINK %s -> %s (ino %d)\n", virtualPath, target, ino)

		} else if d.Type().IsRegular() {
			// Create regular file
			data, err := os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("failed to read file: %w", err)
			}
			ino, err := store.CreateInode(ctx, db.S_IFREG|mode, 0, 0)
			if err != nil {
				return fmt.Errorf("failed to create file inode: %w", err)
			}
			if len(data) > 0 {
				if err := store.WriteData(ctx, ino, 0, data); err != nil {
					return fmt.Errorf("failed to write file data: %w", err)
				}
			}
			if err := store.UpdateSize(ctx, ino, uint64(len(data))); err != nil {
				return fmt.Errorf("failed to update file size: %w", err)
			}
			if err := store.CreateDentry(ctx, parentIno, name, ino); err != nil {
				return fmt.Errorf("failed to create file dentry: %w", err)
			}
			fmt.Printf("FILE %s (%d bytes, ino %d)\n", virtualPath, len(data), ino)
		}

		return nil
	})
}

// ensureWorkspaceDir ensures the workspace directory exists in the database
// and returns its inode number
func ensureWorkspaceDir(ctx context.Context, store *db.Store, workspaceName string) (uint64, error) {
	// Try to look up existing workspace directory
	ino, err := store.Lookup(ctx, 1, workspaceName)
	if err == nil {
		return ino, nil
	}
	if err != db.ErrNotFound {
		return 0, err
	}

	// Create workspace directory under root
	ino, err = store.CreateInode(ctx, db.S_IFDIR|0755, 0, 0)
	if err != nil {
		return 0, err
	}
	if err := store.CreateDentry(ctx, 1, workspaceName, ino); err != nil {
		return 0, err
	}

	return ino, nil
}

// resolvePath resolves a path to an inode number
func resolvePath(ctx context.Context, store *db.Store, path string) (uint64, error) {
	parts := filepath.SplitList(path)
	if len(parts) == 0 {
		parts = []string{path}
	}
	// Split by separator
	parts = splitPath(path)

	ino := uint64(1) // start at root
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		childIno, err := store.Lookup(ctx, ino, part)
		if err != nil {
			return 0, err
		}
		ino = childIno
	}
	return ino, nil
}

func splitPath(path string) []string {
	var parts []string
	for _, p := range filepath.SplitList(path) {
		parts = append(parts, p)
	}
	if len(parts) == 0 || (len(parts) == 1 && parts[0] == path) {
		// filepath.SplitList didn't work, split manually
		parts = nil
		for path != "" {
			dir, file := filepath.Split(path)
			if file != "" {
				parts = append([]string{file}, parts...)
			}
			if dir == "" || dir == path {
				break
			}
			path = filepath.Clean(dir)
		}
	}
	return parts
}
