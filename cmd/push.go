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
	Long:  `Pushes all files from workspace directory into the SQLite database.`,
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
	info, err := os.Stat(inputDir)
	if err != nil {
		return fmt.Errorf("cannot access input directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", inputDir)
	}

	// Open database
	store, err := db.Open(db.DefaultConfig(dbPath))
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Walk the input directory and import everything
	return filepath.WalkDir(inputDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Get relative path from input directory
		relPath, err := filepath.Rel(inputDir, path)
		if err != nil {
			return err
		}

		// Skip the root directory itself
		if relPath == "." {
			return nil
		}

		// Get parent path and name
		parentPath := filepath.Dir(relPath)
		name := filepath.Base(relPath)

		// Resolve parent inode
		parentIno := uint64(1) // root
		if parentPath != "." {
			parentIno, err = resolvePath(ctx, store, parentPath)
			if err != nil {
				return fmt.Errorf("cannot resolve parent %s: %w", parentPath, err)
			}
		}

		// Check if entry already exists
		existingIno, err := store.Lookup(ctx, parentIno, name)
		if err == nil {
			fmt.Printf("SKIP %s (exists as ino %d)\n", relPath, existingIno)
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
			fmt.Printf("DIR  %s (ino %d)\n", relPath, ino)

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
			fmt.Printf("LINK %s -> %s (ino %d)\n", relPath, target, ino)

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
			fmt.Printf("FILE %s (%d bytes, ino %d)\n", relPath, len(data), ino)
		}

		return nil
	})
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
