package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"art/pkg/db"

	"github.com/spf13/cobra"
)

var pullCmd = &cobra.Command{
	Use:   "pull",
	Short: "Pull files from SQLite database to workspace",
	Long:  `Pulls all files from the SQLite database to the workspace directory.`,
	Run: func(cmd *cobra.Command, args []string) {
		if dbPath == "" {
			fmt.Println("Error: --db flag is required")
			os.Exit(1)
		}
		if err := runPull(dbPath, mountDir); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

func init() {
	RootCmd.AddCommand(pullCmd)
}

func runPull(dbPath, outputDir string) error {
	// Open database
	store, err := db.Open(db.DefaultConfig(dbPath))
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer store.Close()

	// Create output directory
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	ctx := context.Background()

	// Export starting from root (ino=1)
	return pullDir(ctx, store, 1, outputDir)
}

func pullDir(ctx context.Context, store *db.Store, ino uint64, path string) error {
	entries, err := store.ListDir(ctx, ino)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		entryPath := filepath.Join(path, entry.Name)

		inode, err := store.GetInode(ctx, entry.Ino)
		if err != nil {
			fmt.Printf("Warning: could not get inode %d: %v\n", entry.Ino, err)
			continue
		}

		if inode.IsDir() {
			// Create directory and recurse
			if err := os.MkdirAll(entryPath, os.FileMode(inode.Mode&0777)); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", entryPath, err)
			}
			fmt.Printf("DIR  %s\n", entryPath)
			if err := pullDir(ctx, store, entry.Ino, entryPath); err != nil {
				return err
			}
		} else if inode.IsSymlink() {
			// Create symlink
			target, err := store.ReadSymlink(ctx, entry.Ino)
			if err != nil {
				fmt.Printf("Warning: could not read symlink %s: %v\n", entryPath, err)
				continue
			}
			// Remove existing symlink if any
			os.Remove(entryPath)
			if err := os.Symlink(target, entryPath); err != nil {
				return fmt.Errorf("failed to create symlink %s: %w", entryPath, err)
			}
			fmt.Printf("LINK %s -> %s\n", entryPath, target)
		} else if inode.IsRegular() {
			// Export file
			data, err := store.ReadData(ctx, entry.Ino, 0, int64(inode.Size))
			if err != nil {
				return fmt.Errorf("failed to read file %s: %w", entryPath, err)
			}
			if err := os.WriteFile(entryPath, data, os.FileMode(inode.Mode&0777)); err != nil {
				return fmt.Errorf("failed to write file %s: %w", entryPath, err)
			}
			fmt.Printf("FILE %s (%d bytes)\n", entryPath, inode.Size)
		}
	}

	return nil
}
