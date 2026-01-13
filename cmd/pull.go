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
	Long: `Pulls files from the SQLite database to the workspace directory.
Only exports the workspace subdirectory (/<workspace-name>/) contents to the host.
The workspace name is derived from the mount directory basename.`,
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
	// Resolve output directory
	absOutputDir, err := filepath.Abs(outputDir)
	if err != nil {
		return fmt.Errorf("cannot resolve output directory: %w", err)
	}

	// Extract workspace name from output directory
	workspaceName := filepath.Base(absOutputDir)
	fmt.Printf("Workspace name: %s\n", workspaceName)
	fmt.Printf("Exporting from: /%s/\n", workspaceName)

	// Open database
	store, err := db.Open(db.DefaultConfig(dbPath))
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer store.Close()

	// Create output directory
	if err := os.MkdirAll(absOutputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	ctx := context.Background()

	// Find the workspace directory in the DB
	workspaceIno, err := store.Lookup(ctx, 1, workspaceName)
	if err != nil {
		if err == db.ErrNotFound {
			fmt.Printf("Workspace directory /%s/ not found in database\n", workspaceName)
			return nil
		}
		return fmt.Errorf("failed to find workspace directory: %w", err)
	}

	fmt.Printf("Workspace directory inode: %d\n", workspaceIno)

	// Export starting from workspace directory
	return pullDir(ctx, store, workspaceIno, absOutputDir)
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
