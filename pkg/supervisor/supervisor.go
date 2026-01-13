package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/creack/pty"
	"golang.org/x/term"

	"art/pkg/db"
	artfs "art/pkg/fs"
	"art/pkg/overlay"
	"art/pkg/tracer"
)

// Config holds supervisor configuration
type Config struct {
	MountDir      string // Host directory to mount (workspace source)
	Interactive   bool
	DBPath        string
	EnableTracer  bool     // Enable ptrace tracer
	TraceLogPath  string   // Path to log syscalls
	TraceSyscalls []string // List of syscalls to log (empty = all)
	Command       []string // Command to run (overrides shell)
}

// guestHomePath is the home directory path inside the sandbox
const guestHomePath = "/home/agent"

// Run starts the bubblewrap sandbox with the given configuration
func Run(cfg Config) error {
	var overlayMounter *artfs.OverlayMounter
	var store *db.Store
	var cleanupFuse func()
	var fuseMountPoint string

	// Resolve MountDir
	absMountDir, err := filepath.Abs(cfg.MountDir)
	if err != nil {
		return fmt.Errorf("error resolving mount path: %w", err)
	}

	// Extract workspace name from mount directory
	workspaceName := filepath.Base(absMountDir)
	guestWorkspacePath := filepath.Join(guestHomePath, workspaceName)

	// Setup FUSE filesystem if database path provided
	if cfg.DBPath != "" {
		// Overlay mode: FUSE backs /home/agent entirely
		// Workspace files from host are accessible at /home/agent/<workspace>
		var err error
		store, err = db.Open(db.DefaultConfig(cfg.DBPath))
		if err != nil {
			return fmt.Errorf("failed to open database: %w", err)
		}
		defer store.Close()

		// Create HostFS from mount directory, mapped to workspace subpath
		hostfs, err := overlay.NewHostFS(absMountDir)
		if err != nil {
			return fmt.Errorf("failed to create host filesystem: %w", err)
		}

		// Create AgentFS for delta layer (entire /home/agent)
		agentfs, err := overlay.NewAgentFS(store)
		if err != nil {
			return fmt.Errorf("failed to create agent filesystem: %w", err)
		}

		// Create OverlayFS with workspace name for host mapping
		overlayfs, err := overlay.NewOverlayFS(hostfs, agentfs, overlay.WithWorkspaceName(workspaceName))
		if err != nil {
			return fmt.Errorf("failed to create overlay filesystem: %w", err)
		}

		// Create temporary mount point
		fuseMountPoint, err = os.MkdirTemp("", "art-overlay-*")
		if err != nil {
			return fmt.Errorf("failed to create temp mount point: %w", err)
		}

		cleanupFuse = func() {
			if overlayMounter != nil {
				overlayMounter.Unmount()
			}
			os.RemoveAll(fuseMountPoint)
		}
		defer cleanupFuse()

		// Mount overlay FUSE filesystem
		overlayMounter, err = artfs.MountOverlay(fuseMountPoint, overlayfs)
		if err != nil {
			os.RemoveAll(fuseMountPoint)
			return fmt.Errorf("failed to mount overlay FUSE: %w", err)
		}

		fmt.Printf("Overlay FUSE mounted at: %s\n", fuseMountPoint)
		fmt.Printf("Host workspace: %s -> %s\n", absMountDir, guestWorkspacePath)
		fmt.Printf("Delta (write): %s\n", cfg.DBPath)

	} else {
		// Direct mount mode - still use the new layout
		fmt.Printf("Direct mount: %s -> %s\n", absMountDir, guestWorkspacePath)
	}

	fmt.Printf("--- Starting Sandbox ---\n")
	fmt.Printf("Guest home: %s\n", guestHomePath)
	fmt.Printf("Guest workspace: %s\n", guestWorkspacePath)
	fmt.Printf("Interactive: %v\n", cfg.Interactive)

	// Build Bubblewrap arguments
	// Mount entire host filesystem read-only for access to packages, tools, etc.
	bwrapArgs := []string{
		// Mount root filesystem read-only
		"--ro-bind", "/", "/",

		// Process information
		"--proc", "/proc",

		// Writable /tmp
		"--tmpfs", "/tmp",

		// Writable /home (overlay on read-only root)
		"--tmpfs", "/home",

		// Devices
		"--dev-bind", "/dev/null", "/dev/null",
		"--dev-bind", "/dev/zero", "/dev/zero",
		"--dev-bind", "/dev/random", "/dev/random",
		"--dev-bind", "/dev/urandom", "/dev/urandom",

		// Isolation
		// "--unshare-net",
		"--unshare-pid",
		"--die-with-parent",
		"--new-session",
	}

	// Mount home directory (FUSE or direct)
	if cfg.DBPath != "" {
		// FUSE overlay mode: bind FUSE mount as /home/agent
		bwrapArgs = append(bwrapArgs,
			"--dir", guestHomePath,
			"--bind", fuseMountPoint, guestHomePath,
		)
	} else {
		// Direct mode: bind workspace directly at /home/agent/<workspace>
		bwrapArgs = append(bwrapArgs,
			"--dir", guestHomePath,
			"--dir", guestWorkspacePath,
			"--bind", absMountDir, guestWorkspacePath,
		)
	}

	// Environment
	bwrapArgs = append(bwrapArgs,
		"--chdir", guestWorkspacePath,
		"--setenv", "HOME", guestHomePath,
		"--setenv", "PATH", "/usr/local/bin:/usr/bin:/bin",
	)

	// Load user config from .art/config/binds.json
	userCfgPath := filepath.Join(absMountDir, ".art", "config", "binds.json")
	if _, err := os.Stat(userCfgPath); err == nil {
		userCfg, err := loadUserConfig(userCfgPath)
		if err != nil {
			fmt.Printf("Warning: failed to load user config: %v\n", err)
		} else {
			for _, bind := range userCfg.Binds {
				if bind.HostPath == "" || bind.GuestPath == "" {
					continue
				}

				// Resolve relative host paths against project root
				hostPath := bind.HostPath
				if !filepath.IsAbs(hostPath) {
					hostPath = filepath.Join(absMountDir, hostPath)
				}

				if bind.ReadOnly {
					bwrapArgs = append(bwrapArgs, "--ro-bind", hostPath, bind.GuestPath)
				} else {
					bwrapArgs = append(bwrapArgs, "--bind", hostPath, bind.GuestPath)
				}
				fmt.Printf("Binding %s -> %s (ro=%v)\n", hostPath, bind.GuestPath, bind.ReadOnly)
			}
		}
	}

	var t *tracer.Tracer
	if cfg.EnableTracer {
		traceCfg := tracer.Config{
			TraceSyscalls: cfg.TraceSyscalls,
		}
		if cfg.TraceLogPath != "" {
			l, err := tracer.NewFileLogger(cfg.TraceLogPath)
			if err != nil {
				return fmt.Errorf("failed to create trace logger: %w", err)
			}
			defer l.Close()
			traceCfg.Logger = l
		} else {
			traceCfg.Logger = tracer.NewStreamLogger(os.Stderr)
		}
		t = tracer.New(traceCfg)
	}

	if cfg.Interactive {
		// Add PTY device for interactive mode
		// Remove --new-session for interactive mode as PTY handles session creation
		bwrapArgs = removeArg(bwrapArgs, "--new-session")
		bwrapArgs = append(bwrapArgs, "--dev", "/dev")

		if len(cfg.Command) > 0 {
			bwrapArgs = append(bwrapArgs, cfg.Command...)
		} else {
			bwrapArgs = append(bwrapArgs, "/bin/bash")
		}
		return runInteractive(bwrapArgs, cleanupFuse, t)
	}

	// Non-interactive mode
	bwrapArgs = append(bwrapArgs, "--dev-bind", "/dev/tty", "/dev/tty")
	if len(cfg.Command) > 0 {
		bwrapArgs = append(bwrapArgs, cfg.Command...)
	} else {
		bwrapArgs = append(bwrapArgs, "/bin/sh")
	}
	return runNonInteractive(bwrapArgs, t)
}

// runInteractive runs the sandbox with a proper PTY for full terminal support
func runInteractive(bwrapArgs []string, cleanup func(), t *tracer.Tracer) error {
	cmd := exec.Command("bwrap", bwrapArgs...)

	var ptmx *os.File
	var err error

	// Context for tracer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Channel to wait for completion
	done := make(chan error, 1)

	if t != nil {
		// Manual PTY setup for Tracing
		var tty *os.File
		ptmx, tty, err = pty.Open()
		if err != nil {
			return fmt.Errorf("failed to start pty: %w", err)
		}
		defer tty.Close()
		defer ptmx.Close()

		cmd.Stdin = tty
		cmd.Stdout = tty
		cmd.Stderr = tty
		if cmd.SysProcAttr == nil {
			cmd.SysProcAttr = &syscall.SysProcAttr{}
		}
		cmd.SysProcAttr.Setsid = true
		cmd.SysProcAttr.Setctty = true

		go func() {
			done <- t.TraceCmd(ctx, cmd, func() {
				tty.Close()
			})
		}()
	} else {
		// Start command with a PTY
		ptmx, err = pty.Start(cmd)
		if err != nil {
			return fmt.Errorf("failed to start pty: %w", err)
		}
		defer ptmx.Close()

		go func() {
			done <- cmd.Wait()
		}()
	}

	// Handle window size changes
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGWINCH)
	go func() {
		for range ch {
			if err := pty.InheritSize(os.Stdin, ptmx); err != nil {
				// Ignore errors, not critical
			}
		}
	}()
	ch <- syscall.SIGWINCH // Initial size sync

	// Set stdin to raw mode for proper terminal handling
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		return fmt.Errorf("failed to set raw mode: %w", err)
	}
	defer term.Restore(int(os.Stdin.Fd()), oldState)

	// Copy stdin -> pty (in goroutine)
	go func() {
		io.Copy(ptmx, os.Stdin)
	}()

	// Copy pty -> stdout (blocks until pty closes)
	io.Copy(os.Stdout, ptmx)

	// Wait for command to finish
	err = <-done

	if err != nil {
		if t == nil {
			// Exit errors are expected when shell exits
			if _, ok := err.(*exec.ExitError); !ok {
				return fmt.Errorf("sandbox exited with error: %w", err)
			}
		} else {
			return fmt.Errorf("trace failed: %w", err)
		}
	}

	fmt.Println("\n--- Sandbox Exited Cleanly ---")
	return nil
}

// runNonInteractive runs the sandbox without PTY (for scripted/automated use)
func runNonInteractive(bwrapArgs []string, t *tracer.Tracer) error {
	cmd := exec.Command("bwrap", bwrapArgs...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Context for tracer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if t != nil {
		if err := t.TraceCmd(ctx, cmd, nil); err != nil {
			return fmt.Errorf("trace failed: %w", err)
		}
	} else {
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("sandbox exited with error: %w", err)
		}
	}

	fmt.Printf("--- Sandbox Exited Cleanly ---\n")
	return nil
}

// removeArg removes an argument from the slice
func removeArg(args []string, arg string) []string {
	result := make([]string, 0, len(args))
	for _, a := range args {
		if a != arg {
			result = append(result, a)
		}
	}
	return result
}

// RunCommand executes a single command in the sandbox and returns output
func RunCommand(cfg Config) ([]byte, error) {
	// reuse Run() logic but capture stdout?
	// For now, this is a placeholder or you might want to implement it properly.
	return nil, fmt.Errorf("not implemented")
}

// UserBind defines a single bind mount configuration
type UserBind struct {
	HostPath  string `json:"host"`
	GuestPath string `json:"guest"`
	ReadOnly  bool   `json:"readonly"`
}

// UserConfig defines the structure of the user configuration file
type UserConfig struct {
	Binds []UserBind `json:"binds"`
}

// loadUserConfig reads and parses the user configuration file
func loadUserConfig(path string) (*UserConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var cfg UserConfig
	if err := json.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
