# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ART (Agent Runtime) is a sandboxed runtime supervisor for AI agents. It uses bubblewrap (bwrap) for container isolation and provides a copy-on-write overlay filesystem backed by SQLite for workspace persistence.

## Build and Run Commands

```bash
# Build the binary
go build -o art .

# Run with overlay filesystem (mount dir read-only, SQLite for writes)
./art --mount /path/to/host/dir --db workspace.db

# Run directly on host directory (no overlay, direct bind)
./art --mount /path/to/host/dir

# Enable ptrace syscall tracing
./art --trace --trace-log trace.log --trace-syscalls openat,read,write

# Import files into SQLite database
./art push --db workspace.db --workspace /source/dir

# Export files from SQLite database
./art pull --db workspace.db --workspace /output/dir
```

## Architecture

### Core Components

- **pkg/supervisor**: Main entry point that orchestrates bubblewrap sandbox, FUSE mounting, and ptrace tracing. Handles both interactive (PTY) and non-interactive modes.

- **pkg/overlay**: Copy-on-write filesystem abstraction with three layers:
  - `FileSystem` interface: Common abstraction for all filesystem backends
  - `HostFS`: Read-only passthrough to host filesystem
  - `AgentFS`: SQLite-backed writable layer
  - `OverlayFS`: Combines host (base) and agent (delta) layers with whiteout support for deletions

- **pkg/db**: SQLite storage layer using modernc.org/sqlite (pure Go). Stores inodes, dentries, file data (chunked), symlinks, whiteouts, and origin mappings.

- **pkg/fs**: FUSE filesystem implementation using go-fuse/v2. Two node types:
  - `Node`: Direct SQLite-backed FUSE node
  - `OverlayNode`: FUSE node backed by the overlay FileSystem interface

- **pkg/tracer**: ptrace-based syscall interception supporting:
  - Syscall logging with configurable filters
  - Fork/clone/exec tracing
  - Entry/exit hooks via Handler interface
  - Architecture-specific register handling (amd64/arm64)

### Data Flow

1. Supervisor creates FUSE mount (overlay or pure SQLite mode)
2. Bubblewrap sandbox binds FUSE mount at `/home/agent`
3. Agent process sees virtual filesystem with host isolation
4. File writes go to SQLite delta layer
5. Optional ptrace tracer intercepts syscalls for logging/policy

### Key Interfaces

- `overlay.FileSystem`: All filesystem operations (stat, read, write, mkdir, etc.)
- `overlay.File`: Open file handle with read/write/sync/truncate
- `tracer.Handler`: Syscall interception with OnEntry/OnExit hooks
