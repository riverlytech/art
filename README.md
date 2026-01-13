# ART - Agent Runtime

A sandboxed runtime supervisor for AI agents. Uses bubblewrap (bwrap) for container isolation and provides a copy-on-write overlay filesystem backed by SQLite for workspace persistence.

## Features

- **Full host filesystem access (read-only)**: Access globally installed packages, tools, and libraries
- **Isolated home directory**: Writable `/home/agent` backed by FUSE + SQLite
- **Copy-on-write overlay**: Changes are stored in SQLite, host files remain untouched
- **Workspace sync**: Push/pull workspace files between host and database
- **Syscall tracing**: Optional ptrace-based syscall logging

## Installation

```bash
# Build the binary
go build -o art .
```

### Dependencies

- [bubblewrap](https://github.com/containers/bubblewrap) (`bwrap`)
- FUSE support (for overlay mode)

## CLI Commands

### `art` - Run Sandbox

Start an interactive sandbox session.

```bash
art [flags] [command...]
```

#### Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--mount` | `-m` | `.` | Host directory to mount as workspace |
| `--db` | `-d` | | SQLite database for persistent filesystem |
| `--interactive` | `-i` | `true` | Run with PTY support (`-i=false` to disable) |
| `--trace` | | `false` | Enable ptrace syscall tracing |
| `--trace-log` | | stderr | Path to syscall log file |
| `--trace-syscalls` | | all | Comma-separated syscalls to trace |

#### Examples

```bash
# Run sandbox with overlay filesystem
art -m workspace/ -d workspace.db

# Run without database (direct bind mount)
art -m workspace/

# Run a specific command
art -m workspace/ -d workspace.db -- python script.py

# Non-interactive mode
art -m workspace/ -d workspace.db -i=false -- make build

# Enable syscall tracing
art -m workspace/ --trace --trace-log trace.log

# Trace specific syscalls
art -m workspace/ --trace --trace-syscalls openat,read,write
```

---

### `art push` - Import Files to Database

Import workspace files from host into the SQLite database.

```bash
art push -m <workspace-dir> -d <database.db>
```

#### Description

- Reads all files from the workspace directory on the host
- Stores them under `/<workspace-name>/` in the virtual filesystem
- Workspace name is derived from the directory basename

#### Example

```bash
# Import workspace/ directory into database
art push -m workspace/ -d workspace.db

# Output:
# Workspace name: workspace
# Files will be stored under: /workspace/
# FILE /workspace/main.py (1234 bytes, ino 2)
# DIR  /workspace/src (ino 3)
# ...
```

---

### `art pull` - Export Files from Database

Export workspace files from the SQLite database to the host.

```bash
art pull -m <workspace-dir> -d <database.db>
```

#### Description

- Reads the `/<workspace-name>/` directory from the database
- Writes contents to the workspace directory on the host
- Only exports the workspace subdirectory (not the entire `/home/agent`)

#### Example

```bash
# Export workspace files from database to host
art pull -m workspace/ -d workspace.db

# Output:
# Workspace name: workspace
# Exporting from: /workspace/
# FILE workspace/main.py (1234 bytes)
# DIR  workspace/src
# ...
```

---

## Architecture

### Sandbox Layout

```
Guest Filesystem:
/                       # Host root (read-only)
├── usr/                # Host packages (read-only)
├── bin/                # Host binaries (read-only)
├── lib/                # Host libraries (read-only)
├── tmp/                # Writable tmpfs
└── home/
    └── agent/          # FUSE mount (writable, SQLite-backed)
        └── <workspace>/  # Workspace directory
```

### Modes

#### Overlay Mode (`--db` flag)

```bash
art -m workspace/ -d workspace.db
```

- FUSE filesystem mounted at `/home/agent`
- Reads from host workspace, writes to SQLite
- Full `/home/agent` persisted in database
- Only workspace syncs with host via push/pull

#### Direct Mode (no `--db` flag)

```bash
art -m workspace/
```

- Workspace bound directly at `/home/agent/<workspace>`
- Changes written directly to host
- No persistence layer

### Data Flow

```
Host                          Guest
─────                         ─────
workspace/  ──ro-bind──>  /home/agent/workspace/
     │                           │
     │                      [FUSE Layer]
     │                           │
     └──── push/pull ────>  [SQLite DB]
```

## Workflow Example

```bash
# 1. Create a workspace
mkdir workspace
echo 'print("hello")' > workspace/main.py

# 2. Initialize database with workspace files
art push -m workspace/ -d workspace.db

# 3. Run sandbox session
art -m workspace/ -d workspace.db

# Inside sandbox:
# - Edit files in /home/agent/workspace/
# - Create files anywhere in /home/agent/
# - Install user packages to ~/.local/
# - All changes saved to SQLite

# 4. Export workspace changes back to host
art pull -m workspace/ -d workspace.db
```

## Configuration

### Custom Bind Mounts

Create `.art/config/binds.json` in your workspace:

```json
{
  "binds": [
    {
      "host": "/path/on/host",
      "guest": "/path/in/sandbox",
      "readonly": true
    },
    {
      "host": "relative/path",
      "guest": "/opt/mydata",
      "readonly": false
    }
  ]
}
```

## Environment Variables

Inside the sandbox:

| Variable | Value |
|----------|-------|
| `HOME` | `/home/agent` |
| `PATH` | `/usr/local/bin:/usr/bin:/bin` |
| `PWD` | `/home/agent/<workspace>` |

## License

MIT
