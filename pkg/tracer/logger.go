package tracer

import (
	"fmt"
	"io"
	"os"
	"strings"
)

// Logger logs syscall events
type Logger interface {
	LogEntry(ctx *SyscallContext)
	LogExit(ctx *SyscallContext)
}

// StreamLogger logs to an io.Writer
type StreamLogger struct {
	Out io.Writer
}

// NewStreamLogger creates a new StreamLogger
func NewStreamLogger(out io.Writer) *StreamLogger {
	return &StreamLogger{Out: out}
}

func (l *StreamLogger) LogEntry(ctx *SyscallContext) {
	name := ctx.SyscallName()
	args := ctx.Args()
	formattedArgs := make([]string, len(args))

	// Default formatting
	for i, arg := range args {
		formattedArgs[i] = fmt.Sprintf("0x%x", arg)
	}

	// Custom formatting for known syscalls
	switch name {
	case "open", "access", "chdir", "mkdir", "rmdir", "unlink", "chmod", "chown", "lchown", "stat", "lstat", "truncate", "readlink":
		// Arg 0 is path
		if s, err := ctx.ReadString(args[0], 4096); err == nil {
			formattedArgs[0] = fmt.Sprintf("%q", s)
		}
	case "creat":
		// Arg 0 is path, Arg 1 is mode
		if s, err := ctx.ReadString(args[0], 4096); err == nil {
			formattedArgs[0] = fmt.Sprintf("%q", s)
		}
		formattedArgs[1] = fmt.Sprintf("0%o", args[1])
	case "openat", "mkdirat", "mknodat", "unlinkat", "fchmodat", "fchownat", "fstatat", "newfstatat", "readlinkat", "faccessat", "utimensat":
		// Arg 0 is dirfd, Arg 1 is path
		if int32(args[0]) == -100 { // AT_FDCWD
			formattedArgs[0] = "AT_FDCWD"
		}
		if s, err := ctx.ReadString(args[1], 4096); err == nil {
			formattedArgs[1] = fmt.Sprintf("%q", s)
		}
	case "execve", "execveat":
		// execve(filename, argv, envp)
		if s, err := ctx.ReadString(args[0], 4096); err == nil {
			formattedArgs[0] = fmt.Sprintf("%q", s)
		}
		// TODO: Decode argv/envp if needed (requires reading pointer arrays)
	case "rename":
		// rename(old, new)
		if s, err := ctx.ReadString(args[0], 4096); err == nil {
			formattedArgs[0] = fmt.Sprintf("%q", s)
		}
		if s, err := ctx.ReadString(args[1], 4096); err == nil {
			formattedArgs[1] = fmt.Sprintf("%q", s)
		}
	case "renameat", "renameat2":
		// renameat(olddfd, old, newdfd, new)
		if int32(args[0]) == -100 {
			formattedArgs[0] = "AT_FDCWD"
		}
		if s, err := ctx.ReadString(args[1], 4096); err == nil {
			formattedArgs[1] = fmt.Sprintf("%q", s)
		}
		if int32(args[2]) == -100 {
			formattedArgs[2] = "AT_FDCWD"
		}
		if s, err := ctx.ReadString(args[3], 4096); err == nil {
			formattedArgs[3] = fmt.Sprintf("%q", s)
		}
	case "symlink":
		// symlink(target, linkpath)
		if s, err := ctx.ReadString(args[0], 4096); err == nil {
			formattedArgs[0] = fmt.Sprintf("%q", s)
		}
		if s, err := ctx.ReadString(args[1], 4096); err == nil {
			formattedArgs[1] = fmt.Sprintf("%q", s)
		}
	case "symlinkat":
		// symlinkat(target, newdfd, linkpath)
		if s, err := ctx.ReadString(args[0], 4096); err == nil {
			formattedArgs[0] = fmt.Sprintf("%q", s)
		}
		if int32(args[1]) == -100 {
			formattedArgs[1] = "AT_FDCWD"
		}
		if s, err := ctx.ReadString(args[2], 4096); err == nil {
			formattedArgs[2] = fmt.Sprintf("%q", s)
		}
	case "mount":
		// mount(source, target, type, flags, data)
		if s, err := ctx.ReadString(args[0], 4096); err == nil {
			formattedArgs[0] = fmt.Sprintf("%q", s)
		}
		if s, err := ctx.ReadString(args[1], 4096); err == nil {
			formattedArgs[1] = fmt.Sprintf("%q", s)
		}
		if s, err := ctx.ReadString(args[2], 4096); err == nil {
			formattedArgs[2] = fmt.Sprintf("%q", s)
		}
		// Arg 4 (data) might be string or binary, depends on fs
	case "umount2":
		// umount2(target, flags)
		if s, err := ctx.ReadString(args[0], 4096); err == nil {
			formattedArgs[0] = fmt.Sprintf("%q", s)
		}
	case "write", "read":
		// write(fd, buf, count) - simplify buf to address
		// Keep as hex
	}

	argStr := strings.Join(formattedArgs, ", ")
	fmt.Fprintf(l.Out, "[TRACE] [%-5d] → %s(%s)\n", ctx.PID, name, argStr)
}

func (l *StreamLogger) LogExit(ctx *SyscallContext) {
	name := ctx.SyscallName()
	if ctx.IsError() {
		fmt.Fprintf(l.Out, "[TRACE] [%-5d] ← %s = -1 (errno=%d)\n", ctx.PID, name, ctx.Errno())
	} else {
		// For some syscalls, return value is special (e.g. mmap returns addr)
		ret := ctx.Return()
		if name == "mmap" || name == "brk" {
			fmt.Fprintf(l.Out, "[TRACE] [%-5d] ← %s = 0x%x\n", ctx.PID, name, ret)
		} else {
			fmt.Fprintf(l.Out, "[TRACE] [%-5d] ← %s = %d\n", ctx.PID, name, ret)
		}
	}
}

// FileLogger logs to a file
type FileLogger struct {
	*StreamLogger
	file *os.File
}

// NewFileLogger creates a logger that writes to a file
func NewFileLogger(path string) (*FileLogger, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &FileLogger{
		StreamLogger: NewStreamLogger(f),
		file:         f,
	}, nil
}

func (l *FileLogger) Close() error {
	return l.file.Close()
}
