// Package tracer provides ptrace-based syscall interception.
// It can trace processes directly or work with bwrap sandboxes.
package tracer

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"syscall"
)

// Tracer intercepts syscalls via ptrace
type Tracer struct {
	handler       Handler
	logger        Logger
	tracees       map[int]*Tracee // pid -> tracee state
	stopping      bool
	traceSyscalls map[string]bool // whitelist of syscalls to log (empty = all)
}

// Tracee represents a traced process
type Tracee struct {
	pid      int
	inSyscall bool // true if we're at syscall exit (already saw entry)
}

// Config for creating a new tracer
type Config struct {
	Handler       Handler // Syscall handler (optional, defaults to PassthroughHandler)
	Logger        Logger  // Logger for syscall events (optional)
	TraceSyscalls []string // List of syscalls to log (optional, empty = all)
}

// New creates a new tracer
func New(cfg Config) *Tracer {
	handler := cfg.Handler
	if handler == nil {
		handler = &PassthroughHandler{}
	}

	traceSyscalls := make(map[string]bool)
	for _, s := range cfg.TraceSyscalls {
		traceSyscalls[s] = true
	}

	return &Tracer{
		handler:       handler,
		logger:        cfg.Logger,
		tracees:       make(map[int]*Tracee),
		traceSyscalls: traceSyscalls,
	}
}

// TraceCommand starts and traces a command
// This is the standalone mode - no bwrap
func (t *Tracer) TraceCommand(ctx context.Context, name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return t.TraceCmd(ctx, cmd, nil)
}

// TraceCmd starts and traces a prepared command
func (t *Tracer) TraceCmd(ctx context.Context, cmd *exec.Cmd, onStart func()) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Ptrace = true

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start command: %w", err)
	}

	if onStart != nil {
		onStart()
	}

	pid := cmd.Process.Pid
	t.tracees[pid] = &Tracee{pid: pid}

	// Wait for initial stop (SIGTRAP from PTRACE_TRACEME)
	var ws syscall.WaitStatus
	if _, err := syscall.Wait4(pid, &ws, 0, nil); err != nil {
		return fmt.Errorf("wait4 failed: %w", err)
	}

	// Set ptrace options to trace forks/clones
	opts := syscall.PTRACE_O_TRACESYSGOOD |
		syscall.PTRACE_O_TRACEFORK |
		syscall.PTRACE_O_TRACEVFORK |
		syscall.PTRACE_O_TRACECLONE |
		syscall.PTRACE_O_TRACEEXEC
	if err := syscall.PtraceSetOptions(pid, opts); err != nil {
		return fmt.Errorf("ptrace setoptions failed: %w", err)
	}

	// Start tracing syscalls
	if err := syscall.PtraceSyscall(pid, 0); err != nil {
		return fmt.Errorf("ptrace syscall failed: %w", err)
	}

	return t.traceLoop(ctx)
}

// AttachAndTrace attaches to an existing process and traces it
// Useful for attaching to bwrap's child
func (t *Tracer) AttachAndTrace(ctx context.Context, pid int) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if err := syscall.PtraceAttach(pid); err != nil {
		return fmt.Errorf("ptrace attach failed: %w", err)
	}

	t.tracees[pid] = &Tracee{pid: pid}

	// Wait for stop
	var ws syscall.WaitStatus
	if _, err := syscall.Wait4(pid, &ws, 0, nil); err != nil {
		return fmt.Errorf("wait4 failed: %w", err)
	}

	// Set ptrace options
	opts := syscall.PTRACE_O_TRACESYSGOOD |
		syscall.PTRACE_O_TRACEFORK |
		syscall.PTRACE_O_TRACEVFORK |
		syscall.PTRACE_O_TRACECLONE |
		syscall.PTRACE_O_TRACEEXEC
	if err := syscall.PtraceSetOptions(pid, opts); err != nil {
		return fmt.Errorf("ptrace setoptions failed: %w", err)
	}

	// Start tracing
	if err := syscall.PtraceSyscall(pid, 0); err != nil {
		return fmt.Errorf("ptrace syscall failed: %w", err)
	}

	return t.traceLoop(ctx)
}

// traceLoop is the main ptrace event loop
func (t *Tracer) traceLoop(ctx context.Context) error {
	for len(t.tracees) > 0 {
		select {
		case <-ctx.Done():
			t.stopping = true
			// Detach from all tracees
			for pid := range t.tracees {
				syscall.PtraceDetach(pid)
			}
			return ctx.Err()
		default:
		}

		// Wait for any child
		var ws syscall.WaitStatus
		pid, err := syscall.Wait4(-1, &ws, 0, nil)
		if err != nil {
			if err == syscall.ECHILD {
				break // No more children
			}
			if err == syscall.EINTR {
				continue
			}
			return fmt.Errorf("wait4 failed: %w", err)
		}

		tracee, ok := t.tracees[pid]
		if !ok {
			// New process from fork/clone
			tracee = &Tracee{pid: pid}
			t.tracees[pid] = tracee
		}

		if ws.Exited() || ws.Signaled() {
			// Process terminated
			delete(t.tracees, pid)
			continue
		}

		// Handle ptrace events
		if ws.Stopped() {
			sig := ws.StopSignal()

			// Check for syscall stop (SIGTRAP | 0x80)
			if sig == syscall.SIGTRAP|0x80 {
				if err := t.handleSyscall(tracee); err != nil {
					// Log error but continue
					fmt.Fprintf(os.Stderr, "syscall handler error: %v\n", err)
				}
				syscall.PtraceSyscall(pid, 0)
				continue
			}

			// Check for ptrace events
			if sig == syscall.SIGTRAP {
				event := ws.TrapCause()
				switch event {
				case syscall.PTRACE_EVENT_FORK, syscall.PTRACE_EVENT_VFORK, syscall.PTRACE_EVENT_CLONE:
					// Get new child pid
					newPid, err := syscall.PtraceGetEventMsg(pid)
					if err == nil {
						t.tracees[int(newPid)] = &Tracee{pid: int(newPid)}
					}
				case syscall.PTRACE_EVENT_EXEC:
					// Process exec'd. We normally get a syscall exit stop for execve after this.
					// So we do NOT reset inSyscall here, ensuring the exit stop is handled correctly as an exit.
				}
				syscall.PtraceSyscall(pid, 0)
				continue
			}

			// Other signal - deliver it to the process
			// fmt.Fprintf(os.Stderr, "[tracer] delivering signal %v to pid %d\n", sig, pid)
			syscall.PtraceSyscall(pid, int(sig))
			continue
		}
	}

	return nil
}

// shouldLog returns true if the syscall should be logged
func (t *Tracer) shouldLog(name string) bool {
	if len(t.traceSyscalls) == 0 {
		return true
	}
	return t.traceSyscalls[name]
}

// handleSyscall processes a syscall entry or exit
func (t *Tracer) handleSyscall(tracee *Tracee) error {
	// Get registers
	var regs syscall.PtraceRegs
	if err := syscall.PtraceGetRegs(tracee.pid, &regs); err != nil {
		return fmt.Errorf("ptrace getregs failed: %w", err)
	}

	sctx := &SyscallContext{
		PID:      tracee.pid,
		regs:     &regs,
		origRegs: regs,
		tracer:   t,
	}

	if !tracee.inSyscall {
		// Syscall entry
		tracee.inSyscall = true
		sctx.Entry = true

		action := t.handler.OnEntry(sctx)

		// Log entry
		if t.logger != nil && t.shouldLog(sctx.SyscallName()) {
			t.logger.LogEntry(sctx)
		}

		switch action {
		case ActionSkip:
			// Skip syscall by setting syscall number to -1
			sctx.setSyscall(^uint64(0)) // -1
			if err := syscall.PtraceSetRegs(tracee.pid, sctx.regs); err != nil {
				return fmt.Errorf("ptrace setregs failed: %w", err)
			}
		case ActionModify:
			// Handler modified args, apply them
			if err := syscall.PtraceSetRegs(tracee.pid, sctx.regs); err != nil {
				return fmt.Errorf("ptrace setregs failed: %w", err)
			}
		}
	} else {
		// Syscall exit
		tracee.inSyscall = false
		sctx.Entry = false

		t.handler.OnExit(sctx)

		// Log exit
		if t.logger != nil && t.shouldLog(sctx.SyscallName()) {
			t.logger.LogExit(sctx)
		}

		// Apply any return value modifications
		if sctx.retModified {
			if err := syscall.PtraceSetRegs(tracee.pid, sctx.regs); err != nil {
				return fmt.Errorf("ptrace setregs failed: %w", err)
			}
		}
	}

	return nil
}

// Stop stops the tracer
func (t *Tracer) Stop() {
	t.stopping = true
}
