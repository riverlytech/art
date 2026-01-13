package tracer

import (
	"syscall"
)

// SyscallContext provides access to syscall state
type SyscallContext struct {
	PID         int
	Entry       bool // true = entry, false = exit
	regs        *syscall.PtraceRegs
	origRegs    syscall.PtraceRegs
	tracer      *Tracer
	retModified bool
}

// SetError sets an error return value
func (c *SyscallContext) SetError(errno syscall.Errno) {
	c.SetReturn(-int64(errno))
}

// ReadString reads a null-terminated string from tracee memory
func (c *SyscallContext) ReadString(addr uint64, maxLen int) (string, error) {
	if addr == 0 {
		return "", nil
	}

	buf := make([]byte, maxLen)
	n, err := c.ReadMemory(addr, buf)
	if err != nil {
		return "", err
	}

	// Find null terminator
	for i := 0; i < n; i++ {
		if buf[i] == 0 {
			return string(buf[:i]), nil
		}
	}
	return string(buf[:n]), nil
}

// ReadMemory reads from tracee memory
func (c *SyscallContext) ReadMemory(addr uint64, buf []byte) (int, error) {
	return syscall.PtracePeekData(c.PID, uintptr(addr), buf)
}

// WriteMemory writes to tracee memory
func (c *SyscallContext) WriteMemory(addr uint64, buf []byte) (int, error) {
	return syscall.PtracePokeData(c.PID, uintptr(addr), buf)
}

// SyscallName returns the name of the current syscall
func (c *SyscallContext) SyscallName() string {
	return GetSyscallName(c.Syscall())
}

// IsError returns true if the return value indicates an error
func (c *SyscallContext) IsError() bool {
	ret := c.Return()
	return ret < 0 && ret >= -4095 // Linux errno range
}

// Errno returns the error number if IsError() is true
func (c *SyscallContext) Errno() syscall.Errno {
	if c.IsError() {
		return syscall.Errno(-c.Return())
	}
	return 0
}
