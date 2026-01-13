//go:build arm64
package tracer

// Syscall returns the syscall number
func (c *SyscallContext) Syscall() uint64 {
	return c.regs.Regs[8]
}

// setSyscall sets the syscall number
func (c *SyscallContext) setSyscall(nr uint64) {
	c.regs.Regs[8] = nr
}

// Arg returns syscall argument by index (0-5)
func (c *SyscallContext) Arg(index int) uint64 {
	if index >= 0 && index < 6 {
		return c.regs.Regs[index]
	}
	return 0
}

// SetArg sets a syscall argument by index (0-5)
func (c *SyscallContext) SetArg(index int, value uint64) {
	if index >= 0 && index < 6 {
		c.regs.Regs[index] = value
	}
}

// Return gets the syscall return value (only valid at exit)
func (c *SyscallContext) Return() int64 {
	return int64(c.regs.Regs[0])
}

// SetReturn sets the syscall return value (only valid at exit)
func (c *SyscallContext) SetReturn(value int64) {
	c.regs.Regs[0] = uint64(value)
	c.retModified = true
}

// Args returns all 6 arguments as a slice
func (c *SyscallContext) Args() [6]uint64 {
	var args [6]uint64
	// On arm64, args are in x0-x5 (Regs[0]-Regs[5])
	copy(args[:], c.regs.Regs[:6])
	return args
}
