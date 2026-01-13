//go:build amd64
package tracer

// Syscall returns the syscall number
func (c *SyscallContext) Syscall() uint64 {
	return c.regs.Orig_rax
}

// setSyscall sets the syscall number
func (c *SyscallContext) setSyscall(nr uint64) {
	c.regs.Orig_rax = nr
}

// Arg returns syscall argument by index (0-5)
func (c *SyscallContext) Arg(index int) uint64 {
	switch index {
	case 0:
		return c.regs.Rdi
	case 1:
		return c.regs.Rsi
	case 2:
		return c.regs.Rdx
	case 3:
		return c.regs.R10
	case 4:
		return c.regs.R8
	case 5:
		return c.regs.R9
	default:
		return 0
	}
}

// SetArg sets a syscall argument by index (0-5)
func (c *SyscallContext) SetArg(index int, value uint64) {
	switch index {
	case 0:
		c.regs.Rdi = value
	case 1:
		c.regs.Rsi = value
	case 2:
		c.regs.Rdx = value
	case 3:
		c.regs.R10 = value
	case 4:
		c.regs.R8 = value
	case 5:
		c.regs.R9 = value
	}
}

// Return gets the syscall return value (only valid at exit)
func (c *SyscallContext) Return() int64 {
	return int64(c.regs.Rax)
}

// SetReturn sets the syscall return value (only valid at exit)
func (c *SyscallContext) SetReturn(value int64) {
	c.regs.Rax = uint64(value)
	c.retModified = true
}

// Args returns all 6 arguments as a slice
func (c *SyscallContext) Args() [6]uint64 {
	return [6]uint64{
		c.regs.Rdi,
		c.regs.Rsi,
		c.regs.Rdx,
		c.regs.R10,
		c.regs.R8,
		c.regs.R9,
	}
}
