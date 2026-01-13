package tracer

// Action determines what happens after OnEntry
type Action int

const (
	// ActionContinue lets the syscall proceed normally
	ActionContinue Action = iota
	// ActionSkip skips the syscall (set return value with SetReturn/SetError)
	ActionSkip
	// ActionModify indicates args were modified, proceed with modified args
	ActionModify
)

// Handler processes syscall events
type Handler interface {
	// OnEntry is called when a syscall is about to be made
	// Return ActionSkip to prevent the syscall, ActionModify if you changed args
	OnEntry(ctx *SyscallContext) Action

	// OnExit is called after the syscall completes
	// Can modify return value with ctx.SetReturn()
	OnExit(ctx *SyscallContext)
}

// PassthroughHandler allows all syscalls without modification
type PassthroughHandler struct{}

func (h *PassthroughHandler) OnEntry(ctx *SyscallContext) Action {
	return ActionContinue
}

func (h *PassthroughHandler) OnExit(ctx *SyscallContext) {}

// FilterHandler filters syscalls based on a whitelist/blacklist
type FilterHandler struct {
	// Blocked syscalls - return EPERM
	Blocked map[uint64]bool
	// OnBlocked is called when a blocked syscall is attempted
	OnBlocked func(ctx *SyscallContext)
}

func (h *FilterHandler) OnEntry(ctx *SyscallContext) Action {
	if h.Blocked[ctx.Syscall()] {
		if h.OnBlocked != nil {
			h.OnBlocked(ctx)
		}
		ctx.SetError(1) // EPERM
		return ActionSkip
	}
	return ActionContinue
}

func (h *FilterHandler) OnExit(ctx *SyscallContext) {}

// CompositeHandler chains multiple handlers
type CompositeHandler struct {
	Handlers []Handler
}

func (h *CompositeHandler) OnEntry(ctx *SyscallContext) Action {
	for _, handler := range h.Handlers {
		action := handler.OnEntry(ctx)
		if action != ActionContinue {
			return action
		}
	}
	return ActionContinue
}

func (h *CompositeHandler) OnExit(ctx *SyscallContext) {
	for _, handler := range h.Handlers {
		handler.OnExit(ctx)
	}
}

// LoggingHandler wraps another handler and logs syscalls
type LoggingHandler struct {
	Inner  Handler
	Logger Logger
}

func (h *LoggingHandler) OnEntry(ctx *SyscallContext) Action {
	if h.Logger != nil {
		h.Logger.LogEntry(ctx)
	}
	if h.Inner != nil {
		return h.Inner.OnEntry(ctx)
	}
	return ActionContinue
}

func (h *LoggingHandler) OnExit(ctx *SyscallContext) {
	if h.Logger != nil {
		h.Logger.LogExit(ctx)
	}
	if h.Inner != nil {
		h.Inner.OnExit(ctx)
	}
}
