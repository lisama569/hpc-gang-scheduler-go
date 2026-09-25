package scheduler

import "errors"

// 领域错误，调用方可用 errors.Is 区分。
var (
	ErrNotFound       = errors.New("not found")
	ErrAlreadyExists  = errors.New("already exists")
	ErrInvalidJob     = errors.New("invalid job")
	ErrTerminalState  = errors.New("job already in terminal state")
	ErrDuplicateEvent = errors.New("duplicate event ignored")
)
