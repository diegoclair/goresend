package goresend

import (
	"context"
	"time"
)

// QuotaStore persists the rate-limit counters. Implementations are expected to
// be backed by a shared store (Redis, etc.) so quotas hold across instances.
//
// Increment atomically adds delta to the counter under key and returns the new
// value. delta is +1 to reserve a slot and -1 to release one. ttl applies only
// on the first write of a window (when key does not yet exist); on an existing
// key the store MUST keep the original expiry so counters reset at the
// UTC day / month boundary.
//
// Get returns the current value, or 0 with a nil error when key is absent.
type QuotaStore interface {
	Increment(ctx context.Context, key string, delta int64, ttl time.Duration) (int64, error)
	Get(ctx context.Context, key string) (int64, error)
}

// Sender is the send surface shared by Client and MockClient.
type Sender interface {
	Send(ctx context.Context, msg Message) error
	DailyQuota() int
	MonthlyQuota() int
	Shutdown(ctx context.Context) error
}

// Logger is the minimal logging surface the package uses. Fields are passed as
// alternating key/value pairs. Pass a no-op implementation to silence output.
type Logger interface {
	Info(ctx context.Context, msg string, keyvals ...any)
	Warn(ctx context.Context, msg string, keyvals ...any)
	Error(ctx context.Context, msg string, keyvals ...any)
}

type noopLogger struct{}

func (noopLogger) Info(context.Context, string, ...any)  {}
func (noopLogger) Warn(context.Context, string, ...any)  {}
func (noopLogger) Error(context.Context, string, ...any) {}

// NoopLogger returns a Logger that discards everything.
func NoopLogger() Logger { return noopLogger{} }
