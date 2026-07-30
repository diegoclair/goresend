# goresend

<p align="center">
 <b>A dependency-free Go client for the Resend email API, with built-in rate limiting and attachments</b><br><br>
    <a href="https://github.com/diegoclair/goresend/tags" alt="GitHub tag">
     <img src="https://img.shields.io/github/tag/diegoclair/goresend.svg" />
    </a>
    <a href='https://coveralls.io/github/diegoclair/goresend?branch=main'>
     <img src='https://coveralls.io/repos/github/diegoclair/goresend/badge.svg?branch=main' alt='Coverage Status' />
    </a>
    <a href="https://github.com/diegoclair/goresend/actions">
     <img src="https://github.com/diegoclair/goresend/actions/workflows/ci.yaml/badge.svg" alt="build status">
    </a>
    <a href="https://opensource.org/licenses/MIT">
     <img src="https://img.shields.io/badge/License-MIT-yellow.svg" />
    </a>
    <a href='https://goreportcard.com/badge/github.com/diegoclair/goresend'>
     <img src='https://goreportcard.com/badge/github.com/diegoclair/goresend' alt='Go Report'/>
    </a>
</p>

A small, dependency-free Go client for the [Resend](https://resend.com) email API,
with built-in daily / monthly / per-second rate limiting and file attachments.

It defines its own storage and logging interfaces, so it is not coupled to any
application. Bring your own cache adapter and logger.

## Install

```sh
go get github.com/diegoclair/goresend
```

## Quota is fail-fast (no hidden defaults)

`New` returns an error if `DailyQuota`, `MonthlyQuota`, or `PerSecond` is `<= 0`,
and if `APIKey` or `FromEmail` is empty. There are no fallback numbers — the
caller must set explicit limits.

## Usage

```go
cfg := goresend.Config{
    APIKey:       os.Getenv("RESEND_API_KEY"),
    FromEmail:    "hello@example.com",
    FromName:     "Example",
    DailyQuota:   100,
    MonthlyQuota: 3000,
    PerSecond:    2,
    Timeout:      30 * time.Second, // optional; zero → 30s default
}

client, err := goresend.New(cfg, myStore, myLogger)
if err != nil {
    log.Fatal(err)
}
defer client.Shutdown(context.Background())

result, err := client.Send(ctx, goresend.Message{
    To:      "user@example.com",
    Subject: "Welcome",
    HTML:    "<h1>Hi</h1>",
    Attachments: []goresend.Attachment{{
        Filename:    "invoice.pdf",
        Content:     pdfBytes, // raw bytes; base64 is handled internally
        ContentType: "application/pdf",
    }},
})
if err != nil {
    log.Fatal(err)
}
fmt.Println(result.ID) // Resend message id
```

`Send` blocks until the email is actually delivered to the Resend API (or the
context is cancelled / the queue is full), returning a `Result` with the
Resend message id on success — the key to correlate a later delivery/open
webhook with this send. The per-second limit is enforced by a single worker
goroutine. `Timeout` bounds each HTTP call to Resend and defaults to 30s when
left zero.

## Quotas: reserve / commit / release (no over-sending)

Before contacting Resend, `Send` **reserves** a slot by atomically incrementing
both the daily and monthly counters. A single round-trip decides: if the value
returned by the increment exceeds the quota, the reservation overflowed and is
rolled back (**release**), and `Send` returns a typed error. If Resend accepts
the message (200), the reservation is **committed** (nothing more to do). If the
send fails at any point before Resend accepts it — marshal error, network error,
timeout, non-200 status — the reservation is **released**.

This makes the check atomic: concurrent sends can never collectively exceed the
quota. Release failures (e.g. store unavailable) are logged and swallowed;
worst case is under-counting by one, which is the safe side — the library never
lets you exceed the limit, only occasionally block slightly early.

Both quotas **block**. The library only reports; the caller decides what to do:

```go
_, err := client.Send(ctx, msg)
switch {
case errors.Is(err, goresend.ErrDailyQuotaExceeded):
    // e.g. retry tomorrow, drop, or alert — your call
case errors.Is(err, goresend.ErrMonthlyQuotaExceeded):
    // e.g. escalate / upgrade plan
case err != nil:
    // transport / API error
}
```

## QuotaStore adapter

Counters are persisted through your own store so limits hold across instances
and restarts. The `ttl` passed to `Increment` applies **only on the first write
of a window** (the store must keep the original expiry on later increments) so
counters reset at the UTC day / month boundary.

```go
type QuotaStore interface {
    // delta is +1 to reserve, -1 to release. Returns the new value.
    Increment(ctx context.Context, key string, delta int64, ttl time.Duration) (int64, error)
    Get(ctx context.Context, key string) (int64, error)
}
```

`Get` must return `0, nil` when the key is absent (no sentinel error required).

Redis adapter example (single method handles both reserve `+1` and release `-1`):

```go
type RedisStore struct{ rdb *redis.Client }

func (s RedisStore) Increment(ctx context.Context, key string, delta int64, ttl time.Duration) (int64, error) {
    n, err := s.rdb.IncrBy(ctx, key, delta).Result()
    if err != nil {
        return 0, err
    }
    // First write of the window sets the expiry; subsequent increments (and
    // releases) must NOT reset it, so the counter resets at the boundary.
    if delta > 0 && n == delta {
        _ = s.rdb.Expire(ctx, key, ttl).Err()
    }
    return n, nil
}

func (s RedisStore) Get(ctx context.Context, key string) (int64, error) {
    n, err := s.rdb.Get(ctx, key).Int64()
    if err == redis.Nil {
        return 0, nil
    }
    return n, err
}
```

## Logger

Minimal, structured-logging friendly. Pass `goresend.NoopLogger()` to silence.

```go
type Logger interface {
    Info(ctx context.Context, msg string, keyvals ...any)
    Warn(ctx context.Context, msg string, keyvals ...any)
    Error(ctx context.Context, msg string, keyvals ...any)
}
```

## Attachments

Attachments map to the Resend `attachments` field as
`[{filename, content, content_type}]`. You pass raw `[]byte`; the package
base64-encodes `content` before sending. `ContentType` is optional (Resend
derives it from the filename when omitted). Resend caps total payload at 40MB
after base64 encoding.

## Mock

`goresend.NewMock(logger, daily, monthly)` returns a `*MockClient` that logs
instead of sending — useful for running dev without real credentials. Its
`Send` matches `*Client`'s signature but always returns an empty `Result`
(there is no real Resend message id to report).

## Templating

This package sends pre-rendered HTML; it does not render templates or ship any
branded copy. Template loading and rendering belong to the application (in
LeaderPro they lived in `template.go`, tied to on-disk template layout and
product-specific variables, so they were intentionally left out).

## Contributing

**Contributions are welcomed. :)**

1. Fork the repository
2. Create a new feature branch (`git checkout -b feature/<FEATURE NAME>`)
3. Make the necessary changes
4. Commit your changes (`git commit -m "Add some feature"`)
5. Push your changes to your forked repository (`git push origin feature/<FEATURE NAME>`)
6. Create a pull request to the main branch of the repository

## License

goresend is [MIT licensed](./LICENSE).
