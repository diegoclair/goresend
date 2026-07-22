package goresend

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	cacheKeyDailyCount   = "goresend:daily_count"
	cacheKeyMonthlyCount = "goresend:monthly_count"
)

// Quota errors are sentinels: the caller decides what to do (retry later, drop,
// alert). The library only blocks and reports; it makes no business decision.
var (
	ErrDailyQuotaExceeded   = errors.New("goresend: daily quota exceeded")
	ErrMonthlyQuotaExceeded = errors.New("goresend: monthly quota exceeded")
)

func timeUntilMidnightUTC() time.Duration {
	now := time.Now().UTC()
	next := now.Truncate(24 * time.Hour).Add(24 * time.Hour)
	return next.Sub(now)
}

func timeUntilNextMonthUTC() time.Duration {
	now := time.Now().UTC()
	next := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, 0)
	return next.Sub(now)
}

// reserve atomically bumps both counters before the send. A single round-trip
// increment decides whether the slot fits: if the returned value exceeds the
// quota, the reservation overflowed and is rolled back. Daily is reserved
// first, so on a monthly overflow the daily reservation is released too.
func (c *Client) reserve(ctx context.Context) error {
	daily, err := c.store.Increment(ctx, cacheKeyDailyCount, 1, timeUntilMidnightUTC())
	if err != nil {
		return fmt.Errorf("goresend: reserve daily count: %w", err)
	}
	if daily > int64(c.dailyQuota) {
		c.releaseKey(ctx, cacheKeyDailyCount)
		return fmt.Errorf("%w (%d/%d)", ErrDailyQuotaExceeded, daily-1, c.dailyQuota)
	}

	monthly, err := c.store.Increment(ctx, cacheKeyMonthlyCount, 1, timeUntilNextMonthUTC())
	if err != nil {
		c.releaseKey(ctx, cacheKeyDailyCount)
		return fmt.Errorf("goresend: reserve monthly count: %w", err)
	}
	if monthly > int64(c.monthlyQuota) {
		c.releaseKey(ctx, cacheKeyMonthlyCount)
		c.releaseKey(ctx, cacheKeyDailyCount)
		return fmt.Errorf("%w (%d/%d)", ErrMonthlyQuotaExceeded, monthly-1, c.monthlyQuota)
	}
	return nil
}

// release undoes a successful reserve when the send fails before Resend accepts
// it. A release failure is logged and swallowed: worst case is under-counting by
// one, the safe side (never let the caller exceed the quota).
func (c *Client) release(ctx context.Context) {
	c.releaseKey(ctx, cacheKeyMonthlyCount)
	c.releaseKey(ctx, cacheKeyDailyCount)
}

func (c *Client) releaseKey(ctx context.Context, key string) {
	if _, err := c.store.Increment(ctx, key, -1, 0); err != nil {
		c.log.Error(ctx, "goresend: failed to release reserved quota", "key", key, "error", err)
	}
}

func (c *Client) processQueue() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.requestInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.shutdown:
			return
		case <-ticker.C:
			select {
			case req := <-c.queue:
				c.sendToAPI(req)
			default:
			}
		}
	}
}
