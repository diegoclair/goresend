package goresend

import "context"

// MockClient logs instead of sending. Use in dev/tests. It ignores quotas.
type MockClient struct {
	log          Logger
	dailyQuota   int
	monthlyQuota int
}

// NewMock returns a MockClient. Quotas are only reported by DailyQuota /
// MonthlyQuota; nothing is enforced.
func NewMock(log Logger, dailyQuota, monthlyQuota int) *MockClient {
	if log == nil {
		log = NoopLogger()
	}
	return &MockClient{log: log, dailyQuota: dailyQuota, monthlyQuota: monthlyQuota}
}

func (c *MockClient) Send(ctx context.Context, msg Message) (Result, error) {
	c.log.Info(ctx, "goresend: [MOCK] would send email",
		"to", msg.To, "subject", msg.Subject,
		"attachments", len(msg.Attachments), "headers", len(msg.Headers))
	return Result{}, nil
}

func (c *MockClient) DailyQuota() int   { return c.dailyQuota }
func (c *MockClient) MonthlyQuota() int { return c.monthlyQuota }

// RemainingDaily always reports the full quota: the mock enforces nothing, so
// pretending to count down would be a number with no meaning behind it.
func (c *MockClient) RemainingDaily(context.Context) (int, error) {
	return c.dailyQuota, nil
}

func (c *MockClient) Shutdown(context.Context) error { return nil }
