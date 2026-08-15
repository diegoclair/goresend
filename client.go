package goresend

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

const (
	resendAPIURL   = "https://api.resend.com/emails"
	defaultTimeout = 30 * time.Second
)

// Config is the explicit configuration for a Client. All quota values must be
// positive; New fails otherwise (no hidden defaults).
type Config struct {
	APIKey       string
	FromEmail    string
	FromName     string
	DailyQuota   int
	MonthlyQuota int
	PerSecond    int
	// Timeout bounds each HTTP request to Resend. Zero uses defaultTimeout (30s).
	Timeout time.Duration
}

// Attachment is a file to attach to an email. Content is raw bytes; the package
// base64-encodes it for the Resend API.
type Attachment struct {
	Filename    string
	Content     []byte
	ContentType string
}

// Message is a single email to send. HTML is the rendered body; templating is
// the caller's responsibility.
type Message struct {
	To          string
	Subject     string
	HTML        string
	Attachments []Attachment
	// Headers are extra SMTP headers, sent verbatim. The reason this exists is
	// List-Unsubscribe + List-Unsubscribe-Post (RFC 8058): Gmail and Yahoo want
	// one-click opt-out from bulk senders, and it can only travel as a header.
	Headers map[string]string
}

// Result is returned by Send on success. ID is the Resend message id, the
// only reliable key to correlate a later delivery/open webhook with the send
// that caused it.
type Result struct {
	ID string
}

// Client sends email through Resend with daily/monthly/per-second rate limits.
type Client struct {
	apiKey     string
	fromEmail  string
	fromName   string
	httpClient *http.Client
	log        Logger
	store      QuotaStore

	dailyQuota      int
	monthlyQuota    int
	requestInterval time.Duration
	queueSize       int

	queue    chan emailRequest
	shutdown chan struct{}
	wg       sync.WaitGroup
}

type emailRequest struct {
	ctx      context.Context
	msg      Message
	resultCh chan sendOutcome
}

// sendOutcome carries both the id and the error through resultCh, since a
// successful send needs the id and a failed one only needs the error.
type sendOutcome struct {
	id  string
	err error
}

type resendAttachment struct {
	Filename    string `json:"filename"`
	Content     string `json:"content"`
	ContentType string `json:"content_type,omitempty"`
}

type resendRequest struct {
	From        string             `json:"from"`
	To          []string           `json:"to"`
	Subject     string             `json:"subject"`
	HTML        string             `json:"html"`
	Attachments []resendAttachment `json:"attachments,omitempty"`
	Headers     map[string]string  `json:"headers,omitempty"`
}

type resendResponse struct {
	ID string `json:"id"`
}

type resendError struct {
	StatusCode int    `json:"statusCode"`
	Name       string `json:"name"`
	Message    string `json:"message"`
}

// New validates cfg and starts the rate-limited email worker. The returned
// Client must be closed with Shutdown to drain the queue.
func New(cfg Config, store QuotaStore, log Logger) (*Client, error) {
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("goresend: APIKey is required")
	}
	if cfg.FromEmail == "" {
		return nil, fmt.Errorf("goresend: FromEmail is required")
	}
	if cfg.DailyQuota <= 0 {
		return nil, fmt.Errorf("goresend: DailyQuota must be > 0, got %d", cfg.DailyQuota)
	}
	if cfg.MonthlyQuota <= 0 {
		return nil, fmt.Errorf("goresend: MonthlyQuota must be > 0, got %d", cfg.MonthlyQuota)
	}
	if cfg.PerSecond <= 0 {
		return nil, fmt.Errorf("goresend: PerSecond must be > 0, got %d", cfg.PerSecond)
	}
	if store == nil {
		return nil, fmt.Errorf("goresend: QuotaStore is required")
	}
	if log == nil {
		log = NoopLogger()
	}

	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = defaultTimeout
	}

	// 10% safety margin below the provider's hard per-second rate.
	requestInterval := time.Duration(float64(time.Second) / float64(cfg.PerSecond) * 1.1)

	c := &Client{
		apiKey:          cfg.APIKey,
		fromEmail:       cfg.FromEmail,
		fromName:        cfg.FromName,
		httpClient:      &http.Client{Timeout: timeout},
		log:             log,
		store:           store,
		dailyQuota:      cfg.DailyQuota,
		monthlyQuota:    cfg.MonthlyQuota,
		requestInterval: requestInterval,
		queueSize:       cfg.DailyQuota + 50,
		shutdown:        make(chan struct{}),
	}
	c.queue = make(chan emailRequest, c.queueSize)

	c.wg.Add(1)
	go c.processQueue()

	return c, nil
}

// Send reserves a quota slot, enqueues the message, and blocks until it is sent,
// the context is done, or the queue is full. The reservation is atomic (reserve
// before send); any failure after reserving releases the slot, so quotas can
// never be exceeded under normal operation. On success, Result carries the
// Resend message id for correlating a later delivery/open webhook.
func (c *Client) Send(ctx context.Context, msg Message) (result Result, err error) {
	if err = c.reserve(ctx); err != nil {
		return Result{}, err
	}
	// Release on every path that fails after a successful reserve; a nil return
	// means Resend accepted the message and the reservation is committed.
	defer func() {
		if err != nil {
			c.release(ctx)
		}
	}()

	resultCh := make(chan sendOutcome, 1)
	req := emailRequest{ctx: ctx, msg: msg, resultCh: resultCh}

	select {
	case c.queue <- req:
		select {
		case outcome := <-resultCh:
			err = outcome.err
			return Result{ID: outcome.id}, err
		case <-ctx.Done():
			err = fmt.Errorf("goresend: context cancelled while waiting for send: %w", ctx.Err())
			return Result{}, err
		}
	case <-ctx.Done():
		err = fmt.Errorf("goresend: context cancelled while queueing: %w", ctx.Err())
		return Result{}, err
	default:
		err = fmt.Errorf("goresend: queue is full (%d waiting), try again later", c.queueSize)
		return Result{}, err
	}
}

func (c *Client) sendToAPI(req emailRequest) {
	body := resendRequest{
		From:    c.fromAddress(),
		To:      []string{req.msg.To},
		Subject: req.msg.Subject,
		HTML:    req.msg.HTML,
		Headers: req.msg.Headers,
	}
	for _, a := range req.msg.Attachments {
		body.Attachments = append(body.Attachments, resendAttachment{
			Filename:    a.Filename,
			Content:     base64.StdEncoding.EncodeToString(a.Content),
			ContentType: a.ContentType,
		})
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		c.reply(req, sendOutcome{err: fmt.Errorf("goresend: marshal request: %w", err)})
		return
	}

	httpReq, err := http.NewRequestWithContext(req.ctx, http.MethodPost, resendAPIURL, bytes.NewReader(jsonBody))
	if err != nil {
		c.reply(req, sendOutcome{err: fmt.Errorf("goresend: create request: %w", err)})
		return
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		c.reply(req, sendOutcome{err: fmt.Errorf("goresend: send request: %w", err)})
		return
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		c.reply(req, sendOutcome{err: fmt.Errorf("goresend: read response: %w", err)})
		return
	}

	if resp.StatusCode != http.StatusOK {
		var errResp resendError
		if json.Unmarshal(respBody, &errResp) != nil || errResp.Message == "" {
			c.reply(req, sendOutcome{err: fmt.Errorf("goresend: API error (status %d): %s", resp.StatusCode, string(respBody))})
		} else {
			c.reply(req, sendOutcome{err: fmt.Errorf("goresend: API error: %s - %s", errResp.Name, errResp.Message)})
		}
		return
	}

	var success resendResponse
	if json.Unmarshal(respBody, &success) == nil {
		c.log.Info(req.ctx, "goresend: email sent", "email_id", success.ID, "to", req.msg.To)
	}

	c.reply(req, sendOutcome{id: success.ID})
}

func (c *Client) fromAddress() string {
	if c.fromName == "" {
		return c.fromEmail
	}
	return fmt.Sprintf("%s <%s>", c.fromName, c.fromEmail)
}

func (c *Client) reply(req emailRequest, outcome sendOutcome) {
	select {
	case req.resultCh <- outcome:
	default:
	}
}

// RemainingDaily reports how many sends still fit today. It exists so a caller
// draining a large batch can stop short of the limit and leave room for the
// messages a user is waiting on — the quota is one bucket, and this package
// deliberately does not rank what goes into it.
//
// The number is a snapshot, not a reservation: concurrent senders may consume
// slots between the read and the next Send. Treat it as a budget, not a promise.
func (c *Client) RemainingDaily(ctx context.Context) (int, error) {
	used, err := c.store.Get(ctx, cacheKeyDailyCount)
	if err != nil {
		return 0, fmt.Errorf("goresend: read daily count: %w", err)
	}

	remaining := c.dailyQuota - int(used)
	if remaining < 0 {
		return 0, nil
	}
	return remaining, nil
}

// DailyQuota returns the configured daily limit.
func (c *Client) DailyQuota() int { return c.dailyQuota }

// MonthlyQuota returns the configured monthly limit.
func (c *Client) MonthlyQuota() int { return c.monthlyQuota }

// Shutdown stops the worker and waits for the in-flight queue to drain, or
// until ctx is done.
func (c *Client) Shutdown(ctx context.Context) error {
	close(c.shutdown)

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("goresend: shutdown timeout: %w", ctx.Err())
	}
}
