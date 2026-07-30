package goresend

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type fakeStore struct {
	mu     sync.Mutex
	counts map[string]int64
}

func newFakeStore() *fakeStore { return &fakeStore{counts: map[string]int64{}} }

func (s *fakeStore) Increment(_ context.Context, key string, delta int64, _ time.Duration) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counts[key] += delta
	return s.counts[key], nil
}

func (s *fakeStore) Get(_ context.Context, key string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[key], nil
}

func (s *fakeStore) get(key string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[key]
}

func baseConfig() Config {
	return Config{
		APIKey:       "test-key",
		FromEmail:    "from@example.com",
		DailyQuota:   10,
		MonthlyQuota: 100,
		PerSecond:    100,
	}
}

// newTestClient builds a Client whose HTTP requests are redirected to srv
// instead of the fixed Resend URL.
func newTestClient(t *testing.T, cfg Config, store QuotaStore, srv *httptest.Server) *Client {
	t.Helper()
	c, err := New(cfg, store, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	withServer(c, srv)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = c.Shutdown(ctx)
	})
	return c
}

func okServer(t *testing.T, seen *[]byte) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if seen != nil {
			*seen = b
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"abc"}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// redirectTransport rewrites the request URL to the test server, since
// resendAPIURL is a fixed const.
type redirectTransport struct {
	base *httptest.Server
	rt   http.RoundTripper
}

func (rt redirectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	u, _ := req.URL.Parse(rt.base.URL)
	req.URL.Scheme = u.Scheme
	req.URL.Host = u.Host
	return rt.rt.RoundTrip(req)
}

func withServer(c *Client, srv *httptest.Server) {
	client := srv.Client()
	client.Transport = redirectTransport{base: srv, rt: client.Transport}
	c.httpClient = client
}

func TestNew_Validation(t *testing.T) {
	store := newFakeStore()
	cases := map[string]func(*Config){
		"zero daily":    func(c *Config) { c.DailyQuota = 0 },
		"zero monthly":  func(c *Config) { c.MonthlyQuota = 0 },
		"empty api key": func(c *Config) { c.APIKey = "" },
		"empty from":    func(c *Config) { c.FromEmail = "" },
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := baseConfig()
			mut(&cfg)
			if _, err := New(cfg, store, nil); err == nil {
				t.Fatalf("expected error for %s", name)
			}
		})
	}
	t.Run("nil store", func(t *testing.T) {
		if _, err := New(baseConfig(), nil, nil); err == nil {
			t.Fatal("expected error for nil store")
		}
	})
}

func TestSend_SuccessIncrementsCounters(t *testing.T) {
	var body []byte
	srv := okServer(t, &body)
	store := newFakeStore()
	c := newTestClient(t, baseConfig(), store, srv)

	if _, err := c.Send(context.Background(), Message{To: "u@example.com", Subject: "s", HTML: "<p>hi</p>"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got := store.get(cacheKeyDailyCount); got != 1 {
		t.Errorf("daily = %d, want 1", got)
	}
	if got := store.get(cacheKeyMonthlyCount); got != 1 {
		t.Errorf("monthly = %d, want 1", got)
	}
}

func TestSend_FailureReleasesReservation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"message":"boom"}`))
	}))
	t.Cleanup(srv.Close)

	store := newFakeStore()
	c := newTestClient(t, baseConfig(), store, srv)

	_, err := c.Send(context.Background(), Message{To: "u@example.com", Subject: "s", HTML: "x"})
	if err == nil {
		t.Fatal("expected send error")
	}
	if got := store.get(cacheKeyDailyCount); got != 0 {
		t.Errorf("daily after release = %d, want 0", got)
	}
	if got := store.get(cacheKeyMonthlyCount); got != 0 {
		t.Errorf("monthly after release = %d, want 0", got)
	}
}

func TestSend_DailyQuotaExceeded(t *testing.T) {
	srv := okServer(t, nil)
	store := newFakeStore()
	cfg := baseConfig()
	cfg.DailyQuota = 2
	c := newTestClient(t, cfg, store, srv)

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := c.Send(ctx, Message{To: "u@example.com", Subject: "s", HTML: "x"}); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	_, err := c.Send(ctx, Message{To: "u@example.com", Subject: "s", HTML: "x"})
	if !errors.Is(err, ErrDailyQuotaExceeded) {
		t.Fatalf("err = %v, want ErrDailyQuotaExceeded", err)
	}
	// Overflow reservation must be rolled back, not left counted.
	if got := store.get(cacheKeyDailyCount); got != 2 {
		t.Errorf("daily = %d, want 2 (overflow released)", got)
	}
}

func TestSend_MonthlyQuotaExceeded(t *testing.T) {
	srv := okServer(t, nil)
	store := newFakeStore()
	cfg := baseConfig()
	cfg.DailyQuota = 100
	cfg.MonthlyQuota = 1
	c := newTestClient(t, cfg, store, srv)

	ctx := context.Background()
	if _, err := c.Send(ctx, Message{To: "u@example.com", Subject: "s", HTML: "x"}); err != nil {
		t.Fatalf("first send: %v", err)
	}
	_, err := c.Send(ctx, Message{To: "u@example.com", Subject: "s", HTML: "x"})
	if !errors.Is(err, ErrMonthlyQuotaExceeded) {
		t.Fatalf("err = %v, want ErrMonthlyQuotaExceeded", err)
	}
	// Daily was reserved first then released on the monthly overflow.
	if got := store.get(cacheKeyDailyCount); got != 1 {
		t.Errorf("daily = %d, want 1 (overflow daily released)", got)
	}
	if got := store.get(cacheKeyMonthlyCount); got != 1 {
		t.Errorf("monthly = %d, want 1 (overflow released)", got)
	}
}

func TestSend_ReserveBlocksThird(t *testing.T) {
	srv := okServer(t, nil)
	store := newFakeStore()
	cfg := baseConfig()
	cfg.DailyQuota = 2
	c := newTestClient(t, cfg, store, srv)

	ctx := context.Background()
	sent := 0
	for i := 0; i < 3; i++ {
		if _, err := c.Send(ctx, Message{To: "u@example.com", Subject: "s", HTML: "x"}); err == nil {
			sent++
		}
	}
	if sent != 2 {
		t.Fatalf("sent = %d, want 2 (third blocked)", sent)
	}
}

func TestSend_AttachmentBase64(t *testing.T) {
	var body []byte
	srv := okServer(t, &body)
	store := newFakeStore()
	c := newTestClient(t, baseConfig(), store, srv)

	raw := []byte("hello-pdf-bytes")
	_, err := c.Send(context.Background(), Message{
		To: "u@example.com", Subject: "s", HTML: "x",
		Attachments: []Attachment{{Filename: "f.pdf", Content: raw, ContentType: "application/pdf"}},
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	var req resendRequest
	if err := json.Unmarshal(body, &req); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if len(req.Attachments) != 1 {
		t.Fatalf("attachments = %d, want 1", len(req.Attachments))
	}
	want := base64.StdEncoding.EncodeToString(raw)
	if req.Attachments[0].Content != want {
		t.Errorf("content = %q, want %q", req.Attachments[0].Content, want)
	}
	if req.Attachments[0].Filename != "f.pdf" {
		t.Errorf("filename = %q", req.Attachments[0].Filename)
	}
}

func TestSend_ReturnsID(t *testing.T) {
	srv := okServer(t, nil)
	store := newFakeStore()
	c := newTestClient(t, baseConfig(), store, srv)

	result, err := c.Send(context.Background(), Message{To: "u@example.com", Subject: "s", HTML: "<p>hi</p>"})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if result.ID != "abc" {
		t.Errorf("ID = %q, want %q", result.ID, "abc")
	}
}

func TestSend_FailureReturnsEmptyResult(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"message":"boom"}`))
	}))
	t.Cleanup(srv.Close)

	store := newFakeStore()
	c := newTestClient(t, baseConfig(), store, srv)

	result, err := c.Send(context.Background(), Message{To: "u@example.com", Subject: "s", HTML: "x"})
	if err == nil {
		t.Fatal("expected send error")
	}
	if result.ID != "" {
		t.Errorf("ID = %q, want empty on failure", result.ID)
	}
}

func TestNew_DefaultTimeout(t *testing.T) {
	store := newFakeStore()
	c, err := New(baseConfig(), store, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown(context.Background()) })
	if c.httpClient.Timeout != defaultTimeout {
		t.Errorf("timeout = %v, want %v", c.httpClient.Timeout, defaultTimeout)
	}

	cfg := baseConfig()
	cfg.Timeout = 5 * time.Second
	c2, err := New(cfg, store, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c2.Shutdown(context.Background()) })
	if c2.httpClient.Timeout != 5*time.Second {
		t.Errorf("timeout = %v, want 5s", c2.httpClient.Timeout)
	}
}
