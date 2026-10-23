package cache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/config"
)

type rangeOutcomeTestResult struct {
	found   bool
	data    []byte
	written int64
	err     error
}

type rangeOutcomeTestClient struct {
	cacheclient.CacheClient
	result        rangeOutcomeTestResult
	outcomeCalls  int
	fallbackCalls int
	key           string
	start, end    int64
}

func (c *rangeOutcomeTestClient) GetRangeStreamOutcome(_ context.Context, key string, start, end int64, w io.Writer) (bool, int64, error) {
	c.outcomeCalls++
	c.key, c.start, c.end = key, start, end
	if len(c.result.data) > 0 {
		n, err := w.Write(c.result.data)
		if err != nil {
			return c.result.found, int64(n), err
		}
		if n != len(c.result.data) {
			return c.result.found, int64(n), io.ErrShortWrite
		}
	}
	return c.result.found, c.result.written, c.result.err
}

func (c *rangeOutcomeTestClient) GetRangeStream(context.Context, string, int64, int64, io.Writer) error {
	c.fallbackCalls++
	return errors.New("legacy range method called for outcome client")
}

func TestGetRangeStreamByKey_OutcomeCapability(t *testing.T) {
	preWriteErr := errors.New("range lookup failed")
	midStreamErr := errors.New("range stream interrupted")
	cases := []struct {
		name            string
		result          rangeOutcomeTestResult
		wantBody        string
		wantErr         error
		wantErrContains string
	}{
		{
			name:     "hit",
			result:   rangeOutcomeTestResult{found: true, data: []byte("bcd"), written: 3},
			wantBody: "bcd",
		},
		{
			name:     "absent body",
			result:   rangeOutcomeTestResult{found: false},
			wantBody: "",
			wantErr:  ErrNotFound,
		},
		{
			name:     "present but zero bytes",
			result:   rangeOutcomeTestResult{found: true},
			wantBody: "",
			wantErr:  ErrNotFound,
		},
		{
			name:     "positive short read",
			result:   rangeOutcomeTestResult{found: true, data: []byte("bc"), written: 2},
			wantBody: "bc",
			wantErr:  io.ErrUnexpectedEOF,
		},
		{
			name:            "over-read",
			result:          rangeOutcomeTestResult{found: true, data: []byte("bcde"), written: 4},
			wantBody:        "bcde",
			wantErrContains: "cache range over-read",
		},
		{
			name:     "error before first byte",
			result:   rangeOutcomeTestResult{err: preWriteErr},
			wantBody: "",
			wantErr:  preWriteErr,
		},
		{
			name:     "error after partial write",
			result:   rangeOutcomeTestResult{found: true, data: []byte("b"), written: 1, err: midStreamErr},
			wantBody: "b",
			wantErr:  midStreamErr,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.NewDefault()
			client := &rangeOutcomeTestClient{
				CacheClient: cacheclient.NewMemoryCache(),
				result:      tc.result,
			}
			store := NewCacheWithClient(client, &cfg.Cache)
			var body bytes.Buffer
			err := store.GetRangeStream(context.Background(), "bucket", "key", `"v1"`, 1, 3, &body)

			if got := body.String(); got != tc.wantBody {
				t.Fatalf("body = %q, want %q", got, tc.wantBody)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want errors.Is(%v)", err, tc.wantErr)
			}
			if tc.wantErrContains != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErrContains)) {
				t.Fatalf("error = %v, want text %q", err, tc.wantErrContains)
			}
			if tc.wantErr == nil && tc.wantErrContains == "" && err != nil {
				t.Fatalf("error = %v, want nil", err)
			}
			if client.outcomeCalls != 1 || client.fallbackCalls != 0 {
				t.Fatalf("range dispatch = (outcome=%d fallback=%d), want (1, 0)", client.outcomeCalls, client.fallbackCalls)
			}
			if client.key != MakeBodyKey("bucket", "key", `"v1"`) || client.start != 1 || client.end != 3 {
				t.Fatalf("range input = (%q, %d, %d), want (%q, 1, 3)", client.key, client.start, client.end, MakeBodyKey("bucket", "key", `"v1"`))
			}
		})
	}
}
