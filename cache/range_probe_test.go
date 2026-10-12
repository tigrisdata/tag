package cache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/config"
)

type rangeProbeClient struct {
	cacheclient.CacheClient
	writes [][]byte
	err    error
	start  int64
	end    int64
}

func (c *rangeProbeClient) GetRangeStream(_ context.Context, _ string, start, end int64, w io.Writer) error {
	c.start = start
	c.end = end
	for _, data := range c.writes {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n != len(data) {
			return io.ErrShortWrite
		}
	}
	return c.err
}

func newRangeProbeCache(t testing.TB, client *rangeProbeClient) *Cache {
	t.Helper()
	cfg := config.NewDefault()
	return NewCacheWithClient(client, &cfg.Cache)
}

func TestGetRangeStream_ByteZeroPreservesProbeSemantics(t *testing.T) {
	backendErr := errors.New("backend unavailable")
	tests := []struct {
		name       string
		writes     [][]byte
		backendErr error
		want       []byte
		wantErr    error
	}{
		{
			name:   "present forwards only first byte",
			writes: [][]byte{{0, 1}},
			want:   []byte{0},
		},
		{
			name:   "fragmented response keeps first nonempty byte",
			writes: [][]byte{nil, {0}, {1}},
			want:   []byte{0},
		},
		{
			name:    "zero-byte response is a miss",
			writes:  [][]byte{nil},
			wantErr: ErrNotFound,
		},
		{
			name:       "backend error before output is preserved",
			backendErr: backendErr,
			wantErr:    backendErr,
		},
		{
			name:       "backend error after output is preserved without forwarding",
			writes:     [][]byte{{'x', 'y'}},
			backendErr: backendErr,
			wantErr:    backendErr,
		},
		{
			name:       "not found after output remains a miss without forwarding",
			writes:     [][]byte{{'x', 'y'}},
			backendErr: errors.New("key not found"),
			wantErr:    ErrNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &rangeProbeClient{writes: tc.writes, err: tc.backendErr}
			c := newRangeProbeCache(t, client)
			var got bytes.Buffer

			err := c.GetRangeStream(context.Background(), "bucket", "key", `"etag"`, 0, 0, &got)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("GetRangeStream error = %v, want %v", err, tc.wantErr)
			}
			if !bytes.Equal(got.Bytes(), tc.want) {
				t.Fatalf("GetRangeStream bytes = %v, want %v", got.Bytes(), tc.want)
			}
			if client.start != 0 || client.end != 1 {
				t.Fatalf("backend range = [%d,%d], want [0,1]", client.start, client.end)
			}
		})
	}
}

type errorWriter struct {
	err   error
	calls int
	data  []byte
}

func (w *errorWriter) Write(p []byte) (int, error) {
	w.calls++
	w.data = append(w.data, p...)
	return 0, w.err
}

func TestGetRangeStream_ByteZeroReturnsOriginalWriterError(t *testing.T) {
	wantErr := errors.New("destination failed")
	client := &rangeProbeClient{writes: [][]byte{{'x', 'y'}}}
	c := newRangeProbeCache(t, client)
	writer := &errorWriter{err: wantErr}

	err := c.GetRangeStream(context.Background(), "bucket", "key", `"etag"`, 0, 0, writer)
	if !errors.Is(err, wantErr) {
		t.Fatalf("GetRangeStream error = %v, want %v", err, wantErr)
	}
	if writer.calls != 1 || !bytes.Equal(writer.data, []byte{'x'}) {
		t.Fatalf("destination writes = (calls=%d, data=%v), want (1, [120])", writer.calls, writer.data)
	}
}

func TestBlockExistsErr_ByteZeroUsesSameRangeProbe(t *testing.T) {
	client := &rangeProbeClient{writes: [][]byte{{'x', 'y'}}}
	c := newRangeProbeCache(t, client)

	present, err := c.BlockExistsErr(context.Background(), "bucket", "key", `"etag"`, 4, 2)
	if !present || err != nil {
		t.Fatalf("BlockExistsErr = (present=%v, err=%v), want (true, nil)", present, err)
	}
	if client.start != 0 || client.end != 1 {
		t.Fatalf("backend range = [%d,%d], want [0,1]", client.start, client.end)
	}
}
