// Package sink delivers change events: NDJSON to stdout or a file, batches to
// an HTTP endpoint, or bulk requests to another Elasticsearch/OpenSearch cluster.
package sink

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Jwrede/es-changefeed/internal/cluster"
)

// Event is one change. Op is "upsert" (the current state of a created or
// updated document) or "delete".
type Event struct {
	Op          string          `json:"op"`
	Index       string          `json:"index"`
	IndexUUID   string          `json:"index_uuid"`
	Shard       int             `json:"shard"`
	ID          string          `json:"id"`
	Routing     string          `json:"routing,omitempty"`
	SeqNo       int64           `json:"seq_no,omitempty"`
	PrimaryTerm int64           `json:"primary_term,omitempty"`
	Version     int64           `json:"version,omitempty"`
	Source      json.RawMessage `json:"source,omitempty"`
}

// MarshalJSON writes seq_no for every upsert, including _seq_no 0, and
// leaves it out for deletes, which have none.
func (e Event) MarshalJSON() ([]byte, error) {
	var seq *int64
	if e.Op != "delete" {
		v := e.SeqNo
		seq = &v
	}
	return json.Marshal(struct {
		Op          string          `json:"op"`
		Index       string          `json:"index"`
		IndexUUID   string          `json:"index_uuid"`
		Shard       int             `json:"shard"`
		ID          string          `json:"id"`
		Routing     string          `json:"routing,omitempty"`
		SeqNo       *int64          `json:"seq_no,omitempty"`
		PrimaryTerm int64           `json:"primary_term,omitempty"`
		Version     int64           `json:"version,omitempty"`
		Source      json.RawMessage `json:"source,omitempty"`
	}{e.Op, e.Index, e.IndexUUID, e.Shard, e.ID, e.Routing, seq, e.PrimaryTerm, e.Version, e.Source})
}

// Sink delivers a batch. Write returns only after the batch is durable at the
// destination; on error the caller retries the same batch.
type Sink interface {
	Write(ctx context.Context, batch []Event) error
	Close() error
}

// Writer writes NDJSON lines to w and flushes after each batch.
type Writer struct {
	mu   sync.Mutex
	w    *bufio.Writer
	f    *os.File // optional, synced after each batch
	keep io.Closer
}

// NewWriter wraps w (for example os.Stdout).
func NewWriter(w io.Writer) *Writer { return &Writer{w: bufio.NewWriterSize(w, 256*1024)} }

// NewFile appends to path and fsyncs after each batch.
func NewFile(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &Writer{w: bufio.NewWriterSize(f, 256*1024), f: f, keep: f}, nil
}

func (s *Writer) Write(_ context.Context, batch []Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range batch {
		b, err := json.Marshal(&batch[i])
		if err != nil {
			return err
		}
		s.w.Write(b)
		s.w.WriteByte('\n')
	}
	if err := s.w.Flush(); err != nil {
		return err
	}
	if s.f != nil {
		return s.f.Sync()
	}
	return nil
}

func (s *Writer) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.w.Flush()
	if s.keep != nil {
		if e := s.keep.Close(); err == nil {
			err = e
		}
	}
	return err
}

// HTTP posts each batch as an NDJSON body. Any 2xx response acknowledges the batch.
type HTTP struct {
	URL     string
	Headers http.Header
	Client  *http.Client
}

func (s *HTTP) Write(ctx context.Context, batch []Event) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for i := range batch {
		if err := enc.Encode(&batch[i]); err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, "POST", s.URL, &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	for k, v := range s.Headers {
		req.Header[k] = v
	}
	c := s.Client
	if c == nil {
		c = &http.Client{Timeout: 60 * time.Second}
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("http sink: %s: HTTP %d: %s", s.URL, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func (s *HTTP) Close() error { return nil }

// Bulk mirrors events into another cluster with the _bulk API: upserts become
// index actions with the original routing, deletes become delete actions.
// IndexTemplate names the target index; "{index}" is replaced by the source
// index name.
type Bulk struct {
	Client        *cluster.Client
	IndexTemplate string
}

func (s *Bulk) target(ev *Event) string {
	t := s.IndexTemplate
	if t == "" {
		t = "{index}"
	}
	return strings.ReplaceAll(t, "{index}", ev.Index)
}

func (s *Bulk) Write(ctx context.Context, batch []Event) error {
	if len(batch) == 0 {
		return nil
	}
	var buf bytes.Buffer
	for i := range batch {
		ev := &batch[i]
		meta := map[string]any{"_index": s.target(ev), "_id": ev.ID}
		if ev.Routing != "" {
			meta["routing"] = ev.Routing
		}
		switch ev.Op {
		case "upsert":
			b, _ := json.Marshal(map[string]any{"index": meta})
			buf.Write(b)
			buf.WriteByte('\n')
			buf.Write(ev.Source)
			buf.WriteByte('\n')
		case "delete":
			b, _ := json.Marshal(map[string]any{"delete": meta})
			buf.Write(b)
			buf.WriteByte('\n')
		default:
			return fmt.Errorf("bulk sink: unknown op %q", ev.Op)
		}
	}
	fails, err := s.Client.Bulk(ctx, buf.Bytes())
	if err != nil {
		return err
	}
	if len(fails) > 0 {
		for i, f := range fails {
			return fmt.Errorf("bulk sink: %d of %d items failed, e.g. %s %s: HTTP %d %s",
				len(fails), len(batch), batch[i].Op, batch[i].ID, f.Status, f.Reason)
		}
	}
	return nil
}

func (s *Bulk) Close() error { return nil }
