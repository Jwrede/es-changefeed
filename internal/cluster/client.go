// Package cluster is a small HTTP client for the Elasticsearch and OpenSearch
// REST APIs that es-changefeed needs: index resolution, shard-level sequence
// number stats, refresh, range searches on _seq_no, scroll and bulk.
package cluster

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Config describes how to reach a cluster.
type Config struct {
	URL      string
	User     string
	Password string
	APIKey   string // sent as "Authorization: ApiKey <key>"
	CACert   string // path to a PEM file
	Insecure bool
	Timeout  time.Duration
	Retries  int
}

// Client talks to one cluster.
type Client struct {
	base    *url.URL
	cfg     Config
	http    *http.Client
	retries int
}

// Info is the subset of GET / that is used.
type Info struct {
	Distribution string // "elasticsearch" or "opensearch"
	Version      string
}

// StatusError is returned for non-2xx responses.
type StatusError struct {
	Method, Path string
	Status       int
	Body         string
}

func (e *StatusError) Error() string {
	b := e.Body
	if len(b) > 400 {
		b = b[:400] + "..."
	}
	return fmt.Sprintf("%s %s: HTTP %d: %s", e.Method, e.Path, e.Status, b)
}

// New builds a client from cfg.
func New(cfg Config) (*Client, error) {
	u, err := url.Parse(strings.TrimRight(cfg.URL, "/"))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("invalid cluster URL %q", cfg.URL)
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tc := &tls.Config{InsecureSkipVerify: cfg.Insecure} //nolint:gosec // opt-in flag
	if cfg.CACert != "" {
		pem, err := os.ReadFile(cfg.CACert)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates in %s", cfg.CACert)
		}
		tc.RootCAs = pool
	}
	tr.TLSClientConfig = tc
	if cfg.Timeout == 0 {
		cfg.Timeout = 60 * time.Second
	}
	r := cfg.Retries
	if r == 0 {
		r = 5
	}
	return &Client{base: u, cfg: cfg, http: &http.Client{Transport: tr, Timeout: cfg.Timeout}, retries: r}, nil
}

// Do sends a request and decodes a JSON response into out (if non-nil).
// Connection errors and 429/502/503/504 are retried with backoff.
func (c *Client) Do(ctx context.Context, method, path string, query url.Values, body []byte, contentType string, out any) error {
	var lastErr error
	for attempt := 0; attempt <= c.retries; attempt++ {
		if attempt > 0 {
			d := time.Duration(1<<uint(attempt-1)) * 200 * time.Millisecond
			if d > 5*time.Second {
				d = 5 * time.Second
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(d):
			}
		}
		err := c.once(ctx, method, path, query, body, contentType, out)
		if err == nil {
			return nil
		}
		lastErr = err
		var se *StatusError
		if errors.As(err, &se) {
			if se.Status != 429 && se.Status != 502 && se.Status != 503 && se.Status != 504 {
				return err
			}
		} else if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return lastErr
}

func (c *Client) once(ctx context.Context, method, path string, query url.Values, body []byte, contentType string, out any) error {
	u := *c.base
	u.Path = c.base.Path + path
	u.RawPath = ""
	if query != nil {
		u.RawQuery = query.Encode()
	}
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), rd)
	if err != nil {
		return err
	}
	if body != nil {
		if contentType == "" {
			contentType = "application/json"
		}
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("Accept", "application/json")
	switch {
	case c.cfg.APIKey != "":
		req.Header.Set("Authorization", "ApiKey "+c.cfg.APIKey)
	case c.cfg.User != "":
		req.SetBasicAuth(c.cfg.User, c.cfg.Password)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return &StatusError{Method: method, Path: path, Status: resp.StatusCode, Body: string(data)}
	}
	if out == nil {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	return dec.Decode(out)
}

// Info returns the distribution and version of the cluster.
func (c *Client) Info(ctx context.Context) (Info, error) {
	var r struct {
		Version struct {
			Number       string `json:"number"`
			Distribution string `json:"distribution"`
		} `json:"version"`
	}
	if err := c.Do(ctx, "GET", "/", nil, nil, "", &r); err != nil {
		return Info{}, err
	}
	d := r.Version.Distribution
	if d == "" {
		d = "elasticsearch"
	}
	return Info{Distribution: d, Version: r.Version.Number}, nil
}

// Index is a concrete index behind a pattern, alias or data stream.
type Index struct {
	Name   string
	UUID   string
	Shards int
	Closed bool
}

// escapeTarget normalizes a target expression. Index names cannot contain
// characters that need escaping in a URL path, so the URL encoder is enough.
func escapeTarget(t string) string {
	parts := strings.Split(t, ",")
	for i, p := range parts {
		parts[i] = strings.TrimSpace(p)
	}
	return strings.Join(parts, ",")
}

// ResolveIndices lists concrete indices matching target (comma-separated
// names, wildcards, aliases or data streams). expand is passed as
// expand_wildcards, for example "open,closed".
func (c *Client) ResolveIndices(ctx context.Context, target, expand string) ([]Index, error) {
	q := url.Values{}
	q.Set("format", "json")
	q.Set("h", "index,uuid,status,pri")
	q.Set("expand_wildcards", expand)
	var r []struct {
		Index  string `json:"index"`
		UUID   string `json:"uuid"`
		Status string `json:"status"`
		Pri    string `json:"pri"`
	}
	if err := c.Do(ctx, "GET", "/_cat/indices/"+escapeTarget(target), q, nil, "", &r); err != nil {
		var se *StatusError
		if errors.As(err, &se) && se.Status == 404 {
			return nil, nil
		}
		return nil, err
	}
	out := make([]Index, 0, len(r))
	for _, v := range r {
		n, err := strconv.Atoi(v.Pri)
		if err != nil || v.UUID == "" {
			return nil, fmt.Errorf("index %s: unexpected _cat/indices row", v.Index)
		}
		out = append(out, Index{Name: v.Index, UUID: v.UUID, Shards: n, Closed: v.Status == "close"})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// ShardSeq is the sequence number state of one shard, taken from the primary
// copy. GlobalCheckpoint is the highest sequence number below which every
// operation has been processed by all in-sync copies.
type ShardSeq struct {
	Shard            int
	MaxSeqNo         int64
	LocalCheckpoint  int64
	GlobalCheckpoint int64
	PrimaryNode      string
}

// SeqStats returns per-shard sequence number state for every open index
// matching target, keyed by index name.
func (c *Client) SeqStats(ctx context.Context, target string) (map[string]map[int]ShardSeq, error) {
	q := url.Values{}
	q.Set("level", "shards")
	q.Set("expand_wildcards", "open")
	q.Set("ignore_unavailable", "true")
	q.Set("filter_path", "indices.*.shards.*.routing,indices.*.shards.*.seq_no")
	var r struct {
		Indices map[string]struct {
			Shards map[string][]struct {
				Routing struct {
					Primary bool   `json:"primary"`
					Node    string `json:"node"`
					State   string `json:"state"`
				} `json:"routing"`
				SeqNo *struct {
					Max   int64 `json:"max_seq_no"`
					Local int64 `json:"local_checkpoint"`
					GCP   int64 `json:"global_checkpoint"`
				} `json:"seq_no"`
			} `json:"shards"`
		} `json:"indices"`
	}
	if err := c.Do(ctx, "GET", "/"+escapeTarget(target)+"/_stats/docs", q, nil, "", &r); err != nil {
		return nil, err
	}
	out := map[string]map[int]ShardSeq{}
	for name, ix := range r.Indices {
		m := map[int]ShardSeq{}
		for k, copies := range ix.Shards {
			n, err := strconv.Atoi(k)
			if err != nil {
				return nil, fmt.Errorf("index %s: bad shard key %q", name, k)
			}
			for _, cp := range copies {
				if !cp.Routing.Primary || cp.SeqNo == nil {
					continue
				}
				m[n] = ShardSeq{Shard: n, MaxSeqNo: cp.SeqNo.Max, LocalCheckpoint: cp.SeqNo.Local,
					GlobalCheckpoint: cp.SeqNo.GCP, PrimaryNode: cp.Routing.Node}
			}
		}
		out[name] = m
	}
	return out, nil
}

// Refresh makes all operations processed so far on every copy of index visible to search.
// It fails if any shard copy did not refresh.
func (c *Client) Refresh(ctx context.Context, index string) error {
	var r struct {
		Shards struct {
			Total, Successful, Failed int
		} `json:"_shards"`
	}
	if err := c.Do(ctx, "POST", "/"+index+"/_refresh", nil, nil, "", &r); err != nil {
		return err
	}
	if r.Shards.Failed > 0 {
		return fmt.Errorf("refresh %s: %d of %d shard copies failed", index, r.Shards.Failed, r.Shards.Total)
	}
	return nil
}

// Hit is one document returned by a search.
type Hit struct {
	Index       string          `json:"_index"`
	ID          string          `json:"_id"`
	Routing     string          `json:"_routing,omitempty"`
	SeqNo       int64           `json:"_seq_no"`
	PrimaryTerm int64           `json:"_primary_term"`
	Version     int64           `json:"_version"`
	Source      json.RawMessage `json:"_source"`
}

// SeqRange returns documents of one shard with after < _seq_no <= upto,
// sorted by _seq_no ascending, at most size of them.
func (c *Client) SeqRange(ctx context.Context, index string, shard int, after, upto int64, size int) ([]Hit, error) {
	q := url.Values{}
	q.Set("preference", "_shards:"+strconv.Itoa(shard))
	body := map[string]any{
		"size":                size,
		"seq_no_primary_term": true,
		"version":             true,
		"track_total_hits":    false,
		"query":               map[string]any{"range": map[string]any{"_seq_no": map[string]any{"gt": after, "lte": upto}}},
		"sort":                []any{map[string]any{"_seq_no": "asc"}},
	}
	b, _ := json.Marshal(body)
	var r struct {
		Hits struct {
			Hits []Hit `json:"hits"`
		} `json:"hits"`
		Shards struct {
			Total, Successful, Failed int
		} `json:"_shards"`
	}
	if err := c.Do(ctx, "POST", "/"+index+"/_search", q, b, "", &r); err != nil {
		return nil, err
	}
	if r.Shards.Failed > 0 || r.Shards.Successful < 1 {
		return nil, fmt.Errorf("index %s shard %d: search reached %d of %d shards", index, shard, r.Shards.Successful, r.Shards.Total)
	}
	return r.Hits.Hits, nil
}

// DocRef identifies a live document.
type DocRef struct {
	ID      string
	Routing string
}

// ListIDs returns the id and routing of every document visible in index,
// read through one scroll context so the listing is a consistent snapshot.
func (c *Client) ListIDs(ctx context.Context, index string, pageSize int) ([]DocRef, error) {
	q := url.Values{}
	q.Set("scroll", "2m")
	body, _ := json.Marshal(map[string]any{
		"size": pageSize, "_source": false, "sort": []string{"_doc"}, "track_total_hits": false,
		"query": map[string]any{"match_all": map[string]any{}},
	})
	type page struct {
		ScrollID string `json:"_scroll_id"`
		Hits     struct {
			Hits []struct {
				ID      string `json:"_id"`
				Routing string `json:"_routing"`
			} `json:"hits"`
		} `json:"hits"`
		Shards struct {
			Total, Successful, Failed int
		} `json:"_shards"`
	}
	var p page
	if err := c.Do(ctx, "POST", "/"+index+"/_search", q, body, "", &p); err != nil {
		return nil, err
	}
	var out []DocRef
	sid := p.ScrollID
	defer func() {
		if sid != "" {
			b, _ := json.Marshal(map[string]any{"scroll_id": []string{sid}})
			_ = c.Do(context.Background(), "DELETE", "/_search/scroll", nil, b, "", nil)
		}
	}()
	for {
		if p.Shards.Failed > 0 {
			return nil, fmt.Errorf("index %s: scroll failed on %d shards", index, p.Shards.Failed)
		}
		for _, h := range p.Hits.Hits {
			out = append(out, DocRef{ID: h.ID, Routing: h.Routing})
		}
		if len(p.Hits.Hits) == 0 {
			return out, nil
		}
		b, _ := json.Marshal(map[string]any{"scroll": "2m", "scroll_id": sid})
		p = page{}
		if err := c.Do(ctx, "POST", "/_search/scroll", nil, b, "", &p); err != nil {
			return nil, err
		}
		if p.ScrollID != "" {
			sid = p.ScrollID
		}
	}
}

// BulkItemError describes a failed bulk item.
type BulkItemError struct {
	Status int
	Reason string
}

// Bulk sends an NDJSON bulk body and returns per-item failures (by position).
func (c *Client) Bulk(ctx context.Context, body []byte) (map[int]BulkItemError, error) {
	var r struct {
		Errors bool `json:"errors"`
		Items  []map[string]struct {
			Status int             `json:"status"`
			Error  json.RawMessage `json:"error"`
		} `json:"items"`
	}
	if err := c.Do(ctx, "POST", "/_bulk", nil, body, "application/x-ndjson", &r); err != nil {
		return nil, err
	}
	fails := map[int]BulkItemError{}
	if !r.Errors {
		return fails, nil
	}
	for i, it := range r.Items {
		for _, v := range it {
			if v.Status >= 300 && !(v.Status == 404 && len(v.Error) == 0) {
				fails[i] = BulkItemError{Status: v.Status, Reason: string(v.Error)}
			}
		}
	}
	return fails, nil
}
