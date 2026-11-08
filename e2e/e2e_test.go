// End-to-end tests against a real Elasticsearch or OpenSearch cluster.
// They run only when ESCF_E2E_URL is set, for example:
//
//	ESCF_E2E_URL=http://localhost:9200 go test ./e2e/ -v
package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Jwrede/es-changefeed/internal/cluster"
	"github.com/Jwrede/es-changefeed/internal/feed"
	"github.com/Jwrede/es-changefeed/internal/sink"
	"github.com/Jwrede/es-changefeed/internal/state"
)

func client(t *testing.T) *cluster.Client {
	t.Helper()
	u := os.Getenv("ESCF_E2E_URL")
	if u == "" {
		t.Skip("ESCF_E2E_URL not set")
	}
	c, err := cluster.New(cluster.Config{URL: u, User: os.Getenv("ESCF_E2E_USER"), Password: os.Getenv("ESCF_E2E_PASSWORD"), Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func do(t *testing.T, c *cluster.Client, method, path string, body any) map[string]any {
	t.Helper()
	var b []byte
	switch v := body.(type) {
	case nil:
	case string:
		b = []byte(v)
	default:
		b, _ = json.Marshal(v)
	}
	var out map[string]any
	if err := c.Do(context.Background(), method, path, nil, b, "", &out); err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return out
}

func bulk(t *testing.T, c *cluster.Client, lines []string) {
	t.Helper()
	fails, err := c.Bulk(context.Background(), []byte(strings.Join(lines, "\n")+"\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(fails) > 0 {
		t.Fatalf("bulk failures: %v", fails)
	}
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// docs returns id -> source (canonical JSON) for every document in index.
func docs(t *testing.T, c *cluster.Client, index string) map[string]string {
	t.Helper()
	do(t, c, "POST", "/"+index+"/_refresh", nil)
	out := map[string]string{}
	var r struct {
		ScrollID string `json:"_scroll_id"`
		Hits     struct {
			Hits []struct {
				ID     string          `json:"_id"`
				Source json.RawMessage `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	body, _ := json.Marshal(map[string]any{"size": 1000, "sort": []string{"_doc"}})
	if err := c.Do(context.Background(), "POST", "/"+index+"/_search", map[string][]string{"scroll": {"1m"}}, body, "", &r); err != nil {
		t.Fatal(err)
	}
	for len(r.Hits.Hits) > 0 {
		for _, h := range r.Hits.Hits {
			var v any
			json.Unmarshal(h.Source, &v)
			b, _ := json.Marshal(v)
			out[h.ID] = string(b)
		}
		b, _ := json.Marshal(map[string]any{"scroll": "1m", "scroll_id": r.ScrollID})
		r.Hits.Hits = nil
		if err := c.Do(context.Background(), "POST", "/_search/scroll", nil, b, "", &r); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func diff(a, b map[string]string) string {
	var lines []string
	for k, v := range a {
		if w, ok := b[k]; !ok {
			lines = append(lines, "only in source: "+k)
		} else if w != v {
			lines = append(lines, fmt.Sprintf("%s: source %s mirror %s", k, v, w))
		}
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			lines = append(lines, "only in mirror: "+k)
		}
	}
	sort.Strings(lines)
	if len(lines) > 20 {
		lines = append(lines[:20], fmt.Sprintf("... %d more", len(lines)-20))
	}
	return strings.Join(lines, "\n")
}

func TestInfo(t *testing.T) {
	c := client(t)
	info, err := c.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("cluster: %s %s", info.Distribution, info.Version)
}

// SeqRange must return hits sorted by _seq_no within the requested range.
func TestSeqRangeOnRealCluster(t *testing.T) {
	c := client(t)
	ix := "escf-seq"
	do(t, c, "PUT", "/"+ix, map[string]any{"settings": map[string]any{"number_of_shards": 1, "number_of_replicas": 0}})
	t.Cleanup(func() { c.Do(context.Background(), "DELETE", "/"+ix, nil, nil, "", nil) })
	var lines []string
	for i := 0; i < 100; i++ {
		lines = append(lines, fmt.Sprintf(`{"index":{"_index":"%s","_id":"d%d"}}`, ix, i%40), fmt.Sprintf(`{"i":%d}`, i))
	}
	bulk(t, c, lines)
	do(t, c, "POST", "/"+ix+"/_refresh", nil)
	st, err := c.SeqStats(context.Background(), ix)
	if err != nil {
		t.Fatal(err)
	}
	s := st[ix][0]
	if s.MaxSeqNo != 99 || s.GlobalCheckpoint != 99 || s.LocalCheckpoint != 99 {
		t.Fatalf("seq stats: %+v", s)
	}
	hits, err := c.SeqRange(context.Background(), ix, 0, 59, 99, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 40 {
		t.Fatalf("got %d hits for (59, 99], want the 40 latest versions", len(hits))
	}
	for i, h := range hits {
		if h.SeqNo != int64(60+i) {
			t.Fatalf("hit %d has _seq_no %d", i, h.SeqNo)
		}
	}
	hits, _ = c.SeqRange(context.Background(), ix, 0, 59, 99, 7)
	if len(hits) != 7 || hits[6].SeqNo != 66 {
		t.Fatalf("limited range: %d hits, last %d", len(hits), hits[len(hits)-1].SeqNo)
	}
}

// Writers index, update and delete documents from several goroutines while the
// feed runs; the feed mirrors into a second index on the same cluster with the
// bulk sink. Afterwards the mirror must equal the source.
func TestConcurrentWritersMirror(t *testing.T) {
	c := client(t)
	src, dst := "escf-src", "escf-dst-escf-src"
	for _, ix := range []string{src, dst} {
		c.Do(context.Background(), "DELETE", "/"+ix, nil, nil, "", nil)
	}
	replicas := 0
	if os.Getenv("ESCF_E2E_REPLICAS") != "" {
		fmt.Sscan(os.Getenv("ESCF_E2E_REPLICAS"), &replicas)
	}
	do(t, c, "PUT", "/"+src, map[string]any{"settings": map[string]any{"number_of_shards": 3, "number_of_replicas": replicas, "refresh_interval": "200ms"}})
	t.Cleanup(func() {
		c.Do(context.Background(), "DELETE", "/"+src, nil, nil, "", nil)
		c.Do(context.Background(), "DELETE", "/"+dst, nil, nil, "", nil)
	})
	// Some documents exist before the feed starts (initial snapshot).
	var lines []string
	for i := 0; i < 500; i++ {
		lines = append(lines, fmt.Sprintf(`{"index":{"_index":"%s","_id":"pre%d"}}`, src, i), fmt.Sprintf(`{"pre":%d}`, i))
	}
	bulk(t, c, lines)

	st, err := state.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	f := feed.New(feed.Config{Target: src, Refresh: true, Deletes: true, ScanInterval: 2 * time.Second, Batch: 200, Interval: 100 * time.Millisecond},
		c, st, &sink.Bulk{Client: c, IndexTemplate: "escf-dst-{index}"}, quiet(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.Run(ctx) }()

	var wg sync.WaitGroup
	deadline := time.Now().Add(8 * time.Second)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w)))
			for n := 0; time.Now().Before(deadline); n++ {
				var lines []string
				for k := 0; k < 20; k++ {
					id := fmt.Sprintf("d%d", rng.Intn(400))
					routing := ""
					if rng.Intn(2) == 0 {
						id = "r" + id
						routing = fmt.Sprintf(`,"routing":"g%d"`, rng.Intn(5))
					}
					if rng.Intn(4) == 0 {
						lines = append(lines, fmt.Sprintf(`{"delete":{"_index":"%s","_id":"%s"%s}}`, src, id, routing))
					} else {
						lines = append(lines, fmt.Sprintf(`{"index":{"_index":"%s","_id":"%s"%s}}`, src, id, routing),
							fmt.Sprintf(`{"w":%d,"n":%d,"k":%d}`, w, n, k))
					}
				}
				body := strings.Join(lines, "\n") + "\n"
				// During a node restart some requests fail; the source index is
				// the ground truth either way, so errors only cost a retry.
				if _, err := c.Bulk(context.Background(), []byte(body)); err != nil {
					t.Logf("writer %d: %v", w, err)
					time.Sleep(200 * time.Millisecond)
				}
			}
		}(w)
	}
	// One pre-existing document is deleted while the feed runs.
	bulk(t, c, []string{fmt.Sprintf(`{"delete":{"_index":"%s","_id":"pre7"}}`, src)})
	if node := os.Getenv("ESCF_E2E_KILL_NODE"); node != "" {
		// Kill a node while writers and the feed run: its primaries fail over
		// to replicas on the other nodes, then it rejoins and recovers.
		time.Sleep(2 * time.Second)
		if b, err := exec.Command("docker", "kill", node).CombinedOutput(); err != nil {
			t.Fatalf("docker kill: %v %s", err, b)
		}
		t.Logf("killed %s", node)
		time.Sleep(3 * time.Second)
		if b, err := exec.Command("docker", "start", node).CombinedOutput(); err != nil {
			t.Fatalf("docker start: %v %s", err, b)
		}
	}
	wg.Wait()
	if os.Getenv("ESCF_E2E_KILL_NODE") != "" {
		var h map[string]any
		q := map[string][]string{"wait_for_status": {"green"}, "timeout": {"120s"}}
		if err := c.Do(context.Background(), "GET", "/_cluster/health/"+src, q, nil, "", &h); err != nil || h["status"] != "green" {
			t.Fatalf("cluster did not recover: %v %v", err, h)
		}
	}
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("feed stopped with %v", err)
	}
	// Catch up after the writers stopped, including a final delete scan.
	if err := f.Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	a, b := docs(t, c, src), docs(t, c, dst)
	if d := diff(a, b); d != "" {
		t.Fatalf("mirror differs (%d source docs, %d mirror docs):\n%s", len(a), len(b), d)
	}
	if _, ok := b["pre7"]; ok {
		t.Fatal("deleted pre-existing document still in mirror")
	}
	t.Logf("mirror equals source: %d documents", len(a))
}

// A data stream rolls over while the feed follows it by name.
func TestDataStreamRollover(t *testing.T) {
	c := client(t)
	ds := "escf-logs-app"
	do(t, c, "PUT", "/_index_template/escf-logs", map[string]any{
		"index_patterns": []string{"escf-logs-*"}, "data_stream": map[string]any{}, "priority": 500,
		"template": map[string]any{"settings": map[string]any{"number_of_shards": 2, "number_of_replicas": 0}},
	})
	t.Cleanup(func() {
		c.Do(context.Background(), "DELETE", "/_data_stream/"+ds, nil, nil, "", nil)
		c.Do(context.Background(), "DELETE", "/_index_template/escf-logs", nil, nil, "", nil)
	})
	write := func(from, to int) {
		var lines []string
		for i := from; i < to; i++ {
			lines = append(lines, `{"create":{"_index":"`+ds+`"}}`, fmt.Sprintf(`{"@timestamp":"2026-11-08T00:00:%02dZ","i":%d}`, i%60, i))
		}
		bulk(t, c, lines)
	}
	write(0, 30)
	var out bytes.Buffer
	st, _ := state.Open(t.TempDir())
	defer st.Close()
	f := feed.New(feed.Config{Target: ds, Refresh: true}, c, st, sink.NewWriter(&out), quiet(), nil)
	if err := f.Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	do(t, c, "POST", "/"+ds+"/_rollover", nil)
	write(30, 50)
	if err := f.Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	seen := map[float64]bool{}
	backing := map[string]bool{}
	sc := bufio.NewScanner(&out)
	for sc.Scan() {
		var ev struct {
			Index  string `json:"index"`
			Source struct {
				I float64 `json:"i"`
			} `json:"source"`
		}
		json.Unmarshal(sc.Bytes(), &ev)
		seen[ev.Source.I] = true
		backing[ev.Index] = true
	}
	if len(seen) != 50 || len(backing) != 2 {
		t.Fatalf("got %d distinct documents from %d backing indices, want 50 from 2", len(seen), len(backing))
	}
}

// The command-line binary in -once mode writes NDJSON to stdout.
func TestCLIOnce(t *testing.T) {
	c := client(t)
	ix := "escf-cli"
	do(t, c, "PUT", "/"+ix, map[string]any{"settings": map[string]any{"number_of_shards": 2, "number_of_replicas": 0}})
	t.Cleanup(func() { c.Do(context.Background(), "DELETE", "/"+ix, nil, nil, "", nil) })
	bulk(t, c, []string{
		`{"index":{"_index":"escf-cli","_id":"1"}}`, `{"title":"first"}`,
		`{"index":{"_index":"escf-cli","_id":"2"}}`, `{"title":"second"}`,
	})
	bin := filepath.Join(t.TempDir(), "es-changefeed")
	if b, err := exec.Command("go", "build", "-o", bin, "../cmd/es-changefeed").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, b)
	}
	stateDir := t.TempDir()
	run := func() []map[string]any {
		cmd := exec.Command(bin, "-url", os.Getenv("ESCF_E2E_URL"), "-insecure", "-index", ix, "-state-dir", stateDir, "-deletes", "-once")
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("es-changefeed: %v\n%s", err, stderr.String())
		}
		var evs []map[string]any
		for _, l := range strings.Split(strings.TrimSpace(stdout.String()), "\n") {
			if l == "" {
				continue
			}
			var m map[string]any
			if err := json.Unmarshal([]byte(l), &m); err != nil {
				t.Fatalf("bad line %q", l)
			}
			evs = append(evs, m)
		}
		return evs
	}
	if evs := run(); len(evs) != 2 {
		t.Fatalf("first run: %v", evs)
	} else {
		b, _ := json.Marshal(evs[0])
		t.Logf("example event: %s", b)
	}
	bulk(t, c, []string{`{"update":{"_index":"escf-cli","_id":"1"}}`, `{"doc":{"title":"edited"}}`, `{"delete":{"_index":"escf-cli","_id":"2"}}`})
	evs := run()
	if len(evs) != 2 || evs[0]["op"] != "upsert" || evs[0]["id"] != "1" || evs[1]["op"] != "delete" || evs[1]["id"] != "2" {
		t.Fatalf("second run: %v", evs)
	}
	for _, e := range evs {
		b, _ := json.Marshal(e)
		t.Logf("example event: %s", b)
	}
}
