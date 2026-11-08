package feed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Jwrede/es-changefeed/internal/cluster"
	"github.com/Jwrede/es-changefeed/internal/fakees"
	"github.com/Jwrede/es-changefeed/internal/sink"
	"github.com/Jwrede/es-changefeed/internal/state"
)

type memSink struct {
	mu     sync.Mutex
	events []sink.Event
	failN  int // fail the next N writes
}

func (m *memSink) Write(_ context.Context, b []sink.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failN > 0 {
		m.failN--
		return errors.New("sink down")
	}
	m.events = append(m.events, b...)
	return nil
}
func (m *memSink) Close() error { return nil }

func (m *memSink) take() []sink.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.events
	m.events = nil
	return e
}

// apply replays events into a map id -> source, as a downstream consumer would.
func apply(dst map[string]string, evs []sink.Event) {
	for _, e := range evs {
		switch e.Op {
		case "upsert":
			dst[e.ID] = string(e.Source)
		case "delete":
			delete(dst, e.ID)
		}
	}
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type rig struct {
	t     *testing.T
	es    *fakees.Server
	dir   string
	sink  *memSink
	cfg   Config
	store *state.Store
	f     *Feed
}

func newRig(t *testing.T, cfg Config) *rig {
	t.Helper()
	es := fakees.New()
	t.Cleanup(es.Close)
	r := &rig{t: t, es: es, dir: t.TempDir(), sink: &memSink{}, cfg: cfg}
	if r.cfg.Target == "" {
		r.cfg.Target = "logs-*"
	}
	r.cfg.Refresh = cfg.Settle == 0 // a Settle value in a test means -no-refresh
	r.open()
	return r
}

func (r *rig) open() {
	r.t.Helper()
	if r.store != nil {
		r.store.Close()
	}
	st, err := state.Open(r.dir)
	if err != nil {
		r.t.Fatal(err)
	}
	r.store = st
	c, err := cluster.New(cluster.Config{URL: r.es.URL(), Retries: 1})
	if err != nil {
		r.t.Fatal(err)
	}
	r.f = New(r.cfg, c, st, r.sink, quiet(), nil)
}

func (r *rig) once() []sink.Event {
	r.t.Helper()
	if err := r.f.Once(context.Background()); err != nil {
		r.t.Fatal(err)
	}
	return r.sink.take()
}

func ids(evs []sink.Event, op string) []string {
	var out []string
	for _, e := range evs {
		if e.Op == op {
			out = append(out, e.ID)
		}
	}
	sort.Strings(out)
	return out
}

func TestSnapshotThenChanges(t *testing.T) {
	r := newRig(t, Config{Batch: 7})
	r.es.CreateIndex("logs-a", 3)
	for i := 0; i < 50; i++ {
		r.es.Put("logs-a", fmt.Sprintf("d%02d", i), "", fmt.Sprintf(`{"n":%d}`, i))
	}
	evs := r.once()
	if len(evs) != 50 {
		t.Fatalf("snapshot: got %d events, want 50", len(evs))
	}
	// Per shard, events come in _seq_no order.
	last := map[int]int64{}
	for _, e := range evs {
		if p, ok := last[e.Shard]; ok && e.SeqNo <= p {
			t.Fatalf("shard %d out of order: %d after %d", e.Shard, e.SeqNo, p)
		}
		last[e.Shard] = e.SeqNo
	}
	if evs := r.once(); len(evs) != 0 {
		t.Fatalf("no changes: got %d events", len(evs))
	}
	r.es.Put("logs-a", "d03", "", `{"n":3,"edited":true}`)
	r.es.Put("logs-a", "new", "", `{"n":99}`)
	evs = r.once()
	if got := ids(evs, "upsert"); strings.Join(got, ",") != "d03,new" {
		t.Fatalf("changes: got %v", got)
	}
	for _, e := range evs {
		if e.ID == "d03" && (string(e.Source) != `{"n":3,"edited":true}` || e.Version != 2) {
			t.Fatalf("d03: got %s version %d", e.Source, e.Version)
		}
	}
}

// An operation with a lower _seq_no can become visible after a higher one.
// Reading only up to the global checkpoint means the higher one is not
// consumed early and the lower one is not skipped.
func TestOutOfOrderVisibility(t *testing.T) {
	r := newRig(t, Config{})
	r.es.CreateIndex("logs-a", 1)
	r.es.Put("logs-a", "a", "", `{"v":1}`)
	r.once()
	_, pending := r.es.PutPending("logs-a", "b", "", `{"v":2}`)
	r.es.Put("logs-a", "c", "", `{"v":3}`) // processed, would be visible after a refresh
	if evs := r.once(); len(evs) != 0 {
		t.Fatalf("got %v before the global checkpoint passed seq %d", ids(evs, "upsert"), pending)
	}
	r.es.Process("logs-a", 0, pending)
	if got := ids(r.once(), "upsert"); strings.Join(got, ",") != "b,c" {
		t.Fatalf("after processing: got %v", got)
	}
}

func TestGlobalCheckpointLag(t *testing.T) {
	r := newRig(t, Config{})
	r.es.CreateIndex("logs-a", 1)
	for i := 0; i < 5; i++ {
		r.es.Put("logs-a", fmt.Sprint(i), "", `{}`)
	}
	r.es.SetGCPLag("logs-a", 0, 2)
	if got := ids(r.once(), "upsert"); strings.Join(got, ",") != "0,1,2" {
		t.Fatalf("with lag: got %v", got)
	}
	r.es.SetGCPLag("logs-a", 0, 0)
	if got := ids(r.once(), "upsert"); strings.Join(got, ",") != "3,4" {
		t.Fatalf("after lag: got %v", got)
	}
}

func TestDeletes(t *testing.T) {
	r := newRig(t, Config{Deletes: true, ScanInterval: time.Hour})
	r.es.CreateIndex("logs-a", 2)
	for _, id := range []string{"a", "b", "c", "d"} {
		r.es.Put("logs-a", id, "r-"+id, `{}`)
	}
	r.once()
	r.es.Delete("logs-a", "a", "r-a")
	r.es.Delete("logs-a", "b", "r-b")
	r.es.Put("logs-a", "b", "r-b", `{"again":true}`) // deleted and re-created between rounds
	r.es.Put("logs-a", "tmp", "", `{}`)
	r.es.Delete("logs-a", "tmp", "") // never delivered, so no delete
	evs := r.once()
	if got := ids(evs, "delete"); strings.Join(got, ",") != "a" {
		t.Fatalf("deletes: got %v", got)
	}
	if got := ids(evs, "upsert"); strings.Join(got, ",") != "b" {
		t.Fatalf("upserts: got %v", got)
	}
	for _, e := range evs {
		if e.Op == "delete" && e.Routing != "r-a" {
			t.Fatalf("delete lost routing: %+v", e)
		}
	}
	// Delete detection survives a restart (id log on disk).
	r.open()
	r.es.Delete("logs-a", "c", "r-c")
	if got := ids(r.once(), "delete"); strings.Join(got, ",") != "c" {
		t.Fatalf("after restart: got %v", got)
	}
}

// A document that is deleted and then re-created by an operation that is not
// yet processed when the scan runs: the delete must come before the upsert.
func TestDeleteThenPendingRecreate(t *testing.T) {
	r := newRig(t, Config{Deletes: true})
	r.es.CreateIndex("logs-a", 1)
	r.es.Put("logs-a", "x", "", `{"v":1}`)
	r.once()
	r.es.Delete("logs-a", "x", "")
	_, seq := r.es.PutPending("logs-a", "x", "", `{"v":2}`)
	evs := r.once()
	r.es.Process("logs-a", 0, seq)
	evs = append(evs, r.once()...)
	mirror := map[string]string{"x": `{"v":1}`}
	apply(mirror, evs)
	if mirror["x"] != `{"v":2}` {
		t.Fatalf("mirror has %q, events %+v", mirror["x"], evs)
	}
}

func TestIndexLifecycle(t *testing.T) {
	r := newRig(t, Config{})
	r.es.CreateIndex("logs-1", 1)
	r.es.Put("logs-1", "a", "", `{}`)
	r.once()
	// Rollover: a new index appears and is read from the start.
	r.es.CreateIndex("logs-2", 2)
	r.es.Put("logs-2", "b", "", `{}`)
	if got := ids(r.once(), "upsert"); strings.Join(got, ",") != "b" {
		t.Fatalf("new index: got %v", got)
	}
	// A closed index is skipped and keeps its state.
	r.es.SetClosed("logs-1", true)
	r.once()
	if r.store.Get(r.es.UUID("logs-1")) == nil {
		t.Fatal("closed index lost its state")
	}
	r.es.SetClosed("logs-1", false)
	if evs := r.once(); len(evs) != 0 {
		t.Fatalf("reopened index re-emitted %d events", len(evs))
	}
	// Deleted and re-created under the same name: new uuid, read from the start.
	old := r.es.UUID("logs-1")
	r.es.DeleteIndex("logs-1")
	r.once()
	if r.store.Get(old) != nil {
		t.Fatal("state of deleted index kept")
	}
	r.es.CreateIndex("logs-1", 1)
	r.es.Put("logs-1", "c", "", `{}`)
	if got := ids(r.once(), "upsert"); strings.Join(got, ",") != "c" {
		t.Fatalf("recreated index: got %v", got)
	}
}

func TestFromNow(t *testing.T) {
	r := newRig(t, Config{From: "now", Deletes: true})
	r.es.CreateIndex("logs-a", 2)
	r.es.Put("logs-a", "old1", "", `{}`)
	r.es.Put("logs-a", "old2", "", `{}`)
	if evs := r.once(); len(evs) != 0 {
		t.Fatalf("from=now emitted %d existing documents", len(evs))
	}
	r.es.Put("logs-a", "new", "", `{}`)
	r.es.Delete("logs-a", "old1", "")
	evs := r.once()
	if ids(evs, "upsert")[0] != "new" || strings.Join(ids(evs, "delete"), ",") != "old1" {
		t.Fatalf("got %+v", evs)
	}
	// "now" applies only to the first run: a later index starts from the beginning.
	r.es.CreateIndex("logs-b", 1)
	r.es.Put("logs-b", "b1", "", `{}`)
	if got := ids(r.once(), "upsert"); strings.Join(got, ",") != "b1" {
		t.Fatalf("later index: got %v", got)
	}
}

func TestSinkFailureRetriesAndRestartReplays(t *testing.T) {
	r := newRig(t, Config{Batch: 2})
	r.es.CreateIndex("logs-a", 1)
	for i := 0; i < 6; i++ {
		r.es.Put("logs-a", fmt.Sprint(i), "", `{}`)
	}
	r.sink.failN = 2 // retried with backoff inside deliver
	if got := ids(r.once(), "upsert"); len(got) != 6 {
		t.Fatalf("after retries: got %v", got)
	}
	// Cancel while the sink is down: nothing is acknowledged, a restart resumes.
	r.es.Put("logs-a", "6", "", `{}`)
	r.sink.failN = 1000
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := r.f.Once(ctx); err == nil {
		t.Fatal("expected an error while the sink is down")
	}
	r.sink.failN = 0
	r.open()
	if got := ids(r.once(), "upsert"); strings.Join(got, ",") != "6" {
		t.Fatalf("after restart: got %v", got)
	}
}

func TestSearchErrorsAreRetried(t *testing.T) {
	r := newRig(t, Config{})
	r.es.CreateIndex("logs-a", 1)
	r.es.Put("logs-a", "a", "", `{}`)
	r.es.FailSearches = 1
	if got := ids(r.once(), "upsert"); strings.Join(got, ",") != "a" {
		t.Fatalf("got %v", got)
	}
}

func TestNoRefreshSettle(t *testing.T) {
	r := newRig(t, Config{Settle: 5 * time.Second})
	now := time.Unix(1000, 0)
	r.f.now = func() time.Time { return now }
	r.es.CreateIndex("logs-a", 1)
	r.es.Put("logs-a", "a", "", `{}`)
	if err := r.f.Round(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if evs := r.sink.take(); len(evs) != 0 {
		t.Fatalf("read a checkpoint that had not settled: %v", evs)
	}
	r.es.Refresh("logs-a") // the cluster's own scheduled refresh
	now = now.Add(6 * time.Second)
	if err := r.f.Round(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if got := ids(r.sink.take(), "upsert"); strings.Join(got, ",") != "a" {
		t.Fatalf("after settle: got %v", got)
	}
	if n := r.es.Requests["POST _refresh"]; n != 0 {
		t.Fatalf("-no-refresh called _refresh %d times", n)
	}
}

func TestIdleIndexIsNotRefreshed(t *testing.T) {
	r := newRig(t, Config{})
	r.es.CreateIndex("logs-a", 1)
	r.es.Put("logs-a", "a", "", `{}`)
	r.once()
	before := r.es.Requests["POST _refresh"]
	r.once()
	r.once()
	if n := r.es.Requests["POST _refresh"] - before; n != 0 {
		t.Fatalf("idle index refreshed %d times", n)
	}
}

// Random workload with out-of-order processing, checkpoint lag, routing,
// deletes and re-creates. Replaying the events must reproduce the source.
func TestRandomWorkloadMirrors(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			r := newRig(t, Config{Deletes: true, ScanInterval: time.Duration(rng.Intn(3)) * time.Nanosecond, Batch: 1 + rng.Intn(5)})
			r.es.CreateIndex("logs-a", 1+rng.Intn(4))
			mirror := map[string]string{}
			type pend struct {
				shard int
				seq   int64
			}
			var pending []pend
			routing := func(id string) string {
				if id[len(id)-1]%2 == 0 {
					return "r" + id
				}
				return ""
			}
			for round := 0; round < 40; round++ {
				for k := rng.Intn(8); k > 0; k-- {
					id := fmt.Sprintf("d%d", rng.Intn(25))
					src := fmt.Sprintf(`{"r":%d,"k":%d}`, round, k)
					switch x := rng.Intn(10); {
					case x < 5:
						r.es.Put("logs-a", id, routing(id), src)
					case x < 7:
						sh, seq := r.es.PutPending("logs-a", id, routing(id), src)
						pending = append(pending, pend{sh, seq})
					default:
						r.es.Delete("logs-a", id, routing(id))
					}
				}
				if len(pending) > 0 && rng.Intn(2) == 0 {
					i := rng.Intn(len(pending))
					r.es.Process("logs-a", pending[i].shard, pending[i].seq)
					pending = append(pending[:i], pending[i+1:]...)
				}
				if err := r.f.Round(context.Background(), rng.Intn(3) == 0); err != nil {
					t.Fatal(err)
				}
				apply(mirror, r.sink.take())
				if rng.Intn(10) == 0 {
					r.open() // restart
				}
			}
			for _, p := range pending {
				r.es.Process("logs-a", p.shard, p.seq)
			}
			apply(mirror, r.once())
			want := r.es.Live("logs-a")
			if !equal(mirror, want) {
				t.Fatalf("mirror differs from source\nmirror: %v\nsource: %v", mirror, want)
			}
		})
	}
}

func equal(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func TestBulkSinkMirror(t *testing.T) {
	src := fakees.New()
	defer src.Close()
	dst := fakees.New()
	defer dst.Close()
	src.CreateIndex("logs-a", 3)
	for i := 0; i < 30; i++ {
		src.Put("logs-a", fmt.Sprint(i), fmt.Sprint(i%4), fmt.Sprintf(`{"i":%d}`, i))
	}
	sc, _ := cluster.New(cluster.Config{URL: src.URL()})
	dc, _ := cluster.New(cluster.Config{URL: dst.URL()})
	st, err := state.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := New(Config{Target: "logs-a", Refresh: true, Deletes: true}, sc, st, &sink.Bulk{Client: dc, IndexTemplate: "mirror-{index}"}, quiet(), nil)
	if err := f.Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	src.Delete("logs-a", "5", "1")
	src.Put("logs-a", "7", "3", `{"i":70}`)
	if err := f.Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, want := dst.Live("mirror-logs-a"), src.Live("logs-a"); !equal(got, want) {
		b, _ := json.Marshal(got)
		t.Fatalf("mirror differs: %s", b)
	}
}
