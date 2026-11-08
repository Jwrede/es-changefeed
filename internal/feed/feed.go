// Package feed turns per-shard sequence numbers into a change stream.
//
// For every primary shard the feed keeps the highest sequence number whose
// effect has been delivered (the checkpoint). Each round it reads the shard's
// global checkpoint G, which is the sequence number up to which every
// operation has been processed by all in-sync copies, refreshes the index so
// those operations are visible, and searches for documents with
// checkpoint < _seq_no <= G. Each hit is the current state of a document
// that was created or updated in that range. Operations above G are never
// read, so operations that a primary failover rolls back are never emitted.
//
// Deleted documents leave no searchable trace. With delete detection on, the
// feed keeps the set of delivered ids and periodically lists the ids that
// still exist; the difference is emitted as delete events (see scan).
package feed

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"time"

	"github.com/Jwrede/es-changefeed/internal/cluster"
	"github.com/Jwrede/es-changefeed/internal/metrics"
	"github.com/Jwrede/es-changefeed/internal/sink"
	"github.com/Jwrede/es-changefeed/internal/state"
)

// Config controls a feed.
type Config struct {
	Target string // index pattern, alias or data stream (comma-separated)
	Expand string // expand_wildcards for index resolution; default "open,closed"
	// From applies only when the state directory is empty: "start" emits every
	// existing document first, "now" starts at the current global checkpoint.
	// Indices that appear later always start from the beginning.
	From         string
	Refresh      bool          // call _refresh before reading (default true)
	Settle       time.Duration // without Refresh: only read up to checkpoints at least this old
	Batch        int           // documents per search and per sink write
	Interval     time.Duration // pause between rounds
	Deletes      bool          // track delivered ids and emit deletes
	ScanInterval time.Duration // how often to list ids for delete detection
	ScanPage     int           // ids per scroll page
}

// Feed runs rounds until the context ends.
type Feed struct {
	cfg   Config
	c     *cluster.Client
	store *state.Store
	sink  sink.Sink
	log   *slog.Logger
	m     *metrics.Registry
	now   func() time.Time

	fresh   bool                 // state directory was empty at start
	history map[string][]observe // uuid -> recent checkpoint observations (settle mode)
}

type observe struct {
	at  time.Time
	gcp map[int]int64
}

// New creates a feed.
func New(cfg Config, c *cluster.Client, store *state.Store, sk sink.Sink, log *slog.Logger, m *metrics.Registry) *Feed {
	if cfg.Batch <= 0 {
		cfg.Batch = 1000
	}
	if cfg.Interval <= 0 {
		cfg.Interval = time.Second
	}
	if cfg.ScanPage <= 0 {
		cfg.ScanPage = 5000
	}
	if cfg.Deletes && cfg.ScanInterval <= 0 {
		cfg.ScanInterval = 5 * time.Minute
	}
	if cfg.Expand == "" {
		cfg.Expand = "open,closed"
	}
	if cfg.From == "" {
		cfg.From = "start"
	}
	if m == nil {
		m = metrics.New()
	}
	return &Feed{cfg: cfg, c: c, store: store, sink: sk, log: log, m: m, now: time.Now,
		fresh: len(store.UUIDs()) == 0, history: map[string][]observe{}}
}

// Run loops until ctx is cancelled. It returns ctx.Err() on cancellation.
func (f *Feed) Run(ctx context.Context) error {
	for {
		if err := f.Round(ctx, false); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			f.m.Inc("escf_round_errors_total", nil, 1)
			f.log.Warn("round failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(f.cfg.Interval):
		}
	}
}

// Once catches every shard up to the global checkpoint read at its start,
// runs a delete scan if delete detection is on, and returns.
func (f *Feed) Once(ctx context.Context) error {
	return f.Round(ctx, true)
}

// Round processes every matching index once. forceScan runs a delete scan
// regardless of ScanInterval.
func (f *Feed) Round(ctx context.Context, forceScan bool) error {
	indices, err := f.c.ResolveIndices(ctx, f.cfg.Target, f.cfg.Expand)
	if err != nil {
		return fmt.Errorf("resolve %s: %w", f.cfg.Target, err)
	}
	seen := map[string]bool{}
	open := 0
	for _, ix := range indices {
		seen[ix.UUID] = true
		if !ix.Closed {
			open++
		}
	}
	for _, uuid := range f.store.UUIDs() {
		if !seen[uuid] {
			st := f.store.Get(uuid)
			f.log.Info("index no longer exists, dropping its state", "index", st.Name, "uuid", uuid)
			if err := f.store.Drop(uuid); err != nil {
				return err
			}
			delete(f.history, uuid)
		}
	}
	f.m.Set("escf_indices", nil, float64(open))
	if open == 0 {
		return nil
	}
	stats, err := f.c.SeqStats(ctx, f.cfg.Target)
	if err != nil {
		return fmt.Errorf("stats %s: %w", f.cfg.Target, err)
	}
	var firstErr error
	for _, ix := range indices {
		if ix.Closed {
			continue
		}
		ss, ok := stats[ix.Name]
		if !ok {
			continue // closed or deleted between the two calls
		}
		if err := f.index(ctx, ix, ss, forceScan); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			f.log.Warn("index failed", "index", ix.Name, "err", err)
			if firstErr == nil {
				firstErr = fmt.Errorf("%s: %w", ix.Name, err)
			}
		}
	}
	f.fresh = false
	return firstErr
}

func (f *Feed) index(ctx context.Context, ix cluster.Index, ss map[int]cluster.ShardSeq, forceScan bool) error {
	st := f.store.Get(ix.UUID)
	if st == nil {
		if err := f.initIndex(ctx, ix, ss); err != nil {
			return err
		}
		st = f.store.Get(ix.UUID)
	}

	upto := map[int]int64{}
	for n, s := range ss {
		upto[n] = s.GlobalCheckpoint
	}
	if !f.cfg.Refresh {
		upto = f.settled(ix.UUID, upto)
	}

	work := false
	for n := 0; n < ix.Shards; n++ {
		last := shardPos(st, n)
		g, ok := upto[n]
		lbl := map[string]string{"index": ix.Name, "shard": strconv.Itoa(n)}
		if s, ok2 := ss[n]; ok2 {
			f.m.Set("escf_shard_lag_ops", lbl, float64(s.GlobalCheckpoint-last))
		}
		if ok && g > last {
			work = true
		}
	}
	if work && f.cfg.Refresh {
		if err := f.c.Refresh(ctx, ix.Name); err != nil {
			return err
		}
	}
	for n := 0; n < ix.Shards && work; n++ {
		g, ok := upto[n]
		if !ok {
			continue
		}
		if err := f.shard(ctx, ix, n, shardPos(st, n), g); err != nil {
			return err
		}
	}

	if f.cfg.Deletes {
		st = f.store.Get(ix.UUID)
		due := forceScan || f.now().Sub(time.Unix(st.LastScan, 0)) >= f.cfg.ScanInterval
		if due {
			if err := f.scan(ctx, ix); err != nil {
				return fmt.Errorf("delete scan: %w", err)
			}
		}
	}
	return nil
}

func shardPos(st *state.IndexState, n int) int64 {
	if v, ok := st.Shards[strconv.Itoa(n)]; ok {
		return v
	}
	return -1
}

// settled returns, per shard, the newest observed global checkpoint that is at
// least Settle old, so that a scheduled refresh has happened since it was read.
func (f *Feed) settled(uuid string, cur map[int]int64) map[int]int64 {
	now := f.now()
	h := append(f.history[uuid], observe{at: now, gcp: cur})
	pick := -1
	for i, o := range h {
		if now.Sub(o.at) >= f.cfg.Settle {
			pick = i
		}
	}
	if pick < 0 {
		f.history[uuid] = h
		return map[int]int64{}
	}
	f.history[uuid] = append([]observe(nil), h[pick:]...)
	return h[pick].gcp
}

func (f *Feed) initIndex(ctx context.Context, ix cluster.Index, ss map[int]cluster.ShardSeq) error {
	fromNow := f.fresh && f.cfg.From == "now"
	for n := 0; n < ix.Shards; n++ {
		pos := int64(-1)
		if fromNow {
			s, ok := ss[n]
			if !ok {
				return fmt.Errorf("shard %d has no started primary; cannot start at \"now\"", n)
			}
			pos = s.GlobalCheckpoint
		}
		f.store.SetShard(ix.UUID, ix.Name, n, pos)
	}
	if fromNow && f.cfg.Deletes {
		// The sink is assumed to hold the existing documents already; record
		// them so that their later deletion is detected.
		if err := f.c.Refresh(ctx, ix.Name); err != nil {
			return err
		}
		ids, err := f.store.IDs(ix.UUID)
		if err != nil {
			return err
		}
		for n := 0; n < ix.Shards; n++ {
			refs, err := f.c.ListIDs(ctx, ix.Name, n, f.cfg.ScanPage)
			if err != nil {
				return err
			}
			for _, r := range refs {
				if err := ids.Add(n, r.ID, r.Routing); err != nil {
					return err
				}
			}
		}
		if err := ids.Sync(); err != nil {
			return err
		}
		f.store.SetLastScan(ix.UUID, f.now().Unix())
	}
	f.log.Info("tracking index", "index", ix.Name, "uuid", ix.UUID, "shards", ix.Shards, "from", map[bool]string{true: "now", false: "start"}[fromNow])
	return f.store.Save()
}

// shard delivers every document with last < _seq_no <= upto.
func (f *Feed) shard(ctx context.Context, ix cluster.Index, n int, last, upto int64) error {
	for last < upto {
		hits, err := f.c.SeqRange(ctx, ix.Name, n, last, upto, f.cfg.Batch)
		if err != nil {
			return err
		}
		next := upto
		if len(hits) >= f.cfg.Batch {
			next = hits[len(hits)-1].SeqNo
		}
		batch := make([]sink.Event, 0, len(hits))
		for _, h := range hits {
			if h.SeqNo <= last || h.SeqNo > upto {
				return fmt.Errorf("shard %d: search returned _seq_no %d outside (%d, %d]", n, h.SeqNo, last, upto)
			}
			batch = append(batch, sink.Event{Op: "upsert", Index: ix.Name, IndexUUID: ix.UUID, Shard: n,
				ID: h.ID, Routing: h.Routing, SeqNo: h.SeqNo, PrimaryTerm: h.PrimaryTerm, Version: h.Version, Source: h.Source})
		}
		if !sort.SliceIsSorted(batch, func(i, j int) bool { return batch[i].SeqNo < batch[j].SeqNo }) {
			sort.SliceStable(batch, func(i, j int) bool { return batch[i].SeqNo < batch[j].SeqNo })
		}
		// Record the ids before delivering: a write that fails from our side
		// may still have reached the sink, and if the document is deleted
		// before the retry, only the id set can produce the delete. A recorded
		// id that never arrived costs at most a delete of a missing document.
		if f.cfg.Deletes && len(batch) > 0 {
			ids, err := f.store.IDs(ix.UUID)
			if err != nil {
				return err
			}
			for _, ev := range batch {
				if err := ids.Add(ev.Shard, ev.ID, ev.Routing); err != nil {
					return err
				}
			}
			if err := ids.Sync(); err != nil {
				return err
			}
		}
		if err := f.deliver(ctx, batch); err != nil {
			return err
		}
		f.store.SetShard(ix.UUID, ix.Name, n, next)
		if err := f.store.Save(); err != nil {
			return err
		}
		f.m.Inc("escf_events_total", map[string]string{"op": "upsert"}, float64(len(batch)))
		f.m.Set("escf_shard_position", map[string]string{"index": ix.Name, "shard": strconv.Itoa(n)}, float64(next))
		last = next
	}
	return nil
}

// scan lists the ids that still exist and emits deletes for delivered ids
// that are gone. It runs between shard reads of the same index, so every
// delivered upsert stems from an operation at or below a global checkpoint
// read earlier. Those operations are processed on every in-sync copy, so the
// refresh makes them visible to the scroll. An id missing from the listing was
// therefore deleted, and an operation that re-creates it but is not yet
// visible has a sequence number above every delivered one, so it is delivered
// after this delete.
func (f *Feed) scan(ctx context.Context, ix cluster.Index) error {
	start := f.now()
	ids, err := f.store.IDs(ix.UUID)
	if err != nil {
		return err
	}
	if err := f.c.Refresh(ctx, ix.Name); err != nil {
		return err
	}
	live := map[state.DocKey]struct{}{}
	for n := 0; n < ix.Shards; n++ {
		refs, err := f.c.ListIDs(ctx, ix.Name, n, f.cfg.ScanPage)
		if err != nil {
			return err
		}
		for _, r := range refs {
			live[state.DocKey{Shard: n, ID: r.ID}] = struct{}{}
		}
	}
	var gone []sink.Event
	ids.Range(func(k state.DocKey, routing string) {
		if _, ok := live[k]; !ok {
			gone = append(gone, sink.Event{Op: "delete", Index: ix.Name, IndexUUID: ix.UUID, Shard: k.Shard, ID: k.ID, Routing: routing})
		}
	})
	sort.Slice(gone, func(i, j int) bool {
		if gone[i].Shard != gone[j].Shard {
			return gone[i].Shard < gone[j].Shard
		}
		return gone[i].ID < gone[j].ID
	})
	for i := 0; i < len(gone); i += f.cfg.Batch {
		j := i + f.cfg.Batch
		if j > len(gone) {
			j = len(gone)
		}
		if err := f.deliver(ctx, gone[i:j]); err != nil {
			return err
		}
		for _, ev := range gone[i:j] {
			if err := ids.Remove(ev.Shard, ev.ID); err != nil {
				return err
			}
		}
		if err := ids.Sync(); err != nil {
			return err
		}
	}
	if err := ids.Compact(); err != nil {
		return err
	}
	f.store.SetLastScan(ix.UUID, f.now().Unix())
	if err := f.store.Save(); err != nil {
		return err
	}
	f.m.Inc("escf_events_total", map[string]string{"op": "delete"}, float64(len(gone)))
	f.m.Set("escf_tracked_ids", map[string]string{"index": ix.Name}, float64(ids.Len()))
	f.log.Info("delete scan", "index", ix.Name, "live", len(live), "deleted", len(gone), "took", f.now().Sub(start).Round(time.Millisecond).String())
	return nil
}

// deliver writes a batch to the sink, retrying with backoff until it succeeds
// or ctx ends.
func (f *Feed) deliver(ctx context.Context, batch []sink.Event) error {
	if len(batch) == 0 {
		return nil
	}
	delay := 200 * time.Millisecond
	for {
		err := f.sink.Write(ctx, batch)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var perm interface{ Permanent() bool }
		if errors.As(err, &perm) && perm.Permanent() {
			return err
		}
		f.m.Inc("escf_sink_errors_total", nil, 1)
		f.log.Warn("sink write failed, retrying", "events", len(batch), "err", err, "retry_in", delay.String())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		if delay < 30*time.Second {
			delay *= 2
		}
	}
}
