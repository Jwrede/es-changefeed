// Package fakees is an in-memory stand-in for the parts of the Elasticsearch
// REST API that es-changefeed uses. It models what matters for correctness:
// per-shard sequence numbers, operations that are assigned a sequence number
// but processed later (so a higher _seq_no can become visible before a lower
// one), the local/global checkpoint, and search visibility that only changes
// on refresh. Deleted documents disappear from search, as in Lucene.
package fakees

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type doc struct {
	ID      string
	Routing string
	Seq     int64
	Term    int64
	Version int64
	Source  json.RawMessage
}

type op struct {
	seq     int64
	delete  bool
	id      string
	routing string
	source  json.RawMessage
}

type shard struct {
	nextSeq    int64
	processed  map[int64]bool
	pending    map[int64]op
	live       map[string]doc
	searchable map[string]doc
	versions   map[string]int64
	lcp        int64
	gcpLag     int64
}

type index struct {
	name, uuid string
	closed     bool
	shards     []*shard
}

// Server is the fake cluster.
type Server struct {
	mu      sync.Mutex
	indices map[string]*index
	n       int
	scrolls map[string][]map[string]any
	// AutoRefresh makes every write immediately searchable.
	AutoRefresh bool
	// Requests counts requests by "METHOD /path-kind" for assertions.
	Requests map[string]int
	// FailSearches makes the next N shard searches return HTTP 503.
	FailSearches int
	TS           *httptest.Server
}

// New starts a fake cluster.
func New() *Server {
	s := &Server{indices: map[string]*index{}, scrolls: map[string][]map[string]any{}, Requests: map[string]int{}}
	s.TS = httptest.NewServer(http.HandlerFunc(s.serve))
	return s
}

// Close stops the server.
func (s *Server) Close() { s.TS.Close() }

// URL is the base URL.
func (s *Server) URL() string { return s.TS.URL }

// CreateIndex adds an index with the given number of shards.
func (s *Server) CreateIndex(name string, shards int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	ix := &index{name: name, uuid: fmt.Sprintf("uuid-%s-%d", name, s.n)}
	for i := 0; i < shards; i++ {
		ix.shards = append(ix.shards, &shard{processed: map[int64]bool{}, pending: map[int64]op{},
			live: map[string]doc{}, searchable: map[string]doc{}, versions: map[string]int64{}, lcp: -1})
	}
	s.indices[name] = ix
}

// DeleteIndex removes an index.
func (s *Server) DeleteIndex(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.indices, name)
}

// SetClosed opens or closes an index.
func (s *Server) SetClosed(name string, closed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.indices[name].closed = closed
}

// UUID returns the uuid of an index.
func (s *Server) UUID(name string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.indices[name].uuid
}

// ShardOf returns the shard a routing key maps to.
func (s *Server) ShardOf(name, id, routing string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return shardOf(len(s.indices[name].shards), id, routing)
}

func shardOf(n int, id, routing string) int {
	k := id
	if routing != "" {
		k = routing
	}
	h := fnv.New32a()
	h.Write([]byte(k))
	return int(h.Sum32() % uint32(n))
}

// Put indexes a document and processes the operation. It returns the shard and _seq_no.
func (s *Server) Put(idx, id, routing, source string) (int, int64) {
	return s.write(idx, op{id: id, routing: routing, source: json.RawMessage(source)}, true)
}

// PutPending assigns a sequence number but does not process the operation
// until Process is called, like an indexing thread that has not finished.
func (s *Server) PutPending(idx, id, routing, source string) (int, int64) {
	return s.write(idx, op{id: id, routing: routing, source: json.RawMessage(source)}, false)
}

// Delete deletes a document.
func (s *Server) Delete(idx, id, routing string) (int, int64) {
	return s.write(idx, op{id: id, routing: routing, delete: true}, true)
}

// Process finishes a pending operation.
func (s *Server) Process(idx string, sh int, seq int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ix := s.indices[idx]
	p := ix.shards[sh]
	o, ok := p.pending[seq]
	if !ok {
		panic(fmt.Sprintf("no pending op %d on shard %d", seq, sh))
	}
	delete(p.pending, seq)
	s.apply(ix, p, o)
}

// SetGCPLag makes the global checkpoint trail the local checkpoint, as when a
// replica has not acknowledged the latest operations yet.
func (s *Server) SetGCPLag(idx string, sh int, lag int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.indices[idx].shards[sh].gcpLag = lag
}

// Refresh makes processed operations searchable.
func (s *Server) Refresh(idx string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refresh(s.indices[idx])
}

// Live returns the current documents of an index as id -> source.
func (s *Server) Live(idx string) map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]string{}
	ix := s.indices[idx]
	if ix == nil {
		return out
	}
	for _, p := range ix.shards {
		for id, d := range p.live {
			out[id] = string(d.Source)
		}
	}
	return out
}

func (s *Server) refresh(ix *index) {
	for _, p := range ix.shards {
		p.searchable = make(map[string]doc, len(p.live))
		for k, v := range p.live {
			p.searchable[k] = v
		}
	}
}

func (s *Server) write(idx string, o op, process bool) (int, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ix := s.indices[idx]
	if ix == nil {
		panic("no index " + idx)
	}
	sh := shardOf(len(ix.shards), o.id, o.routing)
	p := ix.shards[sh]
	o.seq = p.nextSeq
	p.nextSeq++
	if process {
		s.apply(ix, p, o)
	} else {
		p.pending[o.seq] = o
	}
	return sh, o.seq
}

func (s *Server) apply(ix *index, p *shard, o op) {
	p.processed[o.seq] = true
	for p.processed[p.lcp+1] {
		p.lcp++
	}
	if o.delete {
		delete(p.live, o.id)
	} else {
		p.versions[o.id]++
		// Out-of-order processing: an older op must not overwrite a newer one.
		if cur, ok := p.live[o.id]; !ok || cur.Seq < o.seq {
			p.live[o.id] = doc{ID: o.id, Routing: o.routing, Seq: o.seq, Term: 1, Version: p.versions[o.id], Source: o.source}
		}
	}
	if s.AutoRefresh {
		s.refresh(ix)
	}
}

func (s *Server) match(target string) []*index {
	var out []*index
	for _, t := range strings.Split(target, ",") {
		for _, ix := range s.indices {
			if ok, _ := path.Match(t, ix.name); ok {
				out = append(out, ix)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	body, _ := io.ReadAll(r.Body)
	p := r.URL.Path
	parts := strings.Split(strings.Trim(p, "/"), "/")
	kind := p
	if len(parts) >= 2 {
		kind = parts[len(parts)-1]
	}
	s.Requests[r.Method+" "+kind]++
	switch {
	case p == "/" && r.Method == "GET":
		writeJSON(w, 200, map[string]any{"version": map[string]any{"number": "8.99.0-fake"}})
	case len(parts) == 3 && parts[0] == "_cat" && parts[1] == "indices":
		var rows []map[string]string
		for _, ix := range s.match(parts[2]) {
			st := "open"
			if ix.closed {
				st = "close"
			}
			rows = append(rows, map[string]string{"index": ix.name, "uuid": ix.uuid, "status": st, "pri": strconv.Itoa(len(ix.shards))})
		}
		if rows == nil {
			rows = []map[string]string{}
		}
		writeJSON(w, 200, rows)
	case len(parts) == 3 && parts[1] == "_stats":
		out := map[string]any{}
		for _, ix := range s.match(parts[0]) {
			if ix.closed {
				continue
			}
			shards := map[string]any{}
			for i, sh := range ix.shards {
				shards[strconv.Itoa(i)] = []any{map[string]any{
					"routing": map[string]any{"primary": true, "node": "n1", "state": "STARTED"},
					"seq_no":  map[string]any{"max_seq_no": sh.nextSeq - 1, "local_checkpoint": sh.lcp, "global_checkpoint": sh.lcp - sh.gcpLag},
				}}
			}
			out[ix.name] = map[string]any{"shards": shards}
		}
		writeJSON(w, 200, map[string]any{"indices": out})
	case len(parts) == 2 && parts[1] == "_refresh" && r.Method == "POST":
		ix := s.indices[parts[0]]
		if ix == nil {
			writeJSON(w, 404, map[string]any{"error": "index_not_found"})
			return
		}
		s.refresh(ix)
		writeJSON(w, 200, map[string]any{"_shards": map[string]int{"total": len(ix.shards), "successful": len(ix.shards), "failed": 0}})
	case len(parts) == 2 && parts[0] == "_search" && parts[1] == "scroll":
		s.scroll(w, r, body)
	case len(parts) == 2 && parts[1] == "_search":
		s.search(w, r, parts[0], body)
	case p == "/_bulk" && r.Method == "POST":
		s.bulk(w, body)
	default:
		writeJSON(w, 400, map[string]any{"error": "fake: unsupported " + r.Method + " " + p})
	}
}

func hitJSON(ix *index, d doc, withSource bool) map[string]any {
	h := map[string]any{"_index": ix.name, "_id": d.ID, "_seq_no": d.Seq, "_primary_term": d.Term, "_version": d.Version}
	if d.Routing != "" {
		h["_routing"] = d.Routing
	}
	if withSource {
		h["_source"] = d.Source
	}
	return h
}

func (s *Server) search(w http.ResponseWriter, r *http.Request, name string, body []byte) {
	ix := s.indices[name]
	if ix == nil {
		writeJSON(w, 404, map[string]any{"error": "index_not_found"})
		return
	}
	var q struct {
		Size  int `json:"size"`
		Query struct {
			Range map[string]struct {
				GT  *int64 `json:"gt"`
				LTE *int64 `json:"lte"`
			} `json:"range"`
		} `json:"query"`
	}
	json.Unmarshal(body, &q)
	if r.URL.Query().Get("scroll") != "" {
		var hits []map[string]any
		for _, sh := range ix.shards {
			ids := make([]string, 0, len(sh.searchable))
			for id := range sh.searchable {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			for _, id := range ids {
				hits = append(hits, hitJSON(ix, sh.searchable[id], false))
			}
		}
		var pages []map[string]any
		for i := 0; i < len(hits); i += q.Size {
			j := i + q.Size
			if j > len(hits) {
				j = len(hits)
			}
			pages = append(pages, map[string]any{"hits": hits[i:j]})
		}
		sid := fmt.Sprintf("scroll-%d", len(s.scrolls)+1)
		first := map[string]any{"hits": []map[string]any{}}
		if len(pages) > 0 {
			first, pages = pages[0], pages[1:]
		}
		s.scrolls[sid] = pages
		writeJSON(w, 200, map[string]any{"_scroll_id": sid, "hits": first, "_shards": map[string]int{"total": len(ix.shards), "successful": len(ix.shards)}})
		return
	}
	if s.FailSearches > 0 {
		s.FailSearches--
		writeJSON(w, 503, map[string]any{"error": "fake: unavailable"})
		return
	}
	pref := r.URL.Query().Get("preference")
	n, err := strconv.Atoi(strings.TrimPrefix(pref, "_shards:"))
	if err != nil || n < 0 || n >= len(ix.shards) {
		writeJSON(w, 400, map[string]any{"error": "fake: need preference=_shards:N"})
		return
	}
	rg := q.Query.Range["_seq_no"]
	var docs []doc
	for _, d := range ix.shards[n].searchable {
		if rg.GT != nil && d.Seq <= *rg.GT {
			continue
		}
		if rg.LTE != nil && d.Seq > *rg.LTE {
			continue
		}
		docs = append(docs, d)
	}
	sort.Slice(docs, func(i, j int) bool { return docs[i].Seq < docs[j].Seq })
	if len(docs) > q.Size {
		docs = docs[:q.Size]
	}
	hits := []map[string]any{}
	for _, d := range docs {
		hits = append(hits, hitJSON(ix, d, true))
	}
	writeJSON(w, 200, map[string]any{"hits": map[string]any{"hits": hits}, "_shards": map[string]int{"total": 1, "successful": 1}})
}

func (s *Server) scroll(w http.ResponseWriter, r *http.Request, body []byte) {
	var q struct {
		ScrollID any `json:"scroll_id"`
	}
	json.Unmarshal(body, &q)
	if r.Method == "DELETE" {
		if ids, ok := q.ScrollID.([]any); ok {
			for _, id := range ids {
				delete(s.scrolls, fmt.Sprint(id))
			}
		}
		writeJSON(w, 200, map[string]any{"succeeded": true})
		return
	}
	sid := fmt.Sprint(q.ScrollID)
	pages, ok := s.scrolls[sid]
	if !ok {
		writeJSON(w, 404, map[string]any{"error": "search_context_missing"})
		return
	}
	page := map[string]any{"hits": []map[string]any{}}
	if len(pages) > 0 {
		page, s.scrolls[sid] = pages[0], pages[1:]
	}
	writeJSON(w, 200, map[string]any{"_scroll_id": sid, "hits": page, "_shards": map[string]int{"total": 1, "successful": 1}})
}

func (s *Server) bulk(w http.ResponseWriter, body []byte) {
	lines := strings.Split(strings.TrimRight(string(body), "\n"), "\n")
	var items []any
	errs := false
	for i := 0; i < len(lines); i++ {
		var meta map[string]struct {
			Index   string `json:"_index"`
			ID      string `json:"_id"`
			Routing string `json:"routing"`
		}
		json.Unmarshal([]byte(lines[i]), &meta)
		for action, m := range meta {
			ix := s.indices[m.Index]
			if ix == nil {
				s.n++
				ix = &index{name: m.Index, uuid: fmt.Sprintf("uuid-%s-%d", m.Index, s.n)}
				ix.shards = []*shard{{processed: map[int64]bool{}, pending: map[int64]op{}, live: map[string]doc{}, searchable: map[string]doc{}, versions: map[string]int64{}, lcp: -1}}
				s.indices[m.Index] = ix
			}
			p := ix.shards[shardOf(len(ix.shards), m.ID, m.Routing)]
			o := op{id: m.ID, routing: m.Routing, seq: p.nextSeq}
			p.nextSeq++
			status := 200
			switch action {
			case "index":
				i++
				o.source = json.RawMessage(lines[i])
			case "delete":
				o.delete = true
				if _, ok := p.live[m.ID]; !ok {
					status = 404
				}
			default:
				status = 400
				errs = true
			}
			s.apply(ix, p, o)
			items = append(items, map[string]any{action: map[string]any{"_index": m.Index, "_id": m.ID, "status": status}})
		}
	}
	writeJSON(w, 200, map[string]any{"errors": errs, "items": items})
}
