// Package state persists the per-shard checkpoints and, when delete detection
// is on, the set of document ids that have been delivered to the sink.
package state

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
)

// IndexState is the stored progress of one concrete index.
type IndexState struct {
	Name string `json:"name"`
	// Shards maps shard number to the highest sequence number whose effect
	// has been delivered. -1 means nothing delivered yet.
	Shards map[string]int64 `json:"shards"`
	// LastScan is the Unix time of the last completed delete scan.
	LastScan int64 `json:"last_scan,omitempty"`
}

type file struct {
	Version int                    `json:"version"`
	Indices map[string]*IndexState `json:"indices"`
}

// Store is the on-disk state directory.
type Store struct {
	dir string
	mu  sync.Mutex
	f   file
	ids map[string]*IDSet
}

// Open loads (or creates) the state directory.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, f: file{Version: 1, Indices: map[string]*IndexState{}}, ids: map[string]*IDSet{}}
	b, err := os.ReadFile(filepath.Join(dir, "checkpoints.json"))
	switch {
	case os.IsNotExist(err):
	case err != nil:
		return nil, err
	default:
		if err := json.Unmarshal(b, &s.f); err != nil {
			return nil, fmt.Errorf("checkpoints.json: %w", err)
		}
		if s.f.Version != 1 {
			return nil, fmt.Errorf("checkpoints.json: unsupported version %d", s.f.Version)
		}
		if s.f.Indices == nil {
			s.f.Indices = map[string]*IndexState{}
		}
	}
	return s, nil
}

// Get returns a copy of the stored state for an index uuid, or nil.
func (s *Store) Get(uuid string) *IndexState {
	s.mu.Lock()
	defer s.mu.Unlock()
	is := s.f.Indices[uuid]
	if is == nil {
		return nil
	}
	c := &IndexState{Name: is.Name, Shards: map[string]int64{}, LastScan: is.LastScan}
	for k, v := range is.Shards {
		c.Shards[k] = v
	}
	return c
}

// UUIDs lists the stored index uuids.
func (s *Store) UUIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.f.Indices))
	for k := range s.f.Indices {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// SetShard records progress for one shard. Call Save to persist.
func (s *Store) SetShard(uuid, name string, shard int, seq int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	is := s.f.Indices[uuid]
	if is == nil {
		is = &IndexState{Name: name, Shards: map[string]int64{}}
		s.f.Indices[uuid] = is
	}
	is.Name = name
	is.Shards[strconv.Itoa(shard)] = seq
}

// SetLastScan records the time of a completed delete scan.
func (s *Store) SetLastScan(uuid string, t int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if is := s.f.Indices[uuid]; is != nil {
		is.LastScan = t
	}
}

// Drop forgets an index (it was deleted from the cluster).
func (s *Store) Drop(uuid string) error {
	s.mu.Lock()
	delete(s.f.Indices, uuid)
	set := s.ids[uuid]
	delete(s.ids, uuid)
	s.mu.Unlock()
	if set != nil {
		set.close()
	}
	if err := os.Remove(s.idPath(uuid)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return s.Save()
}

// Save writes checkpoints.json atomically.
func (s *Store) Save() error {
	s.mu.Lock()
	b, err := json.MarshalIndent(s.f, "", "  ")
	s.mu.Unlock()
	if err != nil {
		return err
	}
	return writeAtomic(filepath.Join(s.dir, "checkpoints.json"), b)
}

func writeAtomic(path string, b []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if d, err := os.Open(filepath.Dir(path)); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}

func (s *Store) idPath(uuid string) string { return filepath.Join(s.dir, "ids-"+uuid+".log") }

// IDs returns the delivered-id set of an index, loading it from disk once.
func (s *Store) IDs(uuid string) (*IDSet, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if set := s.ids[uuid]; set != nil {
		return set, nil
	}
	set, err := loadIDSet(s.idPath(uuid))
	if err != nil {
		return nil, err
	}
	s.ids[uuid] = set
	return set, nil
}

// Close flushes and closes open id logs.
func (s *Store) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, set := range s.ids {
		set.close()
	}
}

// IDSet maps delivered document ids to their routing value. Changes are
// appended to a log file; Compact rewrites the file from memory.
type IDSet struct {
	path string
	m    map[string]string
	f    *os.File
	w    *bufio.Writer
}

type logLine struct {
	Op      string `json:"o"`
	ID      string `json:"i"`
	Routing string `json:"r,omitempty"`
}

func loadIDSet(path string) (*IDSet, error) {
	set := &IDSet{path: path, m: map[string]string{}}
	f, err := os.Open(path)
	if err == nil {
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
		ln := 0
		for sc.Scan() {
			ln++
			var l logLine
			if err := json.Unmarshal(sc.Bytes(), &l); err != nil {
				// A torn last line after a crash is expected; anything else is not.
				if !sc.Scan() {
					break
				}
				f.Close()
				return nil, fmt.Errorf("%s:%d: %w", path, ln, err)
			}
			switch l.Op {
			case "+":
				set.m[l.ID] = l.Routing
			case "-":
				delete(set.m, l.ID)
			}
		}
		f.Close()
		if err := sc.Err(); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	// Rewrite on load so a torn tail never stays in the file.
	if err := set.Compact(); err != nil {
		return nil, err
	}
	return set, nil
}

// Len is the number of ids.
func (s *IDSet) Len() int { return len(s.m) }

// Has reports whether id is in the set.
func (s *IDSet) Has(id string) bool { _, ok := s.m[id]; return ok }

// Snapshot returns a copy of the set.
func (s *IDSet) Snapshot() map[string]string {
	c := make(map[string]string, len(s.m))
	for k, v := range s.m {
		c[k] = v
	}
	return c
}

// Add records delivered ids (buffered until Sync).
func (s *IDSet) Add(id, routing string) error {
	if r, ok := s.m[id]; ok && r == routing {
		return nil
	}
	s.m[id] = routing
	return s.write(logLine{Op: "+", ID: id, Routing: routing})
}

// Remove records a delivered delete (buffered until Sync).
func (s *IDSet) Remove(id string) error {
	if _, ok := s.m[id]; !ok {
		return nil
	}
	delete(s.m, id)
	return s.write(logLine{Op: "-", ID: id})
}

func (s *IDSet) write(l logLine) error {
	b, _ := json.Marshal(l)
	b = append(b, '\n')
	_, err := s.w.Write(b)
	return err
}

// Sync flushes buffered changes to disk.
func (s *IDSet) Sync() error {
	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.f.Sync()
}

// Compact rewrites the log as one "+" line per id.
func (s *IDSet) Compact() error {
	s.close()
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	keys := make([]string, 0, len(s.m))
	for k := range s.m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b, _ := json.Marshal(logLine{Op: "+", ID: k, Routing: s.m[k]})
		w.Write(b)
		w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	f, err = os.OpenFile(s.path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	s.f = f
	s.w = bufio.NewWriterSize(f, 256*1024)
	return nil
}

func (s *IDSet) close() {
	if s.f != nil {
		if s.w != nil {
			s.w.Flush()
		}
		s.f.Close()
		s.f, s.w = nil, nil
	}
}
