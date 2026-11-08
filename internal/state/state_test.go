package state

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheckpointsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.SetShard("u1", "logs-1", 0, 41)
	s.SetShard("u1", "logs-1", 2, -1)
	s.SetLastScan("u1", 1234)
	if err := s.Save(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	got := s2.Get("u1")
	if got.Name != "logs-1" || got.Shards["0"] != 41 || got.Shards["2"] != -1 || got.LastScan != 1234 {
		t.Fatalf("got %+v", got)
	}
	if err := s2.Drop("u1"); err != nil {
		t.Fatal(err)
	}
	s3, _ := Open(dir)
	if s3.Get("u1") != nil {
		t.Fatal("dropped index still stored")
	}
}

func TestIDLogSurvivesTornTail(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	ids, err := s.IDs("u1")
	if err != nil {
		t.Fatal(err)
	}
	ids.Add(0, "a", "")
	ids.Add(1, "b", "r1")
	ids.Add(0, "c", "")
	ids.Add(1, "c", "r2") // same _id on another shard is another document
	ids.Remove(0, "a")
	if err := ids.Sync(); err != nil {
		t.Fatal(err)
	}
	s.Close()
	// Simulate a crash in the middle of writing a line.
	p := filepath.Join(dir, "ids-u1.log")
	f, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString(`{"o":"+","i":"half`)
	f.Close()
	s2, _ := Open(dir)
	ids2, err := s2.IDs("u1")
	if err != nil {
		t.Fatal(err)
	}
	if ids2.Len() != 3 || !ids2.Has(1, "b") || ids2.Routing(1, "b") != "r1" || !ids2.Has(0, "c") || !ids2.Has(1, "c") || ids2.Has(0, "a") {
		t.Fatalf("got %d ids", ids2.Len())
	}
	// A corrupt line in the middle is an error, not silently skipped.
	os.WriteFile(p, []byte("{\"o\":\"+\",\"i\":\"x\"}\nnot json\n{\"o\":\"+\",\"i\":\"y\"}\n"), 0o644)
	s3, _ := Open(dir)
	if _, err := s3.IDs("u1"); err == nil {
		t.Fatal("expected an error for a corrupt id log")
	}
}
