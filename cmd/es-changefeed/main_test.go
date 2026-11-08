package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/Jwrede/es-changefeed/internal/fakees"
)

func TestRunOnce(t *testing.T) {
	es := fakees.New()
	defer es.Close()
	es.CreateIndex("orders", 2)
	es.Put("orders", "o1", "", `{"total":12}`)
	es.Put("orders", "o2", "c7", `{"total":30}`)
	dir := t.TempDir()
	var out, errb bytes.Buffer
	if code := run([]string{"-url", es.URL(), "-index", "orders", "-state-dir", dir, "-deletes", "-once"}, &out, &errb); code != 0 {
		t.Fatalf("exit %d: %s", code, errb.String())
	}
	if n := strings.Count(out.String(), "\n"); n != 2 || !strings.Contains(out.String(), `"routing":"c7"`) {
		t.Fatalf("got %s", out.String())
	}
	es.Delete("orders", "o1", "")
	out.Reset()
	if code := run([]string{"-url", es.URL(), "-index", "orders", "-state-dir", dir, "-deletes", "-once"}, &out, &errb); code != 0 {
		t.Fatalf("exit %d: %s", code, errb.String())
	}
	if !strings.HasPrefix(out.String(), `{"op":"delete","index":"orders"`) || !strings.Contains(out.String(), `"id":"o1"`) {
		t.Fatalf("got %s", out.String())
	}
}

func TestUsageErrors(t *testing.T) {
	var out, errb bytes.Buffer
	for _, args := range [][]string{{}, {"-index", "x", "-from", "later"}, {"-index", "x", "-sink", "kafka://x"}} {
		errb.Reset()
		if code := run(args, &out, &errb); code != 2 {
			t.Fatalf("%v: exit %d, want 2 (%s)", args, code, errb.String())
		}
	}
}
