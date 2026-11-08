package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPSink(t *testing.T) {
	var got []string
	var auth string
	fail := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail {
			fail = false
			http.Error(w, "busy", 503)
			return
		}
		b, _ := io.ReadAll(r.Body)
		got = strings.Split(strings.TrimSpace(string(b)), "\n")
		auth = r.Header.Get("Authorization")
	}))
	defer srv.Close()
	s := &HTTP{URL: srv.URL, Headers: http.Header{"Authorization": {"Bearer x"}}}
	batch := []Event{{Op: "upsert", Index: "i", ID: "1", SeqNo: 3, Source: json.RawMessage(`{"a":1}`)}, {Op: "delete", Index: "i", ID: "2", Shard: -1}}
	err := s.Write(context.Background(), batch)
	if err == nil || !strings.Contains(err.Error(), "HTTP 503") {
		t.Fatalf("expected a 503 error, got %v", err)
	}
	if err := s.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || auth != "Bearer x" || !strings.Contains(got[0], `"source":{"a":1}`) {
		t.Fatalf("got %q auth %q", got, auth)
	}
}

func TestWriter(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.Write(context.Background(), []Event{
		{Op: "upsert", Index: "i", IndexUUID: "u", Shard: 0, ID: "a", SeqNo: 0, PrimaryTerm: 1, Version: 1, Source: json.RawMessage(`{}`)},
		{Op: "delete", Index: "i", IndexUUID: "u", Shard: 2, ID: "x", Routing: "r"},
	})
	want := `{"op":"upsert","index":"i","index_uuid":"u","shard":0,"id":"a","seq_no":0,"primary_term":1,"version":1,"source":{}}` + "\n" +
		`{"op":"delete","index":"i","index_uuid":"u","shard":2,"id":"x","routing":"r"}` + "\n"
	if buf.String() != want {
		t.Fatalf("got %s", buf.String())
	}
}
