// Package metrics is a minimal Prometheus text-format registry.
package metrics

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
)

type key struct {
	name   string
	labels string
}

// Registry holds gauges and counters.
type Registry struct {
	mu   sync.Mutex
	vals map[key]float64
	kind map[string]string
}

// New returns an empty registry.
func New() *Registry { return &Registry{vals: map[key]float64{}, kind: map[string]string{}} }

func labelString(l map[string]string) string {
	if len(l) == 0 {
		return ""
	}
	ks := make([]string, 0, len(l))
	for k := range l {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	parts := make([]string, len(ks))
	for i, k := range ks {
		v := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(l[k])
		parts[i] = fmt.Sprintf(`%s="%s"`, k, v)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// Set sets a gauge.
func (r *Registry) Set(name string, l map[string]string, v float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.kind[name] = "gauge"
	r.vals[key{name, labelString(l)}] = v
}

// Inc adds to a counter.
func (r *Registry) Inc(name string, l map[string]string, v float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.kind[name] = "counter"
	r.vals[key{name, labelString(l)}] += v
}

// Get returns a value (for tests).
func (r *Registry) Get(name string, l map[string]string) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.vals[key{name, labelString(l)}]
}

// Write writes the text exposition format.
func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ks := make([]key, 0, len(r.vals))
	for k := range r.vals {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool {
		if ks[i].name != ks[j].name {
			return ks[i].name < ks[j].name
		}
		return ks[i].labels < ks[j].labels
	})
	last := ""
	for _, k := range ks {
		if k.name != last {
			fmt.Fprintf(w, "# TYPE %s %s\n", k.name, r.kind[k.name])
			last = k.name
		}
		fmt.Fprintf(w, "%s%s %g\n", k.name, k.labels, r.vals[k])
	}
}

// Handler serves the registry.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		r.Write(w)
	})
}
