// Command es-changefeed streams document changes out of Elasticsearch or
// OpenSearch indices, based on per-shard sequence numbers.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Jwrede/es-changefeed/internal/cluster"
	"github.com/Jwrede/es-changefeed/internal/feed"
	"github.com/Jwrede/es-changefeed/internal/metrics"
	"github.com/Jwrede/es-changefeed/internal/sink"
	"github.com/Jwrede/es-changefeed/internal/state"
)

var version = "dev"

type headers []string

func (h *headers) String() string     { return strings.Join(*h, ", ") }
func (h *headers) Set(v string) error { *h = append(*h, v); return nil }

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("es-changefeed", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintf(stderr, `Usage: es-changefeed -url URL -index TARGET [flags]

Streams creates, updates and (with -deletes) deletes from Elasticsearch or
OpenSearch indices to stdout, a file, an HTTP endpoint or another cluster.

Flags:
`)
		fs.PrintDefaults()
	}
	var (
		urlF      = fs.String("url", env("ESCF_URL", "http://localhost:9200"), "source cluster URL (env ESCF_URL)")
		user      = fs.String("user", env("ESCF_USER", ""), "basic auth user (env ESCF_USER)")
		apiKey    = fs.String("api-key", "", "API key, sent as 'ApiKey <key>' (env ESCF_API_KEY)")
		caCert    = fs.String("ca-cert", "", "PEM file with the CA of the source cluster")
		insecure  = fs.Bool("insecure", false, "skip TLS verification for the source cluster")
		target    = fs.String("index", "", "index name, pattern, alias or data stream, comma-separated (required)")
		expand    = fs.String("expand-wildcards", "open,closed", "expand_wildcards used to resolve -index (add 'hidden' for hidden indices)")
		stateDir  = fs.String("state-dir", "es-changefeed-state", "directory for checkpoints and delivered ids")
		from      = fs.String("from", "start", "on the first run only: 'start' emits all existing documents, 'now' only later changes")
		sinkF     = fs.String("sink", "stdout", "stdout, file:PATH, http(s)://URL (NDJSON POST) or es+http(s)://HOST (bulk into a cluster)")
		sinkIndex = fs.String("sink-index", "{index}", "target index for es+ sinks; {index} is the source index name")
		sinkUser  = fs.String("sink-user", env("ESCF_SINK_USER", ""), "basic auth user for es+ sinks (env ESCF_SINK_USER; password in ESCF_SINK_PASSWORD)")
		sinkCA    = fs.String("sink-ca-cert", "", "PEM file with the CA of the sink cluster")
		sinkInsec = fs.Bool("sink-insecure", false, "skip TLS verification for es+ and https sinks")
		batch     = fs.Int("batch", 1000, "documents per search and per sink write")
		interval  = fs.Duration("interval", time.Second, "pause between rounds")
		noRefresh = fs.Bool("no-refresh", false, "never call _refresh; read only up to checkpoints older than -settle")
		settle    = fs.Duration("settle", 5*time.Second, "with -no-refresh: age a checkpoint must have before it is read (must exceed index.refresh_interval)")
		deletes   = fs.Bool("deletes", false, "detect deletes by tracking delivered ids and listing live ids every -scan-interval")
		scanEvery = fs.Duration("scan-interval", 5*time.Minute, "with -deletes: how often to list live ids")
		once      = fs.Bool("once", false, "catch up to the current checkpoints (and run a delete scan with -deletes), then exit")
		metricsF  = fs.String("metrics", "", "listen address for /metrics and /healthz, e.g. :9464")
		logFormat = fs.String("log-format", "text", "text or json (logs go to stderr)")
		showVer   = fs.Bool("version", false, "print version and exit")
	)
	var hdrs headers
	fs.Var(&hdrs, "sink-header", "extra header for http sinks, 'Name: value' (repeatable)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *showVer {
		fmt.Fprintln(stdout, "es-changefeed", version)
		return 0
	}
	if *target == "" {
		fmt.Fprintln(stderr, "es-changefeed: -index is required")
		fs.Usage()
		return 2
	}
	if *from != "start" && *from != "now" {
		fmt.Fprintln(stderr, "es-changefeed: -from must be 'start' or 'now'")
		return 2
	}
	if *apiKey == "" {
		*apiKey = os.Getenv("ESCF_API_KEY")
	}

	var h slog.Handler = slog.NewTextHandler(stderr, nil)
	if *logFormat == "json" {
		h = slog.NewJSONHandler(stderr, nil)
	}
	log := slog.New(h)

	src, err := cluster.New(cluster.Config{URL: *urlF, User: *user, Password: os.Getenv("ESCF_PASSWORD"),
		APIKey: *apiKey, CACert: *caCert, Insecure: *insecure})
	if err != nil {
		fmt.Fprintln(stderr, "es-changefeed:", err)
		return 2
	}

	var sk sink.Sink
	switch s := *sinkF; {
	case s == "stdout" || s == "-":
		sk = sink.NewWriter(stdout)
	case strings.HasPrefix(s, "file:"):
		sk, err = sink.NewFile(strings.TrimPrefix(s, "file:"))
	case strings.HasPrefix(s, "es+"):
		var c *cluster.Client
		c, err = cluster.New(cluster.Config{URL: strings.TrimPrefix(s, "es+"), User: *sinkUser,
			Password: os.Getenv("ESCF_SINK_PASSWORD"), APIKey: os.Getenv("ESCF_SINK_API_KEY"), CACert: *sinkCA, Insecure: *sinkInsec})
		sk = &sink.Bulk{Client: c, IndexTemplate: *sinkIndex}
	case strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://"):
		hs := &sink.HTTP{URL: s, Headers: http.Header{}}
		for _, hv := range hdrs {
			k, v, ok := strings.Cut(hv, ":")
			if !ok {
				fmt.Fprintf(stderr, "es-changefeed: bad -sink-header %q\n", hv)
				return 2
			}
			hs.Headers.Add(strings.TrimSpace(k), strings.TrimSpace(v))
		}
		if *sinkInsec {
			tr := http.DefaultTransport.(*http.Transport).Clone()
			tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // opt-in flag
			hs.Client = &http.Client{Transport: tr, Timeout: 60 * time.Second}
		}
		sk = hs
	default:
		fmt.Fprintf(stderr, "es-changefeed: unknown -sink %q\n", s)
		return 2
	}
	if err != nil {
		fmt.Fprintln(stderr, "es-changefeed:", err)
		return 2
	}
	defer sk.Close()

	store, err := state.Open(*stateDir)
	if err != nil {
		fmt.Fprintln(stderr, "es-changefeed:", err)
		return 1
	}
	defer store.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	info, err := src.Info(ctx)
	if err != nil {
		fmt.Fprintln(stderr, "es-changefeed: cannot reach source cluster:", err)
		return 1
	}
	log.Info("connected", "distribution", info.Distribution, "version", info.Version, "index", *target)

	reg := metrics.New()
	if *metricsF != "" {
		ln, err := net.Listen("tcp", *metricsF)
		if err != nil {
			fmt.Fprintln(stderr, "es-changefeed:", err)
			return 1
		}
		mux := http.NewServeMux()
		mux.Handle("/metrics", reg.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "ok") })
		srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		go srv.Serve(ln)
		defer srv.Close()
	}

	f := feed.New(feed.Config{Target: *target, Expand: *expand, From: *from, Refresh: !*noRefresh, Settle: *settle,
		Batch: *batch, Interval: *interval, Deletes: *deletes, ScanInterval: *scanEvery}, src, store, sk, log, reg)
	if *once {
		if *noRefresh {
			fmt.Fprintln(stderr, "es-changefeed: -once needs refresh (drop -no-refresh)")
			return 2
		}
		if err := f.Once(ctx); err != nil {
			fmt.Fprintln(stderr, "es-changefeed:", err)
			return 1
		}
		return 0
	}
	if err := f.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(stderr, "es-changefeed:", err)
		return 1
	}
	return 0
}
