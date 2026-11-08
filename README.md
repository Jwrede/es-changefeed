# es-changefeed

A change feed for Elasticsearch and OpenSearch that runs outside the cluster.
It follows indices, aliases or data streams and emits every created or updated
document (and, optionally, every deleted one) to stdout, a file, an HTTP
endpoint or another cluster. It needs no plugin, no paid license and no
timestamp field in the documents.

```
$ es-changefeed -url http://localhost:9200 -index orders -deletes
{"op":"upsert","index":"orders","index_uuid":"c1Xo...","shard":1,"id":"1","seq_no":2,"primary_term":1,"version":2,"source":{"title":"edited"}}
{"op":"delete","index":"orders","index_uuid":"c1Xo...","shard":-1,"id":"2"}
```

## Why

Elasticsearch has no API that tells a client what changed.
[elastic/elasticsearch#1242 "Changes API"](https://github.com/elastic/elasticsearch/issues/1242)
has been open since August 2011 and has 330 reactions and 173 comments (as of
2026-11-08). The requests in it are for cache invalidation, syncing to other
databases, disaster-recovery copies, and an Elastic engineer noted in 2021 that
Elastic's own APM product needed the same thing. The answers available today:

- **Cross-cluster replication** reads the internal shard history, but only into
  another Elasticsearch cluster and only with a Platinum or Enterprise license.
  OpenSearch has its own replication plugin with the same limitation to
  cluster-to-cluster copies.
- **Polling on a timestamp field** (for example the Logstash `elasticsearch`
  input with a `schedule`) needs every writer to set that field, misses
  documents whose timestamp lags the poll window, and never sees deletes.
- **In-cluster plugins** linked in the issue
  ([derryx/elasticsearch-changes-plugin](https://github.com/derryx/elasticsearch-changes-plugin),
  archived, last change 2016;
  [ZhendongBai/elasticsearch-cdc](https://github.com/ZhendongBai/elasticsearch-cdc),
  last change 2021) must be built for each exact cluster version and cannot be
  installed on hosted clusters.

I did not find a maintained tool that produces a change stream from outside the
cluster.

## How it works

Since 6.0, every write to a shard gets a sequence number (`_seq_no`), and each
document stores the sequence number of the operation that last wrote it. For
every primary shard the feed keeps a checkpoint: the highest sequence number
whose effect has been delivered. Each round it

1. reads the shard's **global checkpoint** G from the index stats. G is the
   number below which every operation has been processed by all in-sync copies.
2. calls `_refresh` (only for indices that have new operations), so that
   everything up to G is visible to search on every copy.
3. searches the shard (`preference=_shards:N`) for documents with
   `checkpoint < _seq_no <= G`, sorted by `_seq_no`, and delivers them.
4. moves the checkpoint to G after the sink acknowledged the batch.

Two details make this correct where the naive approach (poll for
`_seq_no > max seen`) is not:

- Indexing threads finish out of order. A document with `_seq_no` 7 can be
  searchable before one with 6. Reading only up to the global checkpoint means
  6 is never skipped.
- Operations above the global checkpoint can be rolled back when a primary
  fails. The feed never reads them.

Each event is the current state of a document. If a document changes several
times between two rounds, only its latest version is emitted, with the
`_seq_no`, `_primary_term` and `_version` of that version. Per shard, events
arrive in `_seq_no` order, and each document lives on one shard, so a consumer
that applies events in order ends up with the same documents as the source.
Delivery is at-least-once: after a crash the feed repeats the last
unacknowledged batch.

### Deletes

A deleted document leaves nothing that search can find. With `-deletes`, the
feed records the id (and routing) of every document it delivered, in an
append-only log in the state directory. Every `-scan-interval` it refreshes
the index, lists the ids that still exist with one scroll, and emits a
`delete` event for every delivered id that is gone. The scan runs between the
shard reads of the same index. A document that is deleted and re-created
before the scan stays an upsert. A re-creation that is not yet visible during
the scan has a higher sequence number than anything delivered, so its upsert
arrives after the delete. The random-workload test in
`internal/feed/feed_test.go` checks this ordering.

Cost: the id set is held in memory (about 110 bytes per document with
20-character ids, measured with one million ids), a scan additionally holds the
list of live ids, and each scan reads every id of the index. Deletes are therefore detected with a delay
of up to `-scan-interval`. Delete events carry the shard and routing of the
deleted document but no sequence number.

Documents are tracked per shard. Elasticsearch enforces `_id` uniqueness only
within a shard, so with custom routing the same `_id` can exist on two shards
as two documents; the feed reports them separately. The `es+` sink reproduces
that only if the target index has the same number of shards.

### Indices, rollover, data streams

`-index` accepts names, wildcards, aliases and data streams, comma-separated.
Indices are tracked by uuid. An index that appears later (a rollover, a new
backing index of a data stream) is read from its first document. An index that
is deleted and re-created under the same name has a new uuid and is read from
the start. Closed indices are skipped and keep their checkpoints. When an
index disappears, its state is dropped. Shrink, split, clone and restore create
new indices, which the feed reads in full.

## Installation

Download a binary from the releases page, or with Go 1.24 or newer:

```
go install github.com/Jwrede/es-changefeed/cmd/es-changefeed@latest
```

The program uses only the Go standard library.

## Usage

```
# Everything that exists, then follow changes, as NDJSON on stdout
es-changefeed -url https://es.example:9200 -ca-cert ca.pem -index 'logs-*'

# Only changes from now on, including deletes, posted in batches to a webhook
ESCF_API_KEY=... es-changefeed -url https://es.example:9200 -index orders \
  -from now -deletes -sink https://hooks.example/orders -sink-header 'Authorization: Bearer ...'

# Mirror into another cluster (index, delete with the original routing)
ESCF_SINK_PASSWORD=... es-changefeed -url http://old:9200 -index products \
  -deletes -sink es+https://new:9200 -sink-user elastic -sink-index '{index}-copy'

# From cron: catch up, run one delete scan, exit
timeout 600 es-changefeed -index orders -deletes -once -sink file:/var/lib/orders.ndjson
```

| Flag | Default | Meaning |
|---|---|---|
| `-url` | `http://localhost:9200` | source cluster (env `ESCF_URL`); auth with `-user` + `ESCF_PASSWORD` or `ESCF_API_KEY` |
| `-index` | | names, patterns, aliases or data streams (required) |
| `-state-dir` | `es-changefeed-state` | checkpoints and delivered ids |
| `-from` | `start` | first run only: `start` emits existing documents, `now` only later changes |
| `-sink` | `stdout` | `stdout`, `file:PATH`, `http(s)://URL`, `es+http(s)://HOST` |
| `-deletes` | off | detect deletes (see above) |
| `-scan-interval` | `5m` | how often the delete scan runs |
| `-batch` | `1000` | documents per search and per sink write |
| `-interval` | `1s` | pause between rounds |
| `-no-refresh` | off | never refresh for reading; only read checkpoints older than `-settle` |
| `-once` | off | catch up, scan, exit |
| `-metrics` | | address for `/metrics` (Prometheus) and `/healthz` |

`-no-refresh` is for clusters where an extra `_refresh` per round is
unwanted. It relies on the index's own `refresh_interval` being shorter than
`-settle`. The delete scan still refreshes once per scan, because a stale
replica could otherwise make a live document look deleted.

Metrics: `escf_shard_lag_ops` (global checkpoint minus delivered position, per
shard), `escf_events_total{op}`, `escf_sink_errors_total`,
`escf_round_errors_total`, `escf_tracked_ids`, `escf_indices`.

### Sinks

- **stdout / file**: one JSON object per line. The file sink fsyncs after each
  batch before the checkpoint moves.
- **http**: each batch is one POST with an NDJSON body. Any 2xx acknowledges the
  batch. Failures are retried with backoff (up to 30 s between attempts).
- **es+**: `_bulk` into another cluster. Upserts become `index` actions with
  the original `_id` and routing, deletes become `delete` actions.
  `-sink-index` names the target, `{index}` is the source index.

## Testing

- `go test ./...` runs unit tests against an in-memory fake cluster
  (`internal/fakees`). The fake models what the algorithm depends on:
  sequence numbers assigned before an operation is processed, a local and
  global checkpoint, search visibility that changes only on refresh, deletes
  that disappear from search. A randomized test (20 seeds, 40 rounds each)
  mixes out-of-order operations, checkpoint lag, routing, deletes, re-creates
  and restarts, and checks that replaying the events reproduces the source.
  Mutations of the core logic (reading up to `max_seq_no` instead of the global
  checkpoint, skipping the refresh, moving the checkpoint past a full batch,
  not recording ids) each make tests fail.
- `ESCF_E2E_URL=http://localhost:9200 go test ./e2e/` runs against a real
  cluster. Four writers index, update and delete for 8 seconds with bulk
  requests while the feed mirrors into a second index with the `es+` sink;
  afterwards both indices must contain the same documents. Further tests cover
  `_seq_no` range searches, a data stream rollover and the CLI.
- CI runs the end-to-end tests against Elasticsearch 9.5.5, 8.19.23 and
  7.17.28, OpenSearch 3.9.0 and 2.19.7, Elasticsearch 9.5.5 with TLS and a
  password, and a three-node Elasticsearch cluster with one replica per shard
  where a node is killed with `docker kill` while the writers run and restarted
  3 seconds later.

## Limitations

- Intermediate versions are not emitted. If a document is written twice
  between two rounds, the feed sees only the second write.
- Delete detection costs a full id listing per scan and memory for all
  delivered ids. Without `-deletes`, deletes are not reported.
- Turning on `-deletes` after documents were already delivered without it
  leaves those documents untracked; use a new state directory.
- Every round makes one `_cat/indices` and one `_stats` request for the whole
  target, plus one `_refresh` and one search per shard with new operations.
  Indices are processed one after another.
- Not tested: Elastic Cloud and Amazon OpenSearch Service (the APIs used are
  available there, but I have not run it against them), clusters larger than
  three nodes, indices with more than a few thousand documents, and a primary
  failover with operations above the global checkpoint that are actually rolled
  back (the failover test kills a node, which does not force a rollback).
- Elasticsearch and OpenSearch before 7.x are not tested. `_seq_no` exists
  since 6.0.

## License

MIT
