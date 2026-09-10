# Tuning

The model behind every knob: a read costs one block fetch per cold block it touches and nothing for warm ones. Tuning moves along two lines, how many bytes a fetch brings back against how many of those bytes are used, and how much is held against how much is re-fetched.

## Block size

`buckets.block_size` is the one setting worth deciding per workload.

| Access pattern | Block size | Why |
| --- | --- | --- |
| Random small reads, index lookups, footer reads | `64 KiB` to `256 KiB` | A `4` KiB read of a `1` MiB block fetches `256x` what it uses. Small blocks keep amplification and RAM per hot object down. |
| Mixed, columnar files, segment tails | `1 MiB` | The default. Row groups and index pages fit in a few blocks, sequential scans run at `8` MiB per origin request. |
| Sequential scans, whole-object reads, model weights | `4 MiB` to `16 MiB` | Fewer keys, fewer origin requests per byte, larger sequential writes on disk. |

Two readers of the same object only coalesce when they agree on block boundaries, which they always do within a namespace. Changing the block size of a namespace changes every key, so the cache goes cold for that namespace.

## Windows

`fetch_window` sets bytes per origin request, `block_size * fetch_window`. `8` MiB by default is a good size for S3, where per-request latency dominates below a few MiB and throughput per connection saturates above. `read_window` sets how much a single stream holds, and `read_window / fetch_window` is the number of origin requests one sequential reader keeps in flight. Raise `read_window` for a few high-throughput scans, lower it for thousands of concurrent slow consumers.

## Readahead

Readahead helps when access looks sequential to Nestor but each request is small: an HTTP client fetching a large file in `1` MiB ranges, a Parquet reader walking row groups in order. It hurts when the consumer already prefetches or reads whole objects, because it adds background fetches nothing will wait for. Stream stores and anything reading with `ReadRange::Full` should set `readahead = 0`.

## Consistency

`immutable` is the cheapest mode: no metadata round trip on cold bounded reads, no `If-Match` headers, no TTL. Use it whenever object names are never reused, which covers most data-lake and log-segment layouts.

In `etag` mode the TTL is a staleness bound, not a cache lifetime. Blocks stay cached past it, the TTL only decides when the next read spends a conditional `GET` to confirm them. A `60` s TTL on a hot object costs one `304` per minute. Set it to how long a reader may see an overwritten object and no shorter. Writers going through Nestor do not need a TTL at all, they invalidate directly.

## Memory and disk

RAM should hold the hot set and disk the working set. The hit ratio metrics tell which is short. A high disk hit share with a low RAM hit share means RAM is too small for the hot set. A low combined hit ratio on a workload that revisits data means disk is too small, or the working set is larger than believed.

Keep `memory` at least a few hundred MiB. Below that the shard count drops to keep each shard above `32` MiB, and eviction gets coarser. On a machine dedicated to Nestor, leave a few GiB to the kernel and give the rest to `memory`, direct I/O means the page cache is not competing.

Disk `region_size` trades reclaim granularity for write size. `64` MiB is fine for NVMe. Compression is worth enabling only for text and other compressible data, most Parquet, images and model weights are already compressed and pay CPU for nothing.

## Origin load

`origin_concurrency` bounds concurrent origin `GET`s per process. `64` is conservative for S3 and generous for a single self-hosted MinIO. The cost of setting it too high is a surge on cold start when every read is a miss, the cost of setting it too low is misses queueing behind each other. Watch `nestor_origin_ttfb_seconds` while raising it, an origin under pressure shows up there before it shows up as errors.

Hedging adds at most `hedge_concurrency` extra requests at any moment and normally far fewer. With the default `factor = 3.0` a hedge fires only for headers or a block three times slower than the recent mean. Lower the factor toward `2.0` on origins with a fat tail, raise it or disable hedging on origins that charge per request and have none. `quantile = 0.99` fixes the hedge rate at about one in a hundred regardless of the shape of the distribution, which is the better choice when the mean is dominated by a few very slow responses. `min` floors both stages, so on a fast origin it decides how long a block may stall before a second request goes out. The default `50` ms is several times the per-block time of S3 at `1` MiB blocks; raise it if `nestor_hedges_total{phase="body"}` grows without wins.

## Fetch timeouts

`[buckets.fetch]` defaults are loose, `5s` to first byte and `60s` per fetch, so a slow origin degrades rather than fails.

| Setting | Set to |
| --- | --- |
| `deadline` | The reader's own latency budget |
| `first_byte` | A few times the origin's p99 |
| `attempt` | At least `fetch_window` blocks at the origin's throughput |

`nestor_origin_timeouts_total` rising while `nestor_origin_ttfb_seconds` is flat means `first_byte` is below the origin's tail, not that the origin got slower.

## Cluster

`cluster.block_size` should equal `buckets.block_size` on the nodes, or be a small multiple of it. Larger routing blocks mean fewer node requests per read and less parallelism per object. `load_limit` should be high enough that a node is only ever skipped when it is truly saturated, spilled blocks are cached twice. `down_for` shorter than the time a node takes to restart causes reconnect churn, longer delays recovery.

## Measuring

`cargo bench -p nestor` runs the engine benchmarks against an in-memory origin with injectable latency, useful for comparing block sizes and windows in isolation. The end-to-end scenarios in `nestor-e2e/` run against RustFS and report the same metrics a production node would.
