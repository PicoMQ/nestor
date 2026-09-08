# Metrics

Nestor instruments the engine through the [`metrics`](https://docs.rs/metrics) facade, so a library user attaches whatever exporter the host already runs. The `nestor` binary serves Prometheus text on `server.metrics` at `/metrics`, together with foyer's own tier metrics.

```toml
[server]
metrics = "0.0.0.0:9100"
```

## Engine

Every series carries a `namespace` label. In the S3 endpoint that is the bucket name.

| Metric | Type | Meaning |
| --- | --- | --- |
| `nestor_blocks_hit_total` | counter | Blocks served from RAM or disk. |
| `nestor_blocks_miss_total` | counter | Blocks this read had to fetch. |
| `nestor_blocks_joined_total` | counter | Blocks this read waited on because another read was already fetching them. |
| `nestor_blocks_stale_total` | counter | Fetches abandoned because the object changed mid-read. |
| `nestor_origin_requests_total` | counter | `GET`s to the origin, hedges excluded. |
| `nestor_origin_bytes_total` | counter | Bytes received from the origin. |
| `nestor_origin_errors_total` | counter | Origin `GET`s that failed, before retry. |
| `nestor_origin_retries_total` | counter | Retries issued. |
| `nestor_origin_ttfb_seconds` | histogram | Time to first byte per origin `GET`. |
| `nestor_hedges_total` | counter | Secondary requests issued. |
| `nestor_hedge_wins_total` | counter | Secondary requests that answered before the primary. |
| `nestor_readahead_blocks_total` | counter | Blocks scheduled by readahead. |
| `nestor_meta_heads_total` | counter | `HEAD` requests to the origin. |
| `nestor_bytes_served_total` | counter | Bytes returned to callers. |

The ratios that matter:

- **Hit ratio** is `hit / (hit + miss + joined)`. `joined` counts as neither a hit nor an origin request, it is the coalescing at work. A high `joined` share means many readers on the same cold data.
- **Bytes amplification** is `origin_bytes / bytes_served`. Above `1` for random small reads because whole blocks are fetched, well below `1` once the cache is warm. Persistently above `1` on a warm cache suggests the block size is too large for the access pattern.
- **Hedge effectiveness** is `hedge_wins / hedges`. Near zero means the hedge delay is too short, hedges fire but the primary still wins. Near one means the origin has a real tail and hedging is paying for itself.
- **`meta_heads`** should stay near zero. Cold reads learn metadata from their first `GET`, so a rising count means suffix reads on cold objects or `HEAD` requests from clients.

## Tiers

foyer exports its own series for the RAM and disk tiers, prefixed `foyer_`. They cover memory usage and eviction, disk region fill and reclaim, and I/O latency by operation, and are emitted on the same `/metrics` page. Their names and labels are foyer's and follow its releases.

## Cluster

A gateway is an endpoint whose origin is the cluster, so its `nestor_origin_*` series describe requests to cluster nodes rather than to the real origin. Each node's own metrics describe its traffic to the origin. Comparing `nestor_origin_requests_total` on the gateway with the sum over nodes gives the cluster hit ratio.

## Health

`GET /-/health` on the S3 listener returns `200 ok` while the process is serving. It does not check the origin, a healthy Nestor with an unreachable origin still serves hits and fails misses with the origin's error.
