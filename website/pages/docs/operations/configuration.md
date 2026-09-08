# Configuration

The `nestor` binary is configured by one TOML file passed as `--config` or `NESTOR_CONFIG`. Every key has a default except `origin.endpoint`, and every key can be set from the environment as `NESTOR_SECTION__KEY`, which wins over the file. Unknown keys are rejected.

```bash
nestor check --config nestor.toml    # parse, validate, resolve credentials, exit
nestor serve --config nestor.toml
NESTOR_CACHE__MEMORY='"2 GiB"' NESTOR_SERVER__LISTEN='"0.0.0.0:9000"' nestor serve
```

Environment values are parsed as TOML values, quoting strings is always safe. Nested keys merge with the file, so a file can hold `credentials = { source = "static" }` and the environment the keys under it. Sizes accept `MiB`, `GiB` and friends, durations accept `50ms`, `2s`, `10m`.

The reference file with every default is [`nestor-cli/nestor.toml`](https://github.com/picomq/nestor/blob/main/nestor-cli/nestor.toml). The sections below follow it.

## `[server]`

| Key | Default | Purpose |
| --- | --- | --- |
| `listen` | `127.0.0.1:9000` | The S3 listener. |
| `metrics` | unset | Prometheus listener serving `/metrics`. Unset disables it. |
| `tls` | unset | `{ cert = "...", key = "..." }` in PEM. Terminates TLS on `listen`. |
| `addressing` | `{ style = "path" }` | Or `{ style = "virtual_hosted", domain = "s3.internal" }` to take the bucket from the `Host` header. |

Serving anonymous plaintext on a non-loopback address is allowed and logs a warning at startup. Anyone who can reach the socket can then read any object the origin credentials can.

## `[origin]`

| Key | Default | Purpose |
| --- | --- | --- |
| `endpoint` | `https://s3.us-east-1.amazonaws.com` | Any S3-compatible endpoint. |
| `region` | `us-east-1` | Used in the SigV4 scope of forwarded and fetched requests. |
| `virtual_hosted` | `false` | Address the origin as `bucket.endpoint` instead of `endpoint/bucket`. |
| `credentials` | `{ source = "default" }` | How Nestor authenticates to the origin. |

Credential sources:

- `default` resolves the AWS chain: environment variables, shared profile, ECS or IMDS role, IRSA. Requires an AWS-style environment and is the only AWS-specific setting in the file.
- `static` takes `access_key`, `secret_key` and an optional `session_token`. The usual choice for MinIO, RustFS and other compatible stores.
- `anonymous` sends unsigned requests, for public buckets.

## `[auth]`

| Key | Default | Purpose |
| --- | --- | --- |
| `mode` | `anonymous` | `anonymous` accepts everything, `static` verifies SigV4 against one key pair. |
| `access_key`, `secret_key` | | Required with `static`. What clients sign with. |

Client credentials and origin credentials are independent. A deployment usually hands clients a dedicated key pair that exists nowhere but in this file, and keeps the origin's credentials on the Nestor host only.

## `[cache]`

| Key | Default | Purpose |
| --- | --- | --- |
| `memory` | `256 MiB` | RAM tier size. |
| `shards` | derived | Two per core, capped so each shard holds at least `32` MiB. |
| `meta_entries` | `100000` | Objects whose size and `ETag` are remembered. |
| `origin_concurrency` | `64` | Foreground origin `GET`s in flight. |
| `readahead_concurrency` | `16` | Background origin `GET`s in flight. |
| `hedge_concurrency` | `16` | Hedge requests in flight. |
| `retry` | `{ attempts = 3, base = "50ms", max = "2s" }` | Backoff for I/O errors and short reads. |

## `[cache.disk]`

Absent by default. Present enables the disk tier.

| Key | Default | Purpose |
| --- | --- | --- |
| `path` | `/var/lib/nestor` | Directory foyer owns. Must be writable, existing regions are recovered. |
| `capacity` | `8 GiB` | Disk tier size. |
| `region_size` | `64 MiB` | Append and reclaim unit. |
| `direct_io` | `true` | `O_DIRECT` on Linux. Ignored elsewhere. |
| `compression` | `none` | `lz4` or `zstd`. |
| `recover` | `quiet` | `none`, `quiet` or `strict`. |

## `[buckets]`

The caching policy every bucket gets as a namespace.

| Key | Default | Purpose |
| --- | --- | --- |
| `block_size` | `1 MiB` | Power of two from `64 KiB` to `16 MiB`. |
| `fetch_window` | `8` | Blocks per origin `GET` on a miss. |
| `read_window` | `16` | Blocks in flight per stream. |
| `consistency` | `{ mode = "etag", ttl = "60s" }` | Or `{ mode = "immutable" }`. |
| `readahead` | `8` | Blocks prefetched on sequential access. `0` disables. |
| `hedge` | `{ factor = 3.0, min = "50ms", max = "2s" }` | Tail hedging against the origin. Omit the key to disable. |
| `populate_max` | `16 MiB` | Largest `PUT` body inserted into the cache on the way through. |

## `[cluster]`

Absent by default. Present turns this binary into a gateway that reads from a cluster of nodes instead of the origin. Exactly one of `nodes` and `dns` must be set.

| Key | Default | Purpose |
| --- | --- | --- |
| `nodes` | | Static list of `host:port`. |
| `dns` | | A name resolved to the node set, `host:port`. |
| `refresh` | `10s` | How often `dns` is re-resolved. |
| `block_size` | `1 MiB` | Routing unit. Must be a multiple of `buckets.block_size`. |
| `read_window` | `16` | Cluster blocks in flight per read. |
| `load_limit` | `256` | In-flight requests per node before spilling to the next choice. |
| `down_for` | `5s` | How long a node is skipped after a connection failure. |
| `hedge` | `{ factor = 3.0, min = "50ms", max = "2s" }` | Hedging across nodes. |
| `tls` | `false` | Use `https` toward nodes. |
| `credentials` | unset | `{ access_key, secret_key }` matching the nodes' `[auth]`. |
| `warm_on_write` | `false` | After a `PUT`, fetch each block on its owning node. |

A node in the cluster is a plain `nestor` binary with `[origin]` and no `[cluster]`. The gateway and the nodes should agree on `buckets.block_size`, and the gateway's `cluster.block_size` is normally the same value.

## Logging

Logs go to stderr through `tracing` and are filtered by `RUST_LOG`, `info` by default. Startup, shutdown, membership changes and configuration warnings are logged. Per-request behaviour is not, it is exposed as [metrics](/docs/operations/metrics) instead.
