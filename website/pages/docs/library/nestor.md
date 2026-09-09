# nestor

The `nestor` crate is the engine. It has no opinion about S3, HTTP or where objects come from. Its inputs are a cache configuration, a set of namespaces each with an origin, and byte range requests.

```toml
[dependencies]
nestor = { git = "https://github.com/picomq/nestor" }
```

The `serde` feature derives `Deserialize` for `NamespaceConfig`, `Consistency`, `HedgeConfig` and `RetryConfig`, for applications that want the same TOML shape as the binary.

## Building

```rust
use std::sync::Arc;
use std::time::Duration;
use nestor::{BlockSize, CacheConfig, Consistency, DiskConfig, Namespace, Nestor};

let cache = CacheConfig::memory(2 << 30)
    .disk(DiskConfig::new("/var/lib/myservice/cache", 100 << 30));

let nestor = Nestor::builder(cache)
    .namespace(
        Namespace::new("segments", segments_origin)
            .block_size(BlockSize::new(4 << 20).unwrap())
            .consistency(Consistency::Immutable)
            .readahead(0),
    )
    .namespace(
        Namespace::new("manifests", manifests_origin)
            .block_size(BlockSize::new(64 << 10).unwrap())
            .consistency(Consistency::Etag { ttl: Duration::from_secs(5) }),
    )
    .origin_concurrency(128)
    .build()
    .await?;
```

`Nestor` is `Clone` and cheap to share, it is an `Arc` around the engine. Namespaces can also be registered after `build` with `register`, which returns the `NamespaceId` used in every call. `namespace(name)` looks an id up by name.

| Builder method | Default | Purpose |
| --- | --- | --- |
| `namespace` | | Adds a namespace. Any number. |
| `origin_concurrency` | `64` | Foreground origin `GET`s in flight across all namespaces. |
| `readahead_concurrency` | `16` | Background origin `GET`s. |
| `hedge_concurrency` | `16` | Hedge requests. |
| `meta_capacity` | `100000` | Objects whose size and `ETag` are remembered. |
| `metrics_registry` | none | A `mixtrics` registry for foyer's tier metrics. Engine metrics use the `metrics` facade regardless. |

`Namespace` takes the same settings as `[buckets]` in the binary: `block_size`, `fetch_window`, `read_window`, `consistency`, `readahead` and `fetch`. `fetch` is a [`FetchPolicy`](/docs/design/fetches#fetch-policy), `FetchPolicy::default().hedge(None)` disables hedging. A `NamespaceConfig` can be built once and applied with `config`.

Build the `object_store` client under an `ObjectStoreOrigin` with `nestor_store::Transport`, which turns client retries and request timeout off:

```rust
use nestor_store::Transport;

let transport = Transport::default();
let store = AmazonS3Builder::new()
    .with_client_options(transport.client_options())
    .with_retry(transport.retry_config())
    /* bucket, region, endpoint, credentials */
    .build()?;
```

## Reading

```rust
use nestor::ReadRange;

let ns = nestor.namespace("segments").unwrap();

// Whole range into memory
let bytes = nestor.read(ns, "topic/0/000123.log", 4096..8192).await?;

// Streamed
let mut stream = nestor.get(ns, "topic/0/000123.log", ReadRange::From(1 << 20)).await?;
let meta = stream.ready().await?;
println!("{} bytes, etag {:?}", meta.size, meta.etag);
while let Some(chunk) = stream.next().await {
    let chunk = chunk?;
}

// Metadata only
let meta = nestor.head(ns, "topic/0/000123.log").await?;

let options = ReadOptions::range(0..4096).fetch(FetchOverrides {
    first_byte: Some(Duration::from_millis(500)),
    attempts: Some(1),
    ..FetchOverrides::default()
});
let stream = nestor.get_opts(ns, "topic/0/000123.log", options).await?;
```

`get` accepts anything that converts into a `ReadRange`: a `Range<u64>`, `ReadRange::Full`, `ReadRange::From(start)` or `ReadRange::Suffix(n)`. It returns a `ReadStream` that yields `Bytes` in order, one block or block slice at a time, with at most `read_window` blocks in flight behind it. `get_opts` takes `ReadOptions`, a range plus `FetchOverrides` applied over the namespace's `FetchPolicy`. `FetchOverrides::parse` reads the same text as `X-Nestor-Fetch`.

`ReadStream::ready` waits until the object's metadata is known, which for a cold object means the first origin response, and returns it without consuming any data. `range` gives the resolved byte range once metadata is known, `collect` drains the stream into one `Bytes`. `read` is `get` followed by `collect`.

Errors are `NestorError`: `NotFound`, `Range(requested, size)` for a range past the end, `Stale` when the object changed under a read that could not be restarted, `UnknownNamespace`, `Origin` wrapping the origin's error including `OriginError::Timeout`, `Cache` for a foyer failure, and `Closed` after `close`.

## Writing to the cache

```rust
nestor.insert(ns, "manifests/current.json", Some(etag), &body)?;
nestor.invalidate(ns, "manifests/current.json")?;
```

`insert` splits a whole object into blocks and records its metadata, so the next read is served with no origin request. It is meant for the write path of an application that has just uploaded the object and holds its bytes and the `ETag` the origin returned. `invalidate` drops the metadata and, for immutable namespaces, the blocks. In `etag` namespaces the old blocks are unreachable under the new tag.

## Origins

An origin implements two methods.

```rust
use async_trait::async_trait;
use nestor::{GetOptions, GetResponse, ObjectMeta, Origin, OriginError};

#[async_trait]
impl Origin for MyBackend {
    async fn get(&self, object: &str, options: GetOptions) -> Result<GetResponse, OriginError> {
        // honour options.range, options.if_match, options.if_none_match
    }

    async fn head(&self, object: &str) -> Result<ObjectMeta, OriginError> { .. }
}
```

`GetResponse` is the object's metadata, the byte range actually returned and a body stream. The fetcher relies on the range being exactly what was asked or a prefix of it ending at the object size. `OriginError` variants tell the fetcher what to do: `Io`, `ShortRead` and `Timeout` are retried, `NotFound`, `PreconditionFailed` and `NotModified` are final. An origin does not need to recognise a range past the end of the object, the fetcher settles that with a `HEAD`.

Most applications will not implement this. `nestor_store::ObjectStoreOrigin` covers every `object_store` backend and `nestor_client::ClusterOrigin` covers a cluster of nodes. `MemoryOrigin`, in this crate, is an in-process origin for tests with `set_latency`, `slow_next` and `fail_next` to script origin behaviour and `gets` and `heads` counters to assert on it.

## Shutdown

`close` flushes the disk tier and stops the engine. Call it once from the owner on shutdown, later calls are no-ops. Dropping without `close` loses only the unflushed write buffer.

## Metrics

Engine counters and histograms are emitted through the `metrics` facade with a `namespace` label. Install any `metrics` recorder before building and they appear there. The series are listed in [Metrics](/docs/operations/metrics).
