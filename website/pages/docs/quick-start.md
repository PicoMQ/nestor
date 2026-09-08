# Quick start

The `nestor` binary needs an origin to read from and a place to cache. For a first run the cache is RAM only and the origin is any S3-compatible endpoint you already have.

## Install

Build from source with the Rust toolchain:

```bash
git clone https://github.com/picomq/nestor && cd nestor
cargo install --path nestor-cli
```

This puts `nestor` in `~/.cargo/bin`. To build without installing, use `cargo build --release -p nestor-cli` and run `./target/release/nestor`. The [Docker](#docker) section below skips the host install.

## Run the endpoint

Configuration is one TOML file. Everything has a default except the origin.

```toml
# nestor.toml
[origin]
endpoint = "http://minio:9000"
region = "us-east-1"
credentials = { source = "static", access_key = "minioadmin", secret_key = "minioadmin" }

[cache]
memory = "1 GiB"

[cache.disk]
path = "/var/lib/nestor"
capacity = "50 GiB"
```

```bash
nestor check --config nestor.toml    # validate without starting
nestor serve --config nestor.toml
```

The endpoint listens on `http://127.0.0.1:9000`. Any key can also come from the environment as `NESTOR_SECTION__KEY`, for example `NESTOR_CACHE__MEMORY='"2 GiB"'`. The full file with every default is in [Configuration](/docs/operations/configuration).

Against AWS the origin block is shorter, credentials resolve the usual way through the environment, a profile or the instance role:

```toml
[origin]
endpoint = "https://s3.us-east-1.amazonaws.com"
region = "us-east-1"
```

::: info Note
The listener defaults to loopback with `auth.mode = "anonymous"`. Nestor reads from the origin with its own credentials, so binding to a wider address needs `auth.mode = "static"` and `[server.tls]`. See [S3 endpoint](/docs/design/endpoint).
:::

## Point a client at it

Nothing changes on the client besides the endpoint. Buckets and keys are the origin's.

```bash
export AWS_ENDPOINT_URL_S3=http://127.0.0.1:9000
aws s3 cp s3://bucket/key ./key            # served from cache after the first read
aws s3api get-object --bucket bucket --key key --range bytes=0-4095 /dev/stdout
aws s3 cp ./key s3://bucket/key            # forwarded to the origin, cache updated
aws s3 ls s3://bucket/                     # forwarded to the origin
```

Health is at `GET /-/health`. Prometheus metrics are on a second listener once `server.metrics` is set, see [Metrics](/docs/operations/metrics).

<div class="kakapo-or">or</div>

## Docker

The image is published as `ghcr.io/picomq/nestor`. It expects the config at `/etc/nestor/nestor.toml` and exposes `9000` for S3 and `9100` for metrics.

```bash
docker run --rm -p 9000:9000 \
    -v ./nestor.toml:/etc/nestor/nestor.toml:ro \
    -v nestor-disk:/var/lib/nestor \
    ghcr.io/picomq/nestor
```

The repository ships compose stacks with RustFS as the origin under `nestor-e2e/`. `single` is one node, `cluster` is three nodes and a gateway, `library` is RustFS alone for the embedded scenario:

```bash
docker build -t nestor-e2e:local .
docker compose -f nestor-e2e/single/compose.yml up
```

RustFS is on `:19000`, nestor on `:19001` and its metrics on `:19100`, with `nestor` / `nestornestor` as credentials. These are the same stacks the end-to-end tests run against, see [Deployment](/docs/operations/deployment).

## Use it as a library

The engine takes namespaces, each an origin plus caching settings, and answers byte ranges.

```rust
use std::sync::Arc;
use nestor::{BlockSize, CacheConfig, Consistency, Namespace, Nestor, ReadRange};
use nestor_store::ObjectStoreOrigin;

let store: Arc<dyn object_store::ObjectStore> = Arc::new(
    object_store::aws::AmazonS3Builder::from_env().with_bucket_name("bucket").build()?,
);

let nestor = Nestor::builder(CacheConfig::memory(512 << 20))
    .namespace(
        Namespace::new("segments", Arc::new(ObjectStoreOrigin::new(store.clone())))
            .block_size(BlockSize::new(4 << 20).unwrap())
            .consistency(Consistency::Immutable)
            .readahead(0),
    )
    .build()
    .await?;
let ns = nestor.namespace("segments").unwrap();

let bytes = nestor.read(ns, "topic/0/000123.log", 4096..8192).await?;
let stream = nestor.get(ns, "topic/0/000123.log", ReadRange::Full).await?;
```

Code that already works against `object_store` wraps the store instead and keeps its API:

```rust
let cached: Arc<dyn ObjectStore> = Arc::new(nestor_store::NestorStore::new(nestor, ns, store));
```

Reads go through the cache, writes go to the wrapped store and update the cache on the way. The crates are covered in [Library](/docs/library/nestor).
