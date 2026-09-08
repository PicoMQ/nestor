# Nestor

Nestor is a read-through block cache for S3-compatible object storage, in RAM and on local disk.

[Documentation](https://nestor.picomq.com/docs/) · [Discord](https://discord.gg/qsMy5sSpYX) · [Quick start](https://nestor.picomq.com/docs/quick-start)

## Install

```bash
cargo install --path nestor-cli
```

This puts the `nestor` binary in `~/.cargo/bin`. Or run it in place with `cargo run -p nestor-cli -- <args>`.

## Run the S3 endpoint

```bash
cat > nestor.toml <<'EOF'
[origin]
endpoint = "http://minio:9000"
credentials = { source = "static", access_key = "minioadmin", secret_key = "minioadmin" }

[cache]
memory = "1 GiB"
[cache.disk]
path = "/var/lib/nestor"
capacity = "50 GiB"
EOF

nestor serve --config nestor.toml
```

Point any S3 client at it:

```bash
export AWS_ENDPOINT_URL_S3=http://127.0.0.1:9000
aws s3 cp s3://bucket/key ./key
```

Every setting and its default is in [`nestor-cli/nestor.toml`](nestor-cli/nestor.toml). Any key can be set from the environment as `NESTOR_SECTION__KEY`. `nestor check --config nestor.toml` validates without starting. Works with any S3-compatible origin, only `credentials = { source = "default" }` is AWS-specific.

The listener defaults to loopback with `auth.mode = "anonymous"`. Nestor reads from the origin with its own credentials, so binding wider needs `auth.mode = "static"` and `[server.tls]`.

## Use it as a library

```rust
use std::sync::Arc;
use nestor::{BlockSize, CacheConfig, Consistency, Namespace, Nestor, ReadRange};
use nestor_store::ObjectStoreOrigin;

let store: Arc<dyn object_store::ObjectStore> = /* AmazonS3Builder, LocalFileSystem, ... */;
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

Code that already uses `object_store` wraps the store instead and keeps its API:

```rust
let cached: Arc<dyn ObjectStore> = Arc::new(nestor_store::NestorStore::new(nestor, ns, store));
```

## Metrics

`nestor_*` counters and histograms go through the `metrics` facade, labelled by namespace. The `nestor` binary serves them together with foyer's own metrics on `server.metrics` at `/metrics`.

## Test

```bash
cargo test --workspace
cargo bench -p nestor

./nestor-e2e/e2e.sh       # single node, cluster and library scenarios against RustFS, needs docker
```
