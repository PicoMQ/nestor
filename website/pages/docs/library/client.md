# nestor-client

`nestor-client` is the client side of a [cluster](/docs/design/cluster). It holds the node list, computes block ownership, bounds load per node, hedges across nodes and presents each bucket of the cluster as an `Origin`. The `nestor` binary uses it for the `[cluster]` section, and a Rust service uses it to read from a shared cluster with a local tier in process.

## Cluster

```rust
use std::sync::Arc;
use std::time::Duration;
use nestor_client::{Cluster, ClusterConfig, Credentials, Membership};

let membership = Membership::dns("nestor.cache.svc", 9000).refresh(Duration::from_secs(10));

let cluster = Cluster::new(
    membership,
    ClusterConfig {
        credentials: Some(Credentials {
            access_key: "cache".into(),
            secret_key: secret,
        }),
        ..ClusterConfig::default()
    },
)
.await?;
```

`Membership::Static(vec![addr, ..])` is a fixed list. `Membership::dns(host, port)` resolves the name at start and, with `refresh`, on an interval for as long as the `Cluster` is alive. `Cluster::new` fails with `ClusterError::NoNodes` if the initial resolution is empty.

| `ClusterConfig` field | Default | Purpose |
| --- | --- | --- |
| `block_size` | `1 MiB` | Routing unit. A multiple of the nodes' block size. |
| `read_window` | `16` | Cluster blocks in flight per read. |
| `load_limit` | `256` | In-flight requests per node before spilling to the next ranked node. |
| `down_for` | `5s` | How long a node is skipped after a connection failure. |
| `hedge` | `Some(HedgeConfig::default())` | Cross-node hedging. `None` disables. |
| `transport` | `Transport::default()` | Node connections, `5s` connect timeout. Client retries and request timeouts are off, the cluster and the engine retry. |
| `tls` | `false` | `https` toward nodes. |
| `credentials` | `None` | SigV4 credentials matching the nodes' `[auth]`. `None` sends unsigned requests. |

The cluster is an `Arc<Cluster>`. `node_count` reports the current membership size and `config` the settings in force.

## ClusterOrigin

```rust
use nestor::{CacheConfig, Consistency, Namespace, Nestor};

let origin = Arc::new(cluster.origin("data"));

let nestor = Nestor::builder(CacheConfig::memory(1 << 30))
    .namespace(
        Namespace::new("data", origin.clone())
            .consistency(Consistency::Immutable),
    )
    .build()
    .await?;
```

`cluster.origin(bucket)` returns a `ClusterOrigin` implementing `Origin` for one bucket. Every `get` is split into cluster blocks, each sent as a ranged S3 `GET` to its owning node, and the bodies are chained into one response with `read_window` blocks in flight. `head` goes to the owner of block `0`.

Plugging it into a local `Nestor` gives two tiers: the local RAM and disk absorb repeat reads on this host, the cluster absorbs everything else, and the real origin sees a miss only when neither has the block. The local namespace's `block_size` should be the routing `block_size` or a divisor of it, so a local miss maps onto whole cluster blocks.

## Warming

```rust
origin.warm("reports/q3.parquet", size).await?;
```

`warm` fetches every block of an object on its owning node and discards the bytes, so the object is cached cluster wide. A write path that has just uploaded an object calls it with the size it uploaded. The gateway's `warm_on_write` is this call after a `PUT`.

## Failure semantics

A connection error to a node marks it down for `down_for` and retries the request once on the next ranked node. `404`, `412` and `416` from a node are answers and are returned as the corresponding `OriginError`. A cluster whose every node is down surfaces the connection error of the last attempt. Readers built on `nestor` see these as `NestorError::Origin`, and the engine's own retry policy applies on top.
