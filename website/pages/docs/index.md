# Introduction

Nestor is a read-through block cache for S3-compatible object storage. Objects are split into fixed-size blocks held in RAM and on local disk. A read of any byte range resolves to the blocks covering it, hits are served locally and misses are fetched from the origin once, no matter how many readers wait on them.

<div class="kakapo-diagram">
<svg viewBox="0 0 720 260" width="720" role="img" aria-label="Readers send byte ranges to nestor. Nestor answers from RAM or disk and fetches missing blocks from the S3-compatible origin.">
  <defs>
    <marker id="arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="94" width="140" height="72" class="box"/>
  <text x="90" y="126" text-anchor="middle" class="label">readers</text>
  <text x="90" y="144" text-anchor="middle" class="sub">byte ranges</text>
  <rect x="240" y="30" width="240" height="200" class="box-accent"/>
  <text x="360" y="56" text-anchor="middle" class="label">nestor</text>
  <rect x="264" y="76" width="192" height="52" class="box"/>
  <text x="360" y="98" text-anchor="middle" class="label">RAM</text>
  <text x="360" y="115" text-anchor="middle" class="sub">blocks, S3-FIFO eviction</text>
  <rect x="264" y="148" width="192" height="52" class="box"/>
  <text x="360" y="170" text-anchor="middle" class="label">disk</text>
  <text x="360" y="187" text-anchor="middle" class="sub">blocks, optional tier</text>
  <text x="360" y="220" text-anchor="middle" class="sub">one fetch per missing block</text>
  <rect x="560" y="94" width="140" height="72" class="box"/>
  <text x="630" y="126" text-anchor="middle" class="label">origin</text>
  <text x="630" y="144" text-anchor="middle" class="sub">S3 compatible</text>
  <path d="M160 130 L232 130" class="edge" marker-end="url(#arr)"/>
  <path d="M480 130 L552 130" class="edge" marker-end="url(#arr)"/>
  <text x="196" y="120" text-anchor="middle" class="sub">GET</text>
  <text x="516" y="120" text-anchor="middle" class="sub">miss</text>
</svg>
</div>

The origin can be AWS S3 or anything speaking its API, such as MinIO, RustFS, Ceph or Cloudflare R2. Nestor only needs `GET` and `HEAD` on the objects it caches.

## Three shapes

The same engine runs in three ways, chosen per deployment rather than per codebase.

- **Library.** The `nestor` crate embeds in a Rust service. Namespaces map to origins, reads return `Bytes` or a stream. `nestor-store` wraps it as an `object_store::ObjectStore`, so existing code keeps its API and gains the cache.
- **S3 endpoint.** The `nestor` binary listens as an S3 endpoint. `GET` and `HEAD` are served from cache, every other request is forwarded to the origin and re-signed. A client switches by changing `AWS_ENDPOINT_URL_S3`, on the same host or on a dedicated machine.
- **Cluster.** Several `nestor` binaries share one cache. Clients, or a `nestor` gateway in front of them, route each block to its owner with rendezvous hashing, so the cluster holds each block once and grows by adding nodes.

## Features

- **Block-level caching.** Objects are cached as blocks, `1` MiB by default and configurable from `64` KiB to `16` MiB per namespace. A `4` KiB read of a `10` GiB object costs one block, a full scan streams block by block with a bounded window in flight.
- **Hybrid RAM and disk.** RAM is sharded and evicted with S3-FIFO. Disk is an optional second tier managed by [foyer](https://github.com/foyer-rs/foyer), with direct I/O and io_uring on Linux. Both tiers hold the same block keys.
- **Origin protection.** Concurrent misses on a block are coalesced into one fetch. Fetches are bounded by semaphores, retried with backoff and hedged against slow responses. A cold read costs one round trip, the first `GET` carries size and `ETag`.
- **Strong consistency.** Every block key carries a content tag derived from the object's `ETag`. A changed object never aliases the old blocks. Namespaces choose between `ETag` revalidation on a TTL and immutable objects that are never re-checked.
- **Drop-in deployment.** The S3 endpoint verifies SigV4 signatures and presigned URLs from clients, re-signs forwarded requests with its own credentials and invalidates or populates the cache on writes that pass through it.

## Where it fits

Nestor suits read-heavy access to objects on S3-compatible storage where the same bytes are read more than once: segment files of a log or stream store, Parquet and Iceberg data files behind a query engine, model weights and datasets, build artifacts. Random small reads and large sequential reads both map to blocks, so one cache serves both.

It is a cache, not a store. Writes are forwarded to the origin unchanged, and durability is the origin's. A cache node can be stopped, wiped and restarted, the only cost is a cold start.
