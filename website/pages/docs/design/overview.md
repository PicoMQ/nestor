# Overview

Nestor is one engine with three front doors. The engine caches blocks and talks to origins. Everything else is an adapter on one of its two sides: what asks for bytes, and where bytes come from.

<div class="kakapo-diagram">
<svg viewBox="0 0 720 330" width="720" role="img" aria-label="Callers, the S3 endpoint and the object_store adapter sit in front of the nestor engine. Origins behind it are object_store backends or a cluster of nestor nodes.">
  <defs>
    <marker id="arro" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="30" width="180" height="56" class="box"/>
  <text x="110" y="54" text-anchor="middle" class="label">Rust service</text>
  <text x="110" y="72" text-anchor="middle" class="sub">get, read, head</text>
  <rect x="20" y="130" width="180" height="56" class="box"/>
  <text x="110" y="154" text-anchor="middle" class="label">object_store user</text>
  <text x="110" y="172" text-anchor="middle" class="sub">NestorStore</text>
  <rect x="20" y="230" width="180" height="56" class="box"/>
  <text x="110" y="254" text-anchor="middle" class="label">S3 client</text>
  <text x="110" y="272" text-anchor="middle" class="sub">nestor-s3 endpoint</text>
  <rect x="270" y="30" width="180" height="256" class="box-accent"/>
  <text x="360" y="60" text-anchor="middle" class="label">nestor</text>
  <text x="360" y="90" text-anchor="middle" class="sub">namespaces</text>
  <text x="360" y="112" text-anchor="middle" class="sub">block index</text>
  <text x="360" y="134" text-anchor="middle" class="sub">in flight table</text>
  <text x="360" y="156" text-anchor="middle" class="sub">fetcher, hedges, retries</text>
  <text x="360" y="178" text-anchor="middle" class="sub">readahead</text>
  <text x="360" y="200" text-anchor="middle" class="sub">metadata cache</text>
  <text x="360" y="222" text-anchor="middle" class="sub">RAM + disk tiers</text>
  <text x="360" y="262" text-anchor="middle" class="sub">Origin trait</text>
  <rect x="520" y="80" width="180" height="56" class="box"/>
  <text x="610" y="104" text-anchor="middle" class="label">object_store backend</text>
  <text x="610" y="122" text-anchor="middle" class="sub">ObjectStoreOrigin</text>
  <rect x="520" y="180" width="180" height="56" class="box"/>
  <text x="610" y="204" text-anchor="middle" class="label">nestor cluster</text>
  <text x="610" y="222" text-anchor="middle" class="sub">ClusterOrigin</text>
  <path d="M200 58 L262 58" class="edge" marker-end="url(#arro)"/>
  <path d="M200 158 L262 158" class="edge" marker-end="url(#arro)"/>
  <path d="M200 258 L262 258" class="edge" marker-end="url(#arro)"/>
  <path d="M450 108 L512 108" class="edge" marker-end="url(#arro)"/>
  <path d="M450 208 L512 208" class="edge" marker-end="url(#arro)"/>
</svg>
</div>

## Crates

| Crate | Role |
| --- | --- |
| `nestor` | The engine. Namespaces, blocks, tiers, fetch scheduling, consistency. Depends on the `Origin` trait only. |
| `nestor-store` | Both directions of `object_store`. `ObjectStoreOrigin` makes any `ObjectStore` an origin, `NestorStore` makes a cached namespace look like an `ObjectStore`. |
| `nestor-s3` | The S3 endpoint as an `axum` router. SigV4 verification, addressing, `GET` and `HEAD` from cache, forwarding and re-signing of everything else. |
| `nestor-client` | Client-side cluster routing. Rendezvous hashing, bounded load, membership, hedging across nodes. Exposes the cluster as an `Origin`. |
| `nestor-cli` | The `nestor` binary. TOML configuration, TLS, Prometheus export, and the wiring that turns the crates above into a node or a gateway. |
| `nestor-e2e` | End-to-end scenarios against RustFS in Docker Compose. |

Dependencies point one way. `nestor` knows nothing about S3 wire formats, HTTP or clusters. `nestor-s3` and `nestor-client` depend on `nestor` and `nestor-store`. `nestor-cli` depends on all of them and holds no logic of its own beyond configuration.

## Core model

A **namespace** is an origin plus caching policy: block size, fetch and read windows, consistency mode, readahead, hedging. A `Nestor` instance holds many namespaces over one shared cache. In the S3 endpoint every bucket becomes a namespace with the `[buckets]` policy. In a library, a namespace is whatever the application decides, one per object class is typical.

A **block** is a fixed-size slice of an object identified by namespace, object name, a content tag and its index. The tag is the reason a changed object cannot be served from old blocks, see [Consistency](/docs/design/consistency).

A **read** is a byte range against one object. It is resolved to a block range, served block by block as a stream with a bounded number of blocks in flight, and finished without ever buffering the whole object. [Blocks & reads](/docs/design/reads) covers the path in detail.

An **origin** is anything implementing `head` and `get` with ranges and preconditions. `object_store` gives Nestor every major backend. A cluster of other Nestor nodes is also an origin, which is how a local tier stacks on a shared one.

## Deployment shapes

<div class="kakapo-diagram">
<svg viewBox="0 0 720 250" width="720" role="img" aria-label="Three shapes. Embedded: a service with the nestor crate inside. Endpoint: clients talk S3 to a nestor binary. Cluster: clients or a gateway route blocks to several nodes.">
  <defs>
    <marker id="arrs" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <text x="120" y="30" text-anchor="middle" class="label">embedded</text>
  <rect x="30" y="50" width="180" height="90" class="box"/>
  <text x="120" y="76" text-anchor="middle" class="sub">your service</text>
  <rect x="55" y="90" width="130" height="36" class="box-accent"/>
  <text x="120" y="113" text-anchor="middle" class="sub">nestor crate</text>
  <rect x="60" y="180" width="120" height="40" class="box"/>
  <text x="120" y="205" text-anchor="middle" class="sub">origin</text>
  <path d="M120 140 L120 172" class="edge" marker-end="url(#arrs)"/>

  <text x="360" y="30" text-anchor="middle" class="label">endpoint</text>
  <rect x="300" y="50" width="120" height="40" class="box"/>
  <text x="360" y="75" text-anchor="middle" class="sub">S3 clients</text>
  <rect x="300" y="115" width="120" height="40" class="box-accent"/>
  <text x="360" y="140" text-anchor="middle" class="sub">nestor</text>
  <rect x="300" y="180" width="120" height="40" class="box"/>
  <text x="360" y="205" text-anchor="middle" class="sub">origin</text>
  <path d="M360 90 L360 107" class="edge" marker-end="url(#arrs)"/>
  <path d="M360 155 L360 172" class="edge" marker-end="url(#arrs)"/>

  <text x="600" y="30" text-anchor="middle" class="label">cluster</text>
  <rect x="530" y="50" width="140" height="40" class="box"/>
  <text x="600" y="75" text-anchor="middle" class="sub">clients, gateway</text>
  <rect x="500" y="115" width="60" height="40" class="box-accent"/>
  <text x="530" y="140" text-anchor="middle" class="sub">node</text>
  <rect x="570" y="115" width="60" height="40" class="box-accent"/>
  <text x="600" y="140" text-anchor="middle" class="sub">node</text>
  <rect x="640" y="115" width="60" height="40" class="box-accent"/>
  <text x="670" y="140" text-anchor="middle" class="sub">node</text>
  <rect x="540" y="180" width="120" height="40" class="box"/>
  <text x="600" y="205" text-anchor="middle" class="sub">origin</text>
  <path d="M580 90 L535 108" class="edge" marker-end="url(#arrs)"/>
  <path d="M600 90 L600 107" class="edge" marker-end="url(#arrs)"/>
  <path d="M620 90 L665 108" class="edge" marker-end="url(#arrs)"/>
  <path d="M535 155 L580 173" class="edge" marker-end="url(#arrs)"/>
  <path d="M600 155 L600 172" class="edge" marker-end="url(#arrs)"/>
  <path d="M665 155 L620 173" class="edge" marker-end="url(#arrs)"/>
</svg>
</div>

The shapes compose. A `nestor` binary with a `[cluster]` section is a gateway: its own RAM and disk are a local tier, and misses go to the cluster instead of the origin. A service can embed the `nestor` crate with a `ClusterOrigin` and get the same two-tier layout in process. Writes always go to the real origin, the cluster only ever serves reads.
