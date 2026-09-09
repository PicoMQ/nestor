# Cluster

A cluster is a set of `nestor` endpoints that together hold each block once. Nodes do not talk to each other and share no state. Routing is entirely client side: every client computes the same owner for a block from the node list and the block's identity, so the cluster has no coordinator, no membership protocol and no rebalancing traffic.

<div class="kakapo-diagram">
<svg viewBox="0 0 720 270" width="720" role="img" aria-label="A client splits a range into cluster blocks and sends each block to its rendezvous owner among three nodes. Each node caches its blocks and fetches misses from the origin.">
  <defs>
    <marker id="arrk" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="96" width="150" height="76" class="box"/>
  <text x="95" y="122" text-anchor="middle" class="label">client</text>
  <text x="95" y="140" text-anchor="middle" class="sub">nestor-client or</text>
  <text x="95" y="156" text-anchor="middle" class="sub">nestor gateway</text>
  <rect x="290" y="30" width="160" height="56" class="box-accent"/>
  <text x="370" y="54" text-anchor="middle" class="label">node A</text>
  <text x="370" y="72" text-anchor="middle" class="sub">blocks 0, 3</text>
  <rect x="290" y="106" width="160" height="56" class="box-accent"/>
  <text x="370" y="130" text-anchor="middle" class="label">node B</text>
  <text x="370" y="148" text-anchor="middle" class="sub">block 2</text>
  <rect x="290" y="182" width="160" height="56" class="box-accent"/>
  <text x="370" y="206" text-anchor="middle" class="label">node C</text>
  <text x="370" y="224" text-anchor="middle" class="sub">block 1</text>
  <rect x="560" y="106" width="140" height="56" class="box"/>
  <text x="630" y="130" text-anchor="middle" class="label">origin</text>
  <text x="630" y="148" text-anchor="middle" class="sub">misses only</text>
  <path d="M170 120 L282 60" class="edge" marker-end="url(#arrk)"/>
  <path d="M170 134 L282 134" class="edge" marker-end="url(#arrk)"/>
  <path d="M170 148 L282 208" class="edge" marker-end="url(#arrk)"/>
  <path d="M450 60 L552 124" class="edge-soft" marker-end="url(#arrk)"/>
  <path d="M450 134 L552 134" class="edge-soft" marker-end="url(#arrk)"/>
  <path d="M450 208 L552 144" class="edge-soft" marker-end="url(#arrk)"/>
  <text x="200" y="262" class="sub">owner(block) = argmax over nodes of hash(node, object, block)</text>
</svg>
</div>

## Rendezvous hashing

For every block the client scores each node with a hash of the node address, the object and the block index, and the highest score owns the block. All clients use the same seeded hash, so they agree without exchanging anything. Adding a node moves to it exactly the blocks it now wins, about `1/n` of the total, and nothing else. Removing a node reassigns only that node's blocks, each to whatever was second in its ranking. There is no ring to rebalance and no virtual node table to distribute.

Because ownership is per block rather than per object, one large object is spread across the whole cluster. A `10` GiB scan is served by every node in parallel, and no single node has to hold a hot object entirely.

## Routing unit

The cluster has its own `block_size`, `1` MiB by default, which must be a multiple of the block size the nodes cache with. A read is split into cluster blocks, the first is requested to learn the object's size and `ETag`, and the rest are requested in order with `read_window` blocks in flight and their bodies chained into one stream. Every request is a ranged S3 `GET` to a node's endpoint, so from a node's point of view the client is just another S3 client and a routed block lands exactly on whole node blocks.

Subsequent blocks carry the `ETag` from the first as `If-Match`, so a read across nodes is one version or a `412`, the same guarantee as a single node, see [Consistency](/docs/design/consistency).

## Bounded load

Each node has a `load_limit` of in-flight requests from a given client, `256` by default. A block whose owner is at the limit is sent to the next node in its ranking instead, which either has the block from an earlier spill or fetches it from the origin. Hot spots cost some duplication rather than queueing behind one node. When every node is saturated the owner is used anyway.

## Failures and tails

A node whose connection fails is marked down for `down_for`, `5` s by default, and skipped in every ranking until then. The request that observed the failure is retried once on the next ranked node. Application-level errors such as `404` or `412` are answers and are not failed over.

Hedging works across nodes. Each node keeps the same latency histogram a namespace does, and a request that has not answered after the hedge delay is duplicated to the second ranked node, the first answer wins. A connection failure on either side while the other is in flight hands over to it. The rule and the defaults are the engine's, see [Hedging](/docs/design/fetches#hedging).

## Membership

The node list is either static or a DNS name.

```toml
[cluster]
nodes = ["10.0.1.5:9000", "10.0.1.6:9000", "10.0.1.7:9000"]
```

```toml
[cluster]
dns = "nestor.cache.svc:9000"
refresh = "10s"
```

A DNS name is re-resolved every `refresh` and the node set updated in place, so scale-out and node loss are picked up without a restart. A resolution that returns no addresses is ignored and the current set kept. A headless Kubernetes service or an ECS service discovery name is the intended source.

Nodes authenticate clients like any endpoint. `cluster.credentials` is what the nodes' `[auth]` expects and `cluster.tls` selects `https`.

## Two tiers

A `nestor` binary with a `[cluster]` section is a gateway. Its own RAM and disk are a first tier in front of the cluster, and misses go to node owners rather than to the origin. Writes still go to `[origin]` directly, the cluster is read only. With `warm_on_write = true` a `PUT` that passes through the gateway is followed by a fetch of each block on its owning node, so the object is hot cluster wide before anyone reads it.

<div class="kakapo-diagram">
<svg viewBox="0 0 720 200" width="720" role="img" aria-label="S3 clients talk to a gateway. Its local tier answers hits, misses are routed to cluster nodes, and writes bypass the cluster to the origin.">
  <defs>
    <marker id="arrg" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="72" width="120" height="56" class="box"/>
  <text x="80" y="104" text-anchor="middle" class="label">S3 clients</text>
  <rect x="200" y="72" width="140" height="56" class="box-accent"/>
  <text x="270" y="96" text-anchor="middle" class="label">gateway</text>
  <text x="270" y="114" text-anchor="middle" class="sub">local RAM + disk</text>
  <rect x="410" y="30" width="140" height="56" class="box-accent"/>
  <text x="480" y="54" text-anchor="middle" class="label">cluster nodes</text>
  <text x="480" y="72" text-anchor="middle" class="sub">shared tier</text>
  <rect x="580" y="72" width="120" height="56" class="box"/>
  <text x="640" y="104" text-anchor="middle" class="label">origin</text>
  <path d="M140 100 L192 100" class="edge" marker-end="url(#arrg)"/>
  <path d="M340 90 L402 62" class="edge" marker-end="url(#arrg)"/>
  <path d="M550 62 L572 88" class="edge-soft" marker-end="url(#arrg)"/>
  <path d="M340 112 L572 112" class="edge-soft" marker-end="url(#arrg)"/>
  <text x="360" y="54" class="sub">read miss</text>
  <text x="440" y="128" class="sub">writes, forwarded</text>
</svg>
</div>

The same layout is available in process. A service embeds the `nestor` crate with a `ClusterOrigin` from `nestor-client` as the namespace's origin and gets a local tier over the shared one without running a gateway, see [nestor-client](/docs/library/client).
