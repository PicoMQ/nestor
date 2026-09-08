# Deployment

A `nestor` process holds nothing the origin does not also hold. It can be placed anywhere between clients and the origin, restarted at will, and scaled by running more of them. This page covers the placements that work and what each needs.

## Placements

<div class="kakapo-diagram">
<svg viewBox="0 0 720 230" width="720" role="img" aria-label="Three placements. Sidecar: nestor on the same host as the application, loopback only. Shared: one nestor endpoint for many clients with auth and TLS. Cluster: several nodes behind a DNS name with clients or gateways routing to them.">
  <defs>
    <marker id="arrd" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <text x="120" y="30" text-anchor="middle" class="label">sidecar</text>
  <rect x="30" y="48" width="180" height="110" class="box"/>
  <text x="120" y="70" text-anchor="middle" class="sub">host or pod</text>
  <rect x="50" y="84" width="140" height="28" class="box"/>
  <text x="120" y="103" text-anchor="middle" class="sub">application</text>
  <rect x="50" y="120" width="140" height="28" class="box-accent"/>
  <text x="120" y="139" text-anchor="middle" class="sub">nestor, 127.0.0.1</text>
  <text x="120" y="190" text-anchor="middle" class="sub">anonymous, no TLS</text>

  <text x="360" y="30" text-anchor="middle" class="label">shared</text>
  <rect x="290" y="48" width="140" height="28" class="box"/>
  <text x="360" y="67" text-anchor="middle" class="sub">many clients</text>
  <rect x="290" y="108" width="140" height="36" class="box-accent"/>
  <text x="360" y="130" text-anchor="middle" class="sub">nestor, 0.0.0.0</text>
  <path d="M360 76 L360 100" class="edge" marker-end="url(#arrd)"/>
  <text x="360" y="190" text-anchor="middle" class="sub">static auth, TLS, disk</text>

  <text x="600" y="30" text-anchor="middle" class="label">cluster</text>
  <rect x="530" y="48" width="140" height="28" class="box"/>
  <text x="600" y="67" text-anchor="middle" class="sub">clients, gateways</text>
  <rect x="520" y="108" width="50" height="36" class="box-accent"/>
  <rect x="575" y="108" width="50" height="36" class="box-accent"/>
  <rect x="630" y="108" width="50" height="36" class="box-accent"/>
  <path d="M570 76 L548 100" class="edge" marker-end="url(#arrd)"/>
  <path d="M600 76 L600 100" class="edge" marker-end="url(#arrd)"/>
  <path d="M630 76 L652 100" class="edge" marker-end="url(#arrd)"/>
  <text x="600" y="190" text-anchor="middle" class="sub">one DNS name, N nodes</text>
</svg>
</div>

**Sidecar.** One `nestor` per application host, listening on loopback. The application sets `AWS_ENDPOINT_URL_S3=http://127.0.0.1:9000` and nothing else. Anonymous auth is safe because only local processes reach the socket. RAM sizing follows the host's spare memory, disk is optional. This is the smallest change to an existing service, and if the service is Rust the [library](/docs/library/nestor) removes the hop entirely.

**Shared endpoint.** One or a few `nestor` processes serving a fleet. The listener is bound to a routable address, so `auth.mode = "static"` and `[server.tls]` are required in practice. Give it a disk tier, this is where a large working set pays off. Behind a load balancer, several shared endpoints each hold their own copy of hot blocks, which is fine for a small fleet and wasteful for a large one.

**Cluster.** Many `nestor` nodes behind one DNS name, each holding its share of blocks. Clients route with `nestor-client`, or point at a `nestor` gateway with a `[cluster]` section and get routing plus a local tier. Capacity is the sum of the nodes. See [Cluster](/docs/design/cluster) for how blocks are placed.

## Docker

```bash
docker run --rm \
    -p 9000:9000 -p 9100:9100 \
    -v ./nestor.toml:/etc/nestor/nestor.toml:ro \
    -v nestor-disk:/var/lib/nestor \
    ghcr.io/picomq/nestor
```

The image runs `nestor serve --config /etc/nestor/nestor.toml`. Configuration can also be entirely environment variables, `NESTOR_ORIGIN__ENDPOINT` and friends, with no file mounted. The disk tier path should be a volume, a bind mount on a local SSD, or left out. `curl -fsS http://127.0.0.1:9000/-/health` is a suitable health check.

The compose stacks in `nestor-e2e/` are complete working examples with RustFS as the origin. `single/` is one node with a disk tier, `cluster/` is three nodes behind a compose network alias plus a gateway with `dns = "nodes:9000"` and `warm_on_write = true`. Both are run by the end-to-end tests, so they are kept correct.

## Kubernetes

A cluster is a `Deployment` or `StatefulSet` plus a headless `Service`. The headless service gives one DNS name that resolves to every ready pod, which is exactly what `cluster.dns` wants.

- Set `server.listen = "0.0.0.0:9000"` and `server.metrics = "0.0.0.0:9100"`.
- Readiness and liveness probe `GET /-/health` on `9000`.
- Mount the disk tier on a local volume. An `emptyDir` on SSD-backed nodes is appropriate, the data is a cache.
- Keep `credentials = { source = "static" }` in the config and inject the keys from a `Secret` as `NESTOR_ORIGIN__CREDENTIALS__ACCESS_KEY` and `NESTOR_ORIGIN__CREDENTIALS__SECRET_KEY`, or use IRSA with `credentials = { source = "default" }`.
- Set `cluster.refresh` on the gateways to something close to the pod churn you expect, `10` s is a fine default.

Nodes are interchangeable, so rolling updates work with no coordination. A restarted pod with a persistent volume comes back warm from disk, one with `emptyDir` comes back cold and refills from the origin at the rate reads arrive.

## Sizing

RAM should be at least a few hundred MiB so the shards are not starved, and the disk tier should be sized to the working set rather than the total object footprint. A node with `16` GiB of RAM and a `1` TiB NVMe serves a working set of `1` TiB at NVMe latency with RAM absorbing the hottest blocks.

Origin request rate is bounded by `cache.origin_concurrency` per process, `64` by default, times the number of processes. A cluster of `20` nodes can have `1280` origin `GET`s in flight, which is well within S3's per-prefix limits but worth knowing against a self-hosted origin.

## Shutdown

`SIGTERM` or `SIGINT` stops accepting connections, drains in-flight requests for up to `30` s, then flushes the disk tier and exits. A `terminationGracePeriodSeconds` of `45` leaves room for that. Killing the process without a drain loses only what was in the write buffer, the disk tier recovers the rest on the next start.
