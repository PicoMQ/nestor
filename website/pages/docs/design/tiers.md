# Cache tiers

Blocks live in a hybrid cache built on [foyer](https://github.com/foyer-rs/foyer): a sharded RAM tier and an optional disk tier that share one key space. A lookup checks RAM, then disk. An insert goes to RAM and, when disk is configured, is written through to disk in the same step.

<div class="kakapo-diagram">
<svg viewBox="0 0 720 250" width="720" role="img" aria-label="A block lookup checks RAM shards then the disk regions. Inserts write to RAM and through to disk when configured.">
  <defs>
    <marker id="arrt" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="96" width="120" height="56" class="box"/>
  <text x="80" y="120" text-anchor="middle" class="label">block key</text>
  <text x="80" y="138" text-anchor="middle" class="sub">get or insert</text>
  <rect x="210" y="30" width="230" height="190" class="box-accent"/>
  <text x="325" y="56" text-anchor="middle" class="label">RAM</text>
  <rect x="230" y="72" width="60" height="40" class="box"/>
  <rect x="295" y="72" width="60" height="40" class="box"/>
  <rect x="360" y="72" width="60" height="40" class="box"/>
  <rect x="230" y="118" width="60" height="40" class="box"/>
  <rect x="295" y="118" width="60" height="40" class="box"/>
  <rect x="360" y="118" width="60" height="40" class="box"/>
  <text x="325" y="182" text-anchor="middle" class="sub">shards, 2 per core</text>
  <text x="325" y="200" text-anchor="middle" class="sub">S3-FIFO eviction</text>
  <rect x="510" y="30" width="190" height="190" class="box"/>
  <text x="605" y="56" text-anchor="middle" class="label">disk</text>
  <rect x="530" y="72" width="150" height="24" class="box-accent"/>
  <rect x="530" y="101" width="150" height="24" class="box-accent"/>
  <rect x="530" y="130" width="150" height="24" class="box-accent"/>
  <text x="605" y="182" text-anchor="middle" class="sub">regions, FIFO reclaim</text>
  <text x="605" y="200" text-anchor="middle" class="sub">direct I/O, io_uring</text>
  <path d="M140 124 L202 124" class="edge" marker-end="url(#arrt)"/>
  <path d="M440 124 L502 124" class="edge" marker-end="url(#arrt)"/>
  <text x="471" y="114" text-anchor="middle" class="sub">miss</text>
</svg>
</div>

## RAM

The RAM tier is bounded by bytes, not entries, and each block is weighted by its length plus its key. Eviction is [S3-FIFO](https://s3fifo.com/), which keeps one-hit blocks from a scan out of the main queue so a full-object read does not flush the working set of random readers behind it.

The tier is split into shards that each lock and evict independently. The default is two shards per core, capped so that every shard holds at least `32` MiB. A `256` MiB cache on a `16` core machine gets `8` shards rather than `32`, since a shard smaller than a few dozen blocks evicts erratically. Set `shards` explicitly when the default is wrong for the workload.

Blocks are `Bytes`, so a hit hands the caller a reference-counted slice of the cached buffer. No copy is made on the read path.

## Disk

Disk is optional. Without it, RAM is the only tier and a block that falls out of RAM is fetched again. With it, a block evicted from RAM is still a hit as long as it is on disk, which makes the working set the disk size rather than the RAM size.

| Setting | Default | Meaning |
| --- | --- | --- |
| `path` | | Directory foyer manages. Existing content is recovered on start. |
| `capacity` | | Bytes on disk. Filled before anything is reclaimed. |
| `region_size` | `64 MiB` | Append unit and reclaim unit. Larger regions mean fewer, larger sequential writes. |
| `direct_io` | `true` | Bypass the page cache on Linux so RAM stays available for the RAM tier. |
| `compression` | `none` | `lz4` or `zstd` per block. Worth it for text, not for already compressed data. |
| `recover` | `quiet` | `none` starts empty, `quiet` recovers what it can, `strict` fails on any corruption. |

The disk is written as a log of regions. A block is appended to the current region, regions are reclaimed FIFO when capacity is reached, and blocks within a reclaimed region are gone. There is no read-modify-write, so write amplification is one and a consumer-grade SSD lasts.

Inserts are written to disk at insertion time rather than on eviction from RAM. With this policy a block is durable on disk as soon as the origin has delivered it, so a restart shortly after warming does not lose the warm set, and RAM eviction never causes a burst of disk writes. Writes are buffered in a pool shared by the flushers, sized from capacity and clamped to `[flushers * region_size, 256 MiB]`.

On Linux the disk tier uses io_uring for reads and writes. Elsewhere it uses positioned synchronous I/O on a thread pool. Both paths are correct, the Linux one has lower per-request overhead at high queue depths.

## Restart

Recovery scans the regions on disk and rebuilds the index, so an endpoint restarted with the same path serves hits from disk immediately. Only the metadata cache is lost, and it refills from the first read of each object as described in [Consistency](/docs/design/consistency).

Removing the path, or starting with `recover = "none"`, is a full cold start. Since Nestor holds no state the origin does not also hold, that is always safe.
