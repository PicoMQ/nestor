# Blocks & reads

Every object in a namespace is divided into blocks of one fixed size. A block is the unit of caching, of origin fetching and of coalescing. A read never touches bytes outside the blocks covering its range, and never holds more than a bounded window of blocks in memory.

## Block size

Block size is a per-namespace power of two between `64` KiB and `16` MiB, `1` MiB by default. Block `i` covers bytes `[i * B, (i + 1) * B)`, the last block of an object is short. Alignment is what lets every reader of an object agree on block boundaries without coordination: any request for offset `n` in the object resolves to block `n / B` for everyone.

<div class="kakapo-diagram">
<svg viewBox="0 0 720 200" width="720" role="img" aria-label="A byte range request from offset 1.5 MiB to 3.2 MiB resolves to blocks 1, 2 and 3 of a 1 MiB block size object.">
  <defs>
    <marker id="arrb" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <text x="40" y="44" class="sub">object, 4.6 MiB, block size 1 MiB</text>
  <rect x="40" y="56" width="128" height="44" class="box"/>
  <text x="104" y="83" text-anchor="middle" class="sub">block 0</text>
  <rect x="168" y="56" width="128" height="44" class="box-accent"/>
  <text x="232" y="83" text-anchor="middle" class="label">block 1</text>
  <rect x="296" y="56" width="128" height="44" class="box-accent"/>
  <text x="360" y="83" text-anchor="middle" class="label">block 2</text>
  <rect x="424" y="56" width="128" height="44" class="box-accent"/>
  <text x="488" y="83" text-anchor="middle" class="label">block 3</text>
  <rect x="552" y="56" width="77" height="44" class="box"/>
  <text x="590" y="83" text-anchor="middle" class="sub">block 4</text>
  <path d="M232 128 L232 108" class="edge" marker-end="url(#arrb)"/>
  <path d="M578 128 L578 108" class="edge" marker-end="url(#arrb)"/>
  <path d="M232 140 L578 140" class="edge"/>
  <text x="405" y="164" text-anchor="middle" class="sub">read 1.5 MiB .. 3.2 MiB</text>
  <text x="405" y="182" text-anchor="middle" class="sub">blocks 1..4, sliced at both ends</text>
</svg>
</div>

The cache stores whole blocks. The bytes a caller gets back are slices of those blocks, so a `4` KiB read of a cold block costs one `1` MiB fetch and every later read within that block is a hit. Larger blocks mean fewer origin requests for sequential readers, smaller blocks mean less over-fetch for random ones. The trade-off is discussed in [Tuning](/docs/operations/tuning).

## Ranges

A read is a `ReadRange`: the whole object, a bounded `start..end`, an open `start..` or a suffix of the last `n` bytes. Bounded ranges resolve to blocks immediately. The other three need the object size, which the read learns from the metadata cache or from the first origin response, see [Origin fetches](/docs/design/fetches). A range past the end of the object fails with the object size attached, which the S3 endpoint turns into a `416`.

## The read path

A `get` returns a `ReadStream` that yields `Bytes` in block order. Behind it a reader walks the block range with a window of blocks in flight.

<div class="kakapo-diagram">
<svg viewBox="0 0 720 280" width="720" role="img" aria-label="For each block in the window the fetcher checks the cache, then the in flight table, and otherwise groups misses into one origin GET.">
  <defs>
    <marker id="arrp" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="112" width="150" height="56" class="box"/>
  <text x="95" y="136" text-anchor="middle" class="label">reader</text>
  <text x="95" y="154" text-anchor="middle" class="sub">window of blocks</text>
  <rect x="240" y="30" width="180" height="56" class="box"/>
  <text x="330" y="54" text-anchor="middle" class="label">cache</text>
  <text x="330" y="72" text-anchor="middle" class="sub">RAM, then disk</text>
  <rect x="240" y="112" width="180" height="56" class="box"/>
  <text x="330" y="136" text-anchor="middle" class="label">in flight table</text>
  <text x="330" y="154" text-anchor="middle" class="sub">someone already fetching</text>
  <rect x="240" y="194" width="180" height="56" class="box-accent"/>
  <text x="330" y="218" text-anchor="middle" class="label">fetch group</text>
  <text x="330" y="236" text-anchor="middle" class="sub">fetch_window blocks</text>
  <rect x="520" y="194" width="180" height="56" class="box"/>
  <text x="610" y="218" text-anchor="middle" class="label">origin</text>
  <text x="610" y="236" text-anchor="middle" class="sub">one ranged GET</text>
  <path d="M170 130 L232 58" class="edge" marker-end="url(#arrp)"/>
  <path d="M170 140 L232 140" class="edge" marker-end="url(#arrp)"/>
  <path d="M170 150 L232 222" class="edge" marker-end="url(#arrp)"/>
  <path d="M420 222 L512 222" class="edge" marker-end="url(#arrp)"/>
  <text x="182" y="88" class="sub">hit</text>
  <text x="182" y="132" class="sub">join</text>
  <text x="182" y="196" class="sub">own</text>
  <text x="330" y="272" text-anchor="middle" class="sub">blocks are inserted into the cache as they stream in</text>
</svg>
</div>

For each block in the window the fetcher does one of three things.

- **Hit.** The block is in RAM or on disk. The handle resolves at once.
- **Join.** Another read is already fetching the block. The handle waits on that fetch. The origin sees one request however many readers arrive.
- **Own.** Nothing has the block. The read registers itself as owner and the block joins a fetch group.

Consecutive owned blocks are grouped into one origin `GET` of up to `fetch_window` blocks, aligned to `fetch_window` boundaries so two readers moving through the same object produce identical groups and coalesce. The response body is consumed as a stream, each block is inserted into the cache and its waiters are resolved as soon as its bytes arrive, before the rest of the group has downloaded.

## Windows

Two settings bound how much a read has in motion.

| Setting | Default | Meaning |
| --- | --- | --- |
| `fetch_window` | `8` | Maximum blocks in one origin `GET`. With `1` MiB blocks a miss costs one `8` MiB request. |
| `read_window` | `16` | Maximum blocks a single stream has in flight, hits included. Bounds memory per reader. |

A stream yields blocks in order and only pulls new blocks into its window as the caller consumes. A slow consumer holds at most `read_window` blocks, a fast one keeps `read_window / fetch_window` origin requests in flight, which is what makes a sequential scan approach origin throughput without any explicit prefetch.

Readahead is separate and covered with the fetch scheduler. It extends the window past what the caller asked for when access looks sequential, and runs at a lower priority so it never delays a foreground miss.

## Whole-object writes

`insert` puts a whole object into the cache without an origin round trip, splitting it into blocks and recording its metadata. `invalidate` drops the metadata and, for immutable namespaces, the blocks. The S3 endpoint uses both on `PUT` and `DELETE`, see [S3 endpoint](/docs/design/endpoint). Objects larger than a configured limit are not populated on write, since a multi-gigabyte upload is not worth holding in RAM while it uploads.
