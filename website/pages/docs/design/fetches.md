# Origin fetches

Every byte Nestor serves was fetched from an origin exactly once, by one fetch group, on behalf of every reader waiting for it. This page covers how those fetches are issued: how a cold read finds out about the object, how many requests can be in flight, and what happens when the origin is slow or fails.

## Cold reads

A read needs the object's size and `ETag` before it can resolve open ranges or tag blocks. Fetching them with a `HEAD` would put a full round trip in front of every cold read. Instead the first `GET` doubles as the probe.

<div class="kakapo-diagram">
<svg viewBox="0 0 720 230" width="720" role="img" aria-label="A read checks the metadata cache. When fresh it proceeds to blocks. Otherwise the first block GET carries size and ETag back, and only suffix ranges on unknown objects need a HEAD.">
  <defs>
    <marker id="arrf" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="86" width="150" height="56" class="box"/>
  <text x="95" y="110" text-anchor="middle" class="label">metadata cache</text>
  <text x="95" y="128" text-anchor="middle" class="sub">size, ETag, age</text>
  <rect x="270" y="20" width="200" height="50" class="box"/>
  <text x="370" y="41" text-anchor="middle" class="label">fresh</text>
  <text x="370" y="58" text-anchor="middle" class="sub">straight to blocks</text>
  <rect x="270" y="90" width="200" height="50" class="box-accent"/>
  <text x="370" y="111" text-anchor="middle" class="label">stale or missing</text>
  <text x="370" y="128" text-anchor="middle" class="sub">GET first fetch group</text>
  <rect x="270" y="160" width="200" height="50" class="box"/>
  <text x="370" y="181" text-anchor="middle" class="label">suffix, unknown size</text>
  <text x="370" y="198" text-anchor="middle" class="sub">HEAD, then blocks</text>
  <rect x="540" y="90" width="160" height="50" class="box"/>
  <text x="620" y="111" text-anchor="middle" class="label">response</text>
  <text x="620" y="128" text-anchor="middle" class="sub">size, ETag, body</text>
  <path d="M170 104 L262 45" class="edge" marker-end="url(#arrf)"/>
  <path d="M170 114 L262 114" class="edge" marker-end="url(#arrf)"/>
  <path d="M170 124 L262 185" class="edge" marker-end="url(#arrf)"/>
  <path d="M470 114 L532 114" class="edge" marker-end="url(#arrf)"/>
</svg>
</div>

The probe requests the blocks the read would fetch first anyway. The `Content-Range` total gives the size, the `ETag` header gives the tag, and the body is consumed as a normal fetch group so nothing is thrown away. If the metadata cache had an expired entry, the probe carries `If-None-Match` and a `304` refreshes the entry for free. Bounded reads in immutable namespaces skip the probe entirely because they need neither the size nor a tag.

The only read that still pays a `HEAD` is a suffix range on an object of unknown size, since its first block cannot be computed without the total.

## Concurrency

Three semaphores bound outstanding origin work for the whole instance.

| Permit | Default | Held by |
| --- | --- | --- |
| `origin_concurrency` | `64` | Foreground fetch groups, one per origin `GET` |
| `readahead_concurrency` | `16` | Background fetch groups issued by readahead |
| `hedge_concurrency` | `16` | Secondary requests issued by hedging |

Foreground and background pools are separate so a burst of readahead cannot starve a cache miss a caller is waiting on. Hedges take from their own pool and when it is exhausted the primary request is awaited, so hedging degrades to nothing rather than amplifying load on an origin that is already struggling.

## Fetch policy

One `FetchPolicy` per namespace decides how a miss goes to the origin. A read overrides it with `ReadOptions` in the library or `X-Nestor-Fetch` on the [endpoint](/docs/design/endpoint#fetch-overrides). A fetch group runs under the policy of the read that opened it.

| Setting | Default | Purpose |
| --- | --- | --- |
| `attempts` | `3` | Tries per fetch, including the first |
| `backoff`, `backoff_max` | `50ms`, `2s` | Delay before a retry, doubling per attempt |
| `first_byte` | `5s` | Time for one attempt to return headers |
| `attempt` | `30s` | Time for one attempt to deliver its body |
| `deadline` | `60s` | The fetch as a whole, across attempts and backoff |
| `hedge` | `{ factor = 3.0, min = "50ms", max = "2s" }` | Secondary request timing, `false` disables |

The `object_store` client under `nestor-store` runs with its own retries and request timeout off (`Transport`), so this is the only policy in effect and `nestor_origin_retries_total` counts every retry.

### Timeouts

| Timeout | Catches | Scope |
| --- | --- | --- |
| `first_byte` | Hung connection, origin that accepted and stalled | Headers of one attempt, hedge included |
| `attempt` | Body that trickles while holding a concurrency permit | One attempt, headers and body |
| `deadline` | Worst case a reader waits | All attempts and backoff, a retry runs only if its backoff still fits |

A timeout is `OriginError::Timeout` and retryable. Blocks are inserted only once complete, so an attempt cut off mid-body leaves nothing partial and the retry resumes at the first unfinished block.

### Hedging

Time to first byte is tracked per namespace as an EWMA. A request unanswered after `factor` times that average, clamped to `[min, max]`, gets a second identical request and the first answer wins. A `20` ms origin hedges at `60` ms. Before any observation the delay is `max`. The cluster client additionally hedges across nodes, see [Cluster](/docs/design/cluster).

### Retries

| Error | Retried |
| --- | --- |
| I/O, short read, timeout | Yes, with backoff |
| `404`, precondition failed | No, they are answers |

A `GET` that fails while the object's size is unknown may have asked past the end, and origins report that like any other error. One `HEAD` settles it: a range starting at or beyond the size resolves to empty blocks, anything else goes to the retry policy with the original error.

Out of attempts fails the waiting readers with the last error, out of deadline fails them with a timeout. A short read is a body that ended before the requested range did.

## Readahead

A read that starts where the previous read of the same object ended is sequential. When a namespace has `readahead` set, such a read schedules the next `readahead` blocks past its own range as background fetch groups, bounded by the object size. Blocks already cached or in flight are skipped, so readahead costs nothing on a warm object.

The default is `8` blocks. Consumers with their own prefetching, such as a stream store that already reads whole segments in order, should set it to `0` and keep the cache purely demand driven.

## Origin trait

All of this sits on one small interface:

```rust
#[async_trait]
pub trait Origin: Send + Sync + 'static {
    async fn get(&self, object: &str, options: GetOptions) -> Result<GetResponse, OriginError>;
    async fn head(&self, object: &str) -> Result<ObjectMeta, OriginError>;
}
```

`GetOptions` carries an optional byte range plus `If-Match` and `If-None-Match`. `GetResponse` carries the object metadata, the range actually returned and a body stream. `OriginError` distinguishes not found, precondition failed, not modified, short read, timeout and I/O so the fetcher can decide what to retry. `nestor-store` implements it over `object_store`, `nestor-client` over a cluster of nodes, and `MemoryOrigin` in the `nestor` crate is an in-process origin with injectable latency and failures for tests.
