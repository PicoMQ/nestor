# Consistency

A cache in front of mutable objects has two failure modes: serving bytes from an old version, and mixing blocks from two versions in one response. Nestor rules out the second by construction and bounds the first with a per-namespace policy.

## Content tags

A block key is `(namespace, object, tag, index)`. The tag is a `64` bit hash of the object's `ETag`, or of its size when the origin returns none. Two versions of an object have different tags, so their blocks are different keys that never collide in the cache. There is no invalidation race because old blocks are never overwritten, they stop being referenced and age out.

<div class="kakapo-diagram">
<svg viewBox="0 0 720 210" width="720" role="img" aria-label="Blocks of version A and version B of the same object live under different tags. A read pins one tag for its whole life.">
  <defs>
    <marker id="arrc" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <text x="40" y="40" class="label">reports/q3.parquet</text>
  <text x="40" y="76" class="sub">tag 7f3a (ETag "a1b2")</text>
  <rect x="220" y="58" width="90" height="30" class="box"/>
  <text x="265" y="78" text-anchor="middle" class="sub">block 0</text>
  <rect x="320" y="58" width="90" height="30" class="box"/>
  <text x="365" y="78" text-anchor="middle" class="sub">block 1</text>
  <rect x="420" y="58" width="90" height="30" class="box"/>
  <text x="465" y="78" text-anchor="middle" class="sub">block 2</text>
  <text x="40" y="126" class="sub">tag c904 (ETag "e5f6")</text>
  <rect x="220" y="108" width="90" height="30" class="box-accent"/>
  <text x="265" y="128" text-anchor="middle" class="sub">block 0</text>
  <rect x="320" y="108" width="90" height="30" class="box-accent"/>
  <text x="365" y="128" text-anchor="middle" class="sub">block 1</text>
  <rect x="420" y="108" width="90" height="30" class="box-accent"/>
  <text x="465" y="128" text-anchor="middle" class="sub">block 2</text>
  <rect x="520" y="108" width="90" height="30" class="box-accent"/>
  <text x="565" y="128" text-anchor="middle" class="sub">block 3</text>
  <path d="M640 40 L640 100" class="edge" marker-end="url(#arrc)"/>
  <text x="640" y="30" text-anchor="middle" class="sub">PUT</text>
  <text x="365" y="180" text-anchor="middle" class="sub">a read that started under 7f3a only ever sees 7f3a blocks</text>
</svg>
</div>

A read pins its tag when it starts and sends the matching `If-Match` with every origin `GET`. If the object changes mid-read the origin answers `412` instead of new bytes, the fetch fails with a stale error and the reader restarts once from the current metadata. A reader therefore observes one version or fails, never a splice.

## Modes

Each namespace declares how its objects behave.

| Mode | Tag | Revalidation |
| --- | --- | --- |
| `etag`, `ttl` | Hash of `ETag` | Metadata older than `ttl` is revalidated with a conditional `GET` on the next read |
| `immutable` | Constant | Never. An object name is assumed to map to one content forever |

**ETag mode** is the default, with a `60` s TTL. A fresh metadata entry is trusted as is. An expired entry is not discarded, its `ETag` goes out as `If-None-Match` on the read's first `GET`. A `304` costs one round trip with no body and confirms every cached block is still current. A `200` with a new `ETag` brings a new tag and the read proceeds against it while the old blocks fall out of the cache on their own. The TTL is the staleness bound: a reader may see the previous version for at most that long after an overwrite, unless the write went through Nestor.

**Immutable mode** skips the metadata round trip entirely for bounded reads. It is the right setting for content-addressed or append-only layouts, segment files with a sequence number in the name, build artifacts keyed by digest, or any store where overwriting a key is a bug. The saving is real: a bounded read on a cold object is one `GET` with no probe, and on a warm object is zero origin requests forever.

## Writes through Nestor

When a write passes through the S3 endpoint or a `NestorStore`, the staleness bound disappears for that object. `PUT` invalidates the old metadata and, when the body is small enough, inserts the new blocks under the new tag before the response reaches the client. `DELETE` and `CompleteMultipartUpload` invalidate. The next read sees the new version with no TTL wait.

Writes that bypass Nestor, another client uploading straight to the origin, are covered by the TTL in ETag mode and not at all in immutable mode. A deployment that mixes the two paths should keep the TTL short or route all writers through the cache.

## Metadata cache

Size and `ETag` per object live in a separate LRU, `100000` entries by default and shared across namespaces. Entries record when they were fetched. Each read consults it once, and every origin response refreshes it, so a hot object's metadata never expires in practice: the reads themselves keep it current.

Metadata is in RAM only. After a restart the block tiers may still hold data from disk but every object's first read pays one probe to learn its tag again, after which the on-disk blocks are hits.
