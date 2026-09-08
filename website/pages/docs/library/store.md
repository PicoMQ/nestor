# nestor-store

`nestor-store` connects Nestor to [`object_store`](https://docs.rs/object_store) in both directions. `ObjectStoreOrigin` turns any `ObjectStore` into an origin Nestor can read from. `NestorStore` turns a cached namespace back into an `ObjectStore`, so code written against the trait gains the cache without changing.

<div class="kakapo-diagram">
<svg viewBox="0 0 720 150" width="720" role="img" aria-label="Application code calls an ObjectStore. NestorStore serves reads from the nestor engine, which reads from ObjectStoreOrigin, which wraps the real backend. Writes go from NestorStore straight to the backend.">
  <defs>
    <marker id="arrst" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="40" width="120" height="56" class="box"/>
  <text x="80" y="64" text-anchor="middle" class="label">application</text>
  <text x="80" y="82" text-anchor="middle" class="sub">dyn ObjectStore</text>
  <rect x="190" y="40" width="130" height="56" class="box-accent"/>
  <text x="255" y="64" text-anchor="middle" class="label">NestorStore</text>
  <text x="255" y="82" text-anchor="middle" class="sub">reads via cache</text>
  <rect x="370" y="40" width="110" height="56" class="box-accent"/>
  <text x="425" y="64" text-anchor="middle" class="label">nestor</text>
  <rect x="530" y="40" width="170" height="56" class="box"/>
  <text x="615" y="64" text-anchor="middle" class="label">ObjectStoreOrigin</text>
  <text x="615" y="82" text-anchor="middle" class="sub">object_store backend</text>
  <path d="M140 68 L182 68" class="edge" marker-end="url(#arrst)"/>
  <path d="M320 68 L362 68" class="edge" marker-end="url(#arrst)"/>
  <path d="M480 68 L522 68" class="edge" marker-end="url(#arrst)"/>
  <path d="M255 96 L255 120 L615 120 L615 104" class="edge-soft" marker-end="url(#arrst)"/>
  <text x="435" y="136" text-anchor="middle" class="sub">writes, lists, deletes go straight to the backend</text>
</svg>
</div>

## ObjectStoreOrigin

```rust
use std::sync::Arc;
use nestor::Namespace;
use nestor_store::ObjectStoreOrigin;
use object_store::aws::AmazonS3Builder;

let s3: Arc<dyn object_store::ObjectStore> = Arc::new(
    AmazonS3Builder::from_env().with_bucket_name("data").build()?,
);
let namespace = Namespace::new("data", Arc::new(ObjectStoreOrigin::new(s3)));
```

The origin maps `GetOptions` onto `object_store::GetOptions`, ranges become `GetRange::Bounded`, `if_match` and `if_none_match` pass through, and `object_store` errors are classified into `OriginError` variants so the fetcher retries and fails correctly. Anything `object_store` supports is an origin: S3 and compatible stores, GCS, Azure, HTTP, the local filesystem and `InMemory` for tests.

## NestorStore

```rust
use nestor_store::NestorStore;

let cached: Arc<dyn ObjectStore> = Arc::new(NestorStore::new(nestor, ns, s3.clone()));
```

`NestorStore` takes the engine, a namespace id and the store to forward writes to. The wrapped store is normally the same one behind the namespace's origin, but it need not be: a write-only credential can back writes while the origin reads with another.

| Operation | Behaviour |
| --- | --- |
| `get`, `get_opts`, `get_range`, `get_ranges` | Served through the cache. `GetOptions` preconditions and `head` are honoured, `GetRange::Offset` and `Suffix` map to `ReadRange`. |
| `head` | Metadata cache, origin `HEAD` on a miss. |
| `put`, `put_opts` | Forwarded. On success the object is invalidated and, by default, inserted into the cache with the `ETag` the store returned. |
| `put_multipart` | Forwarded. The object is invalidated when the upload completes. |
| `delete`, `delete_stream` | Forwarded, then invalidated. |
| `copy`, `rename` and their conditional forms | Forwarded, destinations and sources invalidated. |
| `list`, `list_with_offset`, `list_with_delimiter` | Forwarded untouched. |

`populate_on_write(false)` keeps the invalidation and skips the insert, for write paths whose objects are rarely read back soon or are too large to hold. Populating requires the payload in memory, which `PutPayload` already is.

Errors map back to `object_store::Error`: `NotFound`, `NotModified`, `Precondition` and a generic error carrying the Nestor message otherwise.

## Choosing between the two APIs

`NestorStore` is the right entry point for code that already speaks `object_store`, such as anything built on DataFusion, Parquet readers or Iceberg. Nothing changes but the constructor.

The `nestor` API is the right one for code that owns its read path. It exposes `ReadStream::ready` for early access to metadata, `ReadRange::From` and `Suffix` without allocating an `object_store::GetOptions`, `insert` for write paths that hold the bytes, and `NamespaceId` so one engine can serve many object classes with different block sizes and consistency modes.
