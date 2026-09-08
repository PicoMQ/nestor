# S3 endpoint

The `nestor` binary is an S3 endpoint. Clients keep their SDK, credentials model and bucket layout and change one setting, the endpoint URL. Object reads are answered from cache. Everything else is passed to the origin, re-signed with Nestor's own credentials, and its effect on the cache is applied when the origin accepts it.

<div class="kakapo-diagram">
<svg viewBox="0 0 720 290" width="720" role="img" aria-label="A request is verified, resolved to a bucket and key, then either served from cache when it is a plain GET or HEAD on an object or forwarded to the origin. Accepted writes invalidate or populate the cache.">
  <defs>
    <marker id="arre" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="106" width="130" height="56" class="box"/>
  <text x="85" y="130" text-anchor="middle" class="label">verify</text>
  <text x="85" y="148" text-anchor="middle" class="sub">SigV4, presigned</text>
  <rect x="200" y="106" width="130" height="56" class="box"/>
  <text x="265" y="130" text-anchor="middle" class="label">resolve</text>
  <text x="265" y="148" text-anchor="middle" class="sub">bucket, key</text>
  <rect x="400" y="30" width="160" height="56" class="box-accent"/>
  <text x="480" y="54" text-anchor="middle" class="label">serve</text>
  <text x="480" y="72" text-anchor="middle" class="sub">GET, HEAD on a key</text>
  <rect x="400" y="182" width="160" height="56" class="box"/>
  <text x="480" y="206" text-anchor="middle" class="label">forward</text>
  <text x="480" y="224" text-anchor="middle" class="sub">re-signed, streamed</text>
  <rect x="600" y="30" width="100" height="56" class="box-accent"/>
  <text x="650" y="62" text-anchor="middle" class="label">cache</text>
  <rect x="600" y="182" width="100" height="56" class="box"/>
  <text x="650" y="214" text-anchor="middle" class="label">origin</text>
  <path d="M150 134 L192 134" class="edge" marker-end="url(#arre)"/>
  <path d="M330 124 L392 62" class="edge" marker-end="url(#arre)"/>
  <path d="M330 144 L392 206" class="edge" marker-end="url(#arre)"/>
  <path d="M560 58 L592 58" class="edge" marker-end="url(#arre)"/>
  <path d="M560 210 L592 210" class="edge" marker-end="url(#arre)"/>
  <path d="M650 182 L650 94" class="edge-soft" marker-end="url(#arre)"/>
  <text x="638" y="142" text-anchor="end" class="sub">PUT, DELETE</text>
  <text x="638" y="158" text-anchor="end" class="sub">invalidate</text>
  <text x="480" y="272" text-anchor="middle" class="sub">everything with a query string, on a bucket, or not a read</text>
</svg>
</div>

## What is served

A `GET` or `HEAD` on an object key with no query string is served from cache. That covers `GetObject` with or without `Range`, `HeadObject`, and the conditional headers `If-Match`, `If-None-Match`, `If-Modified-Since` and `If-Unmodified-Since`. Responses carry `ETag`, `Last-Modified`, `Accept-Ranges`, `Content-Length` and, for ranges, `Content-Range` with a `206`. A range past the end answers `416` with the object size in `Content-Range: bytes */size`.

`HEAD` uses the metadata cache and only touches the origin when the entry is missing or expired. `GET` follows the read path in [Blocks & reads](/docs/design/reads), and the response headers are written as soon as the stream knows the object's metadata, which for a cold object is when the first `GET` to the origin answers.

Everything else is forwarded: bucket operations, listings, `PUT`, `DELETE`, multipart uploads, and any object `GET` with a query string, which includes `?versionId`, `?partNumber` and presigned URLs. Forwarding streams both directions without buffering.

## Writes

A write that the origin accepts updates the cache before the response is returned to the client, so the client's next read is consistent with its own write regardless of the namespace TTL.

| Request | Effect once the origin returns `2xx` |
| --- | --- |
| `PutObject` | Invalidate. If the body is at most `populate_max`, insert it under the `ETag` the origin returned. |
| `CopyObject` | Invalidate the destination. |
| `DeleteObject` | Invalidate. |
| `DeleteObjects` | Invalidate every key in the request body. |
| `CompleteMultipartUpload` | Invalidate. |
| `UploadPart` | Nothing. The object does not exist until completion. |

`populate_max` defaults to `16` MiB. A `PUT` up to that size is buffered on the way through so it can be inserted after the origin confirms it, larger bodies stream straight through and are fetched on first read.

## Authentication

Nestor sits between two trust boundaries. Toward the origin it uses its own credentials from `[origin]`, resolved the standard way (environment, profile, instance role, or static keys). Toward clients it verifies according to `[auth]`.

| Mode | Behaviour |
| --- | --- |
| `anonymous` | Every request is accepted. Safe only on a loopback listener. |
| `static` | Requests must be signed with the configured access key and secret. Both `Authorization` header signatures and presigned query signatures are checked, with a `15` minute clock skew window and up to `7` days of presigned validity. |

Signature verification is full SigV4, including the canonical request, so a client signs exactly as it would against S3 itself. Derived signing keys are cached per access key, day and region, verification costs one HMAC per request rather than the four of key derivation.

Forwarded requests are re-signed for the origin with `UNSIGNED-PAYLOAD`, since the body streams through without being hashed. Client-side chunked signing (`aws-chunked`) is decoded on the way.

The binary refuses no configuration but warns loudly when the listener is bound beyond loopback with anonymous auth and no TLS, because every cached object is then readable by anyone who reaches the socket.

## Addressing

Buckets are resolved from the path by default, `http://nestor:9000/bucket/key`. Virtual-hosted style, `http://bucket.s3.internal/key`, is enabled with `server.addressing = { style = "virtual_hosted", domain = "s3.internal" }`. Toward the origin the style is set separately with `origin.virtual_hosted`, so a path-style client can front a virtual-hosted origin and vice versa.

Every bucket seen becomes a namespace with the `[buckets]` policy, created on first use. There is no bucket allow list, the origin's credentials decide what is reachable.

## TLS and health

`[server.tls]` takes a certificate and key and terminates TLS on the listener, for clients that require `https` endpoints or for any non-loopback deployment with static auth. `GET /-/health` returns `ok` while the process is serving and is not authenticated. Metrics are on a separate listener, see [Metrics](/docs/operations/metrics).
