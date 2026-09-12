# Admin API and dashboard

The admin listener is `127.0.0.1:9190` by default. It serves a JSON snapshot of in-memory cache state and a dashboard at `/` that polls every `2` seconds. Nothing is stored. `nestor admin` is a client of the same API.

| Method and path | What it does |
| --- | --- |
| `GET /health` | Liveness. |
| `GET /ready` | Serving flag and listen addresses. |
| `GET /admin/status` | Occupancy, inflight, since-boot counters. |
| `GET /admin/namespaces` | Per-bucket policy and counters. |

Non-loopback binds require `server.admin.insecure_allow_remote = true`. A binary without dashboard assets still serves the JSON API.
