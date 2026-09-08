# Contribute

Thank you for your interest in contributing to Nestor. The project lives at [github.com/picomq/nestor](https://github.com/picomq/nestor). Bug reports, design discussion, and pull requests all go there.

::: tip
If you are new to the project, new to Rust, or just unsure whether a change belongs here, open a PR or an issue anyway. Review is part of how we learn, and there is always more to learn.
:::

## Repository layout

One Cargo workspace, one crate per concern, dependencies pointing one way:

- **`nestor/`** is the engine: blocks, tiers, fetch scheduling, consistency. It depends on the `Origin` trait only and knows nothing about S3, HTTP or clusters.
- **`nestor-store/`**, **`nestor-s3/`** and **`nestor-client/`** are adapters on the engine: `object_store` in both directions, the S3 endpoint, and cluster routing.
- **`nestor-cli/`** is the `nestor` binary: configuration and wiring, no logic another crate could own.

Keeping that direction intact is a review criterion. If a change in an adapter needs something from inside the engine, the right move is to widen the engine's API.

The docs are at `website/` (`VitePress`). The end-to-end scenarios and their compose stacks live in `nestor-e2e/`.

## Build and test

`rust-toolchain.toml` pins the exact version. The default test suite has no external dependencies. `MemoryOrigin` and `object_store::InMemory` stand in for the origin and `wiremock` for the S3 wire:

```bash
cargo build --workspace
cargo test --workspace
```

The end-to-end scenarios need Docker. Each builds the image, starts a compose stack with RustFS, runs its assertions through the S3 API and the metrics endpoint, and tears the stack down:

```bash
nestor-e2e/e2e.sh                  # single, cluster, library
nestor-e2e/e2e.sh cluster          # one scenario
KEEP=1 nestor-e2e/e2e.sh single    # leave the stack up afterwards
```

See [Quick start](/docs/quick-start) for running the same stacks by hand.

## Docs

The site is VitePress. From `website/`:

```bash
npm install
npm run dev
```

Pages are markdown under `website/pages/docs/`, and the sidebar is defined in `website/.vitepress/config.mts`. Docs follow the same review bar as code.

## Pull requests

Small, focused PRs against `main`. A good PR description says *why* the change exists, not just what it touches. If it changes the S3 behaviour, the cache key layout, or an operational default, call that out explicitly. Run `cargo fmt` and `cargo clippy --workspace` before pushing.

AI-generated (or largely generated) pull requests are welcome, provided that you:

- Call out in the PR description that AI was used, and which tool or model.
- Understand the change and can explain it in review.
- Keep PR discussion human. Descriptions, comments, and review replies.
- Have reviewed the diff yourself before opening the PR.

For anything larger than a bug fix (a new origin type, a tier, a change to routing), [open an issue](https://github.com/picomq/nestor/issues) first so the design can be discussed before the code shows up.

By contributing, you agree your work is licensed under Apache 2.0.
