# syntax=docker/dockerfile:1
# The nestor binary. Configuration is a mounted nestor.toml or NESTOR_* environment variables.

FROM node:22-bookworm-slim AS dashboard
WORKDIR /src/dashboard
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY dashboard/ ./
RUN npm run build

FROM rust:1.98-bookworm AS build
WORKDIR /src

RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential cmake pkg-config \
 && rm -rf /var/lib/apt/lists/*

COPY . .
COPY --from=dashboard /src/nestor-server/_dashboard /src/nestor-server/_dashboard
RUN --mount=type=cache,id=nestor-target,sharing=locked,target=/src/target \
    --mount=type=cache,id=nestor-cargo-registry,sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,id=nestor-cargo-git,sharing=locked,target=/usr/local/cargo/git \
    cargo build --locked --release -p nestor-cli \
 && cp /src/target/release/nestor /usr/local/bin/nestor

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

COPY --from=build /usr/local/bin/nestor /usr/local/bin/nestor

EXPOSE 9000 9100 9190
ENTRYPOINT ["nestor"]
CMD ["serve", "--config", "/etc/nestor/nestor.toml"]
