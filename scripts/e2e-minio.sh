#!/usr/bin/env bash
# End-to-end check with MinIO as the origin, nestor as the S3 endpoint and the AWS CLI as the client.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MINIO_PORT="${MINIO_PORT:-19000}"
NESTOR_PORT="${NESTOR_PORT:-19001}"
BUCKET="e2e"
WORK="$(mktemp -d)"
CONTAINER="nestor-e2e-minio-$$"

cleanup() {
    [[ -n "${NESTOR_PID:-}" ]] && kill "$NESTOR_PID" 2>/dev/null || true
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    rm -rf "$WORK"
}
trap cleanup EXIT

export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_DEFAULT_REGION=us-east-1
export AWS_EC2_METADATA_DISABLED=true

docker run -d --rm --name "$CONTAINER" -p "${MINIO_PORT}:9000" \
    -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
    quay.io/minio/minio server /data >/dev/null
for _ in $(seq 1 60); do
    curl -sf "http://127.0.0.1:${MINIO_PORT}/minio/health/live" >/dev/null && break
    sleep 0.5
done

MINIO="http://127.0.0.1:${MINIO_PORT}"
NESTOR="http://127.0.0.1:${NESTOR_PORT}"

aws --endpoint-url "$MINIO" s3 mb "s3://${BUCKET}" >/dev/null
head -c $((7 * 1024 * 1024 + 12345)) /dev/urandom > "$WORK/big.bin"
printf 'hello nestor' > "$WORK/small.txt"
aws --endpoint-url "$MINIO" s3 cp "$WORK/big.bin" "s3://${BUCKET}/big.bin" >/dev/null
aws --endpoint-url "$MINIO" s3 cp "$WORK/small.txt" "s3://${BUCKET}/dir/small.txt" >/dev/null

cat > "$WORK/nestor.toml" <<EOF
[server]
listen = "127.0.0.1:${NESTOR_PORT}"

[origin]
endpoint = "${MINIO}"
region = "us-east-1"
credentials = { source = "static", access_key = "minioadmin", secret_key = "minioadmin" }

[auth]
mode = "static"
access_key = "minioadmin"
secret_key = "minioadmin"

[cache]
memory = "64 MiB"

[buckets]
block_size = "1 MiB"
consistency = { mode = "etag", ttl = "1s" }
EOF

cargo build --locked -q -p nestor-cli --manifest-path "$ROOT/Cargo.toml"
TARGET_DIR="$(cargo metadata --locked --format-version 1 --no-deps --manifest-path "$ROOT/Cargo.toml" \
    | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')"
"$TARGET_DIR/debug/nestor" serve --config "$WORK/nestor.toml" >"$WORK/nestor.log" 2>&1 &
NESTOR_PID=$!
for _ in $(seq 1 60); do
    curl -sf "${NESTOR}/-/health" >/dev/null && break
    sleep 0.2
done

fail() { echo "FAIL: $*"; echo "--- nestor log ---"; cat "$WORK/nestor.log"; exit 1; }
curl -sf "${NESTOR}/-/health" >/dev/null || fail "nestor did not start"

# Full GET through the cache twice, the second is a hit.
for i in 1 2; do
    aws --endpoint-url "$NESTOR" s3 cp "s3://${BUCKET}/big.bin" "$WORK/big.$i" >/dev/null
    cmp -s "$WORK/big.bin" "$WORK/big.$i" || fail "big.bin mismatch on read $i"
done

# Range GET.
aws --endpoint-url "$NESTOR" s3api get-object --bucket "$BUCKET" --key big.bin \
    --range "bytes=1048570-1048600" "$WORK/range.bin" >/dev/null
cmp -s <(tail -c +1048571 "$WORK/big.bin" | head -c 31) "$WORK/range.bin" || fail "range mismatch"

# HEAD and LIST, LIST is forwarded and re-signed.
aws --endpoint-url "$NESTOR" s3api head-object --bucket "$BUCKET" --key dir/small.txt >/dev/null || fail "head"
aws --endpoint-url "$NESTOR" s3 ls "s3://${BUCKET}/dir/" | grep -q small.txt || fail "list"

# PUT through nestor, read back through nestor, verify at origin.
printf 'written via nestor' > "$WORK/put.txt"
aws --endpoint-url "$NESTOR" s3 cp "$WORK/put.txt" "s3://${BUCKET}/put.txt" >/dev/null
aws --endpoint-url "$NESTOR" s3 cp "s3://${BUCKET}/put.txt" "$WORK/put.back" >/dev/null
cmp -s "$WORK/put.txt" "$WORK/put.back" || fail "put readback"
aws --endpoint-url "$MINIO" s3 cp "s3://${BUCKET}/put.txt" "$WORK/put.origin" >/dev/null
cmp -s "$WORK/put.txt" "$WORK/put.origin" || fail "put not at origin"

# Multipart upload through nestor, the aws cli switches to it above 8 MiB.
head -c $((17 * 1024 * 1024)) /dev/urandom > "$WORK/multi.bin"
aws --endpoint-url "$NESTOR" s3 cp "$WORK/multi.bin" "s3://${BUCKET}/multi.bin" >/dev/null
aws --endpoint-url "$NESTOR" s3 cp "s3://${BUCKET}/multi.bin" "$WORK/multi.back" >/dev/null
cmp -s "$WORK/multi.bin" "$WORK/multi.back" || fail "multipart readback"

# Overwrite at the origin, the 1s etag ttl must surface the new content.
printf 'changed at origin' > "$WORK/small2.txt"
aws --endpoint-url "$MINIO" s3 cp "$WORK/small2.txt" "s3://${BUCKET}/dir/small.txt" >/dev/null
sleep 1.2
aws --endpoint-url "$NESTOR" s3 cp "s3://${BUCKET}/dir/small.txt" "$WORK/small.back" >/dev/null
cmp -s "$WORK/small2.txt" "$WORK/small.back" || fail "stale read after origin overwrite"

# DELETE through nestor.
aws --endpoint-url "$NESTOR" s3 rm "s3://${BUCKET}/put.txt" >/dev/null
if aws --endpoint-url "$NESTOR" s3api head-object --bucket "$BUCKET" --key put.txt >/dev/null 2>&1; then
    fail "object still readable after delete"
fi

# Unsigned request must be rejected.
code="$(curl -s -o /dev/null -w '%{http_code}' "${NESTOR}/${BUCKET}/big.bin")"
[[ "$code" == "403" ]] || fail "expected 403 for unsigned request, got $code"

echo "e2e ok"
