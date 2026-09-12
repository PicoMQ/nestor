//! One nestor in front of `RustFS`. Data is written at the origin or through nestor and read back
//! through nestor, with cache behaviour checked on the metrics endpoint.

use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt;
use nestor_e2e::admin::Admin;
use nestor_e2e::compose::Compose;
use nestor_e2e::data::{KIB, MIB, body, payload, slice};
use nestor_e2e::metrics::Metrics;
use nestor_e2e::{BUCKET, env, init, s3, step, wait};
use object_store::path::Path;
use object_store::{Error, GetOptions, GetRange, ObjectStore, ObjectStoreExt, WriteMultipart};
use serde_json::Value;

struct Stack {
    origin: std::sync::Arc<object_store::aws::AmazonS3>,
    nestor: std::sync::Arc<object_store::aws::AmazonS3>,
    nestor_endpoint: String,
    metrics: String,
    admin: Admin,
}

impl Stack {
    async fn connect() -> Self {
        init();
        let rustfs = env("NESTOR_E2E_RUSTFS", "http://127.0.0.1:19000");
        let nestor_endpoint = env("NESTOR_E2E_NESTOR", "http://127.0.0.1:19001");
        let metrics = env("NESTOR_E2E_METRICS", "http://127.0.0.1:19100");
        let admin = env("NESTOR_E2E_ADMIN", "http://127.0.0.1:19190");
        wait::healthy(
            &format!("{nestor_endpoint}/-/health"),
            Duration::from_secs(60),
        )
        .await;
        wait::healthy(&format!("{admin}/ready"), Duration::from_secs(60)).await;
        Self {
            origin: s3::client(&rustfs),
            nestor: s3::client(&nestor_endpoint),
            nestor_endpoint,
            metrics,
            admin: Admin::new(&admin),
        }
    }

    async fn metrics(&self) -> Metrics {
        Metrics::scrape(&self.metrics).await
    }

    async fn read(&self, key: &str) -> Bytes {
        body(self.nestor.get(&Path::from(key)).await.expect("get")).await
    }
}

#[tokio::test]
#[ignore = "needs the single compose stack"]
async fn reads_are_exact_and_the_second_pass_is_served_from_cache() {
    let stack = Stack::connect().await;
    let objects = [("single/tiny", 12), ("single/medium", 3 * MIB + 777)];
    for (key, len) in objects {
        let data = payload(len, len as u64);
        stack
            .origin
            .put(&Path::from(key), data.clone().into())
            .await
            .expect("put at origin");
        step!(key, len, "written at origin");

        let before = stack.metrics().await;
        assert_eq!(stack.read(key).await, data, "{key} first read");
        let after_miss = stack.metrics().await;
        assert!(
            after_miss.counter("nestor_origin_requests_total")
                > before.counter("nestor_origin_requests_total"),
            "{key} first read should hit the origin"
        );

        assert_eq!(stack.read(key).await, data, "{key} second read");
        let after_hit = stack.metrics().await;
        assert_eq!(
            after_hit.counter("nestor_origin_requests_total"),
            after_miss.counter("nestor_origin_requests_total"),
            "{key} second read should not hit the origin"
        );
        assert!(
            after_hit.counter("nestor_blocks_hit_total")
                > after_miss.counter("nestor_blocks_hit_total")
        );
        step!(key, "second read served from cache");
    }
}

#[tokio::test]
#[ignore = "needs the single compose stack"]
async fn ranges_cross_blocks_and_clip_at_the_end() {
    let stack = Stack::connect().await;
    let len = 5 * MIB + 4321;
    let data = payload(len, 5);
    let key = Path::from("single/ranges");
    stack
        .origin
        .put(&key, data.clone().into())
        .await
        .expect("put");

    let ranges = [
        0..1u64,
        (MIB as u64 - 1)..(MIB as u64 + 1),
        (2 * MIB as u64 + 13)..(4 * MIB as u64 + 999),
        (len as u64 - 10)..len as u64,
    ];
    for range in ranges {
        let got = stack
            .nestor
            .get_range(&key, range.clone())
            .await
            .expect("range");
        assert_eq!(got, slice(&data, &range), "{range:?}");
        step!(?range, "range matches");
    }

    let clipped = stack
        .nestor
        .get_opts(
            &key,
            GetOptions {
                range: Some(GetRange::Bounded(
                    (len as u64 - 5)..(len as u64 + MIB as u64),
                )),
                ..GetOptions::default()
            },
        )
        .await
        .expect("clipped range");
    assert_eq!(clipped.range, (len as u64 - 5)..len as u64);
    assert_eq!(body(clipped).await, data.slice(len - 5..));

    let past_end = stack
        .nestor
        .get_range(&key, (len as u64 + 1)..(len as u64 + 2))
        .await;
    assert!(past_end.is_err(), "range past the end must fail");

    let suffix = stack
        .nestor
        .get_opts(
            &key,
            GetOptions {
                range: Some(GetRange::Suffix(100)),
                ..GetOptions::default()
            },
        )
        .await
        .expect("suffix");
    assert_eq!(body(suffix).await, data.slice(len - 100..));
}

#[tokio::test]
#[ignore = "needs the single compose stack"]
async fn writes_through_nestor_land_at_the_origin_and_populate_the_cache() {
    let stack = Stack::connect().await;
    let key = Path::from("single/put");
    let data = payload(2 * MIB + 5, 7);
    stack
        .nestor
        .put(&key, data.clone().into())
        .await
        .expect("put through nestor");
    step!("PUT forwarded to origin");

    let at_origin = body(stack.origin.get(&key).await.expect("origin get")).await;
    assert_eq!(at_origin, data);

    let before = stack.metrics().await;
    assert_eq!(stack.read("single/put").await, data);
    let after = stack.metrics().await;
    assert_eq!(
        after.counter("nestor_origin_requests_total"),
        before.counter("nestor_origin_requests_total"),
        "populate on PUT means the read back needs no origin GET"
    );
    step!("read back served from the populated cache");

    let head = stack.nestor.head(&key).await.expect("head");
    assert_eq!(head.size, data.len() as u64);
    assert!(head.e_tag.is_some());
}

#[tokio::test]
#[ignore = "needs the single compose stack"]
async fn multipart_upload_through_nestor() {
    let stack = Stack::connect().await;
    let key = Path::from("single/multipart");
    let data = payload(40 * MIB, 40);
    let upload = stack
        .nestor
        .put_multipart(&key)
        .await
        .expect("create multipart");
    let mut writer = WriteMultipart::new_with_chunk_size(upload, 8 * MIB);
    for chunk in data.chunks(8 * MIB) {
        writer.write(chunk);
    }
    writer.finish().await.expect("complete multipart");
    step!(parts = 5, "multipart upload completed through nestor");

    assert_eq!(
        stack.origin.head(&key).await.expect("origin head").size,
        data.len() as u64
    );
    assert_eq!(stack.read("single/multipart").await, data);
    let tail = stack
        .nestor
        .get_range(&key, (39 * MIB as u64)..(40 * MIB as u64))
        .await
        .expect("tail");
    assert_eq!(tail, data.slice(39 * MIB..));
}

#[tokio::test]
#[ignore = "needs the single compose stack"]
async fn overwrite_at_origin_is_visible_after_the_etag_ttl() {
    let stack = Stack::connect().await;
    let key = Path::from("single/overwrite");
    let first = payload(64 * KIB, 1);
    let second = payload(64 * KIB + 3, 2);
    stack
        .origin
        .put(&key, first.clone().into())
        .await
        .expect("put v1");
    assert_eq!(stack.read("single/overwrite").await, first);
    step!("v1 cached");

    stack
        .origin
        .put(&key, second.clone().into())
        .await
        .expect("put v2");
    step!("v2 written behind nestor's back");
    wait::until("v2 to surface", Duration::from_secs(10), || async {
        stack.read("single/overwrite").await == second
    })
    .await;
    step!("v2 served after the etag ttl");
}

#[tokio::test]
#[ignore = "needs the single compose stack"]
async fn delete_and_list_go_through_to_the_origin() {
    let stack = Stack::connect().await;
    let prefix = Path::from("single/list");
    for i in 0..5 {
        stack
            .nestor
            .put(
                &prefix.clone().join(format!("{i}.bin")),
                payload(KIB, i).into(),
            )
            .await
            .expect("put");
    }
    let via_nestor: Vec<Path> = stack
        .nestor
        .list(Some(&prefix))
        .map_ok(|m| m.location)
        .try_collect()
        .await
        .expect("list via nestor");
    let at_origin: Vec<Path> = stack
        .origin
        .list(Some(&prefix))
        .map_ok(|m| m.location)
        .try_collect()
        .await
        .expect("list at origin");
    assert_eq!(via_nestor.len(), 5);
    assert_eq!(via_nestor, at_origin);
    step!("LIST forwarded and re-signed");

    let victim = prefix.join("0.bin");
    assert_eq!(stack.read(victim.as_ref()).await, payload(KIB, 0));
    stack.nestor.delete(&victim).await.expect("delete");
    assert!(matches!(
        stack.nestor.get(&victim).await,
        Err(Error::NotFound { .. })
    ));
    assert!(matches!(
        stack.origin.get(&victim).await,
        Err(Error::NotFound { .. })
    ));
    step!("DELETE invalidated the cache and removed the object at the origin");
}

#[tokio::test]
#[ignore = "needs the single compose stack"]
async fn admin_api_follows_real_traffic_and_agrees_with_metrics() {
    let stack = Stack::connect().await;
    assert_eq!(stack.admin.health().await, "ok");
    let ready = stack.admin.ready().await;
    assert_eq!(ready["ready"], true);
    assert_eq!(ready["s3"], "0.0.0.0:9000");
    assert_eq!(ready["metrics"], "0.0.0.0:9100");
    step!("admin listener is up and ready");

    let key = "single/admin";
    let data = payload(3 * MIB + 11, 33);
    stack
        .origin
        .put(&Path::from(key), data.clone().into())
        .await
        .expect("put at origin");

    let before = stack.admin.status().await;
    assert_eq!(
        before["origin"].as_str().unwrap().trim_end_matches('/'),
        "http://rustfs:9000"
    );
    assert!(before["cache"]["memoryCap"].as_u64().unwrap() > 0);
    assert!(before["cache"]["diskCap"].as_u64().unwrap() > 0);
    assert!(before["cache"]["metaCap"].as_u64().unwrap() > 0);

    assert_eq!(stack.read(key).await, data);
    let after_miss = stack.admin.status().await;
    assert!(
        after_miss["totals"]["originRequests"].as_u64().unwrap()
            > before["totals"]["originRequests"].as_u64().unwrap(),
        "first read must show up as origin traffic"
    );
    assert!(
        after_miss["totals"]["bytesServed"].as_u64().unwrap()
            >= before["totals"]["bytesServed"].as_u64().unwrap() + data.len() as u64
    );
    step!("miss counted on /admin/status");

    assert_eq!(stack.read(key).await, data);
    let after_hit = stack.admin.status().await;
    assert_eq!(
        after_hit["totals"]["originRequests"],
        after_miss["totals"]["originRequests"]
    );
    assert!(
        after_hit["totals"]["hits"].as_u64().unwrap()
            > after_miss["totals"]["hits"].as_u64().unwrap()
    );
    assert!(after_hit["hitRatio"].as_f64().unwrap() > 0.0);
    step!("hit counted on /admin/status");

    let metrics = stack.metrics().await;
    let admin = stack.admin.status().await;
    assert_eq!(
        admin["totals"]["hits"].as_u64().unwrap(),
        metrics.counter("nestor_blocks_hit_total")
    );
    assert_eq!(
        admin["totals"]["originRequests"].as_u64().unwrap(),
        metrics.counter("nestor_origin_requests_total")
    );
    step!("admin counters agree with /metrics");

    let namespaces = stack.admin.namespaces().await;
    let bucket = namespaces["namespaces"]
        .as_array()
        .unwrap()
        .iter()
        .find(|ns| ns["name"] == BUCKET)
        .expect("bucket namespace");
    assert_eq!(bucket["blockSize"], MIB as u64);
    assert_eq!(bucket["consistency"]["mode"], "etag");
    assert_eq!(bucket["consistency"]["ttlSeconds"], 1);
    assert!(bucket["counters"]["hits"].as_u64().unwrap() > 0);
    assert!(bucket["hitRatio"].as_f64().unwrap() > 0.0);
    step!("per bucket policy and counters on /admin/namespaces");
}

#[tokio::test]
#[ignore = "needs the single compose stack"]
async fn dashboard_is_embedded_in_the_image() {
    let stack = Stack::connect().await;
    let index = stack.admin.page("/").await;
    assert!(index.status.is_success());
    assert!(index.content_type.starts_with("text/html"));
    assert!(index.body.contains("Nestor Admin"));
    assert!(
        !index.body.contains("built without the dashboard"),
        "the image must ship the compiled dashboard"
    );

    let asset = index
        .body
        .split("/assets/")
        .nth(1)
        .and_then(|s| s.split('"').next())
        .expect("index references a built asset");
    let script = stack.admin.page(&format!("/assets/{asset}")).await;
    assert!(script.status.is_success());
    assert!(script.content_type.starts_with("text/javascript"));
    assert_eq!(script.cache_control, "public, max-age=31536000, immutable");
    step!(asset, "dashboard bundle served with immutable caching");

    let missing = stack.admin.page("/assets/nope.js").await;
    assert_eq!(missing.status, reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "needs the single compose stack"]
async fn cli_admin_client_runs_inside_the_container() {
    let stack = Stack::connect().await;
    let compose = Compose::for_scenario("single");
    let endpoint = "http://127.0.0.1:9190";

    let status: Value = serde_json::from_str(
        &compose
            .exec(
                "nestor",
                &[
                    "nestor",
                    "admin",
                    "--admin-endpoint",
                    endpoint,
                    "--json",
                    "status",
                ],
            )
            .await,
    )
    .expect("status json");
    let live = stack.admin.status().await;
    assert_eq!(status["origin"], live["origin"]);
    assert_eq!(status["cache"]["memoryCap"], live["cache"]["memoryCap"]);
    step!("nestor admin status --json matches the API");

    let text = compose
        .exec(
            "nestor",
            &["nestor", "admin", "--admin-endpoint", endpoint, "status"],
        )
        .await;
    assert!(text.contains("origin=http://rustfs:9000"));
    assert!(text.contains("hitRatio="));

    let namespaces = compose
        .exec(
            "nestor",
            &[
                "nestor",
                "admin",
                "--admin-endpoint",
                endpoint,
                "namespaces",
            ],
        )
        .await;
    assert!(
        namespaces.contains(&format!("ns={BUCKET} ")) || namespaces.contains("no namespaces"),
        "unexpected namespaces output: {namespaces}"
    );
    step!("nestor admin namespaces renders");
}

#[tokio::test]
#[ignore = "needs the single compose stack"]
async fn unsigned_requests_are_rejected() {
    let stack = Stack::connect().await;
    let key = Path::from("single/secret");
    stack
        .origin
        .put(&key, payload(KIB, 9).into())
        .await
        .expect("put");
    let anonymous = s3::unsigned(&stack.nestor_endpoint);
    let result = anonymous.get(&key).await;
    assert!(result.is_err(), "unsigned GET must be rejected");
    step!("unsigned GET rejected");
}
