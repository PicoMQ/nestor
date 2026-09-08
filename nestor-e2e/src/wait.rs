//! Polls until a condition holds or a deadline passes.

use std::future::Future;
use std::time::{Duration, Instant};

use futures::TryStreamExt;
use object_store::ObjectStore;

pub async fn until<F, Fut>(what: &str, timeout: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if check().await {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

pub async fn healthy(url: &str, timeout: Duration) {
    until(url, timeout, || async {
        reqwest::get(url)
            .await
            .is_ok_and(|response| response.status().is_success())
    })
    .await;
}

pub async fn bucket(store: &dyn ObjectStore, timeout: Duration) {
    until("bucket", timeout, || async {
        store.list(None).try_next().await.is_ok()
    })
    .await;
}
