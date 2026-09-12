//! Client for a nestor admin listener: the JSON API, readiness and the embedded dashboard.

use reqwest::StatusCode;
use serde_json::Value;

pub struct Admin {
    base: String,
}

pub struct Page {
    pub status: StatusCode,
    pub content_type: String,
    pub cache_control: String,
    pub body: String,
}

impl Admin {
    pub fn new(base: &str) -> Self {
        Self {
            base: base.trim_end_matches('/').to_owned(),
        }
    }

    pub async fn health(&self) -> String {
        self.page("/health").await.body
    }

    pub async fn ready(&self) -> Value {
        self.json("/ready").await
    }

    pub async fn status(&self) -> Value {
        self.json("/admin/status").await
    }

    pub async fn namespaces(&self) -> Value {
        self.json("/admin/namespaces").await
    }

    pub async fn hits(&self) -> u64 {
        self.status().await["totals"]["hits"]
            .as_u64()
            .expect("totals.hits")
    }

    pub async fn origin_requests(&self) -> u64 {
        self.status().await["totals"]["originRequests"]
            .as_u64()
            .expect("totals.originRequests")
    }

    pub async fn origin_bytes(&self) -> u64 {
        self.status().await["totals"]["originBytes"]
            .as_u64()
            .expect("totals.originBytes")
    }

    pub async fn page(&self, path: &str) -> Page {
        let response = reqwest::get(format!("{}{path}", self.base))
            .await
            .expect("admin request");
        let header = |name: &str| {
            response
                .headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_owned()
        };
        let content_type = header("content-type");
        let cache_control = header("cache-control");
        Page {
            status: response.status(),
            content_type,
            cache_control,
            body: response.text().await.expect("admin body"),
        }
    }

    async fn json(&self, path: &str) -> Value {
        reqwest::get(format!("{}{path}", self.base))
            .await
            .expect("admin request")
            .error_for_status()
            .expect("admin status")
            .json()
            .await
            .expect("admin json")
    }
}
