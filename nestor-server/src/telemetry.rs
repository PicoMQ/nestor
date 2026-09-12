//! Prometheus export for `nestor_*` and foyer metrics on one `/metrics` endpoint.

use std::net::SocketAddr;

use axum::Router;
use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::routing::get;
use eyre::{Context, Report};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use mixtrics::registry::prometheus_0_14::PrometheusMetricsRegistry;
use nestor::BoxedRegistry;
use prometheus::{Encoder, Registry, TextEncoder};

#[derive(Clone)]
pub struct Telemetry {
    nestor: PrometheusHandle,
    foyer: Registry,
}

impl Telemetry {
    pub fn install() -> Result<Self, Report> {
        let nestor = PrometheusBuilder::new()
            .install_recorder()
            .wrap_err("installing metrics recorder")?;
        Ok(Self {
            nestor,
            foyer: Registry::new(),
        })
    }

    pub fn foyer_registry(&self) -> BoxedRegistry {
        Box::new(PrometheusMetricsRegistry::new(self.foyer.clone()))
    }

    pub fn render_nestor(&self) -> String {
        self.nestor.run_upkeep();
        self.nestor.render()
    }

    pub async fn serve(self, addr: SocketAddr) -> Result<(), Report> {
        let router = Router::new()
            .route("/metrics", get(render))
            .with_state(self);
        tracing::info!(%addr, "metrics listener started");
        axum_server::bind(addr)
            .serve(router.into_make_service())
            .await
            .wrap_err_with(|| format!("serving metrics on {addr}"))
    }
}

async fn render(State(telemetry): State<Telemetry>) -> impl IntoResponse {
    telemetry.nestor.run_upkeep();
    let mut body = telemetry.nestor.render().into_bytes();
    if let Err(e) = TextEncoder::new().encode(&telemetry.foyer.gather(), &mut body) {
        tracing::warn!(error = %e, "failed to encode foyer metrics");
    }
    ([(CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}
