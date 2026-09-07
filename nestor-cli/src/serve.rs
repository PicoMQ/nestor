//! Runs the listener, optional TLS and metrics endpoint, and drains on shutdown signals.

use std::net::SocketAddr;
use std::time::Duration;

use axum_server::Handle;
use axum_server::tls_rustls::RustlsConfig;
use eyre::{Context, Report};
use nestor_s3::S3Service;

use crate::config::Config;
use crate::telemetry::Telemetry;

const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

pub async fn run(config: Config) -> Result<(), Report> {
    config.validate()?;

    let mut builder = config.cache.builder()?;
    if let Some(addr) = config.server.metrics {
        let telemetry = Telemetry::install()?;
        builder = builder.metrics_registry(telemetry.foyer_registry());
        tokio::spawn(async move {
            if let Err(e) = telemetry.serve(addr).await {
                tracing::error!(error = %e, "metrics listener stopped");
            }
        });
    }

    let nestor = builder.build().await.wrap_err("initialising cache")?;
    let service = S3Service::new(nestor.clone(), config.s3()?);
    let router = service.router();

    let handle: Handle<SocketAddr> = Handle::new();
    tokio::spawn(shutdown_signal(handle.clone()));

    let listen = config.server.listen;
    let serve = if let Some(tls) = &config.server.tls {
        let rustls = RustlsConfig::from_pem_file(&tls.cert, &tls.key)
            .await
            .wrap_err("loading TLS certificate and key")?;
        tracing::info!(%listen, origin = %config.origin.endpoint, "listening (https)");
        axum_server::bind_rustls(listen, rustls)
            .handle(handle)
            .serve(router.into_make_service())
            .await
    } else {
        tracing::info!(%listen, origin = %config.origin.endpoint, "listening (http)");
        axum_server::bind(listen)
            .handle(handle)
            .serve(router.into_make_service())
            .await
    };
    serve.wrap_err_with(|| format!("serving on {listen}"))?;

    nestor.close().await.wrap_err("flushing cache")?;
    tracing::info!("shutdown complete");
    Ok(())
}

async fn shutdown_signal(handle: Handle<SocketAddr>) {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(term) => term,
                Err(e) => {
                    tracing::warn!(error = %e, "failed to install SIGTERM handler");
                    let _ = ctrl_c.await;
                    handle.graceful_shutdown(Some(DRAIN_TIMEOUT));
                    return;
                }
            };
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
    tracing::info!("shutdown requested, draining connections");
    handle.graceful_shutdown(Some(DRAIN_TIMEOUT));
}
