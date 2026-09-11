use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use axum::Router;
use axum_server::Handle;
use axum_server::tls_rustls::RustlsConfig;
use eyre::{Context, Report};
use nestor::Nestor;

use crate::admin::{self, AdminState};
use crate::telemetry::Telemetry;

const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

pub struct Tls {
    pub cert: PathBuf,
    pub key: PathBuf,
}

pub struct Options {
    pub nestor: Nestor,
    pub s3: Router,
    pub listen: SocketAddr,
    pub tls: Option<Tls>,
    pub metrics: Option<SocketAddr>,
    pub admin: Option<SocketAddr>,
    pub origin: String,
    pub telemetry: Option<Telemetry>,
}

pub async fn run(options: Options) -> Result<(), Report> {
    if let (Some(addr), Some(telemetry)) = (options.metrics, options.telemetry.clone()) {
        tokio::spawn(async move {
            if let Err(e) = telemetry.serve(addr).await {
                tracing::error!(error = %e, "metrics listener stopped");
            }
        });
    }

    if let Some(addr) = options.admin {
        let state = AdminState::new(
            options.nestor.clone(),
            options.telemetry,
            options.listen,
            options.metrics,
            options.origin.clone(),
        );
        tokio::spawn(async move {
            tracing::info!(%addr, "admin listener started");
            if let Err(e) = axum_server::bind(addr)
                .serve(admin::router(state).into_make_service())
                .await
            {
                tracing::error!(error = %e, "admin listener stopped");
            }
        });
    }

    let handle: Handle<SocketAddr> = Handle::new();
    tokio::spawn(shutdown_signal(handle.clone()));

    let listen = options.listen;
    let serve = if let Some(tls) = &options.tls {
        let rustls = RustlsConfig::from_pem_file(&tls.cert, &tls.key)
            .await
            .wrap_err("loading TLS certificate and key")?;
        tracing::info!(%listen, origin = %options.origin, "listening (https)");
        axum_server::bind_rustls(listen, rustls)
            .handle(handle)
            .serve(options.s3.into_make_service())
            .await
    } else {
        tracing::info!(%listen, origin = %options.origin, "listening (http)");
        axum_server::bind(listen)
            .handle(handle)
            .serve(options.s3.into_make_service())
            .await
    };
    serve.wrap_err_with(|| format!("serving on {listen}"))?;

    options.nestor.close().await.wrap_err("flushing cache")?;
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
