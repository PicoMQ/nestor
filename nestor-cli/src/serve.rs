use eyre::{Context, Report};
use nestor_s3::S3Service;
use nestor_server::{Options, Telemetry, Tls};

use crate::cluster::ClusterOrigins;
use crate::config::Config;

pub async fn run(config: Config) -> Result<(), Report> {
    config.validate()?;

    let mut builder = config.cache.builder()?;
    let telemetry = if config.server.metrics.is_some() || config.server.admin.enabled {
        let telemetry = Telemetry::install()?;
        builder = builder.metrics_registry(telemetry.foyer_registry());
        Some(telemetry)
    } else {
        None
    };

    let nestor = builder.build().await.wrap_err("initialising cache")?;
    let mut s3 = config.s3()?;
    if let Some(cluster) = &config.cluster {
        s3.origins = Some(ClusterOrigins::connect(cluster).await?);
    }
    let service = S3Service::new(nestor.clone(), s3);

    nestor_server::run(Options {
        nestor,
        s3: service.router(),
        listen: config.server.listen,
        tls: config.server.tls.map(|tls| Tls {
            cert: tls.cert,
            key: tls.key,
        }),
        metrics: config.server.metrics,
        admin: config
            .server
            .admin
            .enabled
            .then_some(config.server.admin.listen),
        origin: config.origin.endpoint.to_string(),
        telemetry,
    })
    .await
}
