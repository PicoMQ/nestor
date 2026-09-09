//! Bench CLI: `dataset`, `run`, `compare` and `proxy`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use clap::{Args, Parser, Subcommand, ValueEnum};
use eyre::WrapErr;
use nestor::{FetchOverrides, FetchPolicy};
use nestor_bench::dataset::{Dataset, DatasetParams, Profile};
use nestor_bench::proxy::Proxy;
use nestor_bench::report::{Report, compare};
use nestor_bench::run::{Runner, Stack};
use nestor_bench::target::{Endpoint, Library, LibraryConfig, Target};
use nestor_bench::workload::{Params, Scenario};
use nestor_e2e::s3;
use object_store::ObjectStore;

#[derive(Parser)]
#[command(name = "bench", about = "nestor workload bench")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Upload the dataset to the origin.
    Dataset {
        #[command(flatten)]
        dataset: DatasetArgs,
        /// S3 endpoint of the origin.
        #[arg(long, env = "BENCH_ORIGIN", default_value = "http://127.0.0.1:19200")]
        origin: String,
        #[arg(long, default_value_t = 8)]
        concurrency: usize,
        /// Rewrite objects that already exist.
        #[arg(long)]
        force: bool,
    },
    /// Run one scenario against one target and write a report.
    Run(Box<RunArgs>),
    /// Print two reports side by side.
    Compare { a: PathBuf, b: PathBuf },
    /// Run the counting and fault-injecting origin proxy on its own.
    Proxy {
        #[arg(long, default_value = "0.0.0.0:19300")]
        listen: SocketAddr,
        #[arg(long, default_value = "0.0.0.0:19301")]
        control: SocketAddr,
        /// `host:port` of the origin.
        #[arg(long, default_value = "127.0.0.1:19200")]
        upstream: String,
    },
}

#[derive(Args, Clone)]
struct DatasetArgs {
    #[arg(long, default_value_t = 1)]
    seed: u64,
    #[arg(long, default_value_t = 32)]
    objects: usize,
    #[arg(long, value_enum, default_value_t = Profile::StreamSet)]
    profile: Profile,
    #[arg(long, default_value_t = 1000)]
    streams: u32,
}

impl From<DatasetArgs> for DatasetParams {
    fn from(a: DatasetArgs) -> Self {
        Self {
            seed: a.seed,
            objects: a.objects,
            profile: a.profile,
            streams: a.streams,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum TargetKind {
    /// Nestor embedded in this process, reading the origin through the proxy.
    Library,
    /// A nestor binary reached over S3, its origin is the proxy.
    Endpoint,
    /// The proxy itself, no cache, the baseline.
    Origin,
}

#[derive(Args)]
struct RunArgs {
    #[arg(long, value_enum)]
    target: TargetKind,
    #[arg(long, value_enum)]
    scenario: Scenario,
    #[command(flatten)]
    dataset: DatasetArgs,

    /// S3 endpoint of the origin, the proxy forwards here.
    #[arg(long, env = "BENCH_ORIGIN", default_value = "http://127.0.0.1:19200")]
    origin: String,
    /// Where the in-process proxy listens. Containers reach it as host.docker.internal.
    #[arg(long, default_value = "0.0.0.0:19300")]
    proxy_listen: SocketAddr,
    /// How this process reaches the proxy.
    #[arg(long, default_value = "http://127.0.0.1:19300")]
    proxy_url: String,

    /// S3 endpoint of the nestor binary, for `--target endpoint`.
    #[arg(long, default_value = "http://127.0.0.1:19302")]
    endpoint: String,
    #[arg(long, default_value = "http://127.0.0.1:19303")]
    endpoint_metrics: String,
    /// Container name of the nestor binary, for RSS sampling.
    #[arg(long, default_value = "nestor-bench-nestor-1")]
    container: String,
    /// Compose file that runs the nestor binary, for the restart scenario.
    #[arg(long)]
    compose: Option<PathBuf>,

    #[command(flatten)]
    cache: CacheArgs,
    #[command(flatten)]
    workload: WorkloadArgs,

    /// Compare every byte read against the dataset.
    #[arg(long)]
    verify: bool,
    /// Exit non-zero when a scenario assertion fails.
    #[arg(long = "assert")]
    check: bool,
    #[arg(long)]
    out: Option<PathBuf>,
}

/// Nestor settings for `--target library`. `block` also sets the expected request counts for
/// every target, so it must match the nestor binary's config for `--target endpoint`.
#[derive(Args)]
struct CacheArgs {
    #[arg(long, value_parser = parse_bytes, default_value = "1 GiB")]
    memory: u64,
    #[arg(long)]
    disk_path: Option<PathBuf>,
    #[arg(long, value_parser = parse_bytes, default_value = "8 GiB")]
    disk: u64,
    #[arg(long, value_parser = parse_bytes, default_value = "1 MiB")]
    block: u64,
    #[arg(long, default_value_t = 8)]
    fetch_window: u32,
    #[arg(long, default_value_t = 16)]
    read_window: u32,
    #[arg(long, default_value_t = 0)]
    readahead: u32,
    /// `FetchPolicy` overrides in `X-Nestor-Fetch` form, for example `"attempts=5 hedge=off"`.
    #[arg(long, value_parser = FetchOverrides::parse, default_value = "")]
    fetch: FetchOverrides,
    /// `ETag` consistency with a 60 s TTL instead of immutable.
    #[arg(long)]
    etag: bool,
}

#[derive(Args)]
struct WorkloadArgs {
    #[arg(long, default_value_t = 16)]
    consumers: u32,
    #[arg(long, value_parser = parse_duration, default_value = "60s")]
    duration: Duration,
    #[arg(long, value_parser = parse_bytes, default_value = "1 GiB")]
    hot_set: u64,
    #[arg(long, value_parser = parse_duration, default_value = "5s")]
    stagger: Duration,
}

fn parse_bytes(text: &str) -> Result<u64, String> {
    byte_unit::Byte::parse_str(text, true)
        .map(|b| b.as_u64())
        .map_err(|e| e.to_string())
}

fn parse_duration(text: &str) -> Result<Duration, String> {
    humantime::parse_duration(text).map_err(|e| e.to_string())
}

fn authority(url: &str) -> eyre::Result<String> {
    let uri: http::Uri = url.parse().wrap_err_with(|| format!("bad url {url}"))?;
    uri.authority()
        .map(|a| a.to_string())
        .ok_or_else(|| eyre::eyre!("{url} has no host"))
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,object_store=warn,foyer=warn".into()),
        )
        .init();

    match Cli::parse().command {
        Command::Dataset {
            dataset,
            origin,
            concurrency,
            force,
        } => {
            let dataset = Dataset::generate(dataset.into());
            let store: Arc<dyn ObjectStore> = s3::client(&origin);
            tracing::info!(
                objects = dataset.objects.len(),
                mib = dataset.total_bytes() / (1024 * 1024),
                "uploading"
            );
            let uploaded = dataset.upload(store, concurrency, force).await?;
            tracing::info!(uploaded, "dataset ready");
        }
        Command::Run(args) => run(*args).await?,
        Command::Compare { a, b } => {
            let a = Report::read(&a)?;
            let b = Report::read(&b)?;
            print!("{}", compare(&a, &b));
        }
        Command::Proxy {
            listen,
            control,
            upstream,
        } => {
            let proxy = Proxy::new(upstream);
            tracing::info!(%listen, %control, "proxy listening");
            tokio::try_join!(
                Arc::clone(&proxy).serve(listen),
                proxy.serve_control(control)
            )?;
        }
    }
    Ok(())
}

async fn run(args: RunArgs) -> eyre::Result<()> {
    let dataset = Arc::new(Dataset::generate(args.dataset.clone().into()));
    let params = Params {
        consumers: args.workload.consumers,
        duration: args.workload.duration,
        hot_set: args.workload.hot_set,
        stagger: args.workload.stagger,
        block: args.cache.block,
    };

    let proxy = Proxy::new(authority(&args.origin)?);
    let serving = tokio::spawn(Arc::clone(&proxy).serve(args.proxy_listen));
    tokio::time::sleep(Duration::from_millis(50)).await;
    eyre::ensure!(!serving.is_finished(), "proxy failed to start");

    let mut library = None;
    let mut stack = None;
    let (target, config): (Arc<dyn Target>, serde_json::Value) = match args.target {
        TargetKind::Library => {
            let cache = &args.cache;
            let config = LibraryConfig {
                memory: cache.memory as usize,
                disk_path: cache.disk_path.clone(),
                disk_capacity: cache.disk as usize,
                block: u32::try_from(cache.block).wrap_err("block size")?,
                fetch_window: cache.fetch_window,
                read_window: cache.read_window,
                readahead: cache.readahead,
                fetch: FetchPolicy::default().with(&cache.fetch),
                immutable: !cache.etag,
            };
            let lib = Arc::new(Library::start(&config, s3::origin(&args.proxy_url)).await?);
            library = Some(Arc::clone(&lib));
            (lib, serde_json::to_value(&config)?)
        }
        TargetKind::Endpoint => {
            stack = args.compose.clone().map(|compose| Stack {
                compose,
                service: "nestor".into(),
                health: format!("{}/-/health", args.endpoint),
            });
            let target = Endpoint::new(
                "endpoint",
                s3::origin(&args.endpoint),
                Some(args.endpoint_metrics.clone()),
                Some(args.container.clone()),
            );
            (
                Arc::new(target),
                serde_json::json!({ "endpoint": args.endpoint, "block": args.cache.block }),
            )
        }
        TargetKind::Origin => (
            Arc::new(Endpoint::new(
                "origin",
                s3::origin(&args.proxy_url),
                None,
                None,
            )),
            serde_json::json!({ "origin": args.origin }),
        ),
    };

    let runner = Runner {
        target: Arc::clone(&target),
        proxy: Arc::clone(&proxy),
        dataset: Arc::clone(&dataset),
        verify: args.verify,
        stack,
    };
    let phases = args.scenario.phases(&dataset, &params);
    let outcome = runner.run(phases).await?;
    if let Some(lib) = library {
        lib.close().await?;
    }
    serving.abort();

    let mut report = Report {
        target: target.name().to_owned(),
        scenario: args.scenario,
        dataset: dataset.params.clone(),
        params: params.clone(),
        config,
        started_unix: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs()),
        phases: outcome.phases,
        rss_max_mib: outcome.rss_max.map(|b| b as f64 / (1024.0 * 1024.0)),
        failures: Vec::new(),
    };
    report.failures = args.scenario.check(&dataset, &params, &report);

    print!("{}", report.summary());
    if let Some(out) = &args.out {
        report.write(out)?;
        tracing::info!(path = %out.display(), "report written");
    }
    if args.check && !report.failures.is_empty() {
        eyre::bail!("{} assertion(s) failed", report.failures.len());
    }
    Ok(())
}
