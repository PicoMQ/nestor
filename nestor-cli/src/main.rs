//! The `nestor` binary. `serve` runs the S3 frontend, `check` validates a configuration.

mod cluster;
mod config;
mod serve;
mod telemetry;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use eyre::Report;
use tracing_subscriber::EnvFilter;

use crate::config::Config;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
#[command(
    name = "nestor",
    version,
    about = "Block-based read-through cache for object storage"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Serve {
        #[arg(short, long, env = "NESTOR_CONFIG")]
        config: Option<PathBuf>,
    },
    Check {
        #[arg(short, long, env = "NESTOR_CONFIG")]
        config: Option<PathBuf>,
    },
}

fn main() -> Result<(), Report> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    match Cli::parse().command {
        Command::Serve { config } => {
            let config = Config::load(config.as_deref())?;
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(serve::run(config))
        }
        Command::Check { config } => {
            let config = Config::load(config.as_deref())?;
            config.validate()?;
            config.s3()?;
            config.cache.builder()?;
            println!("configuration ok");
            Ok(())
        }
    }
}
