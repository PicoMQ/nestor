use clap::{Args, Subcommand};
use eyre::{Context, Report, bail};
use serde_json::Value;

#[derive(Debug, Args)]
pub struct QueryArgs {
    #[arg(
        long,
        env = "NESTOR_ADMIN_ENDPOINT",
        default_value = "http://127.0.0.1:9190"
    )]
    pub admin_endpoint: String,
    #[arg(long)]
    pub json: bool,
    #[command(subcommand)]
    pub command: QueryCommand,
}

#[derive(Debug, Subcommand)]
pub enum QueryCommand {
    Status,
    Namespaces,
}

pub async fn run(args: QueryArgs) -> Result<(), Report> {
    let base = args.admin_endpoint.trim_end_matches('/');
    let path = match args.command {
        QueryCommand::Status => "/admin/status",
        QueryCommand::Namespaces => "/admin/namespaces",
    };
    let body: Value = reqwest::get(format!("{base}{path}"))
        .await
        .wrap_err("admin request")?
        .error_for_status()
        .wrap_err("admin status")?
        .json()
        .await
        .wrap_err("admin body")?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&body)?);
        return Ok(());
    }
    match args.command {
        QueryCommand::Status => print_status(&body),
        QueryCommand::Namespaces => print_namespaces(&body)?,
    }
    Ok(())
}

fn print_status(body: &Value) {
    let cache = &body["cache"];
    println!(
        "listen={} origin={} memory={}/{} disk={} meta={}/{} inflight={}",
        display(&body["listen"]),
        display(&body["origin"]),
        display(&cache["memoryUsed"]),
        display(&cache["memoryCap"]),
        optional(&cache["diskCap"]),
        display(&cache["metaUsed"]),
        display(&cache["metaCap"]),
        display(&cache["inflight"]),
    );
    let totals = &body["totals"];
    println!(
        "hitRatio={} amplification={} hits={} misses={} joined={} stale={} originErrors={}",
        display(&body["hitRatio"]),
        display(&body["amplification"]),
        display(&totals["hits"]),
        display(&totals["misses"]),
        display(&totals["joined"]),
        display(&totals["stale"]),
        display(&totals["originErrors"]),
    );
}

fn print_namespaces(body: &Value) -> Result<(), Report> {
    let Some(namespaces) = body["namespaces"].as_array() else {
        bail!("admin namespaces response missing namespaces");
    };
    if namespaces.is_empty() {
        println!("no namespaces");
        return Ok(());
    }
    for ns in namespaces {
        println!(
            "ns={} block={} consistency={} hits={} misses={} hitRatio={} originErrors={}",
            display(&ns["name"]),
            display(&ns["blockSize"]),
            display(&ns["consistency"]["mode"]),
            display(&ns["counters"]["hits"]),
            display(&ns["counters"]["misses"]),
            display(&ns["hitRatio"]),
            display(&ns["counters"]["originErrors"]),
        );
    }
    Ok(())
}

fn display(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => "-".into(),
        other => other.to_string(),
    }
}

fn optional(value: &Value) -> String {
    match value {
        Value::Null => "-".into(),
        other => other.to_string(),
    }
}
