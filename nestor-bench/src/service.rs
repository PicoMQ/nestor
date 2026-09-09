//! The nestor binary behind `--target endpoint`: how it is restarted, how its state is dropped and
//! where its RSS is read. Either a compose service or a systemd unit.

use std::path::{Path, PathBuf};
use std::time::Duration;

use eyre::WrapErr;
use nestor_e2e::wait;
use tokio::process::Command;

#[derive(Debug, Clone)]
pub struct Service {
    pub host: Host,
    pub health: String,
}

#[derive(Debug, Clone)]
pub enum Host {
    Compose { file: PathBuf, service: String },
    Systemd { unit: String, disk: Option<PathBuf> },
}

impl Service {
    pub async fn restart(&self) -> eyre::Result<()> {
        match &self.host {
            Host::Compose { .. } => {
                self.compose(&["stop"]).await?;
                self.compose(&["start"]).await?;
            }
            Host::Systemd { .. } => self.systemctl("restart").await?,
        }
        self.wait_healthy().await;
        Ok(())
    }

    pub async fn recreate(&self) -> eyre::Result<()> {
        match &self.host {
            Host::Compose { .. } => {
                self.compose(&["rm", "--stop", "--force", "--volumes"])
                    .await?;
                self.compose(&["up", "--detach", "--wait"]).await?;
            }
            Host::Systemd { disk, .. } => {
                self.systemctl("stop").await?;
                if let Some(disk) = disk {
                    clear_dir(disk).await?;
                }
                self.systemctl("start").await?;
            }
        }
        self.wait_healthy().await;
        Ok(())
    }

    pub async fn rss_bytes(&self) -> Option<u64> {
        match &self.host {
            Host::Compose { file, service } => {
                let id = output(
                    Command::new("docker")
                        .args(["compose", "-f"])
                        .arg(file)
                        .args(["ps", "-q", service]),
                )
                .await?;
                let usage = output(Command::new("docker").args([
                    "stats",
                    "--no-stream",
                    "--format",
                    "{{.MemUsage}}",
                    id.trim(),
                ]))
                .await?;
                let used = usage.split('/').next()?.trim();
                byte_unit::Byte::parse_str(used, true)
                    .ok()
                    .map(|b| b.as_u64())
            }
            Host::Systemd { unit, .. } => {
                let pid = output(
                    Command::new("systemctl").args(["show", "-p", "MainPID", "--value", unit]),
                )
                .await?;
                let status = tokio::fs::read_to_string(format!("/proc/{}/status", pid.trim()))
                    .await
                    .ok()?;
                let kib: u64 = status
                    .lines()
                    .find_map(|line| line.strip_prefix("VmRSS:"))?
                    .trim()
                    .trim_end_matches("kB")
                    .trim()
                    .parse()
                    .ok()?;
                Some(kib * 1024)
            }
        }
    }

    async fn wait_healthy(&self) {
        wait::healthy(&self.health, Duration::from_secs(120)).await;
    }

    async fn compose(&self, args: &[&str]) -> eyre::Result<()> {
        let Host::Compose { file, service } = &self.host else {
            unreachable!("compose on a systemd service");
        };
        let status = Command::new("docker")
            .args(["compose", "-f"])
            .arg(file)
            .args(args)
            .arg(service)
            .status()
            .await
            .wrap_err("docker compose")?;
        eyre::ensure!(
            status.success(),
            "docker compose {} {service} failed",
            args.join(" ")
        );
        Ok(())
    }

    async fn systemctl(&self, verb: &str) -> eyre::Result<()> {
        let Host::Systemd { unit, .. } = &self.host else {
            unreachable!("systemctl on a compose service");
        };
        let status = Command::new("systemctl")
            .args([verb, unit])
            .status()
            .await
            .wrap_err("systemctl")?;
        eyre::ensure!(status.success(), "systemctl {verb} {unit} failed");
        Ok(())
    }
}

async fn output(command: &mut Command) -> Option<String> {
    let output = command.output().await.ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Empties a disk tier directory without touching the directory itself, so its ownership survives.
pub(crate) async fn clear_dir(dir: &Path) -> eyre::Result<()> {
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).wrap_err_with(|| format!("reading {}", dir.display())),
    };
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if entry.file_type().await?.is_dir() {
            tokio::fs::remove_dir_all(&path).await?;
        } else {
            tokio::fs::remove_file(&path).await?;
        }
    }
    Ok(())
}
