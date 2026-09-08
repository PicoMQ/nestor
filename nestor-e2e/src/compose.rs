//! Controls services in the scenario's compose stack, for tests that take nodes down and up.

use std::path::{Path, PathBuf};

use tokio::process::Command;

pub struct Compose {
    file: PathBuf,
}

impl Compose {
    pub fn for_scenario(dir: &str) -> Self {
        Self {
            file: Path::new(env!("CARGO_MANIFEST_DIR"))
                .join(dir)
                .join("compose.yml"),
        }
    }

    pub async fn stop(&self, service: &str) {
        self.run(&["stop", service]).await;
    }

    pub async fn start(&self, service: &str) {
        self.run(&["start", service]).await;
    }

    async fn run(&self, args: &[&str]) {
        let status = Command::new("docker")
            .arg("compose")
            .arg("-f")
            .arg(&self.file)
            .args(args)
            .status()
            .await
            .expect("docker compose");
        assert!(status.success(), "docker compose {args:?} failed");
    }
}
