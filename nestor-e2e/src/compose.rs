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

    pub async fn exec(&self, service: &str, command: &[&str]) -> String {
        let mut args = vec!["exec", "-T", service];
        args.extend_from_slice(command);
        self.run(&args).await
    }

    async fn run(&self, args: &[&str]) -> String {
        let output = Command::new("docker")
            .arg("compose")
            .arg("-f")
            .arg(&self.file)
            .args(args)
            .output()
            .await
            .expect("docker compose");
        assert!(
            output.status.success(),
            "docker compose {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("utf8 output")
    }
}
