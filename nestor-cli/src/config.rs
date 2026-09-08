//! Configuration loaded from TOML and `NESTOR_*` environment variables. The defaults here are the
//! ones documented in `nestor-cli/nestor.toml`.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use byte_unit::Byte;
use eyre::{Context, Report, bail};
use figment::Figment;
use figment::providers::{Env, Format, Toml};
use http::Uri;
use nestor::{
    BlockSize, CacheConfig, Compression, Consistency, DiskConfig, HedgeConfig, NamespaceConfig,
    NestorBuilder, RecoverMode, RetryConfig,
};
use nestor_client::{ClusterConfig, Credentials, Membership};
use nestor_s3::{Addressing, Auth, OriginConfig, S3Config};
use serde::Deserialize;

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub server: Server,
    pub origin: Origin,
    pub auth: AuthMode,
    pub cache: Cache,
    pub buckets: Buckets,
    pub cluster: Option<Cluster>,
}

impl Config {
    pub fn load(path: Option<&Path>) -> Result<Self, Report> {
        let mut figment = Figment::new();
        if let Some(path) = path {
            figment = figment.merge(Toml::file_exact(path));
        }
        figment
            .merge(Env::prefixed("NESTOR_").split("__"))
            .extract()
            .wrap_err("invalid configuration")
    }

    pub fn validate(&self) -> Result<(), Report> {
        if self.origin.endpoint.authority().is_none() {
            bail!("origin.endpoint must include a host");
        }
        if let Some(cluster) = &self.cluster {
            cluster.membership()?;
            let node_block = self.buckets.namespace()?.block_size.bytes();
            let cluster_block = cluster.config()?.block_size.bytes();
            if cluster_block % node_block != 0 {
                bail!(
                    "cluster.block_size ({cluster_block}) must be a multiple of buckets.block_size ({node_block})"
                );
            }
        }
        if !self.server.listen.ip().is_loopback()
            && self.server.tls.is_none()
            && matches!(self.auth, AuthMode::Anonymous)
        {
            tracing::warn!(
                listen = %self.server.listen,
                "serving anonymous plaintext HTTP on a non-loopback address, anyone who can reach this \
                 socket can read every cached object"
            );
        }
        Ok(())
    }

    pub fn s3(&self) -> Result<S3Config, Report> {
        Ok(S3Config {
            origin: self.origin.build()?,
            origins: None,
            auth: Auth::from(&self.auth),
            addressing: Addressing::from(&self.server.addressing),
            buckets: self.buckets.namespace()?,
            populate_max: self
                .buckets
                .populate_max
                .map(|b| bytes(b, "buckets.populate_max"))
                .transpose()?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Server {
    pub listen: SocketAddr,
    pub tls: Option<Tls>,
    pub metrics: Option<SocketAddr>,
    pub addressing: AddressingStyle,
}

impl Default for Server {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from((Ipv4Addr::LOCALHOST, 9000)),
            tls: None,
            metrics: None,
            addressing: AddressingStyle::default(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    pub cert: PathBuf,
    pub key: PathBuf,
}

#[derive(Debug, Default, Deserialize)]
#[serde(tag = "style", rename_all = "snake_case", deny_unknown_fields)]
pub enum AddressingStyle {
    #[default]
    Path,
    VirtualHosted {
        domain: String,
    },
}

impl From<&AddressingStyle> for Addressing {
    fn from(style: &AddressingStyle) -> Self {
        match style {
            AddressingStyle::Path => Self::Path,
            AddressingStyle::VirtualHosted { domain } => Self::VirtualHosted {
                domain: domain.clone(),
            },
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Origin {
    #[serde(deserialize_with = "parse_uri")]
    pub endpoint: Uri,
    pub region: String,
    pub credentials: OriginCredentials,
    pub virtual_hosted: bool,
}

impl Default for Origin {
    fn default() -> Self {
        Self {
            endpoint: Uri::from_static("https://s3.us-east-1.amazonaws.com"),
            region: "us-east-1".into(),
            credentials: OriginCredentials::Default,
            virtual_hosted: false,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
pub enum OriginCredentials {
    #[default]
    Default,
    Anonymous,
    Static {
        access_key: String,
        secret_key: String,
        session_token: Option<String>,
    },
}

impl Origin {
    pub fn build(&self) -> Result<OriginConfig, Report> {
        let config = OriginConfig::anonymous(self.endpoint.clone(), self.region.clone())
            .with_virtual_hosted(self.virtual_hosted);
        Ok(match &self.credentials {
            OriginCredentials::Anonymous => config,
            OriginCredentials::Default => config
                .with_default_credentials()
                .wrap_err("resolving origin credentials from the environment")?,
            OriginCredentials::Static {
                access_key,
                secret_key,
                session_token,
            } => config.with_static_credentials(access_key, secret_key, session_token.clone()),
        })
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuthMode {
    #[default]
    Anonymous,
    Static {
        access_key: String,
        secret_key: String,
    },
}

impl From<&AuthMode> for Auth {
    fn from(mode: &AuthMode) -> Self {
        match mode {
            AuthMode::Anonymous => Self::Anonymous,
            AuthMode::Static {
                access_key,
                secret_key,
            } => Self::Static {
                access_key: access_key.clone(),
                secret_key: secret_key.clone(),
            },
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Cache {
    pub memory: Byte,
    pub shards: Option<usize>,
    pub disk: Option<Disk>,
    pub meta_entries: usize,
    pub origin_concurrency: usize,
    pub readahead_concurrency: usize,
    pub hedge_concurrency: usize,
    pub retry: RetryConfig,
}

impl Default for Cache {
    fn default() -> Self {
        Self {
            memory: Byte::from_u64(256 * 1024 * 1024),
            shards: None,
            disk: None,
            meta_entries: 100_000,
            origin_concurrency: 64,
            readahead_concurrency: 16,
            hedge_concurrency: 16,
            retry: RetryConfig::default(),
        }
    }
}

impl Cache {
    pub fn builder(&self) -> Result<NestorBuilder, Report> {
        let mut cache = CacheConfig::memory(bytes(self.memory, "cache.memory")?);
        if let Some(shards) = self.shards {
            cache.shards = shards;
        }
        if let Some(disk) = &self.disk {
            cache.disk = Some(disk.build()?);
        }
        Ok(NestorBuilder::new(cache)
            .meta_capacity(self.meta_entries)
            .origin_concurrency(self.origin_concurrency)
            .readahead_concurrency(self.readahead_concurrency)
            .hedge_concurrency(self.hedge_concurrency)
            .retry(self.retry))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Disk {
    pub path: PathBuf,
    pub capacity: Byte,
    pub region_size: Byte,
    pub direct_io: bool,
    pub compression: DiskCompression,
    pub recover: DiskRecovery,
}

impl Default for Disk {
    fn default() -> Self {
        let defaults = DiskConfig::new("/var/lib/nestor", 8 * 1024 * 1024 * 1024);
        Self {
            path: defaults.path,
            capacity: Byte::from_u64(defaults.capacity as u64),
            region_size: Byte::from_u64(defaults.region_size as u64),
            direct_io: defaults.direct_io,
            compression: DiskCompression::None,
            recover: DiskRecovery::Quiet,
        }
    }
}

impl Disk {
    fn build(&self) -> Result<DiskConfig, Report> {
        let mut config = DiskConfig::new(&self.path, bytes(self.capacity, "cache.disk.capacity")?);
        config.region_size = bytes(self.region_size, "cache.disk.region_size")?;
        config.direct_io = self.direct_io;
        config.compression = self.compression.into();
        config.recover = self.recover.into();
        Ok(config)
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiskCompression {
    None,
    Lz4,
    Zstd,
}

impl From<DiskCompression> for Compression {
    fn from(compression: DiskCompression) -> Self {
        match compression {
            DiskCompression::None => Self::None,
            DiskCompression::Lz4 => Self::Lz4,
            DiskCompression::Zstd => Self::Zstd,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiskRecovery {
    None,
    Quiet,
    Strict,
}

impl From<DiskRecovery> for RecoverMode {
    fn from(recovery: DiskRecovery) -> Self {
        match recovery {
            DiskRecovery::None => Self::None,
            DiskRecovery::Quiet => Self::Quiet,
            DiskRecovery::Strict => Self::Strict,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Buckets {
    pub block_size: Byte,
    pub fetch_window: u32,
    pub read_window: u32,
    pub consistency: Consistency,
    pub readahead: u32,
    pub hedge: Option<HedgeConfig>,
    pub populate_max: Option<Byte>,
}

impl Default for Buckets {
    fn default() -> Self {
        let defaults = NamespaceConfig::default();
        Self {
            block_size: Byte::from_u64(defaults.block_size.bytes()),
            fetch_window: defaults.fetch_window,
            read_window: defaults.read_window,
            consistency: defaults.consistency,
            readahead: defaults.readahead,
            hedge: defaults.hedge,
            populate_max: Some(Byte::from_u64(16 * 1024 * 1024)),
        }
    }
}

impl Buckets {
    pub fn namespace(&self) -> Result<NamespaceConfig, Report> {
        let block_size = u32::try_from(self.block_size.as_u64())
            .ok()
            .and_then(BlockSize::new)
            .ok_or_else(|| {
                eyre::eyre!(
                    "buckets.block_size must be a power of two between {} and {} bytes",
                    nestor::MIN_BLOCK_SIZE,
                    nestor::MAX_BLOCK_SIZE
                )
            })?;
        Ok(NamespaceConfig {
            block_size,
            fetch_window: self.fetch_window,
            read_window: self.read_window,
            consistency: self.consistency,
            readahead: self.readahead,
            hedge: self.hedge,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Cluster {
    pub nodes: Vec<SocketAddr>,
    pub dns: Option<String>,
    #[serde(with = "humantime_serde")]
    pub refresh: Duration,
    pub block_size: Byte,
    pub read_window: u32,
    pub load_limit: usize,
    #[serde(with = "humantime_serde")]
    pub down_for: Duration,
    pub hedge: Option<HedgeConfig>,
    pub tls: bool,
    pub credentials: Option<ClusterCredentials>,
    pub warm_on_write: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterCredentials {
    pub access_key: String,
    pub secret_key: String,
}

impl Default for Cluster {
    fn default() -> Self {
        let defaults = ClusterConfig::default();
        Self {
            nodes: Vec::new(),
            dns: None,
            refresh: Duration::from_secs(10),
            block_size: Byte::from_u64(defaults.block_size.bytes()),
            read_window: defaults.read_window,
            load_limit: defaults.load_limit,
            down_for: defaults.down_for,
            hedge: defaults.hedge,
            tls: defaults.tls,
            credentials: None,
            warm_on_write: false,
        }
    }
}

impl Cluster {
    pub fn membership(&self) -> Result<Membership, Report> {
        match (&self.dns, self.nodes.is_empty()) {
            (Some(_), false) => bail!("cluster.nodes and cluster.dns are mutually exclusive"),
            (None, true) => bail!("cluster requires either cluster.nodes or cluster.dns"),
            (None, false) => Ok(Membership::Static(self.nodes.clone())),
            (Some(dns), true) => {
                let (host, port) = dns
                    .rsplit_once(':')
                    .ok_or_else(|| eyre::eyre!("cluster.dns must be host:port"))?;
                let port = port
                    .parse()
                    .wrap_err_with(|| format!("cluster.dns has an invalid port: {port}"))?;
                Ok(Membership::dns(host, port).refresh(self.refresh))
            }
        }
    }

    pub fn config(&self) -> Result<ClusterConfig, Report> {
        let block_size = u32::try_from(self.block_size.as_u64())
            .ok()
            .and_then(BlockSize::new)
            .ok_or_else(|| {
                eyre::eyre!(
                    "cluster.block_size must be a power of two between {} and {} bytes",
                    nestor::MIN_BLOCK_SIZE,
                    nestor::MAX_BLOCK_SIZE
                )
            })?;
        Ok(ClusterConfig {
            block_size,
            read_window: self.read_window,
            load_limit: self.load_limit,
            down_for: self.down_for,
            hedge: self.hedge,
            tls: self.tls,
            credentials: self.credentials.as_ref().map(|c| Credentials {
                access_key: c.access_key.clone(),
                secret_key: c.secret_key.clone(),
            }),
        })
    }
}

fn bytes(value: Byte, field: &str) -> Result<usize, Report> {
    usize::try_from(value.as_u64()).wrap_err_with(|| format!("{field} does not fit in usize"))
}

fn parse_uri<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Uri, D::Error> {
    String::deserialize(deserializer)?
        .parse()
        .map_err(serde::de::Error::custom)
}

#[cfg(test)]
#[allow(clippy::result_large_err)]
mod tests {
    use std::time::Duration;

    use figment::Jail;

    use super::*;

    #[test]
    fn defaults_are_loopback_and_anonymous() {
        let config = Config::load(None).unwrap();
        assert!(config.server.listen.ip().is_loopback());
        assert!(matches!(config.auth, AuthMode::Anonymous));
        assert!(matches!(
            config.origin.credentials,
            OriginCredentials::Default
        ));
        assert_eq!(
            config.buckets.namespace().unwrap(),
            NamespaceConfig::default()
        );
    }

    #[test]
    fn parses_full_file_and_env_override() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "nestor.toml",
                r#"
                [server]
                listen = "0.0.0.0:9000"
                metrics = "127.0.0.1:9100"
                tls = { cert = "/etc/nestor/tls.crt", key = "/etc/nestor/tls.key" }
                addressing = { style = "virtual_hosted", domain = "s3.internal" }

                [origin]
                endpoint = "http://minio:9000"
                region = "us-east-1"
                credentials = { source = "static", access_key = "ak", secret_key = "sk" }

                [auth]
                mode = "static"
                access_key = "client"
                secret_key = "secret"

                [cache]
                memory = "2 GiB"
                retry = { attempts = 5, base = "10ms", max = "500ms" }
                [cache.disk]
                path = "/mnt/nestor"
                capacity = "100 GiB"
                compression = "lz4"

                [buckets]
                block_size = "4 MiB"
                consistency = { mode = "etag", ttl = "5m" }
                hedge = { factor = 3.0, min = "20ms", max = "1s" }
                "#,
            )?;
            jail.set_env("NESTOR_CACHE__MEMORY", "\"512 MiB\"");
            let config = Config::load(Some(Path::new("nestor.toml"))).unwrap();
            assert_eq!(config.server.listen.port(), 9000);
            assert!(config.server.tls.is_some());
            assert!(matches!(
                config.server.addressing,
                AddressingStyle::VirtualHosted { ref domain } if domain == "s3.internal"
            ));
            assert_eq!(config.cache.memory.as_u64(), 512 * 1024 * 1024);
            assert_eq!(config.cache.retry.attempts, 5);
            assert_eq!(config.cache.retry.base, Duration::from_millis(10));
            let disk = config.cache.disk.as_ref().unwrap();
            assert_eq!(disk.capacity.as_u64(), 100 * 1024 * 1024 * 1024);
            assert!(matches!(disk.compression, DiskCompression::Lz4));
            let namespace = config.buckets.namespace().unwrap();
            assert_eq!(namespace.block_size.bytes(), 4 * 1024 * 1024);
            assert_eq!(
                namespace.consistency,
                Consistency::Etag {
                    ttl: Duration::from_secs(300)
                }
            );
            assert_eq!(namespace.hedge.unwrap().min, Duration::from_millis(20));
            Ok(())
        });
    }

    #[test]
    fn parses_cluster_and_checks_block_alignment() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "cluster.toml",
                r#"
                [buckets]
                block_size = "1 MiB"

                [cluster]
                dns = "nestor.cache.svc:9000"
                refresh = "5s"
                block_size = "4 MiB"
                load_limit = 32
                credentials = { access_key = "ak", secret_key = "sk" }
                warm_on_write = true
                "#,
            )?;
            let config = Config::load(Some(Path::new("cluster.toml"))).unwrap();
            config.validate().unwrap();
            let cluster = config.cluster.as_ref().unwrap();
            assert_eq!(
                cluster.membership().unwrap(),
                Membership::dns("nestor.cache.svc", 9000).refresh(Duration::from_secs(5))
            );
            let built = cluster.config().unwrap();
            assert_eq!(built.block_size.bytes(), 4 * 1024 * 1024);
            assert_eq!(built.load_limit, 32);
            assert!(built.credentials.is_some());
            assert!(cluster.warm_on_write);

            jail.create_file(
                "misaligned.toml",
                "[buckets]\nblock_size = \"4 MiB\"\n[cluster]\nnodes = [\"10.0.0.1:9000\"]\nblock_size = \"1 MiB\"\n",
            )?;
            let config = Config::load(Some(Path::new("misaligned.toml"))).unwrap();
            assert!(config.validate().is_err());

            jail.create_file(
                "both.toml",
                "[cluster]\nnodes = [\"10.0.0.1:9000\"]\ndns = \"a:1\"\n",
            )?;
            let config = Config::load(Some(Path::new("both.toml"))).unwrap();
            assert!(config.validate().is_err());
            Ok(())
        });
    }

    #[test]
    fn rejects_unknown_fields_and_bad_block_size() {
        Jail::expect_with(|jail| {
            jail.create_file("bad.toml", "[server]\nport = 1\n")?;
            assert!(Config::load(Some(Path::new("bad.toml"))).is_err());
            jail.create_file("block.toml", "[buckets]\nblock_size = \"3 MiB\"\n")?;
            let config = Config::load(Some(Path::new("block.toml"))).unwrap();
            assert!(config.buckets.namespace().is_err());
            Ok(())
        });
    }
}
