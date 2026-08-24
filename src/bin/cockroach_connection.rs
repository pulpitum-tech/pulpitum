use pulpitum::{CockroachDurableBucketStore, CockroachPoolConfig, CockroachTlsConfig};
use std::{env, fs, io};
use tokio_postgres::config::{Config, SslMode};

pub async fn connect(
    database_url: &str,
    pool_config: CockroachPoolConfig,
) -> Result<CockroachDurableBucketStore, Box<dyn std::error::Error>> {
    let parsed: Config = database_url.parse().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "CockroachDB URL is invalid; use sslmode=require for verified TLS or sslmode=disable only for local development",
        )
    })?;
    match parsed.get_ssl_mode() {
        SslMode::Disable => {
            tracing::warn!(
                "CockroachDB TLS is disabled; this connection mode is for local development only"
            );
            Ok(
                CockroachDurableBucketStore::connect_insecure_dev_with_pool_config(
                    database_url,
                    pool_config,
                )
                .await?,
            )
        }
        SslMode::Require => {
            let ca_path = env::var("COCKROACH_CA_CERT_PATH").map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "COCKROACH_CA_CERT_PATH is required when COCKROACH_URL uses sslmode=require",
                )
            })?;
            let ca_pem = fs::read(ca_path)?;
            let mut tls = CockroachTlsConfig::from_ca_pem(&ca_pem)?;
            match (
                env::var("COCKROACH_CLIENT_CERT_PATH").ok(),
                env::var("COCKROACH_CLIENT_KEY_PATH").ok(),
            ) {
                (Some(certificate_path), Some(key_path)) => {
                    let certificate = fs::read(certificate_path)?;
                    let key = fs::read(key_path)?;
                    tls = tls.with_client_auth_pem(&certificate, &key)?;
                }
                (None, None) => {}
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "COCKROACH_CLIENT_CERT_PATH and COCKROACH_CLIENT_KEY_PATH must be configured together",
                    )
                    .into());
                }
            }
            Ok(
                CockroachDurableBucketStore::connect_rustls_with_pool_config(
                    database_url,
                    pool_config,
                    tls,
                )
                .await?,
            )
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "CockroachDB connections must explicitly use sslmode=require or sslmode=disable; sslmode=prefer can downgrade to plaintext",
        )
        .into()),
    }
}
