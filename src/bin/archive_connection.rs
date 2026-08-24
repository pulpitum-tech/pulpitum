use pulpitum::{OpenDalArchiveStore, S3ArchiveConfig, S3ServerSideEncryption};
use std::{env, io};

fn optional_setting(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

pub fn connect(
    prefix_setting: &str,
    default_prefix: &str,
) -> Result<OpenDalArchiveStore, Box<dyn std::error::Error>> {
    let endpoint =
        optional_setting("S3_ENDPOINT").unwrap_or_else(|| "http://127.0.0.1:9000".to_owned());
    let bucket = optional_setting("S3_BUCKET").unwrap_or_else(|| "pulpitum".to_owned());
    let mut config = S3ArchiveConfig::new(
        bucket,
        optional_setting(prefix_setting).unwrap_or_else(|| default_prefix.to_owned()),
    );
    config.endpoint = Some(endpoint.clone());
    config.region = optional_setting("S3_REGION");
    config.allow_http = match optional_setting("S3_ALLOW_HTTP") {
        Some(value) => value.parse()?,
        None => {
            endpoint.starts_with("http://127.0.0.1:") || endpoint.starts_with("http://localhost:")
        }
    };

    let local_development_endpoint =
        endpoint.starts_with("http://127.0.0.1:") || endpoint.starts_with("http://localhost:");
    let mut access_key = optional_setting("S3_ACCESS_KEY");
    let mut secret_key = optional_setting("S3_SECRET_KEY");
    if access_key.is_none() && secret_key.is_none() && local_development_endpoint {
        access_key = Some("minioadmin".into());
        secret_key = Some("minioadmin".into());
    }
    match (access_key, secret_key) {
        (Some(access_key), Some(secret_key)) => {
            config.access_key_id = Some(access_key);
            config.secret_access_key = Some(secret_key);
            config.session_token = optional_setting("S3_SESSION_TOKEN");
        }
        (None, None) => {}
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "S3_ACCESS_KEY and S3_SECRET_KEY must be configured together",
            )
            .into());
        }
    }

    config.server_side_encryption = match optional_setting("S3_SERVER_SIDE_ENCRYPTION").as_deref() {
        None | Some("none") => None,
        Some("s3") | Some("AES256") => Some(S3ServerSideEncryption::S3Managed),
        Some("kms") | Some("aws:kms") => match optional_setting("S3_KMS_KEY_ID") {
            Some(key_id) => Some(S3ServerSideEncryption::CustomerManagedKms(key_id)),
            None => Some(S3ServerSideEncryption::AwsManagedKms),
        },
        Some(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "S3_SERVER_SIDE_ENCRYPTION must be one of none, s3, or kms",
            )
            .into());
        }
    };

    Ok(OpenDalArchiveStore::s3_config(config)?)
}
