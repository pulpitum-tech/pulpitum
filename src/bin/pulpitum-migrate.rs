mod cockroach_connection;

use pulpitum::CockroachPoolConfig;
use std::env;

const RUNTIME_TABLES: &[&str] = &["pulpitum_v4_bucket_metadata", "pulpitum_v4_records"];

fn setting(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn pool_config() -> Result<CockroachPoolConfig, Box<dyn std::error::Error>> {
    let mut config = CockroachPoolConfig::default();
    if let Ok(value) = env::var("COCKROACH_POOL_MAX_CONNECTIONS") {
        config.max_connections = value.parse()?;
    }
    if config.max_connections == 0 {
        return Err("COCKROACH_POOL_MAX_CONNECTIONS must be greater than zero".into());
    }
    Ok(config)
}

fn quoted_role(role: &str) -> Result<String, Box<dyn std::error::Error>> {
    let valid = role
        .chars()
        .enumerate()
        .all(|(index, character)| match index {
            0 => character == '_' || character.is_ascii_alphabetic(),
            _ => character == '_' || character.is_ascii_alphanumeric(),
        });
    if !valid || role.is_empty() {
        return Err("PULPITUM_RUNTIME_ROLE must be a SQL identifier".into());
    }
    Ok(format!("\"{role}\""))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = setting(
        "COCKROACH_MIGRATION_URL",
        "postgresql://root@127.0.0.1:26257/defaultdb?sslmode=disable",
    );
    let runtime_role = quoted_role(&setting("PULPITUM_RUNTIME_ROLE", "pulpitum_runtime"))?;
    let durable = cockroach_connection::connect(&database_url, pool_config()?).await?;
    let pool = durable.pool();

    durable.migrate().await?;
    // Validate the committed migration history and v4 catalog shape before a
    // runtime role receives access to the application tables.
    durable.validate_schema().await?;

    let table_list = RUNTIME_TABLES.join(", ");
    let connection = pool.acquire().await?;
    connection
        .client()
        .batch_execute(&format!(
            "CREATE USER IF NOT EXISTS {runtime_role};
             REVOKE CREATE ON DATABASE defaultdb FROM public;
             REVOKE CREATE ON SCHEMA public FROM public;
             REVOKE CREATE ON DATABASE defaultdb FROM {runtime_role};
             REVOKE CREATE ON SCHEMA public FROM {runtime_role};
             GRANT CONNECT ON DATABASE defaultdb TO {runtime_role};
             GRANT USAGE ON SCHEMA public TO {runtime_role};
             GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {table_list} TO {runtime_role};"
        ))
        .await?;

    println!("Pulpitum schema migrated and runtime role {runtime_role} granted DML-only access");
    Ok(())
}
