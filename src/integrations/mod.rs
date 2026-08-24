//! Optional query-engine and telemetry integrations.

#[cfg(feature = "datafusion")]
pub(crate) mod datafusion;
pub(crate) mod observability;
