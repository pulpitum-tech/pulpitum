//! Production database and object-store adapters.

pub(crate) mod cockroach_durable;
pub(crate) mod cockroach_pool;
pub(crate) mod cockroach_schema;
pub(crate) mod cockroach_tls;
pub(crate) mod immutable_archive_cache;
pub(crate) mod opendal_store;
