use crate::{AdapterConfig, Auth, AuthError};

use dbt_xdbc::{Backend, database};

pub struct DuckdbAuth;

impl Auth for DuckdbAuth {
    fn backend(&self) -> Backend {
        Backend::DuckDB
    }

    fn configure(&self, config: &AdapterConfig) -> Result<database::Builder, AuthError> {
        let mut builder = database::Builder::new(self.backend());

        let path = config.require_string("path")?;

        // Simple URI construction.
        // We assume 'path' is a filesystem path.
        // If it isn't prefixed with 'duckdb:', we add it.
        // Use 'path' option directly instead of URI, as DuckDB ADBC driver might not support 'uri' option key
        // or maps it incorrectly.
        // We strip 'duckdb://' if present as it is a common prefix but DuckDB takes the file path.
        let path = if path.starts_with("duckdb://") {
            &path["duckdb://".len()..]
        } else if path.starts_with("duckdb:") {
            &path["duckdb:".len()..]
        } else {
            path.as_ref()
        };

        builder.with_named_option("path", path)?;

        Ok(builder)
    }
}
