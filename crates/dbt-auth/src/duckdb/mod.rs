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

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::options::{OptionDatabase, OptionValue};
    use dbt_serde_yaml::Mapping;

    fn str_value(value: &OptionValue) -> &str {
        match value {
            OptionValue::String(s) => s.as_str(),
            _ => panic!("unexpected value type"),
        }
    }

    #[test]
    fn test_backend_returns_duckdb() {
        let auth = DuckdbAuth;
        assert_eq!(auth.backend(), Backend::DuckDB);
    }

    #[test]
    fn test_configure_with_path() {
        let auth = DuckdbAuth;
        let config = Mapping::from_iter([("path".into(), "/tmp/test.duckdb".into())]);
        let builder = auth
            .configure(&AdapterConfig::new(config))
            .expect("configure should succeed");

        let mut found_path = false;
        builder.into_iter().for_each(|(k, v)| {
            if let OptionDatabase::Other(ref name) = k {
                if name == "path" {
                    assert_eq!(str_value(&v), "/tmp/test.duckdb");
                    found_path = true;
                }
            }
        });
        assert!(found_path, "path option should be set");
    }

    #[test]
    fn test_configure_strips_duckdb_uri_prefix() {
        let auth = DuckdbAuth;
        let config = Mapping::from_iter([("path".into(), "duckdb:///tmp/test.duckdb".into())]);
        let builder = auth
            .configure(&AdapterConfig::new(config))
            .expect("configure should succeed");

        builder.into_iter().for_each(|(k, v)| {
            if let OptionDatabase::Other(ref name) = k {
                if name == "path" {
                    // Should strip "duckdb://" prefix
                    assert_eq!(str_value(&v), "/tmp/test.duckdb");
                }
            }
        });
    }

    #[test]
    fn test_configure_strips_duckdb_colon_prefix() {
        let auth = DuckdbAuth;
        let config = Mapping::from_iter([("path".into(), "duckdb:/tmp/test.duckdb".into())]);
        let builder = auth
            .configure(&AdapterConfig::new(config))
            .expect("configure should succeed");

        builder.into_iter().for_each(|(k, v)| {
            if let OptionDatabase::Other(ref name) = k {
                if name == "path" {
                    // Should strip "duckdb:" prefix
                    assert_eq!(str_value(&v), "/tmp/test.duckdb");
                }
            }
        });
    }

    #[test]
    fn test_configure_missing_path_error() {
        let auth = DuckdbAuth;
        let config = Mapping::new(); // Empty config, no path
        let result = auth.configure(&AdapterConfig::new(config));
        // Should error when required path option is missing
        assert!(result.is_err(), "configure should fail without path option");
    }
}
