use dbt_frontend_common::dialect::Dialect;
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display, EnumString};

/// The type of the adapter.
///
/// Used to identify the specific database adapter being used.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Display, AsRefStr, EnumString, Deserialize, Serialize,
)]
#[strum(serialize_all = "lowercase", ascii_case_insensitive)]
#[serde(rename_all = "lowercase")]
pub enum AdapterType {
    /// Postgres
    Postgres,
    /// Snowflake
    Snowflake,
    /// Bigquery
    Bigquery,
    /// Databricks
    Databricks,
    /// Redshift
    Redshift,
    /// Salesforce
    Salesforce,
    /// DuckDb
    DuckDb,
}

impl From<AdapterType> for Dialect {
    fn from(value: AdapterType) -> Self {
        match value {
            AdapterType::Postgres => Dialect::Postgresql,
            AdapterType::Snowflake => Dialect::Snowflake,
            AdapterType::Bigquery => Dialect::Bigquery,
            AdapterType::Databricks => Dialect::Databricks,
            AdapterType::Redshift => Dialect::Redshift,
            // Salesforce dialect is unclear, it claims ANSI vaguely
            // https://developer.salesforce.com/docs/data/data-cloud-query-guide/references/data-cloud-query-api-reference/c360a-api-query-v2-call-overview.html
            // falls back to Postgresql at the moment
            AdapterType::Salesforce => Dialect::Postgresql,
            AdapterType::DuckDb => Dialect::DuckDb,
        }
    }
}

pub const DBT_EXECUTION_PHASE_RENDER: &str = "render";
pub const DBT_EXECUTION_PHASE_ANALYZE: &str = "analyze";
pub const DBT_EXECUTION_PHASE_RUN: &str = "run";

pub const DBT_EXECUTION_PHASES: [&str; 3] = [
    DBT_EXECUTION_PHASE_RENDER,
    DBT_EXECUTION_PHASE_ANALYZE,
    DBT_EXECUTION_PHASE_RUN,
];

#[derive(Clone, Copy, Debug)]
pub enum ExecutionPhase {
    Render,
    Analyze,
    Run,
}

impl ExecutionPhase {
    pub const fn as_str(&self) -> &'static str {
        match self {
            ExecutionPhase::Render => DBT_EXECUTION_PHASE_RENDER,
            ExecutionPhase::Analyze => DBT_EXECUTION_PHASE_ANALYZE,
            ExecutionPhase::Run => DBT_EXECUTION_PHASE_RUN,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_adapter_type_duckdb_to_string() {
        assert_eq!(AdapterType::DuckDb.to_string(), "duckdb");
    }

    #[test]
    fn test_adapter_type_duckdb_from_string() {
        assert_eq!(
            AdapterType::from_str("duckdb").unwrap(),
            AdapterType::DuckDb
        );
        assert_eq!(
            AdapterType::from_str("DuckDb").unwrap(),
            AdapterType::DuckDb
        );
        assert_eq!(
            AdapterType::from_str("DUCKDB").unwrap(),
            AdapterType::DuckDb
        );
    }

    #[test]
    fn test_adapter_type_duckdb_to_dialect() {
        let dialect: Dialect = AdapterType::DuckDb.into();
        assert_eq!(dialect, Dialect::DuckDb);
    }

    #[test]
    fn test_adapter_type_duckdb_as_ref() {
        assert_eq!(AdapterType::DuckDb.as_ref(), "duckdb");
    }

    #[test]
    fn test_adapter_type_duckdb_serde() {
        let adapter = AdapterType::DuckDb;
        let serialized = serde_json::to_string(&adapter).unwrap();
        assert_eq!(serialized, "\"duckdb\"");

        let deserialized: AdapterType = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, AdapterType::DuckDb);
    }
}
