use std::sync::Arc;

use crate::AdapterType;
use crate::relation::StaticBaseRelationObject;
use crate::relation::bigquery::BigqueryRelationType;
use crate::relation::databricks::DatabricksRelationType;
use crate::relation::duckdb::DuckdbRelationType;
use crate::relation::postgres::PostgresRelationType;
use crate::relation::redshift::RedshiftRelationType;
use crate::relation::salesforce::SalesforceRelationType;
use crate::relation::snowflake::SnowflakeRelationType;

use dbt_schemas::schemas::common::ResolvedQuoting;
use minijinja::Value;

/// Create a static relation value from an adapter type
/// To be used as api.Relation in the Jinja environment
pub fn create_static_relation(
    adapter_type: AdapterType,
    quoting: ResolvedQuoting,
) -> Option<Value> {
    let result = match adapter_type {
        AdapterType::Snowflake => {
            let snowflake_relation_type = SnowflakeRelationType(quoting);
            StaticBaseRelationObject::new(Arc::new(snowflake_relation_type))
        }
        AdapterType::Postgres => {
            let postgres_relation_type = PostgresRelationType(quoting);
            StaticBaseRelationObject::new(Arc::new(postgres_relation_type))
        }
        AdapterType::Bigquery => {
            let bigquery_relation_type = BigqueryRelationType(quoting);
            StaticBaseRelationObject::new(Arc::new(bigquery_relation_type))
        }
        AdapterType::Databricks => {
            let databricks_relation_type = DatabricksRelationType(quoting);
            StaticBaseRelationObject::new(Arc::new(databricks_relation_type))
        }
        AdapterType::Redshift => {
            let redshift_relation_type = RedshiftRelationType(quoting);
            StaticBaseRelationObject::new(Arc::new(redshift_relation_type))
        }
        AdapterType::Salesforce => {
            let salesforce_relation_type = SalesforceRelationType(quoting);
            StaticBaseRelationObject::new(Arc::new(salesforce_relation_type))
        }
        AdapterType::DuckDb => {
            let duckdb_relation_type = DuckdbRelationType(quoting);
            StaticBaseRelationObject::new(Arc::new(duckdb_relation_type))
        }
    };
    Some(Value::from_object(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_schemas::schemas::common::ResolvedQuoting;

    #[test]
    fn test_create_static_relation_duckdb() {
        let quoting = ResolvedQuoting {
            database: true,
            schema: false,
            identifier: true,
        };
        let value = create_static_relation(AdapterType::DuckDb, quoting);
        assert!(value.is_some());
        
        // The value should be a StaticBaseRelationObject
        let val = value.unwrap();
        assert!(val.as_object().is_some());
        
        // We can't easily downcast minijinja::Value back to our struct here 
        // without more boilerplate, but we've covered the creation path.
    }
}
