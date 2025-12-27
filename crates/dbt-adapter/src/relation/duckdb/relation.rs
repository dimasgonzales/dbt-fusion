use crate::information_schema::InformationSchema;
use crate::relation::{RelationObject, StaticBaseRelation};

use dbt_common::{ErrorCode, FsResult, current_function_name, fs_err};
use dbt_frontend_common::ident::Identifier;
use dbt_schema_store::CanonicalFqn;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::relations::base::{
    BaseRelation, BaseRelationProperties, Policy, RelationPath,
};
use minijinja::arg_utils::{ArgParser, check_num_args};
use minijinja::{Error as MinijinjaError, State, Value};

use std::any::Any;
use std::sync::Arc;

/// A struct representing the DuckDb relation type for use with static methods
#[derive(Clone, Debug)]
pub struct DuckdbRelationType(pub ResolvedQuoting);

impl StaticBaseRelation for DuckdbRelationType {
    fn try_new(
        &self,
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: Option<ResolvedQuoting>,
    ) -> Result<Value, MinijinjaError> {
        Ok(RelationObject::new(Arc::new(DuckdbRelation::try_new(
            database,
            schema,
            identifier,
            relation_type,
            custom_quoting.unwrap_or(self.0),
        )?))
        .into_value())
    }

    fn get_adapter_type(&self) -> String {
        "duckdb".to_string()
    }
}

/// A relation object for duckdb adapter
#[derive(Clone, Debug)]
pub struct DuckdbRelation {
    /// The database, schema, and identifier of the relation
    pub path: RelationPath,
    /// The relation type
    pub relation_type: Option<RelationType>,
    /// Include policy
    pub include_policy: Policy,
    /// Quote policy
    pub quote_policy: Policy,
}

impl DuckdbRelation {
    /// Creates a new DuckDb relation
    pub fn try_new(
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: ResolvedQuoting,
    ) -> Result<Self, MinijinjaError> {
        Self::try_new_with_policy(
            RelationPath {
                database,
                schema,
                identifier,
            },
            relation_type,
            Policy::enabled(),
            custom_quoting,
        )
    }

    /// Creates a new DuckDb relation
    pub fn try_new_with_policy(
        path: RelationPath,
        relation_type: Option<RelationType>,
        include_policy: Policy,
        quote_policy: Policy,
    ) -> Result<Self, MinijinjaError> {
        // DuckDB has no effective identifier length limit, so we skip the check

        Ok(Self {
            path,
            relation_type,
            include_policy,
            quote_policy,
        })
    }
}

impl BaseRelationProperties for DuckdbRelation {
    fn include_policy(&self) -> Policy {
        self.include_policy
    }

    fn quote_policy(&self) -> Policy {
        self.quote_policy
    }

    fn quote_character(&self) -> char {
        '"'
    }
    fn get_database(&self) -> FsResult<String> {
        self.path.database.clone().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "database is required for duckdb relation",
            )
        })
    }

    fn get_schema(&self) -> FsResult<String> {
        self.path.schema.clone().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "schema is required for duckdb relation",
            )
        })
    }

    fn get_identifier(&self) -> FsResult<String> {
        self.path.identifier.clone().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "identifier is required for duckdb relation",
            )
        })
    }

    fn get_canonical_fqn(&self) -> FsResult<CanonicalFqn> {
        let database = if self.quote_policy().database {
            Identifier::new(self.get_database()?)
        } else {
            Identifier::new(self.get_database()?.to_ascii_lowercase())
        };
        let schema = if self.quote_policy().schema {
            Identifier::new(self.get_schema()?)
        } else {
            Identifier::new(self.get_schema()?.to_ascii_lowercase())
        };
        let identifier = if self.quote_policy().identifier {
            Identifier::new(self.get_identifier()?)
        } else {
            Identifier::new(self.get_identifier()?.to_ascii_lowercase())
        };
        Ok(CanonicalFqn::new(&database, &schema, &identifier))
    }
}

impl BaseRelation for DuckdbRelation {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create_from(&self, _: &State, _: &[Value]) -> Result<Value, MinijinjaError> {
        unimplemented!("DuckDb relation creation from Jinja values")
    }

    fn database(&self) -> Value {
        Value::from(self.path.database.clone())
    }

    fn schema(&self) -> Value {
        Value::from(self.path.schema.clone())
    }

    fn identifier(&self) -> Value {
        Value::from(self.path.identifier.clone())
    }

    fn relation_type(&self) -> Option<RelationType> {
        self.relation_type
    }

    fn as_value(&self) -> Value {
        RelationObject::new(Arc::new(self.clone())).into_value()
    }

    fn adapter_type(&self) -> Option<String> {
        Some("duckdb".to_string())
    }

    fn include_inner(&self, include_policy: Policy) -> Result<Value, MinijinjaError> {
        let relation = DuckdbRelation::try_new_with_policy(
            self.path.clone(),
            self.relation_type,
            include_policy,
            self.quote_policy,
        )?;
        Ok(relation.as_value())
    }

    fn relation_max_name_length(&self, args: &[Value]) -> Result<Value, MinijinjaError> {
        let args = ArgParser::new(args, None);
        check_num_args(current_function_name!(), &args, 0, 0)?;
        // Return a very large number or None? Postgres returns MAX_CHARACTERS_IN_IDENTIFIER
        // Let's return 4096 just to be safe and compatible
        Ok(Value::from(4096))
    }

    fn normalize_component(&self, component: &str) -> String {
        component.to_lowercase()
    }

    fn create_relation(
        &self,
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: Policy,
    ) -> Result<Arc<dyn BaseRelation>, MinijinjaError> {
        Ok(Arc::new(DuckdbRelation::try_new(
            database,
            schema,
            identifier,
            relation_type,
            custom_quoting,
        )?))
    }

    fn information_schema_inner(
        &self,
        database: Option<String>,
        view_name: Option<&str>,
    ) -> Result<Value, MinijinjaError> {
        let result = InformationSchema::try_from_relation(database, view_name)?;
        Ok(RelationObject::new(Arc::new(result)).into_value())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_schemas::{dbt_types::RelationType, schemas::relations::DEFAULT_RESOLVED_QUOTING};

    #[test]
    fn test_try_new_via_static_base_relation() {
        let relation = DuckdbRelationType(DEFAULT_RESOLVED_QUOTING)
            .try_new(
                Some("db".to_string()),
                Some("main".to_string()),
                Some("my_table".to_string()),
                Some(RelationType::Table),
                Some(DEFAULT_RESOLVED_QUOTING),
            )
            .unwrap();

        let relation = relation.downcast_object::<RelationObject>().unwrap();
        assert_eq!(
            relation.inner().render_self().unwrap().as_str().unwrap(),
            "\"db\".\"main\".\"my_table\""
        );
        assert_eq!(relation.relation_type().unwrap(), RelationType::Table);
    }

    #[test]
    fn test_duckdb_relation_adapter_type() {
        let relation_type = DuckdbRelationType(DEFAULT_RESOLVED_QUOTING);
        assert_eq!(relation_type.get_adapter_type(), "duckdb");
    }

    #[test]
    fn test_duckdb_relation_quote_character() {
        let relation = DuckdbRelation::try_new(
            Some("db".to_string()),
            Some("schema".to_string()),
            Some("table".to_string()),
            Some(RelationType::Table),
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        assert_eq!(relation.quote_character(), '"');
    }

    #[test]
    fn test_normalize_component_lowercase() {
        let relation = DuckdbRelation::try_new(
            Some("DB".to_string()),
            Some("SCHEMA".to_string()),
            Some("TABLE".to_string()),
            Some(RelationType::Table),
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        assert_eq!(relation.normalize_component("UPPERCASE"), "uppercase");
        assert_eq!(relation.normalize_component("MixedCase"), "mixedcase");
        assert_eq!(relation.normalize_component("lowercase"), "lowercase");
    }

    #[test]
    fn test_relation_max_name_length() {
        let relation = DuckdbRelation::try_new(
            Some("db".to_string()),
            Some("schema".to_string()),
            Some("table".to_string()),
            Some(RelationType::Table),
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        let result = relation.relation_max_name_length(&[]).unwrap();
        // DuckDB has a generous limit of 4096
        assert_eq!(result.as_i64().unwrap(), 4096);
    }

    #[test]
    fn test_duckdb_relation_view_type() {
        let relation = DuckdbRelation::try_new(
            Some("db".to_string()),
            Some("schema".to_string()),
            Some("my_view".to_string()),
            Some(RelationType::View),
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        assert_eq!(relation.relation_type(), Some(RelationType::View));
    }

    #[test]
    fn test_duckdb_relation_include_policy() {
        let relation = DuckdbRelation::try_new_with_policy(
            RelationPath {
                database: Some("db".to_string()),
                schema: Some("schema".to_string()),
                identifier: Some("table".to_string()),
            },
            Some(RelationType::Table),
            Policy {
                database: false,
                schema: true,
                identifier: true,
            },
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        assert!(!relation.include_policy().database);
        assert!(relation.include_policy().schema);
        assert!(relation.include_policy().identifier);
    }

    #[test]
    fn test_duckdb_relation_getters() {
        let relation = DuckdbRelation::try_new(
            Some("my_database".to_string()),
            Some("my_schema".to_string()),
            Some("my_table".to_string()),
            Some(RelationType::Table),
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        assert_eq!(relation.get_database().unwrap(), "my_database");
        assert_eq!(relation.get_schema().unwrap(), "my_schema");
        assert_eq!(relation.get_identifier().unwrap(), "my_table");
    }

    // ==================== Additional Coverage Tests ====================

    #[test]
    fn test_get_canonical_fqn() {
        let relation = DuckdbRelation::try_new(
            Some("MyDB".to_string()),
            Some("MySchema".to_string()),
            Some("MyTable".to_string()),
            Some(RelationType::Table),
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        let fqn = relation.get_canonical_fqn().unwrap();
        // With default quoting (all true), identifiers preserve case
        assert_eq!(fqn.to_string(), "MyDB.MySchema.MyTable");
    }

    #[test]
    fn test_get_canonical_fqn_lowercase_when_not_quoted() {
        let relation = DuckdbRelation::try_new_with_policy(
            RelationPath {
                database: Some("MyDB".to_string()),
                schema: Some("MySchema".to_string()),
                identifier: Some("MyTable".to_string()),
            },
            Some(RelationType::Table),
            Policy::enabled(),
            Policy {
                database: false,
                schema: false,
                identifier: false,
            },
        )
        .unwrap();

        let fqn = relation.get_canonical_fqn().unwrap();
        // Without quoting, identifiers are lowercased
        assert_eq!(fqn.to_string(), "mydb.myschema.mytable");
    }

    #[test]
    fn test_as_any() {
        let relation = DuckdbRelation::try_new(
            Some("db".to_string()),
            Some("schema".to_string()),
            Some("table".to_string()),
            Some(RelationType::Table),
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        let any_ref = relation.as_any();
        assert!(any_ref.downcast_ref::<DuckdbRelation>().is_some());
    }

    #[test]
    fn test_as_value() {
        let relation = DuckdbRelation::try_new(
            Some("db".to_string()),
            Some("schema".to_string()),
            Some("table".to_string()),
            Some(RelationType::Table),
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        let value = relation.as_value();
        assert!(!value.is_undefined());
    }

    #[test]
    fn test_adapter_type_method() {
        let relation = DuckdbRelation::try_new(
            Some("db".to_string()),
            Some("schema".to_string()),
            Some("table".to_string()),
            Some(RelationType::Table),
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        assert_eq!(relation.adapter_type(), Some("duckdb".to_string()));
    }

    #[test]
    fn test_create_relation() {
        let relation = DuckdbRelation::try_new(
            Some("db".to_string()),
            Some("schema".to_string()),
            Some("table".to_string()),
            Some(RelationType::Table),
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        let new_relation = relation
            .create_relation(
                Some("new_db".to_string()),
                Some("new_schema".to_string()),
                Some("new_table".to_string()),
                Some(RelationType::View),
                DEFAULT_RESOLVED_QUOTING,
            )
            .unwrap();

        assert_eq!(new_relation.relation_type(), Some(RelationType::View));
    }

    #[test]
    fn test_information_schema_inner() {
        let relation = DuckdbRelation::try_new(
            Some("db".to_string()),
            Some("schema".to_string()),
            Some("table".to_string()),
            Some(RelationType::Table),
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        let result = relation.information_schema_inner(Some("db".to_string()), Some("tables"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_database_value() {
        let relation = DuckdbRelation::try_new(
            Some("test_db".to_string()),
            Some("schema".to_string()),
            Some("table".to_string()),
            Some(RelationType::Table),
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        let db_value = relation.database();
        assert_eq!(db_value.as_str(), Some("test_db"));
    }

    #[test]
    fn test_schema_value() {
        let relation = DuckdbRelation::try_new(
            Some("db".to_string()),
            Some("test_schema".to_string()),
            Some("table".to_string()),
            Some(RelationType::Table),
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        let schema_value = relation.schema();
        assert_eq!(schema_value.as_str(), Some("test_schema"));
    }

    #[test]
    fn test_identifier_value() {
        let relation = DuckdbRelation::try_new(
            Some("db".to_string()),
            Some("schema".to_string()),
            Some("test_table".to_string()),
            Some(RelationType::Table),
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        let id_value = relation.identifier();
        assert_eq!(id_value.as_str(), Some("test_table"));
    }

    #[test]
    fn test_get_database_missing() {
        let relation = DuckdbRelation::try_new_with_policy(
            RelationPath {
                database: None,
                schema: Some("schema".to_string()),
                identifier: Some("table".to_string()),
            },
            Some(RelationType::Table),
            Policy::enabled(),
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        let result = relation.get_database();
        assert!(result.is_err());
    }

    #[test]
    fn test_get_schema_missing() {
        let relation = DuckdbRelation::try_new_with_policy(
            RelationPath {
                database: Some("db".to_string()),
                schema: None,
                identifier: Some("table".to_string()),
            },
            Some(RelationType::Table),
            Policy::enabled(),
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        let result = relation.get_schema();
        assert!(result.is_err());
    }

    #[test]
    fn test_get_identifier_missing() {
        let relation = DuckdbRelation::try_new_with_policy(
            RelationPath {
                database: Some("db".to_string()),
                schema: Some("schema".to_string()),
                identifier: None,
            },
            Some(RelationType::Table),
            Policy::enabled(),
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        let result = relation.get_identifier();
        assert!(result.is_err());
    }

    #[test]
    fn test_quote_policy() {
        let custom_quoting = Policy {
            database: true,
            schema: false,
            identifier: true,
        };
        let relation = DuckdbRelation::try_new_with_policy(
            RelationPath {
                database: Some("db".to_string()),
                schema: Some("schema".to_string()),
                identifier: Some("table".to_string()),
            },
            Some(RelationType::Table),
            Policy::enabled(),
            custom_quoting,
        )
        .unwrap();

        assert!(relation.quote_policy().database);
        assert!(!relation.quote_policy().schema);
        assert!(relation.quote_policy().identifier);
    }
}
