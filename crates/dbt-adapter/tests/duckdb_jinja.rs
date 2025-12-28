//! DuckDB Jinja adapter test - validates adapter integration with Jinja templates
//!
//! This test is env-gated: set DUCKDB_DRIVER_PATH or DUCKDB_DRIVER_NAME to run.
//! Example: export DUCKDB_DRIVER_PATH=/opt/homebrew/lib/libduckdb.dylib
//!
//! Based on work by Josh Wills in jwills/dbt-fusion.
//!
//! Tests that the adapter can execute queries through the Jinja environment,
//! which is how dbt macros interact with the database.

use std::collections::HashMap;
use std::sync::Arc;

use dbt_adapter::cache::RelationCache;
use dbt_adapter::config::AdapterConfig;
use dbt_adapter::query_comment::QueryCommentConfig;
use dbt_adapter::sql_types::NaiveTypeOpsImpl;
use dbt_adapter::stmt_splitter::NaiveStmtSplitter;
use dbt_adapter::{AdapterEngine, BaseAdapter, BridgeAdapter, ConcreteAdapterForTesting};

use dbt_auth::auth_for_backend;
use dbt_common::adapter::AdapterType;
use dbt_common::cancellation::never_cancels;
use dbt_schemas::schemas::relations::DEFAULT_RESOLVED_QUOTING;
use dbt_serde_yaml::Mapping;
use dbt_xdbc::Backend;

fn duckdb_driver_available() -> bool {
    std::env::var("DUCKDB_DRIVER_PATH").is_ok() || std::env::var("DUCKDB_DRIVER_NAME").is_ok()
}

fn create_duckdb_engine() -> Arc<AdapterEngine> {
    let config = Mapping::from_iter([("database".into(), ":memory:".into())]);
    let auth = auth_for_backend(Backend::DuckDB);
    AdapterEngine::new(
        AdapterType::DuckDb,
        auth.into(),
        AdapterConfig::new(config),
        DEFAULT_RESOLVED_QUOTING,
        Arc::new(NaiveStmtSplitter),
        None,
        QueryCommentConfig::from_query_comment(None, AdapterType::DuckDb, false),
        Box::new(NaiveTypeOpsImpl::new(AdapterType::DuckDb)),
        never_cancels(),
    )
}

#[test]
fn duckdb_jinja_adapter_execute() {
    if !duckdb_driver_available() {
        eprintln!("Skipping duckdb_jinja_adapter_execute: DUCKDB_DRIVER_PATH/NAME not set");
        return;
    }

    // Build engine + typed adapter + bridge
    let engine = create_duckdb_engine();
    let typed = Arc::new(ConcreteAdapterForTesting::new(engine));
    let cache = Arc::new(RelationCache::default());
    let bridge = Arc::new(BridgeAdapter::new(typed, None, cache));

    // Build a minimal minijinja env with adapter global
    let mut env = minijinja::Environment::new();
    env.add_global("adapter", bridge.as_value());
    env.add_global("dialect", minijinja::Value::from("duckdb"));

    // Execute simple query via Jinja adapter call
    let tpl = "{% set _res = adapter.execute('select 1 as x', fetch=true) %}ok";
    let rendered = env
        .render_named_str(
            "test.sql",
            tpl,
            HashMap::<String, minijinja::Value>::new(),
            &[],
        )
        .expect("render ok");
    assert_eq!(rendered, "ok");
}

#[test]
fn duckdb_jinja_adapter_quote() {
    if !duckdb_driver_available() {
        eprintln!("Skipping duckdb_jinja_adapter_quote: DUCKDB_DRIVER_PATH/NAME not set");
        return;
    }

    let engine = create_duckdb_engine();
    let typed = Arc::new(ConcreteAdapterForTesting::new(engine));
    let cache = Arc::new(RelationCache::default());
    let bridge = Arc::new(BridgeAdapter::new(typed, None, cache));

    let mut env = minijinja::Environment::new();
    env.add_global("adapter", bridge.as_value());

    // Test quoting function
    let tpl = "{{ adapter.quote('my_table') }}";
    let rendered = env
        .render_named_str(
            "test.sql",
            tpl,
            HashMap::<String, minijinja::Value>::new(),
            &[],
        )
        .expect("render ok");

    // DuckDB uses double-quote quoting like Postgres
    assert_eq!(rendered, "\"my_table\"");
}
