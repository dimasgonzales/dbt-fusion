//! DuckDB macro datediff test - validates the datediff macro renders correctly
//!
//! This test is env-gated: set DUCKDB_DRIVER_PATH or DUCKDB_DRIVER_NAME to run.
//! Example: export DUCKDB_DRIVER_PATH=/opt/homebrew/lib/libduckdb.dylib
//!
//! Based on work by Josh Wills in jwills/dbt-fusion.
//!
//! Tests that the duckdb__datediff macro in
//! crates/dbt-loader/src/dbt_macro_assets/dbt-duckdb/macros/utils/datediff.sql
//! renders correctly to DuckDB's date_diff SQL function.

use std::fs;
use std::path::PathBuf;

fn duckdb_driver_available() -> bool {
    std::env::var("DUCKDB_DRIVER_PATH").is_ok() || std::env::var("DUCKDB_DRIVER_NAME").is_ok()
}

#[test]
fn duckdb_macro_datediff_file_exists() {
    // Verify the macro file exists
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf();
    let datediff_path =
        repo_root.join("dbt-loader/src/dbt_macro_assets/dbt-duckdb/macros/utils/datediff.sql");

    assert!(
        datediff_path.exists(),
        "datediff macro should exist at {}",
        datediff_path.display()
    );

    // Read and verify content
    let content = fs::read_to_string(&datediff_path).expect("read datediff.sql");
    assert!(
        content.contains("duckdb__datediff"),
        "macro should define duckdb__datediff"
    );
    assert!(
        content.contains("date_diff"),
        "macro should use DuckDB's date_diff function"
    );
}

#[test]
fn duckdb_macro_dateadd_file_exists() {
    // Verify the macro file exists
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf();
    let dateadd_path =
        repo_root.join("dbt-loader/src/dbt_macro_assets/dbt-duckdb/macros/utils/dateadd.sql");

    assert!(
        dateadd_path.exists(),
        "dateadd macro should exist at {}",
        dateadd_path.display()
    );

    // Read and verify content
    let content = fs::read_to_string(&dateadd_path).expect("read dateadd.sql");
    assert!(
        content.contains("duckdb__dateadd"),
        "macro should define duckdb__dateadd"
    );
    assert!(
        content.contains("date_add"),
        "macro should use DuckDB's date_add function"
    );
}

#[test]
fn duckdb_macro_adapters_contains_create_table_as() {
    // Verify the adapters.sql macro file contains create_table_as
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf();
    let adapters_path =
        repo_root.join("dbt-loader/src/dbt_macro_assets/dbt-duckdb/macros/adapters.sql");

    assert!(
        adapters_path.exists(),
        "adapters.sql should exist at {}",
        adapters_path.display()
    );

    // Read and verify content
    let content = fs::read_to_string(&adapters_path).expect("read adapters.sql");
    assert!(
        content.contains("duckdb__create_table_as"),
        "adapters.sql should define duckdb__create_table_as"
    );
    assert!(
        content.contains("duckdb__create_view_as"),
        "adapters.sql should define duckdb__create_view_as"
    );
    assert!(
        content.contains("duckdb__list_relations_without_caching"),
        "adapters.sql should define duckdb__list_relations_without_caching"
    );
}

#[test]
fn duckdb_macro_seed_file_exists() {
    // Verify the seed macro file exists
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf();
    let seed_path = repo_root.join("dbt-loader/src/dbt_macro_assets/dbt-duckdb/macros/seed.sql");

    assert!(
        seed_path.exists(),
        "seed macro should exist at {}",
        seed_path.display()
    );

    // Read and verify content
    let content = fs::read_to_string(&seed_path).expect("read seed.sql");
    assert!(
        content.contains("duckdb__load_csv_rows"),
        "seed.sql should define duckdb__load_csv_rows"
    );
    assert!(
        content.contains("COPY"),
        "seed.sql should use DuckDB's COPY command for fast loading"
    );
}

#[test]
fn duckdb_macro_datediff_renders_day() {
    if !duckdb_driver_available() {
        eprintln!("Skipping duckdb_macro_datediff_renders_day: DUCKDB_DRIVER_PATH/NAME not set");
        return;
    }

    // Verify the macro content renders correctly using minijinja
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf();
    let datediff_path =
        repo_root.join("dbt-loader/src/dbt_macro_assets/dbt-duckdb/macros/utils/datediff.sql");

    let content = fs::read_to_string(&datediff_path).expect("read datediff.sql");

    // Create a test template that includes and calls the macro
    let test_template = format!(
        "{}\n{{{{ duckdb__datediff('2020-01-01', '2020-01-02', 'day') }}}}",
        content
    );

    let mut env = minijinja::Environment::new();
    env.add_template_owned("datediff_test", test_template, None)
        .expect("add template");

    let tmpl = env.get_template("datediff_test").expect("get template");
    let result = tmpl.render((), &[]).expect("render");

    // The macro should produce a date_diff call for 'day' datepart
    assert!(
        result.contains("date_diff('day'"),
        "rendered macro should contain date_diff for day: got {}",
        result
    );
}
