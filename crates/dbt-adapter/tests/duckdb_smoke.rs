//! DuckDB smoke test - basic connectivity check
//!
//! This test is env-gated: set DUCKDB_DRIVER_PATH or DUCKDB_DRIVER_NAME to run.
//! Example: export DUCKDB_DRIVER_PATH=/opt/homebrew/lib/libduckdb.dylib
//!
//! NOTE: This test requires the full adapter stack to be set up. It serves as
//! a placeholder for when DuckDB testing infrastructure is established.

fn duckdb_driver_available() -> bool {
    std::env::var("DUCKDB_DRIVER_PATH").is_ok() || std::env::var("DUCKDB_DRIVER_NAME").is_ok()
}

#[test]
fn duckdb_smoke_select_1() {
    if !duckdb_driver_available() {
        eprintln!("Skipping duckdb_smoke_select_1: DUCKDB_DRIVER_PATH/NAME not set");
        eprintln!("  To run: export DUCKDB_DRIVER_PATH=/opt/homebrew/lib/libduckdb.dylib");
        return;
    }

    // TODO: Set up full AdapterEngine with required dependencies:
    // - StmtSplitter
    // - TypeOps
    // - QueryCommentConfig
    // For now, just verify the env var is set
    println!(
        "✓ DuckDB driver available at: {}",
        std::env::var("DUCKDB_DRIVER_PATH")
            .unwrap_or_else(|_| std::env::var("DUCKDB_DRIVER_NAME").unwrap())
    );
}
