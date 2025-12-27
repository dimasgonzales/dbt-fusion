use crate::dbt_sa_clap::{Cli, Commands, ProjectTemplate};
use dbt_common::cancellation::CancellationToken;
use dbt_common::create_root_info_span;
use dbt_common::io_utils::checkpoint_maybe_exit;
use dbt_common::tracing::emit::{
    emit_error_log_from_fs_error, emit_info_log_message, emit_info_progress_message,
};
use dbt_common::tracing::invocation::create_invocation_attributes;
use dbt_common::tracing::metrics::get_exit_code_from_error_counter;
use dbt_init::init;
use dbt_jinja_utils::invocation_args::InvocationArgs;
use dbt_jinja_utils::listener::DefaultJinjaTypeCheckEventListenerFactory;
use dbt_loader::clean::execute_clean_command;
use dbt_schemas::man::execute_man_command;
use dbt_telemetry::ProgressMessage;

use dbt_common::io_args::{EvalArgs, EvalArgsBuilder};
use dbt_common::{
    ErrorCode, FsResult,
    constants::{DBT_MANIFEST_JSON, INSTALLING, VALIDATING},
    fs_err,
    io_args::{Phases, ShowOptions, SystemArgs},
    logging::init_logger,
    pretty_string::GREEN,
    stdfs,
    tracing::{emit::emit_info_event, span_info::record_span_status},
};
use dbt_telemetry::ShowResult;

use dbt_schemas::schemas::Nodes;
use dbt_schemas::state::{
    GetColumnsInRelationCalls, GetRelationCalls, Macros, PatternedDanglingSources,
};
#[allow(unused_imports)]
use git_version::git_version;

use dbt_schemas::schemas::manifest::build_manifest;
use tracing::Instrument;

use std::sync::Arc;

use dbt_loader::{args::LoadArgs, load};
use dbt_parser::{args::ResolveArgs, resolver::resolve};

use dbt_adapter::adapter_engine::AdapterEngine;
use dbt_adapter::query_comment::QueryCommentConfig;
use dbt_adapter::sql_types::NaiveTypeOpsImpl;
use dbt_adapter::stmt_splitter::NaiveStmtSplitter;
use dbt_auth::{AdapterConfig, auth_for_backend};
use dbt_common::io_args::FsCommand;
use dbt_dag::deps_mgmt::{ensure_all_nodes_defined, topological_sort};
use dbt_schemas::schemas::common::DbtMaterialization;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_xdbc::{Backend, QueryCtx};
use serde_json::to_string_pretty;
use std::collections::{BTreeMap, BTreeSet};

// ------------------------------------------------------------------------------------------------

pub async fn execute_fs(
    system_arg: SystemArgs,
    cli: Cli,
    token: CancellationToken,
) -> FsResult<i32> {
    // Resolve EvalArgs from SystemArgs and Cli. This will create out folders,
    // for commands that need it and canonicalize the paths. May error on invalid paths.
    let eval_arg = cli.to_eval_args(system_arg)?;

    init_logger((&eval_arg.io).into()).expect("Failed to initialize logger");

    // Create the Invocation span as a new root
    let invocation_span = create_root_info_span(create_invocation_attributes("dbt-sa", &eval_arg));

    let result = do_execute_fs(&eval_arg, cli, token)
        .instrument(invocation_span.clone())
        .await;

    // Record span run result
    match result {
        Ok(0) => record_span_status(&invocation_span, None),
        Ok(_) => record_span_status(&invocation_span, Some("Executed with errors")),
        Err(ref e) => record_span_status(&invocation_span, Some(format!("Error: {e}").as_str())),
    };

    result
}

#[allow(clippy::cognitive_complexity)]
async fn do_execute_fs(eval_arg: &EvalArgs, cli: Cli, token: CancellationToken) -> FsResult<i32> {
    if let Commands::Man(_) = &cli.command {
        return match execute_man_command(eval_arg).await {
            Ok(code) => Ok(code),
            Err(e) => {
                emit_error_log_from_fs_error(&e, eval_arg.io.status_reporter.as_ref());

                Ok(1)
            }
        };
    } else if let Commands::Init(init_args) = &cli.command {
        // Handle init command
        use dbt_init::init::run_init_workflow;

        emit_info_progress_message(
            ProgressMessage::new_from_action_and_target(
                INSTALLING.to_string(),
                "dbt project and profile setup".to_string(),
            ),
            eval_arg.io.status_reporter.as_ref(),
        );

        let project_name = if init_args.project_name == "jaffle_shop" {
            None // Use default
        } else {
            Some(init_args.project_name.clone())
        };

        let project_template = match init_args.sample {
            ProjectTemplate::JaffleShop => init::assets::ProjectTemplateAsset::JaffleShop,
            ProjectTemplate::MomsFlowerShop => init::assets::ProjectTemplateAsset::MomsFlowerShop,
        };

        match run_init_workflow(
            project_name,
            init_args.skip_profile_setup,
            init_args.common_args.profile.clone(), // Get profile from common args
            &project_template,
        )
        .await
        {
            Ok(()) => {
                // If profile setup was not skipped, run debug to validate credentials
                if init_args.skip_profile_setup {
                    return Ok(0);
                }

                emit_info_log_message(format!(
                    "{} profile inputs, adapters, and connection\n", // Add empty line for spacing
                    GREEN.apply_to(VALIDATING)
                ));
            }
            Err(e) => {
                emit_error_log_from_fs_error(&e, eval_arg.io.status_reporter.as_ref());

                return Ok(1);
            }
        }
    }

    // Handle project specific commands
    match execute_setup_and_all_phases(eval_arg, cli, &token).await {
        Ok(code) => Ok(code),
        Err(e) => {
            emit_error_log_from_fs_error(&e, eval_arg.io.status_reporter.as_ref());

            Ok(1)
        }
    }
}

#[allow(clippy::cognitive_complexity)]
async fn execute_setup_and_all_phases(
    eval_arg: &EvalArgs,
    cli: Cli,
    token: &CancellationToken,
) -> FsResult<i32> {
    // Header ..
    // current_exe errors when running in dbt-cloud
    // https://github.com/rust-lang/rust/issues/46090
    #[cfg(debug_assertions)]
    {
        use chrono::{DateTime, Local};
        use dbt_common::constants::DBT_SA_CLI;
        use std::env;
        let exe_path = env::current_exe()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get current exe path: {}", e))?;
        let modified_time = stdfs::last_modified(&exe_path)?;

        // Convert SystemTime to DateTime<Local>
        let datetime: DateTime<Local> = DateTime::from(modified_time);
        let formatted_time = datetime.format("%Y-%m-%d %H:%M:%S").to_string();
        let build_time = if eval_arg.from_main {
            let git_hash = git_version!(fallback = "unknown");
            format!(
                "{} ({} {})",
                env!("CARGO_PKG_VERSION"),
                git_hash,
                formatted_time
            )
        } else {
            "".to_string()
        };
        emit_info_progress_message(
            ProgressMessage::new_from_action_and_target(DBT_SA_CLI.to_string(), build_time),
            eval_arg.io.status_reporter.as_ref(),
        );
    }

    // Check if the command is `Clean`
    if let Commands::Clean(ref clean_args) = cli.command {
        match execute_clean_command(eval_arg, &clean_args.files, token).await {
            Ok(code) => Ok(code),
            Err(e) => {
                emit_error_log_from_fs_error(&e, eval_arg.io.status_reporter.as_ref());

                Ok(1)
            }
        }
    } else {
        // Execute all steps of all other commands, if any throws an error we stop
        match execute_all_phases(eval_arg, &cli, token).await {
            Ok(code) => Ok(code),
            Err(e) => {
                emit_error_log_from_fs_error(&e, eval_arg.io.status_reporter.as_ref());

                Ok(1)
            }
        }
    }
}

#[allow(clippy::cognitive_complexity)]
async fn execute_all_phases(
    arg: &EvalArgs,
    _cli: &Cli,
    token: &CancellationToken,
) -> FsResult<i32> {
    // Loads all .yml files + collects all included files
    let load_args = LoadArgs::from_eval_args(arg);
    let invocation_args = InvocationArgs::from_eval_args(arg);
    let (dbt_state, _dbt_cloud_config) = load(&load_args, &invocation_args, token).await?;
    let dbt_state = Arc::new(dbt_state);

    let arg = EvalArgsBuilder::from_eval_args(arg)
        .with_additional(
            dbt_state.dbt_profile.target.to_string(),
            dbt_state.dbt_profile.threads,
            dbt_state.dbt_profile.db_config.adapter_type_if_supported(),
        )
        .build();

    if arg.io.should_show(ShowOptions::InputFiles) {
        emit_info_event(
            ShowResult::new_text(dbt_state.to_string(), "input_files", "Input files"),
            None,
        );
    }

    // This also exits the init command b/c init `to_eval_args` sets the phase to debug
    if let Some(exit_code) = checkpoint_maybe_exit(&arg, Phases::Debug) {
        return Ok(exit_code);
    }
    if let Some(exit_code) = checkpoint_maybe_exit(&arg, Phases::Deps) {
        return Ok(exit_code);
    }

    // Parses (dbt parses) all .sql files with execute == false
    let resolve_args = ResolveArgs::try_from_eval_args(&arg)?;
    let invocation_args = InvocationArgs::from_eval_args(&arg);
    let (resolved_state, _jinja_env) = resolve(
        &resolve_args,
        &invocation_args,
        dbt_state.clone(),
        Macros::default(),
        Nodes::default(),
        GetRelationCalls::default(),
        GetColumnsInRelationCalls::default(),
        PatternedDanglingSources::default(),
        token,
        Arc::new(DefaultJinjaTypeCheckEventListenerFactory::default()), // TODO: use option<>
    )
    .await?;

    let dbt_manifest = build_manifest(&arg.io.invocation_id.to_string(), &resolved_state);

    if arg.write_json {
        let dbt_manifest_path = arg.io.out_dir.join(DBT_MANIFEST_JSON);
        stdfs::create_dir_all(dbt_manifest_path.parent().unwrap())?;
        stdfs::write(dbt_manifest_path, serde_json::to_string(&dbt_manifest)?)?;
    }

    if arg.io.should_show(ShowOptions::Manifest) {
        emit_info_event(
            ShowResult::new_text(to_string_pretty(&dbt_manifest)?, "manifest", "Manifest"),
            None,
        );
    }

    if matches!(arg.command, FsCommand::Run | FsCommand::Build) {
        emit_info_progress_message(
            ProgressMessage::new_from_action_and_target(
                "Running".to_string(),
                "dbt project".to_string(),
            ),
            arg.io.status_reporter.as_ref(),
        );

        let adapter_type = dbt_state
            .dbt_profile
            .db_config
            .adapter_type_if_supported()
            .ok_or_else(|| fs_err!(ErrorCode::Generic, "Unsupported adapter type"))?;

        let backend = match dbt_state.dbt_profile.db_config.adapter_type() {
            "duckdb" => Backend::DuckDB,
            _ => {
                return Err(fs_err!(
                    ErrorCode::Generic,
                    "Only DuckDB supported currently use_warehouse logic not used"
                ));
            }
        };

        let auth = auth_for_backend(backend);
        let config_mapping = dbt_state
            .dbt_profile
            .db_config
            .to_mapping()
            .map_err(|e| fs_err!(ErrorCode::Generic, "Config error: {}", e))?;
        let config = AdapterConfig::new(config_mapping);

        let engine = AdapterEngine::new(
            adapter_type,
            Arc::from(auth),
            config,
            ResolvedQuoting::default(),  // TODO: use actual quoting
            Arc::new(NaiveStmtSplitter), // TODO: use actual splitter
            None,
            QueryCommentConfig::from_query_comment(None, adapter_type, false),
            Box::new(NaiveTypeOpsImpl::new(adapter_type)),
            token.clone(),
        );

        // Build dependency graph
        let mut deps: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

        // Models
        for (unique_id, node) in &resolved_state.nodes.models {
            use dbt_schemas::schemas::InternalDbtNode;
            let mut node_deps = BTreeSet::new();
            for (dep, _) in &node.base().depends_on.nodes_with_ref_location {
                node_deps.insert(dep.clone());
            }
            deps.insert(unique_id.clone(), node_deps);
        }
        // Seeds
        for (unique_id, node) in &resolved_state.nodes.seeds {
            use dbt_schemas::schemas::InternalDbtNode;
            let mut node_deps = BTreeSet::new();
            for (dep, _) in &node.base().depends_on.nodes_with_ref_location {
                node_deps.insert(dep.clone());
            }
            deps.insert(unique_id.clone(), node_deps);
        }
        // Snapshots
        for (unique_id, node) in &resolved_state.nodes.snapshots {
            use dbt_schemas::schemas::InternalDbtNode;
            let mut node_deps = BTreeSet::new();
            for (dep, _) in &node.base().depends_on.nodes_with_ref_location {
                node_deps.insert(dep.clone());
            }
            deps.insert(unique_id.clone(), node_deps);
        }

        let deps = ensure_all_nodes_defined(&deps);
        let sorted_nodes = topological_sort(&deps);

        for unique_id in sorted_nodes {
            // Get SQL from render results
            // Get SQL from render results
            if let Some((sql, _)) = resolved_state
                .render_results
                .rendering_results
                .get(&unique_id)
            {
                use dbt_schemas::schemas::nodes::InternalDbtNodeAttributes;

                let node: Option<&dyn InternalDbtNodeAttributes> =
                    if let Some(n) = resolved_state.nodes.models.get(&unique_id) {
                        Some(n.as_ref())
                    } else if let Some(n) = resolved_state.nodes.seeds.get(&unique_id) {
                        Some(n.as_ref())
                    } else if let Some(n) = resolved_state.nodes.snapshots.get(&unique_id) {
                        Some(n.as_ref())
                    } else {
                        None
                    };

                let sql_to_run = if let Some(node) = node {
                    let schema = node.schema();
                    let alias = node.alias();
                    let materialization = node.materialized();

                    match materialization {
                        DbtMaterialization::View => {
                            format!("CREATE OR REPLACE VIEW {}.{} AS {}", schema, alias, sql)
                        }
                        DbtMaterialization::Table
                        | DbtMaterialization::Incremental
                        | DbtMaterialization::Seed
                        | DbtMaterialization::Snapshot => {
                            format!("CREATE OR REPLACE TABLE {}.{} AS {}", schema, alias, sql)
                        }
                        DbtMaterialization::Ephemeral => {
                            println!("Skipping ephemeral node: {}", unique_id);
                            continue;
                        }
                        _ => {
                            println!(
                                "Warning: Unknown materialization {:?} for {}, executing raw SQL",
                                materialization, unique_id
                            );
                            sql.clone()
                        }
                    }
                } else {
                    sql.clone()
                };

                // Use a new connection for each node execution (simple serial execution)
                let mut conn = engine
                    .new_connection(None, Some(unique_id.clone()))
                    .map_err(|e| fs_err!(ErrorCode::Generic, "Connection error: {}", e))?;

                // Simple execution log
                emit_info_progress_message(
                    ProgressMessage::new_from_action_and_target(
                        "Exec".to_string(),
                        unique_id.clone(),
                    ),
                    arg.io.status_reporter.as_ref(),
                );

                let ctx = QueryCtx::new("dbt run")
                    .with_node_id(unique_id.clone())
                    .with_phase("run");

                // println!("Executing node: {}", unique_id);
                // println!("SQL: {}", sql_to_run);
                engine
                    .execute(None, &mut *conn, &ctx, &sql_to_run)
                    .map_err(|e| fs_err!(ErrorCode::Generic, "Execution error: {}", e))?;
            }
        }
    }

    Ok(get_exit_code_from_error_counter())
}
