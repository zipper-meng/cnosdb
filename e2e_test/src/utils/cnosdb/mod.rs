mod data_node;
mod meta_node;

use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use config::meta::Opt as MetaStoreConfig;
use config::tskv::Config as CnosdbConfig;
pub use data_node::*;
pub use meta_node::*;
use tokio::runtime::Runtime;

use crate::cluster_def::{CnosdbClusterDefinition, DataNodeDefinition, DeploymentMode};
use crate::utils::{execute_command, get_workspace_dir, kill_process};

pub type FnMutMetaStoreConfig = Box<dyn FnMut(&mut MetaStoreConfig)>;
pub type FnMutCnosdbConfig = Box<dyn FnMut(&mut CnosdbConfig)>;

pub fn cargo_build_cnosdb_meta(workspace_dir: impl AsRef<Path>) {
    let workspace_dir = workspace_dir.as_ref();
    println!("- Building 'meta' at '{}'", workspace_dir.display());
    let mut cargo_build = Command::new("cargo");
    #[rustfmt::skip]
    let build_args = if cfg!(feature = "debug") {
        vec!["build", "--package", "meta", "--bin", "cnosdb-meta"]
    } else {
        vec!["build", "--release", "--package", "meta", "--bin", "cnosdb-meta"]
    };
    cargo_build.current_dir(workspace_dir).args(build_args);
    execute_command(cargo_build).expect("Failed to build cnosdb-meta");
    println!("- Build 'meta' at '{}' completed", workspace_dir.display());
}

pub fn cargo_build_cnosdb_data(workspace_dir: impl AsRef<Path>) {
    let workspace_dir = workspace_dir.as_ref();
    println!("Building 'main' at '{}'", workspace_dir.display());
    let mut cargo_build = Command::new("cargo");
    #[rustfmt::skip]
    let build_args = if cfg!(feature = "debug") {
        vec!["build", "--package", "main", "--bin", "cnosdb"]
    } else {
        vec!["build", "--release", "--package", "main", "--bin", "cnosdb"]
    };
    cargo_build.current_dir(workspace_dir).args(build_args);
    execute_command(cargo_build).expect("Failed to build cnosdb");
    println!("Build 'main' at '{}' completed", workspace_dir.display());
}

/// Run CnosDB cluster.
///
/// - Meta server directory: $test_dir/meta
/// - Data server directory: $test_dir/data
///
/// # Arguments
/// - generate_meta_config: If true, regenerate meta node config files.
/// - generate_data_config: If true, regenerate data node config files.
pub fn run_cluster(
    test_dir: impl AsRef<Path>,
    runtime: Arc<Runtime>,
    cluster_def: &CnosdbClusterDefinition,
    generate_meta_config: bool,
    generate_data_config: bool,
) -> (Option<CnosdbMetaTestHelper>, Option<CnosdbDataTestHelper>) {
    run_cluster_with_customized_configs(
        test_dir,
        runtime,
        cluster_def,
        generate_meta_config,
        generate_data_config,
        vec![],
        vec![],
    )
}

/// Run CnosDB cluster with customized configs.
///
/// # Arguments
/// - runtime: If None and need meta nodes in cluster, build a new runtime.
/// - generate_meta_config: If true, regenerate meta node config files.
/// - generate_data_config: If true, regenerate data node config files.
/// - regenerate_update_meta_config: If generate_meta_config is true, and the `ith` optional closure is Some(Fn),
///   alter the default config of the `ith` meta node by the Fn. (Do not change service ports.)
/// - regenerate_update_meta_config: If generate_data_config is true, and the `ith` optional closure is Some(Fn),
///   alter the default config of the `ith` data node by the Fn. (Do not change service ports.)
pub fn run_cluster_with_customized_configs(
    test_dir: impl AsRef<Path>,
    runtime: Arc<Runtime>,
    cluster_def: &CnosdbClusterDefinition,
    generate_meta_config: bool,
    generate_data_config: bool,
    regenerate_update_meta_config: Vec<Option<FnMutMetaStoreConfig>>,
    regenerate_update_data_config: Vec<Option<FnMutCnosdbConfig>>,
) -> (Option<CnosdbMetaTestHelper>, Option<CnosdbDataTestHelper>) {
    let test_dir = test_dir.as_ref().to_path_buf();
    let workspace_dir = get_workspace_dir();
    if !cluster_def.meta_cluster_def.is_empty() {
        cargo_build_cnosdb_meta(&workspace_dir);
    }
    if !cluster_def.data_cluster_def.is_empty() {
        cargo_build_cnosdb_data(&workspace_dir);
    }

    let (mut meta_test_helper, mut data_test_helper) = (
        Option::<CnosdbMetaTestHelper>::None,
        Option::<CnosdbDataTestHelper>::None,
    );

    if !cluster_def.meta_cluster_def.is_empty() {
        // If need to run `cnosdb-meta`
        let meta_test_dir = test_dir.join("meta");
        let configs = write_meta_node_config_files(
            &test_dir,
            &cluster_def.meta_cluster_def,
            generate_meta_config,
            regenerate_update_meta_config,
        );
        let mut meta = CnosdbMetaTestHelper::new(
            runtime,
            &workspace_dir,
            meta_test_dir,
            cluster_def.meta_cluster_def.clone(),
            configs,
        );
        if cluster_def.meta_cluster_def.len() == 1 {
            meta.run_single_meta();
        } else {
            meta.run_cluster();
        }
        meta_test_helper = Some(meta);
    }

    thread::sleep(Duration::from_secs(1));

    if !cluster_def.data_cluster_def.is_empty() {
        // If need to run `cnosdb run`
        let data_test_dir = test_dir.join("data");
        let configs = write_data_node_config_files(
            &test_dir,
            &cluster_def.data_cluster_def,
            generate_data_config,
            regenerate_update_data_config,
        );
        let mut data = CnosdbDataTestHelper::new(
            workspace_dir,
            data_test_dir,
            cluster_def.data_cluster_def.clone(),
            configs,
            false,
        );
        data.run();
        data_test_helper = Some(data);
    }

    (meta_test_helper, data_test_helper)
}

/// Run CnosDB singleton.
///
/// - Data server directory: $test_dir
pub fn run_singleton(
    test_dir: impl AsRef<Path>,
    runtime: Arc<Runtime>,
    data_node_definition: &DataNodeDefinition,
    generate_data_config: bool,
) -> CnosdbDataTestHelper {
    let (_m, d) = run_cluster_with_customized_configs(
        test_dir,
        runtime,
        &CnosdbClusterDefinition {
            meta_cluster_def: vec![],
            data_cluster_def: vec![data_node_definition.clone()],
        },
        false,
        generate_data_config,
        vec![],
        vec![],
    );
    d.expect("msg")
}

/// Kill all 'cnosdb' and 'cnosdb-meta' process with signal 'KILL(9)'.
pub fn kill_all() {
    println!("Killing all test processes...");
    kill_process("cnosdb");
    kill_process("cnosdb-meta");
    println!("Killed all test processes.");
}

/// Copy TLS certificates:
///
/// - $workspace_dir/config/tls/server.crt to $dest_dir/server.crt
/// - $workspace_dir/config/tls/server.key to $dest_dir/server.key
pub fn copy_cnosdb_server_certificate(workspace_dir: impl AsRef<Path>, dest_dir: impl AsRef<Path>) {
    let src_dir = workspace_dir.as_ref().join("config").join("tls");
    let files_to_copy = [src_dir.join("server.crt"), src_dir.join("server.key")];
    let dest_dir = dest_dir.as_ref();
    let _ = std::fs::create_dir_all(dest_dir);
    for src_file in files_to_copy {
        if std::fs::metadata(&src_file).is_err() {
            panic!("certificate file '{}' not found", src_file.display());
        }
        let dst_file = dest_dir.join(src_file.file_name().unwrap());
        println!(
            "- coping certificate file: '{}' to '{}'",
            src_file.display(),
            dst_file.display(),
        );
        std::fs::copy(&src_file, &dst_file).unwrap();
        println!(
            "- copy certificate file completed: '{}' to '{}'",
            src_file.display(),
            dst_file.display(),
        );
    }
}
