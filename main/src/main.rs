mod signal;

use std::path::Path;
use std::sync::Arc;

use clap::{command, Args, Parser, Subcommand};
use config::Config;
use main::{DeploymentMode, VERSION};
use metrics::init_tskv_metrics_recorder;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use trace::{init_process_global_tracing, WorkerGuard};

static GLOBAL_MAIN_LOG_GUARD: Lazy<Arc<Mutex<Option<Vec<WorkerGuard>>>>> =
    Lazy::new(|| Arc::new(Mutex::new(None)));

/// cli examples is here
/// <https://github.com/clap-rs/clap/blob/v3.1.3/examples/git-derive.rs>
#[derive(Debug, Parser)]
#[command(name = "cnosdb", version = & VERSION[..])]
#[command(about = "CnosDB command line tools")]
#[command(long_about = r#"CnosDB and command line tools
Examples:
    # Run the CnosDB:
    cnosdb run
    # Check configuration file:
    cnosdb check server-config ./config/config.toml"#)]
struct Cli {
    #[command(subcommand)]
    subcmd: CliCommand,
}

#[derive(Debug, Subcommand)]
enum CliCommand {
    /// Run CnosDB server.
    Run(RunArgs),
    /// Print default configurations.
    Config,
    /// Check the configuration file in the given path.
    Check {
        #[command(subcommand)]
        subcmd: CheckCommand,
    },
}

#[derive(Debug, Args)]
struct RunArgs {
    /// Number of CPUs on the system, the default value is 4
    #[arg(short, long, global = true)]
    cpu: Option<usize>,

    /// Gigabytes(G) of memory on the system, the default value is 16
    #[arg(short, long, global = true)]
    memory: Option<usize>,

    /// Path to configuration file.
    #[arg(long, global = true)]
    config: Option<String>,

    /// The deployment mode of CnosDB,
    #[arg(short = 'M', long, global = true, value_enum)]
    deployment_mode: Option<DeploymentMode>,
}

#[derive(Debug, Subcommand)]
enum CheckCommand {
    /// Check server configurations.
    #[command(arg_required_else_help = false)]
    ServerConfig {
        /// Print warnings.
        #[arg(short, long)]
        show_warnings: bool,
        /// Path to configuration file.
        config: String,
    },
    // /// Check meta server configurations.
    // #[command(arg_required_else_help = false)]
    // MetaConfig {},
}

#[cfg(unix)]
#[global_allocator]
static A: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// To run cnosdb-cli:
///
/// ```bash
/// cargo run -- run
/// ```
fn main() {
    signal::install_crash_handler();
    let cli = Cli::parse();
    let run_args = match cli.subcmd {
        CliCommand::Run(run_args) => run_args,
        CliCommand::Config => {
            println!("{}", Config::default().to_string_pretty());
            return;
        }
        CliCommand::Check { subcmd } => match subcmd {
            CheckCommand::ServerConfig {
                config,
                show_warnings,
            } => {
                config::check_config(config, show_warnings);
                return;
            }
        },
    };

    let mut config = parse_config(run_args.config.as_ref());
    set_cli_args_to_config(&run_args, &mut config);

    init_process_global_tracing(
        &config.log.path,
        &config.log.level,
        "tsdb.log",
        config.log.tokio_trace.as_ref(),
        &GLOBAL_MAIN_LOG_GUARD,
    );
    init_tskv_metrics_recorder();

    let mut cnosdb = match main::Cnosdb::new(config) {
        Ok(c) => c,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::Other {
                if let Some(inner_e) = e.into_inner() {
                    trace::error!("CnosDB server initializing failed: {inner_e}");
                } else {
                    // Impossible
                    trace::error!("CnosDB server initializing failed: ");
                }
            } else {
                trace::error!("CnosDB server initializing failed: {e}");
            }
            std::process::exit(1);
        }
    };

    if let Err(e) = cnosdb.start() {
        trace::error!("CnosDB server starting failed: {e}");
        if e.kind() == std::io::ErrorKind::Other {
            if let Some(inner_e) = e.into_inner() {
                trace::error!("CnosDB server starting failed: {inner_e}");
            } else {
                // Impossible
                trace::error!("CnosDB server starting failed");
            }
        } else {
            trace::error!("CnosDB server starting failed: {e}");
        }
        std::process::exit(1);
    }

    signal::block_waiting_ctrl_c();
    drop(cnosdb);
    trace::error!("CnosDB server stopped.");
}

fn parse_config(config_path: Option<impl AsRef<Path>>) -> config::Config {
    let global_config = if let Some(p) = config_path {
        println!("----------\nStart with configuration:");
        config::get_config(p).unwrap()
    } else {
        println!("----------\nStart with default configuration:");
        config::Config::default()
    };
    println!("{}----------", global_config.to_string_pretty());

    global_config
}

/// When the command line and the configuration file specify a setting at the same time,
/// the command line has higher priority
fn set_cli_args_to_config(args: &RunArgs, config: &mut Config) {
    if let Some(mode) = args.deployment_mode {
        config.deployment.mode = mode.to_string();
    }

    if let Some(m) = args.memory {
        config.deployment.memory = m;
    }

    if let Some(c) = args.cpu {
        config.deployment.cpu = c;
    }
}
