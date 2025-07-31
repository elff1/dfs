use std::{env, path::PathBuf};

use clap::command;
use clap_derive::{Parser, Subcommand};
use homedir::my_home;

const DEFAULT_BASE_DIR: &str = ".dfs";

fn default_base_path() -> PathBuf {
    let home_path = my_home();
    if let Ok(Some(mut home_path)) = home_path {
        home_path.push(DEFAULT_BASE_DIR);
        return home_path;
    }

    let mut path = env::current_dir().expect("Can not get current directory");
    path.push(DEFAULT_BASE_DIR);

    path
}

#[derive(Subcommand)]
pub enum Commands {
    Start,
}

#[derive(Parser)]
#[command(version, about)]
pub struct Cli {
    #[arg(short, long, env = "DFS_BASE_PATH", default_value = default_base_path().into_os_string())]
    pub base_path: PathBuf,

    #[arg(short, long, env = "DFS_GRPC_PORT", default_value_t = 29999)]
    pub grpc_port: u16,

    #[command(subcommand)]
    pub commands: Commands,
}
