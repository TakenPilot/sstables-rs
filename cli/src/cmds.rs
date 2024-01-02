use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
  /// Level of verbosity
  #[arg(short, long, action = clap::ArgAction::Count)]
  pub verbose: u8,

  #[command(subcommand)]
  pub command: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
  Append {
    /// The file to append to
    #[arg(value_name = "INPUT_PATHS")]
    input_paths: Vec<PathBuf>,

    /// The data to append
    #[arg(short, long, value_name = "KEY")]
    key: String,

    /// The data to append
    #[arg(short, long, value_name = "DATA")]
    data: String,
  },
  /// Exports the contents of the SSTable to another format.
  Export {
    /// The file to export
    #[arg(value_name = "INPUT_PATHS")]
    input_paths: Vec<PathBuf>,

    /// The format to export to
    #[arg(short, long, value_name = "FORMAT")]
    format: Option<String>,
  },
  /// Get a specific key's value from the SSTable, if it exists. Optionally, get
  /// the next N values after the key. Gets every match by default, but can be
  /// limited to the first match with `-n 1`.
  Get {
    /// The file to get the key from
    #[arg(value_name = "INPUT_PATHS")]
    input_paths: Vec<PathBuf>,

    /// The key to get
    #[arg(short, long, value_name = "KEY")]
    key: String,

    /// The number of values to get
    #[arg(short, long, value_name = "N")]
    n: Option<usize>,
  },
  /// Get the data from the index of the SSTable.
  Index {
    /// The file to index
    #[arg(value_name = "INPUT_PATHS")]
    input_paths: Vec<PathBuf>,
  },
  /// Get info about the SSTable.
  Info {
    /// The file to get info on
    #[arg(value_name = "INPUT_PATHS")]
    input_paths: Vec<PathBuf>,
  },
  Keys {
    /// The file to get keys from
    #[arg(value_name = "INPUT_PATHS")]
    input_paths: Vec<PathBuf>,
  },
  /// Sort and merge one or more SSTables.
  Merge {
    /// One or more files to sort and merge.
    #[arg(value_name = "INPUT_PATHS")]
    input_paths: Vec<PathBuf>,

    /// Optional output file. If unset, writes to stdout.
    #[arg(short, long, value_name = "OUTPUT_PATH")]
    output_path: Option<PathBuf>,
  },
  Validate {
    /// The file to validate.
    #[arg(value_name = "INPUT_PATHS")]
    input_paths: Vec<PathBuf>,
  },
  Values {
    /// The file to get values from.
    #[arg(value_name = "INPUT_PATHS")]
    input_paths: Vec<PathBuf>,
  },
}

pub fn get_cli() -> Cli {
  Cli::parse()
}
