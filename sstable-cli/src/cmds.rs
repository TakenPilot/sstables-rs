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
    #[arg(short, long, value_name = "FILE")]
    file: PathBuf,

    /// The data to append
    #[arg(short, long, value_name = "KEY")]
    key: String,

    /// The data to append
    #[arg(short, long, value_name = "DATA")]
    data: String,
  },
  Validate {
    /// The file to validate
    #[arg(short, long, value_name = "FILE")]
    file: PathBuf,
  },
  Info {
    /// The file to get info on
    #[arg(short, long, value_name = "FILE")]
    file: PathBuf,
  },
  /// Exports the contents of the SSTable to another format.
  Export {
    /// The file to export
    #[arg(short, long, value_name = "FILE")]
    file: PathBuf,

    /// The format to export to
    #[arg(short, long, value_name = "FORMAT")]
    format: String,
  },
  Keys {
    /// The file to get keys from
    #[arg(short, long, value_name = "FILE")]
    file: PathBuf,
  },
  Values {
    /// The file to get values from
    #[arg(short, long, value_name = "FILE")]
    file: PathBuf,
  },
  /// Get a specific key's value from the SSTable, if it exists. Optionally, get
  /// the next N values after the key. Gets every match by default, but can be
  /// limited to the first match with `-n 1`.
  Get {
    /// The file to get the key from
    #[arg(short, long, value_name = "FILE")]
    file: PathBuf,

    /// The key to get
    #[arg(short, long, value_name = "KEY")]
    key: String,

    /// The number of values to get
    #[arg(short, long, value_name = "N")]
    n: Option<usize>,
  },
  /// Merge multiple SSTables into one, sorted appropriately.
  Merge {
    /// The files to merge
    #[arg(short, long, value_name = "FILE")]
    files: Vec<PathBuf>,
  },
}

pub fn get_cli() -> Cli {
  Cli::parse()
}
