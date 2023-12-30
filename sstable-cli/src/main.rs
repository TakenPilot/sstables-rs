//! Manipulates SSTables
//!
//! # Examples
//!
//! ```zsh
//!   sstable append -f sstable.sst -d "Hello, world!"
//! ```
pub mod cmds;
pub mod files;

use cmds::{get_cli, Commands};
use sstables::{Append, SSTableWriter, SSTableWriterBuilder};
use std::path::PathBuf;

use crate::files::create_dir_all;

fn main() -> Result<(), Box<dyn std::error::Error>> {
  let cli = get_cli();

  // You can see how many times a particular flag or argument occurred
  // Note, only flags can have multiple occurrences
  match cli.verbose {
    0 => println!("Verbose mode is off"),
    _ => println!("Verbose mode is on"),
  }

  // You can check for the existence of subcommands, and if found use their
  // matches just as you would the top level cmd
  match &cli.command {
    Some(Commands::Append { file, key, data }) => {
      println!(
        "append file:{} exists:{} key:{} data:{}",
        file.to_str().unwrap(),
        file.is_file(),
        key,
        data
      );

      let path_dir = PathBuf::from(file.parent().unwrap());
      create_dir_all(path_dir)?;
      let mut sstable_writer = SSTableWriterBuilder::new(file).build()?;
      sstable_writer.append((key.as_str(), data.as_str()))?;
      sstable_writer.close()?;
    }
    Some(Commands::Validate { file }) => {
      if file.is_file() {
        println!("file.is_file() {}", file.is_file());
      } else {
        println!("Not printing testing lists...");
      }
    }
    Some(Commands::Info { file }) => {
      if file.is_file() {
        println!("file.is_file() {}", file.is_file());
      } else {
        println!("Not printing testing lists...");
      }
    }
    Some(Commands::Export { file, format }) => {
      if file.is_file() {
        println!("file.is_file() {}", file.is_file());
      } else {
        println!("Not printing testing lists...");
      }
    }
    Some(Commands::Keys { file }) => {
      // If file exists, read it with a SSTableReader while printing the keys.
      // If file does not exist, print an error message.
      if !file.is_file() {
        println!("File does not exist.")
      } else {
        let sstable_reader = sstables::SSTableReader::<(String, String)>::from_path(file)?;
        for result in sstable_reader {
          let (key, _) = result?;
          println!("{}", key);
        }
      }
    }
    Some(Commands::Values { file }) => {
      // If file exists, read it with a SSTableReader while printing the keys.
      // If file does not exist, print an error message.
      if !file.is_file() {
        println!("File does not exist.")
      } else {
        let sstable_reader = sstables::SSTableReader::<(String, String)>::from_path(file)?;
        for result in sstable_reader {
          let (_, value) = result?;
          println!("{}", value);
        }
      }
    }
    Some(Commands::Get { file, key, n }) => {
      if file.is_file() {
        println!("file.is_file() {}", file.is_file());
      } else {
        println!("Not printing testing lists...");
      }
    }
    Some(Commands::Merge { files }) => {
      if files.is_empty() {
        println!("files.is_empty() {}", files.is_empty());
      } else {
        println!("Not printing testing lists...");
      }
    }
    None => {}
  }

  Ok(())

  // Continued program logic goes here...
}
