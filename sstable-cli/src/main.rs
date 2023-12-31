//! Manipulates SSTables
//!
//! # Examples
//!
//! ```zsh
//!   sstable append -f sstable.sst -d "Hello, world!"
//! ```

use colored::Colorize;
use sstable_cli::{
  cmds::{get_cli, Commands},
  files::{self, create_dir_all},
  util::{get_min_max, is_sorted_by, is_unique},
};
use sstables::{SSTableIndexReader, SSTableIndexReaderFromPath, SSTableWriterAppend, SSTableWriterBuilder};
use std::path::PathBuf;

const CONSOLE_CHECKMARK: &str = "\u{2714}";
const CONSOLE_CROSS: &str = "\u{2718}";

fn main() -> Result<(), Box<dyn std::error::Error>> {
  let cli = get_cli();

  // You can check for the existence of subcommands, and if found use their
  // matches just as you would the top level cmd
  match &cli.command {
    Some(Commands::Append { input_paths, key, data }) => {
      for input_path in input_paths {
        println!(
          "append file:{} exists:{} key:{} data:{}",
          input_path.to_str().unwrap(),
          input_path.is_file(),
          key,
          data
        );

        let path_dir = PathBuf::from(input_path.parent().unwrap());
        create_dir_all(path_dir)?;
        let mut sstable_writer = SSTableWriterBuilder::new(input_path).build()?;
        sstable_writer.append((key.as_str(), data.as_str()))?;
        sstable_writer.close()?;
      }
    }
    Some(Commands::Validate { input_paths }) => {
      for input_path in input_paths {
        println!(
          "validate file:{} exists:{}",
          input_path.to_str().unwrap(),
          input_path.is_file()
        );
      }
    }
    Some(Commands::Info { input_paths }) => {
      // Convert the input_paths to absolute paths.
      let absolute_input_paths = files::to_absolute_paths(input_paths)?;
      let relative_input_paths = files::to_relative_paths(&absolute_input_paths)?;
      let missing_str = format!("(missing {})", CONSOLE_CROSS).red().bold();
      let exists_str = format!("(exists {})", CONSOLE_CHECKMARK).green().bold();

      for input_path in relative_input_paths {
        // For each input_path, print the following:
        // - The file path to the data file relative to the current working directory
        // - The data file size
        // - The number of data file blocks
        // - The file path to the index file relative to the current working directory
        // - The index file size
        // - The number of index file blocks
        // - The total number of entries
        // - The min of index keys
        // - The max of index keys
        // - The file path to the bloom filter file relative to the current working directory
        // - The bloom filter file size
        // - The number of bloom filter file blocks
        // - The bloom filter file false positive rate

        println!("Test {}", "red".red());

        let data_file_exists = input_path.is_file();
        if !data_file_exists {
          println!("data file: {} {}", input_path.to_str().unwrap(), missing_str);
        } else {
          let data_file_size = input_path.metadata()?.len();

          println!(
            "data file: {} {}\n size: {}",
            input_path.to_str().unwrap(),
            exists_str,
            data_file_size,
          );
        }

        let input_index_path = input_path.with_extension("index.sst");
        let index_file_exists = input_index_path.is_file();
        if !index_file_exists {
          println!("index file: {} {}", input_index_path.to_str().unwrap(), missing_str,);
        } else {
          let index_file_size = input_index_path.metadata()?.len();
          let sstable_index_reader = SSTableIndexReader::<(String, u64)>::from_path(&input_index_path)?;
          let index_entries = sstable_index_reader.indices.len();

          // sorted?
          let sorted = is_sorted_by(&sstable_index_reader.indices, |a, b| a.0 < b.0);

          // min/max
          let mut index_min = sstable_index_reader.indices.first().unwrap().0.clone();
          let mut index_max = sstable_index_reader.indices.last().unwrap().0.clone();
          if !sorted {
            if let Some((min, max)) = get_min_max(&sstable_index_reader.indices) {
              index_min = min.0.clone();
              index_max = max.0.clone();
            }
          }

          let unique = is_unique(&sstable_index_reader.indices, |a, b| a.0 < b.0);

          println!(
            "index file: {} {}\n size: {}\n entries: {}\n min: {}\n max: {}\n sorted: {}\n unique: {}",
            input_index_path.to_str().unwrap(),
            exists_str,
            index_file_size,
            index_entries,
            index_min,
            index_max,
            sorted,
            unique,
          );
        }
      }
    }
    Some(Commands::Export { input_paths, format }) => {
      // If file exists, read it with a SSTableReader while printing the contents.
      // If file does not exist, print an error message.
      // If the format is JSON, print the contents as JSON.
      // If the format is CSV, print the contents as CSV.
      for input_path in input_paths {
        if !input_path.is_file() {
          println!("File does not exist.")
        } else {
          let sstable_reader = sstables::SSTableReader::<(String, String)>::from_path(input_path)?;
          for result in sstable_reader {
            let (key, value) = result?;

            match format.as_deref() {
              Some("json") => {
                println!("{{\"{}\": \"{}\"}},", key, value);
              }
              Some("csv") => {
                println!("{},{}", key, value);
              }
              Some(_) => {
                println!("{}: {}", key, value);
              }
              None => {
                println!("{}: {}", key, value);
              }
            }
          }
        }
      }
    }
    Some(Commands::Keys { input_paths }) => {
      // If file exists, read it with a SSTableReader while printing the keys.
      // If file does not exist, print an error message.
      for input_path in input_paths {
        if !input_path.is_file() {
          println!("File does not exist.")
        } else {
          let sstable_reader = sstables::SSTableReader::<(String, String)>::from_path(input_path)?;
          for result in sstable_reader {
            let (key, _) = result?;
            println!("{}", key);
          }
        }
      }
    }
    Some(Commands::Values { input_paths }) => {
      // If file exists, read it with a SSTableReader while printing the keys.
      // If file does not exist, print an error message.
      for input_path in input_paths {
        if !input_path.is_file() {
          println!("File does not exist.")
        } else {
          let sstable_reader = sstables::SSTableReader::<(String, String)>::from_path(input_path)?;
          for result in sstable_reader {
            let (_, value) = result?;
            println!("{}", value);
          }
        }
      }
    }
    Some(Commands::Get { input_paths, key, n }) => {
      for input_path in input_paths {
        if !input_path.is_file() {
          println!("File does not exist.")
        } else {
          let sstable_reader = sstables::SSTableReader::<(String, String)>::from_path(input_path)?;
          let mut count = 0;
          for result in sstable_reader {
            let (k, v) = result?;
            if &k == key {
              println!("{}", v);
              count += 1;
            }
            if let Some(n) = n {
              if &count >= n {
                break;
              }
            }
          }
        }
      }
    }
    Some(Commands::Merge { input_paths }) => {
      for input_path in input_paths {
        println!(
          "merge file:{} exists:{}",
          input_path.to_str().unwrap(),
          input_path.is_file()
        );
      }
    }
    None => {}
  }

  Ok(())

  // Continued program logic goes here...
}
