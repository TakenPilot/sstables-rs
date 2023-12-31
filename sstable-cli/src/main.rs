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
  files::{self, create_index_path, get_file_size, get_path_str},
  outputs::{KeyValueWriter, OutputDestination, OutputWriter, OutputWriterBuilder},
  util::{get_min_max, is_sorted_by, is_unique},
};
use sstables::{
  cbor::is_cbor_sorted, SSTableIndex, SSTableIndexFromPath, SSTableReader, SSTableWriterAppend, SSTableWriterBuilder,
};
use std::{
  io,
  path::{Path, PathBuf},
};

const CONSOLE_CHECKMARK: &str = "\u{2714}";
const CONSOLE_CROSS: &str = "\u{2718}";

fn get_sorted_sstable_index(index_path: &Path) -> io::Result<SSTableIndex<(String, u64)>> {
  let mut sstable_index = SSTableIndex::<(String, u64)>::from_path(index_path)?;
  // Sort the index file in-place.
  sstable_index.indices.sort_by(|a, b| a.0.cmp(&b.0));
  Ok(sstable_index)
}

fn get_output_writer(output_path: &Option<PathBuf>) -> io::Result<OutputWriter> {
  let output_destination = match output_path {
    Some(output_path) => OutputDestination::File(output_path.clone()),
    None => OutputDestination::Stdout,
  };
  OutputWriterBuilder::new(output_destination).build()
}

/// Merge the SSTables by reading the lowest key from each index and writing it to the output
/// file. Repeat until all keys have been read. This is a naive implementation that is not
/// suitable for large SSTables because it requires all of the indices to be loaded into memory,
/// and it requires all of the data files to be open at the same time. It is useful for testing.
/// It is not useful for production.
fn merge_sorted_sstable_index_pairs(
  sstable_index_pairs: &mut Vec<(SSTableReader<(String, String)>, SSTableIndex<(String, u64)>)>,
  emitter: &mut impl KeyValueWriter,
) -> io::Result<()> {
  let mut is_done = false;
  // Create a vector of cursors to track how far we've read in each index.
  let mut indices_cursors = vec![0; sstable_index_pairs.len()];
  while !is_done {
    let mut min_key: Option<String> = None;
    let mut min_index = 0;
    let mut min_offset = 0;
    for (i, (_, sstable_index)) in sstable_index_pairs.iter().enumerate() {
      let index_cursor = indices_cursors[i];

      if let Some((key, offset)) = sstable_index.indices.get(index_cursor) {
        let is_less_than_min_key = match &min_key {
          Some(min_key) => key < min_key,
          None => true,
        };

        if is_less_than_min_key {
          min_key = Some(key.clone());
          min_index = i;
          min_offset = *offset;
        }
      }
    }

    if let Some(_min_key) = &min_key {
      if let Some((sstable, _)) = sstable_index_pairs.get_mut(min_index) {
        sstable.seek(min_offset)?;
        let (key, value) = sstable.next().unwrap()?;
        emitter.write(&key, &value)?;
        indices_cursors[min_index] += 1;
      }
    } else {
      is_done = true;
    }
  }

  Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
  let cli = get_cli();

  // You can check for the existence of subcommands, and if found use their
  // matches just as you would the top level cmd
  match &cli.command {
    Some(Commands::Append { input_paths, key, data }) => {
      for input_path in input_paths {
        let mut sstable_writer = SSTableWriterBuilder::new(input_path).build()?;
        sstable_writer.append((key.as_str(), data.as_str()))?;
        sstable_writer.close()?;
      }
    }
    Some(Commands::Index { input_paths }) => {
      for input_path in input_paths {
        // If file exists, get the index file path and print out each key and offset.
        // If file does not exist, print an error message.
        if !input_path.is_file() {
          println!("File does not exist: {}", get_path_str(input_path))
        } else {
          let sstable_index = SSTableIndex::<(String, u64)>::from_path(create_index_path(input_path))?;
          for (key, offset) in sstable_index.indices {
            println!("{}: {}", key, offset);
          }
        }
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

        // YAML file spilt marker.
        println!("---");

        let data_file_exists = input_path.is_file();
        let input_path_str = get_path_str(&input_path);
        if !data_file_exists {
          println!("data path: {} {}", input_path_str, missing_str);
        } else {
          println!("data path: {} {}", input_path_str, exists_str);
          println!(" size: {}", get_file_size(&input_path)?);
        }

        let input_index_path = create_index_path(&input_path);
        let index_file_exists = input_index_path.is_file();
        let input_index_path_str = get_path_str(&input_index_path);
        if !index_file_exists {
          println!("index path: {} {}", input_index_path_str, missing_str);
        } else {
          println!("index path: {} {}", input_index_path_str, exists_str);
          println!(" size: {}", get_file_size(&input_index_path)?);

          let sstable_index = SSTableIndex::<(String, u64)>::from_path(&input_index_path)?;
          println!(" count: {}", sstable_index.indices.len());

          let native_sorted = is_sorted_by(&sstable_index.indices, |a, b| a.0 <= b.0);
          let cbor_sorted = is_cbor_sorted(&sstable_index.indices);
          let sorted = match (native_sorted, cbor_sorted) {
            (true, true) => "native,cbor".green(),
            (true, false) => "native".green(),
            (false, true) => "cbor".green(),
            (false, false) => "false".red(),
          };
          println!(" sorted: {}", sorted);

          let mut index_min = sstable_index.indices.first().unwrap().0.clone();
          let mut index_max = sstable_index.indices.last().unwrap().0.clone();
          if !native_sorted {
            if let Some((min, max)) = get_min_max(&sstable_index.indices) {
              index_min = min.0.clone();
              index_max = max.0.clone();
            }
          }
          println!(" min: {}", index_min);
          println!(" max: {}", index_max);
          println!(" unique: {}", is_unique(&sstable_index.indices, |a, b| a.0 < b.0));
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
          println!("File does not exist: {}", get_path_str(input_path))
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
          println!("File does not exist: {}", get_path_str(input_path))
        } else {
          let sstable_reader = sstables::SSTableReader::<(String, String)>::from_path(input_path)?;
          for result in sstable_reader {
            let (key, _) = result?;
            println!("{}", key);
          }
        }
      }
    }

    Some(Commands::Get { input_paths, key, n }) => {
      for input_path in input_paths {
        if !input_path.is_file() {
          println!("File does not exist: {}", get_path_str(input_path))
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
    Some(Commands::Merge {
      input_paths,
      output_path,
    }) => {
      // Pull the index files of each SSTable into memory along with their File objects.
      let mut sstable_index_pairs = input_paths
        .iter()
        .map(|input_path| {
          Ok((
            SSTableReader::<(String, String)>::from_path(input_path)?,
            get_sorted_sstable_index(&create_index_path(input_path))?,
          ))
        })
        .collect::<io::Result<Vec<(SSTableReader<(String, String)>, SSTableIndex<(String, u64)>)>>>()?;

      let mut output_writer = get_output_writer(output_path)?;

      merge_sorted_sstable_index_pairs(&mut sstable_index_pairs, &mut output_writer)?;
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
    Some(Commands::Values { input_paths }) => {
      // If file exists, read it with a SSTableReader while printing the keys.
      // If file does not exist, print an error message.
      for input_path in input_paths {
        if !input_path.is_file() {
          println!("File does not exist: {}", get_path_str(input_path))
        } else {
          let sstable_reader = sstables::SSTableReader::<(String, String)>::from_path(input_path)?;
          for result in sstable_reader {
            let (_, value) = result?;
            println!("{}", value);
          }
        }
      }
    }
    None => {}
  }

  Ok(())

  // Continued program logic goes here...
}
