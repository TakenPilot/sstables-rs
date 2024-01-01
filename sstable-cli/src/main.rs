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
  util::{compare_tuples, get_min_max, is_sorted_by, is_unique},
};
use sstables::{cbor::is_cbor_sorted, Append, FromPath, SSTableIndex, SSTableReader, SSTableWriterBuilder};
use std::{
  cmp::Reverse,
  collections::BinaryHeap,
  io::{self, Seek, SeekFrom},
  path::{Path, PathBuf},
};

const CONSOLE_CHECKMARK: &str = "\u{2714}";
const CONSOLE_CROSS: &str = "\u{2718}";

fn get_sorted_sstable_index(index_path: &Path) -> io::Result<SSTableIndex<(String, u64)>> {
  let mut sstable_index = SSTableIndex::<(String, u64)>::from_path(index_path)?;
  // Sort the index file in-place.
  sstable_index.indices.sort_by(compare_tuples);
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
/// file. This is a simple implementation that does not use a heap. It is O(n^2) in the number of
/// SSTables. It is also not memory efficient, as it reads the entire index of each SSTable into
/// memory. It is also not space efficient, as it writes the entire index of each SSTable to the
/// output file. It is also not time efficient, as it seeks to the offset of each key-value pair
/// in each SSTable. It is also not parallelizable, as it reads and writes to a single file. It is
/// also not fault tolerant, as it does not handle errors.
///
/// Future: To use a custom sort order, use a custom tuple that implements it's own `PartialOrd` and
/// `Ord` traits so that the `BinaryHeap` will sort the keys consistently. We would have to change
/// this function to allow the caller to specify their own tuple types.
///
fn merge_sorted_sstable_index_pairs(
  sstable_index_pairs: &mut [(SSTableReader<(String, String)>, SSTableIndex<(String, u64)>)],
  emitter: &mut impl KeyValueWriter,
) -> io::Result<()> {
  let mut heap = BinaryHeap::new();

  // Initialize the heap with the first key from each SSTable index
  for (pair_index, (_sstable, sstable_index)) in sstable_index_pairs.iter().enumerate() {
    let index_pos = 0;
    if let Some((key, offset)) = sstable_index.indices.get(index_pos) {
      heap.push(Reverse((key.clone(), pair_index, index_pos, *offset)));
    }
  }

  while let Some(Reverse((key, pair_index, index_pos, offset))) = heap.pop() {
    // Process key-value pair
    let (sstable, _) = &mut sstable_index_pairs[pair_index];
    sstable.seek(SeekFrom::Start(offset))?;
    let (_, value) = sstable.next().unwrap()?;
    emitter.write(&key, &value)?;

    // Insert the next key from this SSTable index into the heap
    let next_index_pos = index_pos + 1;
    if let Some((next_key, next_offset)) = sstable_index_pairs[pair_index].1.indices.get(next_index_pos) {
      heap.push(Reverse((next_key.clone(), pair_index, next_index_pos, *next_offset)));
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

          let native_sorted = is_sorted_by(&sstable_index.indices, compare_tuples);
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
