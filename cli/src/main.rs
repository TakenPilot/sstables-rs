//! Manipulates SSTables
//!
//! # Examples
//!
//! ```zsh
//!   sstable append -f sstable.sst -d "Hello, world!"
//! ```

use sstable_cli::{
  cmds::{get_cli, Commands},
  files::{self, create_index_path, get_path_str},
  info::get_info,
  merge::Mergeable,
  traits::{KeyValue, TypeWrite},
  util::compare_tuples,
};
use sstables::{cbor::CborWrite, FromPath, SSTableIndex, SSTableReader, SSTableWriter, SSTableWriterBuilder};
use std::{
  fmt::Display,
  io::{self, Seek},
  path::{Path, PathBuf},
};

fn get_sorted_sstable_index<K>(index_path: &Path) -> io::Result<SSTableIndex<K>>
where
  K: Ord,
  SSTableIndex<K>: FromPath<K>,
{
  let mut sstable_index = SSTableIndex::<K>::from_path(index_path)?;
  // Sort the index file in-place.
  sstable_index.indices.sort_by(compare_tuples);
  Ok(sstable_index)
}

fn get_sorted_sstable_index_pairs(
  input_paths: &[PathBuf],
) -> io::Result<Vec<(SSTableReader<(String, String)>, SSTableIndex<String>)>> {
  input_paths
    .iter()
    .map(|input_path| {
      Ok((
        SSTableReader::<(String, String)>::from_path(input_path)?,
        get_sorted_sstable_index(&create_index_path(input_path))?,
      ))
    })
    .collect()
}

struct Terminal {}

impl<T: std::fmt::Display> TypeWrite<T> for Terminal {
  fn write(&mut self, target: T) -> io::Result<()> {
    println!("{}", target);
    Ok(())
  }
}

enum TypeWriter {
  SSTable(SSTableWriter),
  Terminal(Terminal),
}

impl TypeWriter {
  pub fn new(output_path: &Option<PathBuf>) -> io::Result<TypeWriter> {
    Ok(match output_path {
      Some(output_path) => TypeWriter::SSTable(SSTableWriterBuilder::new(output_path).build()?),
      None => TypeWriter::Terminal(Terminal {}),
    })
  }
}

impl<K: CborWrite + Display + Ord + Clone, V: CborWrite + Display> TypeWrite<(K, V)> for TypeWriter {
  fn write(&mut self, target: (K, V)) -> io::Result<()> {
    let (k, v) = target;
    match self {
      TypeWriter::SSTable(sstable_writer) => sstable_writer.write((k, v)),
      TypeWriter::Terminal(terminal) => terminal.write(KeyValue(k, v)),
    }
  }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
  let cli = get_cli();

  // You can check for the existence of subcommands, and if found use their
  // matches just as you would the top level cmd
  match &cli.command {
    Some(Commands::Append { input_paths, key, data }) => {
      for input_path in input_paths {
        let mut sstable_writer = SSTableWriterBuilder::new(input_path).build()?;
        sstable_writer.write((key.as_str(), data.as_str()))?;
        sstable_writer.close()?;
      }
    }

    Some(Commands::Dump { input_paths, format: _ }) => {
      let mut writer = Terminal {};
      for input_path in input_paths {
        if !input_path.is_file() {
          writer.write(format!("File does not exist: {}", get_path_str(input_path)))?
        } else {
          let mut sstable_reader = SSTableReader::<(String, String)>::from_path(input_path)?;
          loop {
            let pos = sstable_reader.stream_position()?;
            let (key, value) = match sstable_reader.next() {
              Some(Ok(x)) => x,
              Some(Err(e)) => return Err(Box::new(e)),
              None => return Ok(()),
            };

            writer.write(format!("({}) {:?}: {:?}", pos, key, value))?;
          }
        }
      }
    }

    Some(Commands::Index { input_paths }) => {
      let mut writer = Terminal {};
      for input_path in input_paths {
        // If file exists, get the index file path and print out each key and offset.
        // If file does not exist, print an error message.
        if !input_path.is_file() {
          writer.write(format!("File does not exist: {}", get_path_str(input_path)))?;
        } else {
          let sstable_index = SSTableIndex::<String>::from_path(create_index_path(input_path))?;
          for (key, offset) in sstable_index.indices {
            writer.write(format!("{:?}: {:?}", key, offset))?;
          }
        }
      }
    }

    Some(Commands::Info { input_paths }) => {
      // Convert the input_paths to absolute paths.
      let absolute_input_paths = files::to_absolute_paths(input_paths)?;
      let relative_input_paths = files::to_relative_paths(&absolute_input_paths)?;

      get_info(relative_input_paths, &mut Terminal {})?;
    }

    Some(Commands::Export { input_paths, format }) => {
      let mut writer = Terminal {};
      // If file exists, read it with a SSTableReader while printing the contents.
      // If file does not exist, print an error message.
      // If the format is JSON, print the contents as JSON.
      // If the format is CSV, print the contents as CSV.
      for input_path in input_paths {
        if !input_path.is_file() {
          writer.write(format!("File does not exist: {}", get_path_str(input_path)))?
        } else {
          let sstable_reader = SSTableReader::<(String, String)>::from_path(input_path)?;
          for result in sstable_reader {
            let (key, value) = result?;

            match format.as_deref() {
              Some("json") => {
                writer.write(format!("{{\"{}\": \"{}\"}},", key, value))?;
              }
              Some("csv") => {
                writer.write(format!("{},{}", key, value))?;
              }
              Some(_) => {
                writer.write(format!("{}: {}", key, value))?;
              }
              None => {
                writer.write(format!("{}: {}", key, value))?;
              }
            }
          }
        }
      }
    }

    Some(Commands::Keys { input_paths }) => {
      let mut writer = Terminal {};
      // If file exists, read it with a SSTableReader while printing the keys.
      // If file does not exist, print an error message.
      for input_path in input_paths {
        if !input_path.is_file() {
          writer.write(format!("File does not exist: {}", get_path_str(input_path)))?;
        } else {
          let sstable_reader = sstables::SSTableReader::<(String, String)>::from_path(input_path)?;
          for result in sstable_reader {
            let (key, _) = result?;
            writer.write(key.to_string())?;
          }
        }
      }
    }

    Some(Commands::Get { input_paths, key, n }) => {
      let mut writer = Terminal {};
      for input_path in input_paths {
        if !input_path.is_file() {
          writer.write(format!("File does not exist: {}", get_path_str(input_path)))?
        } else {
          let mut sstable_reader = sstables::SSTableReader::<(String, String)>::from_path(input_path)?;
          match SSTableIndex::<String>::from_path(create_index_path(input_path)) {
            Ok(sstable_index) => {
              let index_pos = match sstable_index.indices.binary_search_by(|kv| kv.0.cmp(key)) {
                Ok(x) => x,
                Err(_) => {
                  // Do nothing; we might be searching many files, so a missing key in this file
                  // does not mean it's missing in another file. Don't output anything, since that
                  // would interfere with the output they might be piping elsewhere.
                  return Ok(());
                }
              };
              let (index_key, offset) = match sstable_index.indices.get(index_pos) {
                Some(x) => x,
                None => {
                  // An actual error, stop and tell them.
                  // All indices should point to real entries in the data file.
                  writer.write(format!("Missing index_pos: {}", index_pos))?;
                  return Ok(());
                }
              };
              sstable_reader.seek(io::SeekFrom::Start(*offset))?;
              let mut kv_maybe = sstable_reader.next();
              while let Some(kv_result) = kv_maybe {
                let (key, value) = match kv_result {
                  Ok(x) => x,
                  Err(e) => return Err(Box::new(e)),
                };

                if &key != index_key {
                  break;
                }

                writer.write(format!("{}: {}", key, value))?;
                kv_maybe = sstable_reader.next();
              }
            }
            Err(e) => {
              let mut count = 0;
              for result in sstable_reader {
                let (k, v) = result?;
                if &k == key {
                  writer.write(v.to_string())?;
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
      }
    }

    Some(Commands::Merge {
      input_paths,
      output_path,
    }) => {
      // Pull the index files of each SSTable into memory along with their File objects.
      let sstable_index_pairs = get_sorted_sstable_index_pairs(input_paths)?;
      let mut output_writer = TypeWriter::new(output_path)?;
      sstable_index_pairs.merge(&mut output_writer)?;
    }

    Some(Commands::Sort {
      input_paths,
      output_path,
    }) => {
      // Pull the index files of each SSTable into memory along with their File objects.
      let sstable_index_pairs = get_sorted_sstable_index_pairs(input_paths)?;
      let mut output_writer = TypeWriter::new(output_path)?;
      sstable_index_pairs.merge(&mut output_writer)?;
    }

    Some(Commands::Values { input_paths }) => {
      let mut writer = Terminal {};
      // If file exists, read it with a SSTableReader while printing the keys.
      // If file does not exist, print an error message.
      for input_path in input_paths {
        if !input_path.is_file() {
          writer.write(format!("File does not exist: {}", get_path_str(input_path)))?
        } else {
          let sstable_reader = sstables::SSTableReader::<(String, String)>::from_path(input_path)?;
          for result in sstable_reader {
            let (_, value) = result?;
            writer.write(value.to_string())?;
          }
        }
      }
    }
    None => {}
  }

  Ok(())

  // Continued program logic goes here...
}
