use crate::{
  files::{create_index_path, get_file_size, get_path_str},
  traits::TypeWrite,
  util::{compare_tuples, get_min_max, is_sorted_by, is_unique},
};
use colored::Colorize;
use sstables::{cbor::is_cbor_sorted, FromPath, SSTableIndex};
use std::path::PathBuf;

const CONSOLE_CHECKMARK: &str = "\u{2714}";
const CONSOLE_CROSS: &str = "\u{2718}";

pub fn get_info(
  relative_input_paths: Vec<PathBuf>,
  writer: &mut impl TypeWrite<String>,
) -> Result<(), Box<dyn std::error::Error>> {
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
    writer.write("---".to_string())?;

    let data_file_exists = input_path.is_file();
    let input_path_str = get_path_str(&input_path);
    if !data_file_exists {
      writer.write(format!("data path: {} {}", input_path_str, missing_str))?;
    } else {
      writer.write(format!("data path: {} {}", input_path_str, exists_str))?;
      writer.write(format!(" size: {}", get_file_size(&input_path)?))?;
    }

    let input_index_path = create_index_path(&input_path);
    let index_file_exists = input_index_path.is_file();
    let input_index_path_str = get_path_str(&input_index_path);
    if !index_file_exists {
      writer.write(format!("index path: {} {}", input_index_path_str, missing_str))?;
    } else {
      writer.write(format!("index path: {} {}", input_index_path_str, exists_str))?;
      writer.write(format!(" size: {}", get_file_size(&input_index_path)?))?;

      let sstable_index = SSTableIndex::<String>::from_path(&input_index_path)?;
      writer.write(format!(" count: {}", sstable_index.indices.len()))?;

      let native_sorted = is_sorted_by(&sstable_index.indices, compare_tuples);
      let cbor_sorted = is_cbor_sorted(&sstable_index.indices);
      let sorted = match (native_sorted, cbor_sorted) {
        (true, true) => "native,cbor".green(),
        (true, false) => "native".green(),
        (false, true) => "cbor".green(),
        (false, false) => "false".red(),
      };
      writer.write(format!(" sorted: {}", sorted))?;

      let mut index_min = sstable_index.indices.first().unwrap().0.clone();
      let mut index_max = sstable_index.indices.last().unwrap().0.clone();
      if !native_sorted {
        if let Some((min, max)) = get_min_max(&sstable_index.indices) {
          index_min = min.0.clone();
          index_max = max.0.clone();
        }
      }
      writer.write(format!(" min: {}", index_min))?;
      writer.write(format!(" max: {}", index_max))?;
      writer.write(format!(
        " unique: {}",
        is_unique(&sstable_index.indices, |a, b| a.0 < b.0)
      ))?;
    }
  }
  Ok(())
}
