use crate::{
  files::{create_index_path, get_path_str},
  traits::TypeWrite,
};
use sstables::{cbor::CborRead, FromPath, SSTableIndex, SSTableReader};
use std::{
  fmt::Display,
  fs::File,
  io::{self, Seek},
  path::PathBuf,
};

fn get_index_entry_by_key<K>(sstable_index: &SSTableIndex<K>, key: &K) -> Option<(K, u64)>
where
  K: Ord + Clone,
{
  match sstable_index.indices.binary_search_by(|(k, _)| (k.cmp(key))) {
    Ok(x) => sstable_index.indices.get(x).cloned(),
    Err(_) => None,
  }
}

fn write_next_n_with_key<I, K, V>(
  iterator: &mut I,
  index_key: K,
  n: Option<usize>,
  writer: &mut impl TypeWrite<String>,
) -> io::Result<()>
where
  I: Iterator<Item = io::Result<(K, V)>>,
  K: Ord + Display,
  V: Display,
{
  let mut kv_maybe = iterator.next();
  let mut count = 0;
  while let Some(kv_result) = kv_maybe {
    let (key, value) = match kv_result {
      Ok(x) => x,
      Err(e) => return Err(e),
    };

    if key != index_key {
      break;
    }

    writer.write(format!("{}: {}", key, value))?;
    count += 1;

    if let Some(n) = n {
      if count >= n {
        break;
      }
    }

    kv_maybe = iterator.next();
  }
  Ok(())
}

fn get_kv_by_linear_search<I, K, V>(
  iterator: &mut I,
  key: &K,
  n: Option<usize>,
  writer: &mut impl TypeWrite<String>,
) -> io::Result<()>
where
  I: Iterator<Item = io::Result<(K, V)>>,
  K: Ord + Display,
  V: Display,
{
  let mut count = 0;
  for result in iterator {
    let (k, v) = result?;
    if &k == key {
      writer.write(v.to_string())?;
      count += 1;
    }
    if let Some(n) = n {
      if count >= n {
        break;
      }
    }
  }

  Ok(())
}

pub fn get<K: Ord + Clone + Display, V: Display>(
  input_paths: &[PathBuf],
  key: K,
  n: Option<usize>,
  writer: &mut impl TypeWrite<String>,
) -> io::Result<()>
where
  K: Ord + Clone + Display,
  V: Display,
  io::BufReader<File>: CborRead<K>,
  io::BufReader<File>: CborRead<V>,
  SSTableIndex<K>: FromPath<K>,
{
  for input_path in input_paths {
    if !input_path.is_file() {
      writer.write(format!("File does not exist: {}", get_path_str(input_path)))?
    } else {
      // First check for the presence of the data file.
      let mut sstable_reader = SSTableReader::<(K, V)>::from_path(input_path)?;

      // Second, check for the presence of the index file.
      match SSTableIndex::<K>::from_path(create_index_path(input_path)) {
        Ok(sstable_index) => {
          let (index_key, offset) = match get_index_entry_by_key(&sstable_index, &key) {
            Some(x) => x,
            None => {
              // Don't print any error message if the key is not found in the index.
              return Ok(());
            }
          };
          sstable_reader.seek(io::SeekFrom::Start(offset))?;
          write_next_n_with_key(&mut sstable_reader, index_key, n, writer)?;
        }
        Err(_) => {
          get_kv_by_linear_search(&mut sstable_reader, &key, n, writer)?;
        }
      }
    }
  }

  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;
  use common_testing::assert;

  struct MockTypeWriter<T> {
    pub items: Vec<T>,
    _type: std::marker::PhantomData<T>,
  }

  impl<T> MockTypeWriter<T> {
    pub fn new() -> Self {
      Self {
        items: Vec::new(),
        _type: std::marker::PhantomData,
      }
    }
  }

  impl<T> TypeWrite<T> for MockTypeWriter<T> {
    fn write(&mut self, target: T) -> io::Result<()> {
      self.items.push(target);
      Ok(())
    }
  }

  #[test]
  fn get_index_entry_by_key_works() {
    let sstable_index = SSTableIndex {
      indices: vec![("a".to_string(), 0), ("b".to_string(), 1)],
    };

    assert::equal(
      get_index_entry_by_key(&sstable_index, &"a".to_string()),
      ("a".to_string(), 0u64),
    );
    assert::equal(
      get_index_entry_by_key(&sstable_index, &"b".to_string()),
      ("b".to_string(), 1u64),
    );
    assert::equal(get_index_entry_by_key(&sstable_index, &"d".to_string()), None);
  }

  fn create_iterator() -> impl Iterator<Item = io::Result<(String, String)>> {
    vec![
      Ok(("a".to_string(), "1".to_string())),
      Ok(("a".to_string(), "2".to_string())),
      Ok(("b".to_string(), "3".to_string())),
      Ok(("b".to_string(), "4".to_string())),
      Ok(("c".to_string(), "5".to_string())),
      Ok(("c".to_string(), "6".to_string())),
    ]
    .into_iter()
  }

  #[test]
  fn get_kv_by_linear_search_works() {
    // Start
    let mut iterator = create_iterator();
    let mut writer = MockTypeWriter::new();
    get_kv_by_linear_search(&mut iterator, &"a".to_string(), None, &mut writer).unwrap();
    assert::equal(writer.items, vec!["1".to_string(), "2".to_string()]);

    // End
    let mut iterator = create_iterator();
    let mut writer = MockTypeWriter::new();
    get_kv_by_linear_search(&mut iterator, &"c".to_string(), Some(2), &mut writer).unwrap();
    assert::equal(writer.items, vec!["5".to_string(), "6".to_string()]);
  }

  #[test]
  fn get_kv_by_linear_search_with_limit() {
    let mut iterator = create_iterator();
    let mut writer = MockTypeWriter::new();
    get_kv_by_linear_search(&mut iterator, &"b".to_string(), Some(1), &mut writer).unwrap();
    assert::equal(writer.items, vec!["3".to_string()]);
  }

  #[test]
  fn write_next_n_with_key_works() {
    let mut iterator = create_iterator();
    let mut writer = MockTypeWriter::new();
    write_next_n_with_key(&mut iterator, "a".to_string(), Some(2), &mut writer).unwrap();
    assert::equal(writer.items, vec!["a: 1".to_string(), "a: 2".to_string()]);
  }

  #[test]
  fn write_next_n_with_key_with_limit() {
    let mut iterator = create_iterator();
    let mut writer = MockTypeWriter::new();
    write_next_n_with_key(&mut iterator, "a".to_string(), Some(1), &mut writer).unwrap();
    assert::equal(writer.items, vec!["a: 1".to_string()]);
  }
}
