#![allow(dead_code)]
//! SSTable writer
//!
//! This module contains the `SSTableWriter` struct, which is a convenience wrapper around two
//! `BufWriter`s for appending to a data and index file in a performant manner. The data and index
//! is written as a sequence of CBOR-encoded arrays or maps, and therefore can be read by any CBOR
//! implementation. If written as single entries, the index will be an array of file offsets, and
//! if written as a key-value tuple, the index will be a map of keys to file offsets.
//!
//! This code is designed to be extended, and therefore the `SSTableWriter` struct is generic over
//! the type of data that is being written.
//!
//! The `SSTableWriter` struct is designed to be used in a streaming fashion, and therefore does not
//! buffer the entire file in memory. If you need to write to the same file from
//! multiple threads, you should use a `RwLock`. If you need to write to multiple files, you should
//! use multiple `SSTableWriter`s.
//!
//! # Example
//!
//! ```
//! use sstables::sstable_writer::SSTableWriterBuilder;
//! use sstables::sstable_writer::SSTableWriter;
//! use sstables::traits::Append;
//!
//! let mut writer = SSTableWriterBuilder::new("test")
//!   .build()
//!   .unwrap();
//!
//! writer.append(("hello", "world")).unwrap();
//! ```
//!
//!
//! # Format
//!
//! The data file is a sequence of CBOR-encoded values. If writing a key-value pair, the key is
//! written first, followed by the value. If writing a single value, the value is written directly.
//!
//! The index file is also a sequence of CBOR-encoded values. If writing a key-value pair, the key is
//! written first, followed by the file offset of the value in the data file. If writing a single
//! value, the file offset of the value in the data file is written directly.
//!
//! # Errors
//!
//! The `SSTableWriter` struct will return an error if the file cannot be opened for writing, or if
//! the file cannot be flushed to disk. All errors are standard `io::Error`s.
//!

use crate::cbor::CborWrite;
use crate::read::{create_index_path, get_file_writer};
use std::fs::File;
use std::io::{self, BufWriter, Result, Seek, Write};
use std::path::PathBuf;

/// The default buffer size for the `SSTableWriter`.
const DEFAULT_BUFFER_SIZE: usize = 8 * 1024;

/// Builder for `SSTableWriter`
///
/// # Example
///
/// ```
/// use sstables::sstable_writer::SSTableWriterBuilder;
/// use sstables::sstable_writer::SSTableWriter;
/// use sstables::Append;
///
/// let mut writer = SSTableWriterBuilder::new("test")
///  .build()
///  .unwrap();
///
/// writer.append(("hello", "world")).unwrap();
/// ```
///
/// # Example
///
/// ```
/// use sstables::sstable_writer::SSTableWriterBuilder;
/// use sstables::sstable_writer::SSTableWriter;
/// use sstables::Append;
///
/// let mut writer = SSTableWriterBuilder::new("test")
///  .index_writer_path("test.index")
///  .buffer_size(1024)
///  .build()
///  .unwrap();
///
/// writer.append(("hello", "world")).unwrap();
/// ```
pub struct SSTableWriterBuilder {
  data_writer_path: PathBuf,
  index_writer_path: Option<PathBuf>,
  buffer_size: usize,
}

impl SSTableWriterBuilder {
  pub fn new<P: Into<PathBuf>>(data_writer_path: P) -> Self {
    SSTableWriterBuilder {
      data_writer_path: data_writer_path.into(),
      index_writer_path: None,
      buffer_size: DEFAULT_BUFFER_SIZE,
    }
  }

  /// Set a custom path for the index file. If not set, the index file will be created in the same
  /// directory as the data file, with the same name as the data file, but with "index" prepended
  /// to the extension.
  pub fn index_writer_path<P: Into<PathBuf>>(mut self, path: P) -> Self {
    self.index_writer_path = Some(path.into());
    self
  }

  /// Set a custom buffer size for the `BufWriter`s. If not set, the default buffer size is 8 KiB.
  pub fn buffer_size(mut self, size: usize) -> Self {
    self.buffer_size = size;
    self
  }

  /// Consumes the builder, returning a `SSTableWriter`.
  pub fn build(self) -> io::Result<SSTableWriter> {
    let data_writer_path = self.data_writer_path;
    let data_writer = get_file_writer(&data_writer_path, self.buffer_size)?;

    // If the index writer path is not set, create it from the data writer path.
    let index_writer_path = self
      .index_writer_path
      .unwrap_or_else(|| create_index_path(&data_writer_path));

    let index_writer = get_file_writer(&index_writer_path, self.buffer_size)?;

    Ok(SSTableWriter {
      data_writer_path,
      data_writer,
      index_writer_path,
      index_writer,
    })
  }
}

/// A convenience wrapper around two `BufWriter`s for appending to a data and index file in a
/// performant manner. The data and index is written as a sequence of CBOR-encoded arrays or maps,
/// and therefore can be read by any CBOR implementation. If written as single entries, the index
/// will be an array of file offsets, and if written as a key-value tuple, the index will be a map
/// of keys to file offsets.
pub struct SSTableWriter {
  pub data_writer_path: PathBuf,
  pub data_writer: BufWriter<File>,
  pub index_writer_path: PathBuf,
  pub index_writer: BufWriter<File>,
}

impl SSTableWriter {
  pub fn write<K, V>(&mut self, entry: (K, V)) -> io::Result<()>
  where
    K: CborWrite,
    V: CborWrite,
  {
    let initial_offset = self.data_writer.stream_position()?;
    let writer = &mut self.data_writer;
    let (key, value) = entry;

    key
      .cbor_write(writer)
      .and_then(|_| value.cbor_write(writer))
      .and_then(|_| key.cbor_write(writer))
      .and_then(|_| initial_offset.cbor_write(writer))
  }

  pub fn flush(&mut self) -> Result<()> {
    self.data_writer.flush()?;
    self.index_writer.flush()
  }

  pub fn close(&mut self) -> Result<()> {
    self.flush()?;
    self.data_writer.get_mut().sync_all()?;
    self.index_writer.get_mut().sync_all()
  }

  /// Consumes the writer, returning all inner files.
  pub fn into_files(mut self) -> Result<Vec<(PathBuf, File)>> {
    // Necessary because we're dropping the buffers.
    self.data_writer.flush()?;
    self.index_writer.flush()?;

    Ok(vec![
      (self.data_writer_path, self.data_writer.into_inner()?),
      (self.index_writer_path, self.index_writer.into_inner()?),
    ])
  }
}

#[cfg(test)]
mod tests {
  use crate::cbor::{cbor_binary_search_first, cbor_sort};
  use crate::{FromPath, SSTableIndex, SSTableReader, SSTableWriterBuilder};
  use common_testing::{assert, setup};
  use std::fs;
  use std::io::SeekFrom;

  use super::*;

  const TEST_FILE_NAME: &str = ".tmp/test.sst";
  const TEST_INDEX_FILE_NAME: &str = ".tmp/test.index.sst";

  #[test]
  fn test_append_string_tuple() {
    let _lock = setup::sequential();
    fs::remove_file(TEST_FILE_NAME).unwrap_or_default();
    fs::remove_file(TEST_INDEX_FILE_NAME).unwrap_or_default();

    let mut writer = SSTableWriterBuilder::new(TEST_FILE_NAME).build().unwrap();

    writer.write(("hello", "world")).unwrap();
    writer.close().unwrap();

    let mut reader = SSTableReader::<(String, String)>::from_path(TEST_FILE_NAME).unwrap();

    assert::equal(reader.next(), ("hello".to_string(), "world".to_string()));
  }

  #[test]
  fn test_append_bytes_tuple() {
    let _lock = setup::sequential();
    fs::remove_file(TEST_FILE_NAME).unwrap_or_default();
    fs::remove_file(TEST_INDEX_FILE_NAME).unwrap_or_default();

    let mut writer = SSTableWriterBuilder::new(TEST_FILE_NAME).build().unwrap();

    writer.write((b"hello".as_slice(), b"world".as_slice())).unwrap();
    writer.close().unwrap();

    let mut reader = SSTableReader::<(Vec<u8>, Vec<u8>)>::from_path(TEST_FILE_NAME).unwrap();
    assert::equal(reader.next(), (b"hello".to_vec(), b"world".to_vec()));
    assert::none(&reader.next());
  }

  #[test]
  fn test_append_string_tuple_with_index() {
    let _lock = setup::sequential();
    fs::remove_file(TEST_FILE_NAME).unwrap_or_default();
    fs::remove_file(TEST_INDEX_FILE_NAME).unwrap_or_default();

    // Should create index file
    let mut writer = SSTableWriterBuilder::new(TEST_FILE_NAME).build().unwrap();
    writer.write(("hello", "world")).unwrap();
    writer.close().unwrap();

    // Should use index file
    let mut sstable = SSTableReader::<(String, String)>::from_path(TEST_FILE_NAME).unwrap();
    let sstable_index = SSTableIndex::<String>::from_path(TEST_INDEX_FILE_NAME).unwrap();

    let mut sstable_index_iter = sstable_index.indices.iter();
    assert::equal(sstable.next(), ("hello".to_string(), "world".to_string()));
    assert::equal(sstable_index_iter.next(), &("hello".to_string(), 0));

    assert::none(&sstable.next());
    assert::none(&sstable_index_iter.next());
  }

  #[test]
  fn test_append_bytes_tuple_with_index() {
    let _lock = setup::sequential();
    fs::remove_file(TEST_FILE_NAME).unwrap_or_default();
    fs::remove_file(TEST_INDEX_FILE_NAME).unwrap_or_default();

    // Should create index file
    let mut writer = SSTableWriterBuilder::new(TEST_FILE_NAME).build().unwrap();
    writer.write((b"hello".as_slice(), b"world".as_slice())).unwrap();
    writer.write((b"foo".as_slice(), b"bar".as_slice())).unwrap();
    writer.close().unwrap();

    // Should use index file
    let mut sstable = SSTableReader::<(Vec<u8>, Vec<u8>)>::from_path(TEST_FILE_NAME).unwrap();
    let sstable_index = SSTableIndex::<Vec<u8>>::from_path(TEST_INDEX_FILE_NAME).unwrap();

    let mut sstable_index_iter = sstable_index.indices.iter();
    assert::equal(sstable.next(), (b"hello".to_vec(), b"world".to_vec()));
    assert::equal(sstable_index_iter.next(), &(b"hello".to_vec(), 0));

    assert::equal(sstable.next(), (b"foo".to_vec(), b"bar".to_vec()));
    assert::equal(sstable_index_iter.next(), &(b"foo".to_vec(), 12));

    assert::none(&sstable.next());
    assert::none(&sstable_index_iter.next());
  }

  #[test]
  fn test_index_bytes_binary_search() {
    let _lock = setup::sequential();
    fs::remove_file(TEST_FILE_NAME).unwrap_or_default();
    fs::remove_file(TEST_INDEX_FILE_NAME).unwrap_or_default();

    // Should create index file
    let mut writer = SSTableWriterBuilder::new(TEST_FILE_NAME).build().unwrap();
    writer.write((b"baz", b"qux")).unwrap();
    writer.write((b"corge", b"grault")).unwrap();
    writer.write((b"foo", b"bar")).unwrap();
    writer.write((b"garply", b"waldo")).unwrap();
    writer.write((b"hello", b"world")).unwrap();
    writer.write((b"quux", b"quuz")).unwrap();
    writer.close().unwrap();

    // Should use index file
    let mut sstable_index = SSTableIndex::<Vec<u8>>::from_path(TEST_INDEX_FILE_NAME).unwrap();
    let a = sstable_index
      .indices
      .binary_search_by_key(&b"hello".as_slice(), |(k, _)| k);
    assert::equal(a.unwrap(), 4);

    cbor_sort(&mut sstable_index.indices);
    let b = cbor_binary_search_first(&sstable_index.indices, &b"hello".as_slice());
    assert::equal(b.unwrap(), 4);
  }

  #[test]
  fn test_index_string_binary_search() {
    let _lock = setup::sequential();
    fs::remove_file(TEST_FILE_NAME).unwrap_or_default();
    fs::remove_file(TEST_INDEX_FILE_NAME).unwrap_or_default();

    // Should create index file
    let mut writer = SSTableWriterBuilder::new(TEST_FILE_NAME).build().unwrap();
    writer.write(("baz", "qux")).unwrap();
    writer.write(("corge", "grault")).unwrap();
    writer.write(("foo", "bar")).unwrap();
    writer.write(("garply", "waldo")).unwrap();
    writer.write(("hello", "world")).unwrap();
    writer.write(("quux", "quuz")).unwrap();
    writer.close().unwrap();

    // Should use index file
    let mut sstable_index = SSTableIndex::<String>::from_path(TEST_INDEX_FILE_NAME).unwrap();
    let a = sstable_index.indices.binary_search_by_key(&"hello", |(k, _)| k);
    assert::equal(a, 4);

    cbor_sort(&mut sstable_index.indices);
    let b = cbor_binary_search_first(&sstable_index.indices, &"hello");
    assert::equal(b, 4);
  }

  #[test]
  fn test_index_u64_binary_search() {
    let _lock = setup::sequential();
    fs::remove_file(TEST_FILE_NAME).unwrap_or_default();
    fs::remove_file(TEST_INDEX_FILE_NAME).unwrap_or_default();

    // Should create index file
    let mut writer = SSTableWriterBuilder::new(TEST_FILE_NAME).build().unwrap();
    writer.write((1, b"baz")).unwrap();
    writer.write((2, b"corge")).unwrap();
    writer.write((3, b"foo")).unwrap();
    writer.write((4, b"garply")).unwrap();
    writer.write((5, b"hello")).unwrap();
    writer.write((6, b"quux")).unwrap();
    writer.close().unwrap();

    // Should use index file
    let mut sstable_index = SSTableIndex::<u64>::from_path(TEST_INDEX_FILE_NAME).unwrap();
    let a = sstable_index.indices.binary_search_by_key(&5, |(k, _)| *k);
    assert::equal(a, 4);

    cbor_sort(&mut sstable_index.indices);
    let b = cbor_binary_search_first(&sstable_index.indices, &5);
    assert::equal(b, 4);
  }

  #[test]
  fn test_index_bytes_binary_search_with_duplicates() {
    let _lock = setup::sequential();
    fs::remove_file(TEST_FILE_NAME).unwrap_or_default();
    fs::remove_file(TEST_INDEX_FILE_NAME).unwrap_or_default();

    // Should create index file
    let mut sstable_writer = SSTableWriterBuilder::new(TEST_FILE_NAME).build().unwrap();
    sstable_writer.write((b"baz", b"qux")).unwrap();
    for _ in 0..5 {
      sstable_writer.write((b"foo", b"bar")).unwrap();
    }
    sstable_writer.write((b"garply", b"waldo")).unwrap();
    sstable_writer.write((b"hello", b"world")).unwrap();
    sstable_writer.write((b"quux", b"quuz")).unwrap();
    sstable_writer.close().unwrap();

    // Should use index file
    let mut sstable_index = SSTableIndex::<Vec<u8>>::from_path(TEST_INDEX_FILE_NAME).unwrap();
    let a = sstable_index
      .indices
      .binary_search_by_key(&b"foo".as_slice(), |(k, _)| k);
    assert::equal(a, 4);

    // Use CBOR sort and search to find the first instance of "foo" in the index file. This is
    // useful for finding the first instance of a key in the index file, which is then useful for
    // finding the first instance of a key in the data file.
    cbor_sort(&mut sstable_index.indices);
    let b = cbor_binary_search_first(&sstable_index.indices, &b"foo".as_slice());
    assert::equal(b, 1);

    let mut sstable = SSTableReader::<(Vec<u8>, Vec<u8>)>::from_path(TEST_FILE_NAME).unwrap();

    sstable
      .seek(SeekFrom::Start(sstable_index.indices[b.unwrap()].1))
      .unwrap();
    // We can read five "foo" entries from the data file, because we wrote five "foo" entries to
    // the data file. The index search always refers to the first "foo" entry in the index file.
    for _ in 0..5 {
      assert::equal(sstable.next(), (b"foo".to_vec(), b"bar".to_vec()));
    }
  }
}
