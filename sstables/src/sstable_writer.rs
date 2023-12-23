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
//! the type of data that is being written. The `Append` trait is implemented for several combinations
//! of types, but you can also use them as examples to extend your own.
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
//! use sstables::sstable_writer::Append;
//!
//! let mut writer = SSTableWriterBuilder::new("test")
//!   .build()
//!   .unwrap();
//!
//! writer.append("hello").unwrap();
//! writer.append("world").unwrap();
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

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Result, Seek, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use crate::cbor::{write_cbor_bytes, write_cbor_head, write_cbor_text, MajorType};

const DEFAULT_BUFFER_SIZE: usize = 8 * 1024;

/// Given a path, add "index" to the front of the path's extension. If it has no extension, add ".index".
///
fn create_index_path(path: &Path) -> PathBuf {
  let mut path = path.to_path_buf();
  let ext_maybe = path.extension();
  match ext_maybe {
    Some(ext) => path.set_extension(format!("index.{}", ext.to_str().unwrap())),
    None => path.set_extension("index"),
  };

  path
}

/// Builder for `SSTableWriter`
///
/// # Example
///
/// ```
/// use sstables::sstable_writer::SSTableWriterBuilder;
/// use sstables::sstable_writer::Append;
///
/// let mut writer = SSTableWriterBuilder::new("test")
///  .build()
///  .unwrap();
///
/// writer.append("hello").unwrap();
/// writer.append("world").unwrap();
/// ```
///
/// # Example
///
/// ```
/// use sstables::sstable_writer::SSTableWriterBuilder;
///
/// let mut writer = SSTableWriterBuilder::new("test")
///  .index_writer_path("test.index")
///  .buffer_size(1024)
///  .build()
///  .unwrap();
///
/// writer.append("hello").unwrap();
/// writer.append("world").unwrap();
/// ```
pub struct SSTableWriterBuilder<T> {
  data_writer_path: PathBuf,
  index_writer_path: Option<PathBuf>,
  buffer_size: Option<usize>,
  phantom: PhantomData<T>,
}

impl<T> SSTableWriterBuilder<T> {
  pub fn new<P: Into<PathBuf>>(data_writer_path: P) -> Self {
    SSTableWriterBuilder {
      data_writer_path: data_writer_path.into(),
      index_writer_path: None,
      buffer_size: None,
      phantom: PhantomData,
    }
  }

  pub fn index_writer_path<P: Into<PathBuf>>(mut self, path: P) -> Self {
    self.index_writer_path = Some(path.into());
    self
  }

  pub fn buffer_size(mut self, size: usize) -> Self {
    self.buffer_size = Some(size);
    self
  }

  pub fn build(self) -> io::Result<SSTableWriter<T>> {
    let buffer_size = self.buffer_size.unwrap_or(DEFAULT_BUFFER_SIZE);
    let data_writer_path = self.data_writer_path;
    let data_writer = BufWriter::with_capacity(
      buffer_size,
      OpenOptions::new().create(true).append(true).open(&data_writer_path)?,
    );

    let index_writer_path = self
      .index_writer_path
      .unwrap_or_else(|| create_index_path(&data_writer_path));

    let index_writer = BufWriter::with_capacity(
      buffer_size,
      OpenOptions::new().create(true).append(true).open(&index_writer_path)?,
    );

    Ok(SSTableWriter {
      data_writer_path,
      data_writer,
      index_writer_path,
      index_writer,
      phantom: std::marker::PhantomData,
    })
  }
}

pub struct SSTableWriter<T> {
  pub data_writer_path: PathBuf,
  pub data_writer: BufWriter<File>,
  pub index_writer_path: PathBuf,
  pub index_writer: BufWriter<File>,
  phantom: std::marker::PhantomData<T>,
}

impl<T> SSTableWriter<T> {
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

/// Trait for appending entries to an SSTableWriter
trait Append<T> {
  fn append(&mut self, entry: T) -> io::Result<()>;
}

impl Append<&[u8]> for SSTableWriter<&[u8]> {
  /// Appends a value into the data file, and the file offset position into the index.
  fn append(&mut self, entry: &[u8]) -> io::Result<()> {
    let data_byte_offset = self.data_writer.stream_position()?;

    write_cbor_bytes(&mut self.data_writer, entry)
      .and_then(|_| write_cbor_head(&mut self.index_writer, MajorType::UnsignedInteger, data_byte_offset))
  }
}

impl Append<&str> for SSTableWriter<&str> {
  /// Appends a value into the data file, and the file offset position into the index.
  fn append(&mut self, entry: &str) -> io::Result<()> {
    let data_byte_offset = self.data_writer.stream_position()?;

    write_cbor_text(&mut self.data_writer, entry)
      .and_then(|_| write_cbor_head(&mut self.index_writer, MajorType::UnsignedInteger, data_byte_offset))
  }
}

impl Append<(&[u8], &[u8])> for SSTableWriter<(&[u8], &[u8])> {
  /// Appends a key-value pair into the data file, and the key with the file offset position into the index.
  fn append(&mut self, entry: (&[u8], &[u8])) -> io::Result<()> {
    let data_byte_offset = self.data_writer.stream_position()?;

    write_cbor_bytes(&mut self.data_writer, entry.0)
      .and_then(|_| write_cbor_bytes(&mut self.data_writer, entry.1))
      .and_then(|_| write_cbor_bytes(&mut self.index_writer, entry.0))
      .and_then(|_| write_cbor_head(&mut self.index_writer, MajorType::UnsignedInteger, data_byte_offset))
  }
}

impl Append<(&str, &str)> for SSTableWriter<(&str, &str)> {
  /// Appends a key-value pair into the data file, and the key with the file offset position into the index.
  fn append(&mut self, entry: (&str, &str)) -> io::Result<()> {
    let data_byte_offset = self.data_writer.stream_position()?;

    write_cbor_text(&mut self.data_writer, entry.0)
      .and_then(|_| write_cbor_text(&mut self.data_writer, entry.1))
      .and_then(|_| write_cbor_text(&mut self.index_writer, entry.0))
      .and_then(|_| write_cbor_head(&mut self.index_writer, MajorType::UnsignedInteger, data_byte_offset))
  }
}

impl Append<(u64, &[u8])> for SSTableWriter<(u64, &[u8])> {
  /// Appends a key-value pair into the data file, and the key with the file offset position into the index.
  fn append(&mut self, entry: (u64, &[u8])) -> io::Result<()> {
    let data_byte_offset = self.data_writer.stream_position()?;

    write_cbor_head(&mut self.data_writer, MajorType::UnsignedInteger, entry.0)
      .and_then(|_| write_cbor_bytes(&mut self.data_writer, entry.1))
      .and_then(|_| write_cbor_head(&mut self.index_writer, MajorType::UnsignedInteger, entry.0))
      .and_then(|_| write_cbor_head(&mut self.index_writer, MajorType::UnsignedInteger, data_byte_offset))
  }
}

#[cfg(test)]
mod tests {
  use std::fs;

  use crate::cbor::{CborSearch, CborSort};
  use crate::sstable_reader::{SSTableIndexReader, SSTableIndexReaderTrait, SSTableReader};
  use crate::sstable_writer::SSTableWriterBuilder;

  use super::*;

  const TEST_FILE_NAME: &str = ".tmp/test.sst";
  const TEST_INDEX_FILE_NAME: &str = ".tmp/test.index.sst";

  #[test]
  fn test_append_bytes() {
    fs::remove_file(TEST_FILE_NAME).unwrap_or_default();
    fs::remove_file(TEST_INDEX_FILE_NAME).unwrap_or_default();

    let mut writer = SSTableWriterBuilder::new(TEST_FILE_NAME).build().unwrap();

    writer.append(b"hello".as_slice()).unwrap();
    writer.append(b"world").unwrap();
    writer.close().unwrap();

    let mut reader = SSTableReader::<Vec<u8>>::from_path(TEST_FILE_NAME).unwrap();
    assert_eq!(reader.next().unwrap().unwrap(), b"hello");
    assert_eq!(reader.next().unwrap().unwrap(), b"world");
    assert!(reader.next().is_none());
  }

  #[test]
  fn test_append_strings() {
    fs::remove_file(TEST_FILE_NAME).unwrap_or_default();
    fs::remove_file(TEST_INDEX_FILE_NAME).unwrap_or_default();

    let mut writer = SSTableWriterBuilder::new(TEST_FILE_NAME).build().unwrap();

    writer.append("hello").unwrap();
    writer.append("world").unwrap();
    writer.close().unwrap();

    let mut reader = SSTableReader::<String>::from_path(TEST_FILE_NAME).unwrap();
    assert_eq!(reader.next().unwrap().unwrap(), "hello");
    assert_eq!(reader.next().unwrap().unwrap(), "world");
    assert!(reader.next().is_none());
  }

  #[test]
  fn test_append_string_tuple() {
    fs::remove_file(TEST_FILE_NAME).unwrap_or_default();
    fs::remove_file(TEST_INDEX_FILE_NAME).unwrap_or_default();

    let mut writer = SSTableWriterBuilder::new(TEST_FILE_NAME).build().unwrap();

    writer.append(("hello", "world")).unwrap();
    writer.close().unwrap();

    let mut reader = SSTableReader::<(String, String)>::from_path(TEST_FILE_NAME).unwrap();
    assert_eq!(
      reader.next().unwrap().unwrap(),
      ("hello".to_string(), "world".to_string())
    );
    assert!(reader.next().is_none());
  }

  #[test]
  fn test_append_bytes_tuple() {
    fs::remove_file(TEST_FILE_NAME).unwrap_or_default();
    fs::remove_file(TEST_INDEX_FILE_NAME).unwrap_or_default();

    let mut writer = SSTableWriterBuilder::new(TEST_FILE_NAME).build().unwrap();

    writer.append((b"hello".as_slice(), b"world".as_slice())).unwrap();
    writer.close().unwrap();

    let mut reader = SSTableReader::<(Vec<u8>, Vec<u8>)>::from_path(TEST_FILE_NAME).unwrap();
    assert_eq!(reader.next().unwrap().unwrap(), (b"hello".to_vec(), b"world".to_vec()));
    assert!(reader.next().is_none());
  }

  #[test]
  fn test_append_bytes_with_index() {
    fs::remove_file(TEST_FILE_NAME).unwrap_or_default();
    fs::remove_file(TEST_INDEX_FILE_NAME).unwrap_or_default();

    // Should create index file
    let mut writer = SSTableWriterBuilder::new(TEST_FILE_NAME).build().unwrap();
    writer.append(b"hello".as_slice()).unwrap();
    writer.append(b"world").unwrap();
    writer.close().unwrap();

    // Should use index file
    let mut sstable = SSTableReader::<Vec<u8>>::from_path(TEST_FILE_NAME).unwrap();
    let sstable_index = SSTableIndexReader::<u64>::from_path(TEST_INDEX_FILE_NAME).unwrap();

    let mut sstable_index_iter = sstable_index.indices.iter();
    let entry = sstable.next().unwrap().unwrap();
    let offset = sstable_index_iter.next().unwrap();
    assert_eq!(entry, b"hello");
    assert_eq!(offset, &0);

    let entry = sstable.next().unwrap().unwrap();
    let offset = sstable_index_iter.next().unwrap();
    assert_eq!(entry, b"world");
    assert_eq!(offset, &6);

    assert!(sstable.next().is_none());
    assert!(sstable_index_iter.next().is_none());
  }

  #[test]
  fn test_append_string_with_index() {
    fs::remove_file(TEST_FILE_NAME).unwrap_or_default();
    fs::remove_file(TEST_INDEX_FILE_NAME).unwrap_or_default();

    // Should create index file
    let mut writer = SSTableWriterBuilder::new(TEST_FILE_NAME).build().unwrap();
    writer.append("hello").unwrap();
    writer.append("world").unwrap();
    writer.close().unwrap();

    // Should use index file
    let mut sstable = SSTableReader::<String>::from_path(TEST_FILE_NAME).unwrap();
    let sstable_index = SSTableIndexReader::<u64>::from_path(TEST_INDEX_FILE_NAME).unwrap();

    let mut sstable_index_iter = sstable_index.indices.iter();
    let entry = sstable.next().unwrap().unwrap();
    let offset = sstable_index_iter.next().unwrap();
    assert_eq!(entry, "hello");
    assert_eq!(offset, &0);

    let entry = sstable.next().unwrap().unwrap();
    let offset = sstable_index_iter.next().unwrap();
    assert_eq!(entry, "world");
    assert_eq!(offset, &6);

    assert!(sstable.next().is_none());
    assert!(sstable_index_iter.next().is_none());
  }

  #[test]
  fn test_append_string_tuple_with_index() {
    fs::remove_file(TEST_FILE_NAME).unwrap_or_default();
    fs::remove_file(TEST_INDEX_FILE_NAME).unwrap_or_default();

    // Should create index file
    let mut writer = SSTableWriterBuilder::new(TEST_FILE_NAME).build().unwrap();
    writer.append(("hello", "world")).unwrap();
    writer.close().unwrap();

    // Should use index file
    let mut sstable = SSTableReader::<(String, String)>::from_path(TEST_FILE_NAME).unwrap();
    let sstable_index = SSTableIndexReader::<(String, u64)>::from_path(TEST_INDEX_FILE_NAME).unwrap();

    let mut sstable_index_iter = sstable_index.indices.iter();
    let entry = sstable.next().unwrap().unwrap();
    let offset = sstable_index_iter.next().unwrap();
    assert_eq!(entry, ("hello".to_string(), "world".to_string()));
    assert_eq!(offset, &("hello".to_string(), 0));

    assert!(sstable.next().is_none());
    assert!(sstable_index_iter.next().is_none());
  }

  #[test]
  fn test_append_bytes_tuple_with_index() {
    fs::remove_file(TEST_FILE_NAME).unwrap_or_default();
    fs::remove_file(TEST_INDEX_FILE_NAME).unwrap_or_default();

    // Should create index file
    let mut writer = SSTableWriterBuilder::new(TEST_FILE_NAME).build().unwrap();
    writer.append((b"hello".as_slice(), b"world".as_slice())).unwrap();
    writer.append((b"foo".as_slice(), b"bar".as_slice())).unwrap();
    writer.close().unwrap();

    // Should use index file
    let mut sstable = SSTableReader::<(Vec<u8>, Vec<u8>)>::from_path(TEST_FILE_NAME).unwrap();
    let sstable_index = SSTableIndexReader::<(Vec<u8>, u64)>::from_path(TEST_INDEX_FILE_NAME).unwrap();

    let mut sstable_index_iter = sstable_index.indices.iter();
    let entry = sstable.next().unwrap().unwrap();
    let offset = sstable_index_iter.next().unwrap();
    assert_eq!(entry, (b"hello".to_vec(), b"world".to_vec()));
    assert_eq!(offset, &(b"hello".to_vec(), 0));

    let entry = sstable.next().unwrap().unwrap();
    let offset = sstable_index_iter.next().unwrap();
    assert_eq!(entry, (b"foo".to_vec(), b"bar".to_vec()));
    assert_eq!(offset, &(b"foo".to_vec(), 12));

    assert!(sstable.next().is_none());
    assert!(sstable_index_iter.next().is_none());
  }

  #[test]
  fn test_index_bytes_binary_search() {
    fs::remove_file(TEST_FILE_NAME).unwrap_or_default();
    fs::remove_file(TEST_INDEX_FILE_NAME).unwrap_or_default();

    // Should create index file
    let mut writer = SSTableWriterBuilder::<(&[u8], &[u8])>::new(TEST_FILE_NAME)
      .build()
      .unwrap();
    writer.append((b"baz", b"qux")).unwrap();
    writer.append((b"corge", b"grault")).unwrap();
    writer.append((b"foo", b"bar")).unwrap();
    writer.append((b"garply", b"waldo")).unwrap();
    writer.append((b"hello", b"world")).unwrap();
    writer.append((b"quux", b"quuz")).unwrap();
    writer.close().unwrap();

    // Should use index file
    let mut sstable_index = SSTableIndexReader::<(Vec<u8>, u64)>::from_path(TEST_INDEX_FILE_NAME).unwrap();
    let a = sstable_index
      .indices
      .binary_search_by_key(&b"hello".as_slice(), |(k, _)| k);
    assert_eq!(a.unwrap(), 4);

    sstable_index.indices.cbor_sort();
    let b = sstable_index.indices.cbor_search(b"hello");
    assert_eq!(b.unwrap(), 4);
  }

  #[test]
  fn test_index_string_binary_search() {
    fs::remove_file(TEST_FILE_NAME).unwrap_or_default();
    fs::remove_file(TEST_INDEX_FILE_NAME).unwrap_or_default();

    // Should create index file
    let mut writer = SSTableWriterBuilder::<(&str, &str)>::new(TEST_FILE_NAME)
      .build()
      .unwrap();
    writer.append(("baz", "qux")).unwrap();
    writer.append(("corge", "grault")).unwrap();
    writer.append(("foo", "bar")).unwrap();
    writer.append(("garply", "waldo")).unwrap();
    writer.append(("hello", "world")).unwrap();
    writer.append(("quux", "quuz")).unwrap();
    writer.close().unwrap();

    // Should use index file
    let mut sstable_index = SSTableIndexReader::<(String, u64)>::from_path(TEST_INDEX_FILE_NAME).unwrap();
    let a = sstable_index.indices.binary_search_by_key(&"hello", |(k, _)| k);
    assert_eq!(a.unwrap(), 4);

    sstable_index.indices.cbor_sort();
    let b = sstable_index.indices.cbor_search("hello");
    assert_eq!(b.unwrap(), 4);
  }

  #[test]
  fn test_index_u64_binary_search() {
    fs::remove_file(TEST_FILE_NAME).unwrap_or_default();
    fs::remove_file(TEST_INDEX_FILE_NAME).unwrap_or_default();

    // Should create index file
    let mut writer = SSTableWriterBuilder::<(u64, &[u8])>::new(TEST_FILE_NAME)
      .build()
      .unwrap();
    writer.append((1, b"baz")).unwrap();
    writer.append((2, b"corge")).unwrap();
    writer.append((3, b"foo")).unwrap();
    writer.append((4, b"garply")).unwrap();
    writer.append((5, b"hello")).unwrap();
    writer.append((6, b"quux")).unwrap();
    writer.close().unwrap();

    // Should use index file
    let mut sstable_index = SSTableIndexReader::<(u64, u64)>::from_path(TEST_INDEX_FILE_NAME).unwrap();
    let a = sstable_index.indices.binary_search_by_key(&5, |(k, _)| *k);
    assert_eq!(a.unwrap(), 4);

    sstable_index.indices.cbor_sort();
    let b = sstable_index.indices.cbor_search(&5);
    assert_eq!(b.unwrap(), 4);
  }

  #[test]
  fn test_index_bytes_binary_search_with_duplicates() {
    fs::remove_file(TEST_FILE_NAME).unwrap_or_default();
    fs::remove_file(TEST_INDEX_FILE_NAME).unwrap_or_default();

    // Should create index file
    let mut sstable_writer = SSTableWriterBuilder::<(&[u8], &[u8])>::new(TEST_FILE_NAME)
      .build()
      .unwrap();
    sstable_writer.append((b"baz", b"qux")).unwrap();
    for _ in 0..5 {
      sstable_writer.append((b"foo", b"bar")).unwrap();
    }
    sstable_writer.append((b"garply", b"waldo")).unwrap();
    sstable_writer.append((b"hello", b"world")).unwrap();
    sstable_writer.append((b"quux", b"quuz")).unwrap();
    sstable_writer.close().unwrap();

    // Should use index file
    let mut sstable_index = SSTableIndexReader::<(Vec<u8>, u64)>::from_path(TEST_INDEX_FILE_NAME).unwrap();
    let a = sstable_index
      .indices
      .binary_search_by_key(&b"foo".as_slice(), |(k, _)| k);
    assert_eq!(a.unwrap(), 4);

    // Use CBOR sort and search to find the first instance of "foo" in the index file. This is
    // useful for finding the first instance of a key in the index file, which is then useful for
    // finding the first instance of a key in the data file.
    sstable_index.indices.cbor_sort();
    let b = sstable_index.indices.cbor_search(b"foo");
    assert_eq!(b.unwrap(), 1);

    let mut sstable = SSTableReader::<(Vec<u8>, Vec<u8>)>::from_path(TEST_FILE_NAME).unwrap();

    sstable.seek(sstable_index.indices[b.unwrap()].1).unwrap();
    // We can read five "foo" entries from the data file, because we wrote five "foo" entries to
    // the data file. The index search always refers to the first "foo" entry in the index file.
    for _ in 0..5 {
      assert_eq!(sstable.next().unwrap().unwrap(), (b"foo".to_vec(), b"bar".to_vec()));
    }
  }
}
