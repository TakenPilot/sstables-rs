use std::fs::{self, File};
use std::io::{self, BufReader, Seek};
use std::path::Path;

use crate::cbor::{read_cbor_bytes, read_cbor_text, read_cbor_u64};

/// Reads and holds the indices of an SSTable in memory, so that we can seek to
/// the correct position in the data file. We can perform binary search on the
/// index to find the correct position. There are two implementations of this
/// trait: one for tuples of (key, offset) and one for a simple series of
/// offsets.
pub struct SSTableIndex<T> {
  pub indices: Vec<T>,
}

/// A trait for reading an SSTable index from a file path. There are multiple
/// implementations of this trait, one for each type of index.
pub trait SSTableIndexFromPath<T> {
  fn from_path<P: AsRef<std::path::Path>>(path: P) -> io::Result<Self>
  where
    Self: std::marker::Sized;
}

impl SSTableIndexFromPath<u64> for SSTableIndex<u64> {
  fn from_path<P: AsRef<Path>>(path: P) -> io::Result<Self> {
    let buffer = fs::read(path)?;
    let len = buffer.len() as u64;
    let mut cursor = io::Cursor::new(buffer);
    let mut indices = Vec::new();

    while cursor.position() < len {
      indices.push(read_cbor_u64(&mut cursor)?);
    }

    Ok(SSTableIndex { indices })
  }
}

impl SSTableIndexFromPath<(Vec<u8>, u64)> for SSTableIndex<(Vec<u8>, u64)> {
  fn from_path<P: AsRef<Path>>(path: P) -> io::Result<Self> {
    let buffer = fs::read(path)?;
    let len = buffer.len() as u64;
    let mut cursor = io::Cursor::new(buffer);
    let mut indices = Vec::new();

    while cursor.position() < len {
      indices.push((read_cbor_bytes(&mut cursor)?, read_cbor_u64(&mut cursor)?));
    }

    Ok(SSTableIndex { indices })
  }
}

impl SSTableIndexFromPath<(String, u64)> for SSTableIndex<(String, u64)> {
  fn from_path<P: AsRef<Path>>(path: P) -> io::Result<Self> {
    let buffer = fs::read(path)?;
    let len = buffer.len() as u64;
    let mut cursor = io::Cursor::new(buffer);
    let mut indices = Vec::new();

    while cursor.position() < len {
      indices.push((read_cbor_text(&mut cursor)?, read_cbor_u64(&mut cursor)?));
    }

    Ok(SSTableIndex { indices })
  }
}

impl SSTableIndexFromPath<(u64, u64)> for SSTableIndex<(u64, u64)> {
  fn from_path<P: AsRef<Path>>(path: P) -> io::Result<Self> {
    let buffer = fs::read(path)?;
    let len = buffer.len() as u64;
    let mut cursor = io::Cursor::new(buffer);
    let mut indices = Vec::new();

    while cursor.position() < len {
      indices.push((read_cbor_u64(&mut cursor)?, read_cbor_u64(&mut cursor)?));
    }

    Ok(SSTableIndex { indices })
  }
}

/// A SSTable reader that can read a series of bytes or text from an SSTable.
#[derive(Debug)]
pub struct SSTableReader<T> {
  pub data_reader: BufReader<File>,
  phantom: std::marker::PhantomData<T>,
}

impl<T> SSTableReader<T> {
  pub fn from_path<P: AsRef<Path>>(path: P) -> io::Result<Self> {
    Ok(SSTableReader {
      data_reader: BufReader::new(File::open(path)?),
      phantom: std::marker::PhantomData,
    })
  }

  /// Seeks to the given offset in the data file.
  pub fn seek(&mut self, offset: u64) -> io::Result<u64> {
    self.data_reader.seek(io::SeekFrom::Start(offset))
  }
}

impl Iterator for SSTableReader<(Vec<u8>, Vec<u8>)> {
  type Item = io::Result<(Vec<u8>, Vec<u8>)>;

  fn next(&mut self) -> Option<Self::Item> {
    let result =
      read_cbor_bytes(&mut self.data_reader).and_then(|k| read_cbor_bytes(&mut self.data_reader).map(|v| (k, v)));

    match result {
      Ok((k, v)) => Some(Ok((k, v))),
      Err(e) => match e.kind() {
        io::ErrorKind::UnexpectedEof => None,
        _ => Some(Err(e)),
      },
    }
  }
}

impl Iterator for SSTableReader<(String, String)> {
  type Item = io::Result<(String, String)>;

  fn next(&mut self) -> Option<Self::Item> {
    let result =
      read_cbor_text(&mut self.data_reader).and_then(|k| read_cbor_text(&mut self.data_reader).map(|v| (k, v)));

    match result {
      Ok((k, v)) => Some(Ok((k, v))),
      Err(e) => match e.kind() {
        io::ErrorKind::UnexpectedEof => None,
        _ => Some(Err(e)),
      },
    }
  }
}

impl Iterator for SSTableReader<Vec<u8>> {
  type Item = io::Result<Vec<u8>>;

  fn next(&mut self) -> Option<Self::Item> {
    let result = read_cbor_bytes(&mut self.data_reader);

    match result {
      Ok(v) => Some(Ok(v)),
      Err(e) => match e.kind() {
        io::ErrorKind::UnexpectedEof => None,
        _ => Some(Err(e)),
      },
    }
  }
}

impl Iterator for SSTableReader<String> {
  type Item = io::Result<String>;

  fn next(&mut self) -> Option<Self::Item> {
    let result = read_cbor_text(&mut self.data_reader);

    match result {
      Ok(v) => Some(Ok(v)),
      Err(e) => match e.kind() {
        io::ErrorKind::UnexpectedEof => None,
        _ => Some(Err(e)),
      },
    }
  }
}
