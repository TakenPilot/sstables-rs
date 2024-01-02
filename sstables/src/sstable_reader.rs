use crate::cbor::{read_cbor_bytes, read_cbor_text, read_cbor_u64};
use crate::traits::FromPath;
use std::fs::{self, File};
use std::io::{self, BufReader, Seek};
use std::path::Path;

/// Reads and holds the indices of an SSTable in memory, so that we can seek to
/// the correct position in the data file. We can perform binary search on the
/// index to find the correct position. There are two implementations of this
/// trait: one for tuples of (key, offset) and one for a simple series of
/// offsets.
pub struct SSTableIndex<K> {
  pub indices: Vec<(K, u64)>,
}

impl FromPath<Vec<u8>> for SSTableIndex<Vec<u8>> {
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

impl FromPath<String> for SSTableIndex<String> {
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

impl FromPath<u64> for SSTableIndex<u64> {
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

impl<T> FromPath<T> for SSTableReader<T> {
  fn from_path<P: AsRef<Path>>(path: P) -> io::Result<Self> {
    Ok(SSTableReader {
      data_reader: BufReader::new(File::open(path)?),
      phantom: std::marker::PhantomData,
    })
  }
}

impl<T> Seek for SSTableReader<T> {
  fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
    self.data_reader.seek(pos)
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

#[cfg(test)]
mod tests {
  use super::*;
  use common_testing::{assert, setup};

  #[test]
  fn test_sstable_reader_bytes() {
    let _lock = setup::sequential();
    let fixture_path = "./.tmp/test.sst";

    let mut reader = SSTableReader::<Vec<u8>>::from_path(fixture_path).unwrap();
    assert::equal(reader.next(), vec![67]);
    assert::equal(reader.next(), vec![97, 122]);
    assert::equal(reader.next(), vec![69, 99]);
    // assert::none(&reader.next());
  }
}
