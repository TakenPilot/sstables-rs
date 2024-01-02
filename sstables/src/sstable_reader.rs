use crate::cbor::{read_cbor_u64, CborRead};
use crate::traits::FromPath;
use std::fs::File;
use std::io::{self, BufReader, Seek};
use std::path::Path;

/// Reads and holds the indices of an SSTable in memory, so that we can seek to
/// the correct position in the data file. We can perform binary search on the
/// index to find the correct position.
pub struct SSTableIndex<K> {
  pub indices: Vec<(K, u64)>,
}

/// Implementation of FromPath for SSTableIndex for any type that implements
/// CborRead. The index is stored as a series of CBOR-encoded tuples of
/// (key, offset). The index is read entirely into memory when the SSTableIndex
/// is created.
impl<T> FromPath<T> for SSTableIndex<T>
where
  io::BufReader<File>: CborRead<T>,
{
  fn from_path<P: AsRef<Path>>(path: P) -> io::Result<Self> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut indices = Vec::new();

    // Read the entire file into memory.
    loop {
      let result = match reader
        .cbor_read()
        .and_then(|k| read_cbor_u64(&mut reader).map(|v| (k, v)))
      {
        Ok(x) => Ok(x),
        Err(e) => match e.kind() {
          io::ErrorKind::UnexpectedEof => break,
          _ => Err(e),
        },
      }?;

      indices.push(result);
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

/// Implementation of Seek for SSTableReader. The seek operation is delegated to
/// the underlying reader.
impl<T> Seek for SSTableReader<T> {
  fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
    self.data_reader.seek(pos)
  }
}

/// Implementation of Iterator for SSTableReader for any type that implements
/// CborRead. The iterator returns a series of tuples of (key, value). The
/// iterator will return an error if the underlying reader returns an error, or
/// None if the end of the file is reached.
impl<T> Iterator for SSTableReader<(T, T)>
where
  io::BufReader<File>: CborRead<T>,
{
  type Item = io::Result<(T, T)>;

  fn next(&mut self) -> Option<Self::Item> {
    let reader = &mut self.data_reader;
    let result = reader.cbor_read().and_then(|k| reader.cbor_read().map(|v| (k, v)));

    match result {
      Ok(x) => Some(Ok(x)),
      Err(e) => match e.kind() {
        io::ErrorKind::UnexpectedEof => None,
        _ => Some(Err(e)),
      },
    }
  }
}
