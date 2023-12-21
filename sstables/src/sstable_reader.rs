use cbor::{read_cbor_array, Cbor, CborKey};
use itertools::Itertools;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Error, ErrorKind, Result};

#[derive(Debug)]
pub struct SSTableReader {
  pub reader: BufReader<File>,
}

impl SSTableReader {
  pub fn from_path(path: &str) -> Result<Self> {
    let file = OpenOptions::new()
      .read(true)
      .create(false)
      .append(false)
      .write(false)
      .open(path)?;
    let reader = BufReader::new(file);

    Ok(Self { reader })
  }

  /// Read the next KV<K,V> from the SSTable.
  /// Will return None if none remaining.
  pub fn read_next<'a>(&mut self) -> Result<Option<(CborKey<'a>, Cbor<'a>)>> {
    if let Some((k, v)) = read_cbor_array(&mut self.reader)?
      .into_iter()
      .collect_tuple()
    {
      Ok(Some((k.try_into()?, v)))
    } else {
      Err(Error::new(
        ErrorKind::InvalidData,
        "Expected an array with two elements",
      ))
    }
  }
}
