use cbor::{read_cbor_array, Cbor, CborKey};
use itertools::Itertools;
use std::fs::OpenOptions;
use std::io::{Cursor, Error, ErrorKind, Read, Result};

#[derive(Debug)]
pub struct SSTableIndexReader {}

impl SSTableIndexReader {
  pub fn read_index<'a>(path: &str) -> Result<Vec<(CborKey<'a>, u64)>> {
    let mut index_file = OpenOptions::new()
      .read(true)
      .create(false)
      .append(false)
      .write(false)
      .open(path)?;

    let buf = &mut vec![0; 0];
    index_file.read_to_end(buf)?;
    let len = buf.len() as u64;
    let cursor = &mut Cursor::new(buf);
    let mut list: Vec<(CborKey<'a>, u64)> = Vec::new();

    while cursor.position() < len {
      let Some((key, Cbor::Integer(byte_offset))) = read_cbor_array(cursor)?.into_iter().collect_tuple() else {
        return Err(Error::new(ErrorKind::InvalidData, "Expected only [key, byte_offset] in index file."));
      };
      list.push((key.try_into()?, byte_offset.try_into()?));
    }

    Ok(list)
  }
}
