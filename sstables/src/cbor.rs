use std::io::{self, Write};

use crate::cbor_types::{write_cbor_head, MajorType};

pub fn write_cbor_bytes<W: Write>(bytes: &[u8], writer: &mut W) -> io::Result<()> {
  write_cbor_head(MajorType::Bytes, bytes.len() as u64, writer)?;
  writer.write_all(bytes)
}
