use cbor::{write_cbor_array, Cbor, CborKey};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Result};

#[derive(Debug)]
pub struct SSTableWriter {
  pub writer: BufWriter<File>,
}

impl SSTableWriter {
  pub fn from_path(path: &str) -> Result<Self> {
    // Create a new file if it doesn't exist.
    // Open a new writer for appending only.
    let writer = BufWriter::new(OpenOptions::new().create(true).append(true).open(path)?);

    Ok(Self { writer })
  }

  pub fn write_next<'b, K: AsRef<CborKey<'b>>, V: AsRef<Cbor<'b>>>(
    &mut self,
    key: K,
    value: V,
  ) -> Result<()> {
    let key = key.as_ref();
    let data = &[&key.as_cbor(), value.as_ref()];
    write_cbor_array(data, &mut self.writer)?;
    Ok(())
  }
}
