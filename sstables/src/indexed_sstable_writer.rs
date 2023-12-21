use cbor::{write_cbor_array, Cbor, CborKey};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Result, Seek, Write};
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct IndexedSSTableWriter {
  pub data_writer_path: PathBuf,
  pub data_writer: BufWriter<File>,
  pub index_writer_path: PathBuf,
  pub index_writer: BufWriter<File>,
}

impl IndexedSSTableWriter {
  /// Create a new file if it doesn't exist.
  /// Open a new writer for appending only.
  pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
    let path_ref = path.as_ref();

    let data_writer_path = path_ref.with_extension("cbor");
    let data_writer = BufWriter::new(
      OpenOptions::new()
        .create(true)
        .append(true)
        .open(&data_writer_path)?,
    );

    let index_writer_path = path_ref.with_extension("cbor-index");
    let index_writer = BufWriter::new(
      OpenOptions::new()
        .create(true)
        .append(true)
        .open(&index_writer_path)?,
    );

    Ok(Self {
      data_writer_path,
      data_writer,
      index_writer_path,
      index_writer,
    })
  }

  pub fn write_next<'b, K: AsRef<CborKey<'b>>, V: AsRef<Cbor<'b>>>(
    &mut self,
    key: K,
    value: V,
  ) -> Result<()> {
    let data_byte_offset = self.data_writer.stream_position()?;

    let key = key.as_ref();

    let data: &[&Cbor<'_>; 2] = &[&key.as_cbor(), value.as_ref()];
    write_cbor_array(data, &mut self.data_writer)?;

    Cbor::array([(key.as_cbor().as_ref()), value.as_ref()]).write_cbor(&mut self.data_writer)?;

    let index: &[&Cbor<'_>; 2] = &[&key.as_cbor(), &data_byte_offset.into()];
    write_cbor_array(index, &mut self.index_writer)?;

    Ok(())
  }

  pub fn flush(&mut self) -> Result<()> {
    self.data_writer.flush()?;
    self.index_writer.flush()
  }

  /// Consumes the writer, returning all inner files.
  pub fn into_files(mut self) -> Result<Vec<(PathBuf, File)>> {
    // necessary because we're dropping the buffers.
    self.data_writer.flush()?;
    self.index_writer.flush()?;

    Ok(vec![
      (self.data_writer_path, self.data_writer.into_inner()?),
      (self.index_writer_path, self.index_writer.into_inner()?),
    ])
  }
}
