//! Output writer appends key-values to a file or stdout in various append-only formats.

use sstables::{SSTableWriter, SSTableWriterBuilder};
use std::{
  io::{self, BufWriter, Cursor, Stdout, Write},
  path::PathBuf,
};

pub enum OutputDestination {
  File(PathBuf),
  Stdout,
  Cursor(Cursor<Vec<u8>>),
}

pub struct OutputWriterBuilder {
  destination: OutputDestination,
}

/// Builder for OutputWriter for any SSTableWriter that implements Append.
impl OutputWriterBuilder {
  pub fn new(destination: OutputDestination) -> Self {
    OutputWriterBuilder { destination }
  }

  pub fn build(self) -> io::Result<OutputWriter> {
    match self.destination {
      OutputDestination::File(path) => {
        let sstable_writer = SSTableWriterBuilder::new(path).build()?;
        Ok(OutputWriter::SSTable(sstable_writer))
      }
      OutputDestination::Stdout => Ok(OutputWriter::Stdout(BufWriter::new(io::stdout()))),
      OutputDestination::Cursor(cursor) => Ok(OutputWriter::Cursor(cursor)),
    }
  }
}

pub enum OutputWriter {
  /// Writes to an SSTable.
  SSTable(SSTableWriter),
  /// Writes to stdout.
  Stdout(BufWriter<Stdout>),
  /// Writes to a generic writer.
  Cursor(Cursor<Vec<u8>>),
}

impl OutputWriter {
  pub fn into_cursor(self) -> Option<Cursor<Vec<u8>>> {
    match self {
      OutputWriter::SSTable(_) => None,
      OutputWriter::Stdout(_) => None,
      OutputWriter::Cursor(cursor) => Some(cursor),
    }
  }
}

pub trait Emit<T> {
  fn emit(&mut self, target: T) -> io::Result<()>;
}

impl Emit<(&str, &str)> for OutputWriter {
  fn emit(&mut self, target: (&str, &str)) -> io::Result<()> {
    match self {
      OutputWriter::SSTable(sstable_writer) => sstable_writer.write(target),
      OutputWriter::Stdout(stdout_writer) => {
        let (key, value) = target;
        writeln!(stdout_writer, "{}\t{}", key, value)
      }
      OutputWriter::Cursor(cursor) => {
        let (key, value) = target;
        cursor.write_all(format!("{}\t{}\n", key, value).as_bytes())
      }
    }
  }
}

impl Emit<(&[u8], &[u8])> for OutputWriter {
  fn emit(&mut self, target: (&[u8], &[u8])) -> io::Result<()> {
    match self {
      OutputWriter::SSTable(sstable_writer) => sstable_writer.write(target),
      OutputWriter::Stdout(stdout_writer) => {
        let (key, value) = target;
        writeln!(stdout_writer, "{:x?}\t{:x?}", key, value)
      }
      OutputWriter::Cursor(cursor) => {
        let (key, value) = target;
        cursor.write_all(format!("{:x?}\t{:x?}\n", key, value).as_bytes())
      }
    }
  }
}
