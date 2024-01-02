//! Output writer appends key-values to a file or stdout in various append-only formats.

use sstables::{SSTableWriter, SSTableWriterBuilder};
use std::{
  io::{self, BufWriter, Stdout, Write},
  path::PathBuf,
};

pub trait OutputEmitter<T> {
  /// Appends a record to the file or stdout.
  fn emit(&mut self, output: T) -> io::Result<()>;
}

pub enum OutputDestination {
  File(PathBuf),
  Stdout,
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
    }
  }
}

pub enum OutputWriter {
  SSTable(SSTableWriter),
  Stdout(BufWriter<Stdout>),
}

impl OutputEmitter<(&str, &str)> for OutputWriter {
  fn emit(&mut self, target: (&str, &str)) -> io::Result<()> {
    match self {
      OutputWriter::SSTable(sstable_writer) => sstable_writer.write(target),
      OutputWriter::Stdout(stdout_writer) => {
        let (key, value) = target;
        writeln!(stdout_writer, "{}\t{}", key, value)?;
        Ok(())
      }
    }
  }
}
