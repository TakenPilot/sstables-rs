//! Output writer appends key-values to a file or stdout in various append-only formats.

use sstables::{Append, SSTableWriter, SSTableWriterBuilder};
use std::{
  io::{self, BufWriter, Stdout, Write},
  path::PathBuf,
};

pub trait KeyValueWriter {
  /// Appends a record to the file or stdout.
  fn write(&mut self, key: &str, value: &str) -> io::Result<()>;
}

pub enum OutputDestination {
  File(PathBuf),
  Stdout,
}

pub struct OutputWriterBuilder {
  destination: OutputDestination,
}

impl OutputWriterBuilder {
  pub fn new(destination: OutputDestination) -> Self {
    OutputWriterBuilder { destination }
  }

  pub fn build(self) -> io::Result<OutputWriter> {
    match self.destination {
      OutputDestination::File(path) => {
        let sstable_writer: SSTableWriter<(&str, &str)> = SSTableWriterBuilder::<(&str, &str)>::new(path).build()?;
        Ok(OutputWriter::SSTable(Box::new(sstable_writer)))
      }
      OutputDestination::Stdout => Ok(OutputWriter::Stdout(BufWriter::new(io::stdout()))),
    }
  }
}

pub enum OutputWriter {
  SSTable(Box<dyn for<'a, 'b> Append<(&'a str, &'b str)>>),
  Stdout(BufWriter<Stdout>),
}

impl KeyValueWriter for OutputWriter {
  fn write(&mut self, key: &str, value: &str) -> io::Result<()> {
    match self {
      OutputWriter::SSTable(sstable_writer) => sstable_writer.append((key, value)),
      OutputWriter::Stdout(stdout_writer) => {
        writeln!(stdout_writer, "{}\t{}", key, value)?;
        Ok(())
      }
    }
  }
}
