//! Output writer appends key-values to a file or stdout in various append-only formats.

use sstables::{Append, SSTableWriter, SSTableWriterBuilder};
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

pub struct OutputWriterBuilder<T> {
  destination: OutputDestination,
  _phantom: std::marker::PhantomData<T>,
}

/// Builder for OutputWriter for any SSTableWriter that implements Append.
impl<T> OutputWriterBuilder<T>
where
  sstables::SSTableWriter<T>: sstables::Append<T>,
{
  pub fn new(destination: OutputDestination) -> Self {
    OutputWriterBuilder {
      destination,
      _phantom: std::marker::PhantomData,
    }
  }

  pub fn build<'a>(self) -> io::Result<OutputWriter<T>>
  where
    T: 'a,
  {
    match self.destination {
      OutputDestination::File(path) => {
        let sstable_writer = SSTableWriterBuilder::<T>::new(path).build()?;
        Ok(OutputWriter::SSTable(sstable_writer))
      }
      OutputDestination::Stdout => Ok(OutputWriter::Stdout(BufWriter::new(io::stdout()))),
    }
  }
}

pub enum OutputWriter<T>
where
  SSTableWriter<T>: Append<T>,
{
  SSTable(SSTableWriter<T>),
  Stdout(BufWriter<Stdout>),
}

impl OutputEmitter<(&str, &str)> for OutputWriter<(&str, &str)> {
  fn emit(&mut self, target: (&str, &str)) -> io::Result<()> {
    match self {
      OutputWriter::SSTable(sstable_writer) => sstable_writer.append(target),
      OutputWriter::Stdout(stdout_writer) => {
        let (key, value) = target;
        writeln!(stdout_writer, "{}\t{}", key, value)?;
        Ok(())
      }
    }
  }
}
