use sstables::{cbor::CborWrite, SSTableWriter, SSTableWriterBuilder};
use std::{cmp::Ordering, fmt::Display, io, path::PathBuf};

/// A generic write method for a specific type.
pub trait TypeWrite<T> {
  fn write(&mut self, target: T) -> io::Result<()>;
}

/// Write a (K, V) tuple to a SSTable.
impl<K: CborWrite, V: CborWrite> TypeWrite<(K, V)> for SSTableWriter {
  fn write(&mut self, target: (K, V)) -> io::Result<()> {
    self.write(target)
  }
}

/// A (K, V) tuple where K is Orderable and Cloneable.
#[derive(Debug)]
pub struct KeyValue<K: Ord + Clone, V>(pub K, pub V);

/// Equality for KeyValue is enabled.
impl<K: Ord + Clone, V> Eq for KeyValue<K, V> {}

/// Equality for KeyValue is based on the key.
impl<K: Ord + Clone, V> PartialEq for KeyValue<K, V> {
  fn eq(&self, other: &Self) -> bool {
    self.0 == other.0
  }
}

/// Ordering for KeyValue is based on the key.
impl<K: Ord + Clone, V> PartialOrd for KeyValue<K, V> {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.0.cmp(&other.0))
  }
}

/// Ordering for KeyValue is based on the key.
impl<K: Ord + Clone, V> Ord for KeyValue<K, V> {
  fn cmp(&self, other: &Self) -> Ordering {
    // Implement your custom comparison logic here.
    // For example, you can compare based on the first element.
    self.0.cmp(&other.0)
  }
}

/// Display a (K, V) tuple as (key, value).
impl<K: Ord + Clone + Display, V: Display> Display for KeyValue<K, V> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "({}, {})", self.0, self.1)
  }
}

/// Represents Stdout.
pub struct Terminal {}

/// If they write to the Terminal, then write to Stdout.
impl<T: std::fmt::Display> TypeWrite<T> for Terminal {
  fn write(&mut self, target: T) -> io::Result<()> {
    println!("{}", target);
    Ok(())
  }
}

/// Either a SSTable or a Terminal.
pub enum TypeWriter {
  SSTable(SSTableWriter),
  Terminal(Terminal),
}

/// If the output path exists, then write to a SSTable, otherwise write to the Terminal.
impl TypeWriter {
  pub fn new(output_path: &Option<PathBuf>) -> io::Result<TypeWriter> {
    Ok(match output_path {
      Some(output_path) => TypeWriter::SSTable(SSTableWriterBuilder::new(output_path).build()?),
      None => TypeWriter::Terminal(Terminal {}),
    })
  }
}

/// Write a (K, V) tuple to either a SSTable or a Terminal.
impl<K: CborWrite + Display + Ord + Clone, V: CborWrite + Display> TypeWrite<(K, V)> for TypeWriter {
  fn write(&mut self, target: (K, V)) -> io::Result<()> {
    let (k, v) = target;
    match self {
      TypeWriter::SSTable(sstable_writer) => sstable_writer.write((k, v)),
      TypeWriter::Terminal(terminal) => terminal.write(KeyValue(k, v)),
    }
  }
}
