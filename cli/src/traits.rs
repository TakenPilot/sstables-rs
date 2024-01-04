use std::{cmp::Ordering, fmt::Display, io};

use sstables::{cbor::CborWrite, SSTableWriter};

pub trait TypeWrite<T> {
  fn write(&mut self, target: T) -> io::Result<()>;
}

impl<K: CborWrite, V: CborWrite> TypeWrite<(K, V)> for SSTableWriter {
  fn write(&mut self, target: (K, V)) -> io::Result<()> {
    self.write(target)
  }
}

#[derive(Debug)]
pub struct KeyValue<K: Ord + Clone, V>(pub K, pub V);

impl<K: Ord + Clone, V> Eq for KeyValue<K, V> {}

impl<K: Ord + Clone, V> PartialEq for KeyValue<K, V> {
  fn eq(&self, other: &Self) -> bool {
    self.0 == other.0
  }
}

impl<K: Ord + Clone, V> PartialOrd for KeyValue<K, V> {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.0.cmp(&other.0))
  }
}

impl<K: Ord + Clone, V> Ord for KeyValue<K, V> {
  fn cmp(&self, other: &Self) -> Ordering {
    // Implement your custom comparison logic here.
    // For example, you can compare based on the first element.
    self.0.cmp(&other.0)
  }
}

impl<K: Ord + Clone + Display, V: Display> Display for KeyValue<K, V> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "({}, {})", self.0, self.1)
  }
}
