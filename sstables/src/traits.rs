use std::{io, path::Path};

/// Converts a path to some type.
pub trait FromPath<T> {
  fn from_path<P: AsRef<Path>>(path: P) -> io::Result<Self>
  where
    Self: Sized;
}
