use std::{io, path::Path};

pub trait FromPath<T> {
  fn from_path<P: AsRef<Path>>(path: P) -> io::Result<Self>
  where
    Self: Sized;
}
