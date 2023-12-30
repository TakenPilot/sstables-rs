use std::{io, path::Path};

pub fn create_dir_all<P: AsRef<Path>>(path_dir: P) -> io::Result<()> {
  let path_dir = path_dir.as_ref();
  if !Path::new(path_dir).is_dir() {
    std::fs::create_dir_all(path_dir)?;
  }
  Ok(())
}
