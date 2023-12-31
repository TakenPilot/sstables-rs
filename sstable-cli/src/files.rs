use std::{
  env, io,
  path::{Path, PathBuf},
};

/// Creates a directory and all of its parent components if they are missing.
/// If the directory already exists, no error is returned. This function is similar
/// to [`std::fs::create_dir_all`], but it does not return an error if the path
/// already exists.
///
/// # Examples
///
/// ```
/// use std::path::PathBuf;
///
/// let path_dir = PathBuf::from("/home/username/sstable.sst");
///
/// sstable_cli::files::create_dir_all(path_dir).unwrap();
/// ```
///
/// # Errors
///
/// If the directory cannot be created, an error is returned.
///
/// # Panics
///
/// If the path is not a directory, a panic will occur.
///
pub fn create_dir_all<P: AsRef<Path>>(path_dir: P) -> io::Result<()> {
  let path_dir = path_dir.as_ref();
  if !Path::new(path_dir).is_dir() {
    std::fs::create_dir_all(path_dir)?;
  }
  Ok(())
}

/// Converts a list of relative paths to absolute paths.
///
/// # Examples
///
/// ```
/// use std::path::PathBuf;
///
/// let relative_paths = vec![
///  PathBuf::from("sstable.sst"),
/// PathBuf::from("sstable2.sst"),
/// ];
///
/// let absolute_paths = sstable_cli::files::to_absolute_paths(relative_paths).unwrap();
///
/// assert_eq!(absolute_paths[0], PathBuf::from("/home/username/sstable.sst"));
/// assert_eq!(absolute_paths[1], PathBuf::from("/home/username/sstable2.sst"));
/// ```
///
/// # Errors
///
/// If a path cannot be converted to an absolute path, an error is returned.
///
/// # Panics
///
/// If a path is not relative, a panic will occur.
///
pub fn to_absolute_paths(relative_paths: &[PathBuf]) -> io::Result<Vec<PathBuf>> {
  let mut absolute_paths = Vec::new();

  for path in relative_paths {
    let absolute_path = path.canonicalize()?;
    absolute_paths.push(absolute_path);
  }

  Ok(absolute_paths)
}

/// Converts a list of absolute paths to relative paths.
///
/// # Examples
///
/// ```
/// use std::path::PathBuf;
///
/// let absolute_paths = vec![
///  PathBuf::from("/home/username/sstable.sst"),
///  PathBuf::from("/home/username/sstable2.sst"),
/// ];
///
/// let relative_paths = sstable_cli::files::to_relative_paths(absolute_paths).unwrap();
///
/// assert_eq!(relative_paths[0], PathBuf::from("sstable.sst"));
/// assert_eq!(relative_paths[1], PathBuf::from("sstable2.sst"));
/// ```
///
/// # Errors
///
/// If the current working directory cannot be determined, an error is returned. If a path
/// cannot be converted to a relative path, it is ignored.
///
/// # Panics
///
/// If a path is not absolute, a panic will occur.
///
pub fn to_relative_paths(absolute_paths: &[PathBuf]) -> io::Result<Vec<PathBuf>> {
  let cwd = env::current_dir()?;
  let mut relative_paths = Vec::new();

  for path in absolute_paths {
    match path.strip_prefix(&cwd) {
      Ok(rel_path) => relative_paths.push(rel_path.to_path_buf()),
      Err(_) => println!("Path is not relative to CWD: {:?}", path),
    }
  }

  Ok(relative_paths)
}
