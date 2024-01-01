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
/// let path_dir = PathBuf::from(".tmp");
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

/// Gets the size of a file in bytes.
pub fn get_file_size(path: &PathBuf) -> io::Result<u64> {
  let metadata = std::fs::metadata(path)?;
  Ok(metadata.len())
}

/// Creates a path to the index file for the given path. If the given path has an extension, the
/// extension is replaced with `index.<extension>`. If the given path does not have an extension,
/// the extension is set to `index`.
pub fn create_index_path(path: &Path) -> PathBuf {
  let mut path = path.to_path_buf();
  let ext_maybe = path.extension();
  match ext_maybe {
    Some(ext) => path.set_extension(format!("index.{}", ext.to_str().unwrap())),
    None => path.set_extension("index"),
  };

  path
}

/// Get a displayable string of a path.
pub fn get_path_str(path: &Path) -> &str {
  path.to_str().unwrap()
}
