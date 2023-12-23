use std::io::{self, Read};

#[inline]
pub fn take_byte<T>(b: &mut T) -> io::Result<u8>
where
  T: Read + ?Sized,
{
  let mut buf = [0; 1];
  b.read_exact(&mut buf)?;
  Ok(buf[0])
}

/// Takes a byte array of length `C` from the given reader. This function is only available for
/// arrays of length 1, 2, 4, 8, 16, 32, and 64.
///
/// # Example
///
/// ```
/// use std::io::Cursor;
/// use sstables::read::take_byte_array;
///
/// let mut cursor = Cursor::new([1, 2, 3, 4, 5]);
/// let bytes = take_byte_array::<5, _>(&mut cursor).unwrap();
/// assert_eq!(bytes, [1, 2, 3, 4, 5]);
/// ```
///
/// # Safety
///
/// If the reader does not have enough bytes to satisfy the request, an error is returned.
///
/// # Performance Considerations
///
/// This function allocates a new array of length `C` on the stack. If you want to avoid this
/// allocation, you can use [`take_byte_slice`](crate::read::take_byte_slice) instead, which
/// allocates on the heap instead of the stack. If you need a byte array of a different length,
/// you can use [`take_byte`](crate::read::take_byte) to read the bytes one at a time.
#[inline]
pub fn take_byte_array<const C: usize, T>(b: &mut T) -> io::Result<[u8; C]>
where
  T: Read + ?Sized,
{
  let mut buf = [0; C];
  b.read_exact(&mut buf)?;
  Ok(buf)
}

/// Takes a byte slice of length `len` from the given reader. If the reader does not have enough
/// bytes to satisfy the request, an error is returned.
///
/// # Example
///
/// ```
/// use std::io::Cursor;
/// use sstables::read::take_byte_slice;
///
/// let mut cursor = Cursor::new([1, 2, 3, 4, 5]);
/// let bytes = take_byte_slice(&mut cursor, 3).unwrap();
/// assert_eq!(bytes, [1, 2, 3]);
/// ```
///
/// # Safety
///
/// If the reader does not have enough bytes to satisfy the request, an error is returned.
///
/// # Performance Considerations
///
/// This function allocates a new vector of length `len` on the heap. If you want to avoid this
/// allocation, you can use [`take_byte_array`](crate::read::take_byte_array) instead, which
/// allocates on the stack instead of the heap. However, this function is only available for
/// arrays of length 1, 2, 4, 8, 16, 32, and 64. If you need a byte array of a different length,
/// you can use [`take_byte`](crate::read::take_byte) to read the bytes one at a time. If you
/// need a byte array of a different length and you need to avoid heap allocations, you can
/// implement your own function that uses [`take_byte`](crate::read::take_byte) to read the bytes
/// one at a time.
#[inline]
pub fn take_byte_slice<T>(b: &mut T, len: usize) -> io::Result<Vec<u8>>
where
  T: Read + ?Sized,
{
  let mut buf = vec![0; len];
  b.read_exact(&mut buf)?;

  Ok(buf)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_take_byte() {
    let mut cursor = io::Cursor::new([1, 2, 3, 4, 5]);
    assert_eq!(take_byte(&mut cursor).unwrap(), 1);
    assert_eq!(take_byte(&mut cursor).unwrap(), 2);
    assert_eq!(take_byte(&mut cursor).unwrap(), 3);
    assert_eq!(take_byte(&mut cursor).unwrap(), 4);
    assert_eq!(take_byte(&mut cursor).unwrap(), 5);
    assert_eq!(cursor.position(), 5);
  }

  /// Test that `take_byte` returns an error when there are not enough bytes to satisfy the
  /// request.
  #[test]
  fn test_take_byte_too_many() {
    let mut cursor = io::Cursor::new([1, 2, 3, 4, 5]);
    assert_eq!(take_byte(&mut cursor).unwrap(), 1);
    assert_eq!(take_byte(&mut cursor).unwrap(), 2);
    assert_eq!(take_byte(&mut cursor).unwrap(), 3);
    assert_eq!(take_byte(&mut cursor).unwrap(), 4);
    assert_eq!(take_byte(&mut cursor).unwrap(), 5);
    assert_eq!(take_byte(&mut cursor).unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
    assert_eq!(cursor.position(), 5);
  }

  #[test]
  fn test_take_byte_array() {
    let mut cursor = io::Cursor::new([1, 2, 3, 4, 5]);
    assert_eq!(take_byte_array::<5, _>(&mut cursor).unwrap(), [1, 2, 3, 4, 5]);
    assert_eq!(cursor.position(), 5);
  }

  /// Test that `take_byte_array` returns an error when there are not enough bytes to satisfy the
  /// request.
  #[test]
  fn test_take_byte_array_too_many() {
    let mut cursor = io::Cursor::new([1, 2, 3, 4, 5]);
    assert_eq!(take_byte_array::<5, _>(&mut cursor).unwrap(), [1, 2, 3, 4, 5]);
    assert_eq!(
      take_byte_array::<5, _>(&mut cursor).unwrap_err().kind(),
      io::ErrorKind::UnexpectedEof
    );
    assert_eq!(cursor.position(), 5);
  }

  #[test]
  fn test_take_byte_slice() {
    let mut cursor = io::Cursor::new([1, 2, 3, 4, 5]);
    assert_eq!(take_byte_slice(&mut cursor, 3).unwrap(), [1, 2, 3]);
    assert_eq!(cursor.position(), 3);
  }
}

/// Test that `take_byte_slice` returns an error when there are not enough bytes to satisfy the
/// request.
#[test]
fn test_take_byte_slice_too_many() {
  let mut cursor = io::Cursor::new([1, 2, 3, 4, 5]);
  assert_eq!(take_byte_slice(&mut cursor, 3).unwrap(), [1, 2, 3]);
  assert_eq!(
    take_byte_slice(&mut cursor, 3).unwrap_err().kind(),
    io::ErrorKind::UnexpectedEof
  );
  assert_eq!(cursor.position(), 3);
}
