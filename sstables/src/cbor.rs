//! CBOR types and constants.
//! See https://tools.ietf.org/html/rfc7049 for the spec.
//!
//! This is a subset of the spec, only including the types we need for the internal
//! representation of SSTables that will be compatible with any CBOR implementation.
//!
//! The spec defines a CBOR data item as a major type and an optional additional info.
//! The major type is stored in the first three bits of the initial byte.
//! The additional info is stored in the last five bits of the initial byte.
//!
//! The additional info is used to determine how many bytes are used to store the value.
//! The value is stored in the bytes following the initial byte.
//!

use std::io::{self, Cursor, Read, Write};

use crate::read::{take_byte, take_byte_array, take_byte_slice};

/// A mask used to get the first three bits of a byte, aka 224 or 1110_0000.
///
/// Example: 0xAF & FIRST_THREE_BITS = 0xA0
const FIRST_THREE_BITS: u8 = 0xE0;

/// A mask used to get the last five bits of a byte, aka 31 or 0001_1111.
///
/// Example: 0xAF & LAST_FIVE_BITS = 0x0F
const LAST_FIVE_BITS: u8 = 0x1F;

/// Maximum value that can be embedded in an initial byte.
/// In CBOR, the numbers 0-23 are directly encoded in the last five bits
/// of the initial byte.
const EMBEDDED_MAX_AS_U64: u64 = 23;

/// Maximum value that can be stored in a U8 as a U64.
/// This is used to determine if we can store a value in a U8.
const U8_MAX: u64 = u8::MAX as u64;

/// Maximum value that can be stored in a U16.
/// This is used to determine if we can store a value in a U16.
const U16_MAX: u64 = u16::MAX as u64;

/// Maximum value that can be stored in a U32.
/// This is used to determine if we can store a value in a U32.
const U32_MAX: u64 = u32::MAX as u64;

/// Major types for CBOR data items. Each type corresponds to the high-order
/// 3 bits in the initial byte of a CBOR data item. See Section 2.1.
///
/// See https://tools.ietf.org/html/rfc7049#section-2.1 for the encoding rules.
///
/// # Example
///
/// ```
/// use sstables::cbor::MajorType;
///
/// assert_eq!(MajorType::UnsignedInteger.as_u8(), 0);
/// assert_eq!(MajorType::NegativeInteger.as_u8(), 32);
/// assert_eq!(MajorType::Bytes.as_u8(), 64);
/// assert_eq!(MajorType::Text.as_u8(), 96);
/// assert_eq!(MajorType::Array.as_u8(), 128);
/// assert_eq!(MajorType::Object.as_u8(), 160);
/// assert_eq!(MajorType::SemanticTag.as_u8(), 192);
/// assert_eq!(MajorType::NoContentType.as_u8(), 224);
/// ```
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MajorType {
  /// Any size of positive integer.
  /// Type 0, including 0x00-0x1F.
  UnsignedInteger = 0x00,
  /// Any size of negative integer.
  /// Type 1, including 0x20-0x3F.
  NegativeInteger = 0x20,
  /// A string of bytes.
  /// Type 2, including 0x40-0x5F.
  /// Additional info of 31 means IndefiniteLength.
  Bytes = 0x40,
  /// A string of valid UTF8 encoded text, unescaped so "\" is not a special character.
  /// Type 3, including 0x60-0x7F.
  /// Additional info of 31 means IndefiniteLength.
  Text = 0x60,
  /// A list of items in series.
  /// Type 4, including 0x80-0x9F.
  /// Additional info of 31 means IndefiniteLength.
  Array = 0x80,
  /// A list of key/value pairs in series, so 9 ItemPairs = 18 items.
  /// Type 5, including 0xA0-0xBF.
  /// Additional info of 31 means IndefiniteLength.
  Object = 0xA0,
  /// Optional semantic tagging of other major types. See Section 2.4.
  /// Type 6, including 0xC0-0xDF.
  SemanticTag = 0xC0,
  /// Floating-point numbers and simple data types that need no content, as well as
  /// the "break" stop code. See Section 2.3.
  /// Type 7, including 0xE0-0xFF.
  NoContentType = 0xE0,
}

/// The MajorType is stored in the first three bits of the initial byte.
impl MajorType {
  /// Get the first three bits of the initial byte from a MajorType enum.
  /// This is safe because we've mapped a value for each possible bit.
  #[inline]
  pub fn as_u8(&self) -> u8 {
    *self as u8
  }

  /// Get the MajorType enum from the first three bits of the initial byte.
  /// This is safe because we've mapped a value for each possible bit.
  ///
  /// # Example
  ///
  /// ```
  /// use sstables::cbor::MajorType;
  ///
  /// assert_eq!(MajorType::from_u8(0), MajorType::UnsignedInteger);
  /// assert_eq!(MajorType::from_u8(32), MajorType::NegativeInteger);
  /// assert_eq!(MajorType::from_u8(64), MajorType::Bytes);
  /// assert_eq!(MajorType::from_u8(96), MajorType::Text);
  /// assert_eq!(MajorType::from_u8(128), MajorType::Array);
  /// assert_eq!(MajorType::from_u8(160), MajorType::Object);
  /// assert_eq!(MajorType::from_u8(192), MajorType::SemanticTag);
  /// assert_eq!(MajorType::from_u8(224), MajorType::NoContentType);
  /// ```
  #[inline]
  pub fn from_u8(value: u8) -> Self {
    // Use only first three bits. This is safe because we've mapped a value for each possible bit.
    unsafe { ::std::mem::transmute(value & FIRST_THREE_BITS) }
  }
}

/// Large values are stored in the bytes following the initial byte.
/// The initial byte uses this enum to determine how many.
///
/// # Example
///
/// ```
/// use sstables::cbor::ExtendedSize;
///
/// assert_eq!(ExtendedSize::Embedded.as_u8(), 0);
/// assert_eq!(ExtendedSize::U8.as_u8(), 24);
/// assert_eq!(ExtendedSize::U16.as_u8(), 25);
/// assert_eq!(ExtendedSize::U32.as_u8(), 26);
/// assert_eq!(ExtendedSize::U64.as_u8(), 27);
/// assert_eq!(ExtendedSize::from_u8(0), ExtendedSize::Embedded);
/// assert_eq!(ExtendedSize::from_u8(23), ExtendedSize::Embedded);
/// assert_eq!(ExtendedSize::from_u8(24), ExtendedSize::U8);
/// assert_eq!(ExtendedSize::from_u8(25), ExtendedSize::U16);
/// assert_eq!(ExtendedSize::from_u8(26), ExtendedSize::U32);
/// assert_eq!(ExtendedSize::from_u8(27), ExtendedSize::U64);
/// ```
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtendedSize {
  /// The value is stored in the last five bits of the initial byte.
  /// 0 - 23. 0x00 - 0x17.
  Embedded,
  /// The value is stored in the next byte.
  /// 24.
  U8 = 0x18,
  /// The value is stored in the next two bytes.
  /// 25.
  U16 = 0x19,
  /// The value is stored in the next four bytes.
  /// 26.
  U32 = 0x1A,
  /// The value is stored in the next eight bytes.
  /// 27.
  U64 = 0x1B,
}

/// The ExtendedSize is stored in the last five bits of the initial byte.
impl ExtendedSize {
  /// Convert an ExtendedSize enum to the last five bits of the initial byte.
  #[inline]
  pub fn as_u8(&self) -> u8 {
    *self as u8
  }

  /// Get the number of bytes used to store the value as an ExtendedSize enum.
  #[inline]
  pub fn from_u8(value: u8) -> Self {
    let value = value & LAST_FIVE_BITS;
    if (24..=27).contains(&value) {
      unsafe { ::std::mem::transmute(value) }
    } else {
      ExtendedSize::Embedded
    }
  }
}

/// Get the embedded value, which is the last five bits of a byte.
#[inline]
pub fn get_embedded_value(byte: u8) -> u8 {
  byte & LAST_FIVE_BITS
}

/// Given a large number, return last five bits in the initial byte based on what they want to store.
/// Optimized for the common case of small numbers.
/// See https://tools.ietf.org/html/rfc7049#section-2.1 for the encoding rules.
#[inline]
pub fn get_embedded_value_for_u64(value: u64) -> u8 {
  if value <= EMBEDDED_MAX_AS_U64 {
    value as u8
  } else if value <= U8_MAX {
    24
  } else if value <= U16_MAX {
    25
  } else if value <= U32_MAX {
    26
  } else {
    27
  }
}

/// Given a large number, return the number of bytes needed to store it.
/// Optimized for the common case of small numbers.
/// See https://tools.ietf.org/html/rfc7049#section-2.1 for the encoding rules.
#[inline]
fn get_num_bytes_for_u64(value: u64) -> usize {
  if value <= EMBEDDED_MAX_AS_U64 {
    0
  } else if value <= U8_MAX {
    std::mem::size_of::<u8>()
  } else if value <= U16_MAX {
    std::mem::size_of::<u16>()
  } else if value <= U32_MAX {
    std::mem::size_of::<u32>()
  } else {
    std::mem::size_of::<u64>()
  }
}

/// Read the value in the head using the contnt of the initial byte. May consume up
/// to four additional bytes.
///
/// # Example
///
/// ```
/// use std::io::Cursor;
/// use sstables::read::take_byte;
/// use sstables::cbor::read_cbor_head_u64;
///
/// let mut cursor = Cursor::new([0x18, 0x64]);
/// let byte = take_byte(&mut cursor).unwrap();
/// let value = read_cbor_head_u64(&mut cursor, byte).unwrap();
/// assert_eq!(value, 100);
/// ```
#[inline]
pub fn read_cbor_head_u64<R: Read + ?Sized>(b: &mut R, byte: u8) -> io::Result<u64> {
  Ok(match ExtendedSize::from_u8(byte) {
    ExtendedSize::Embedded => get_embedded_value(byte).into(),
    ExtendedSize::U8 => u8::from_be_bytes(take_byte_array(b)?).into(),
    ExtendedSize::U16 => u16::from_be_bytes(take_byte_array(b)?).into(),
    ExtendedSize::U32 => u32::from_be_bytes(take_byte_array(b)?).into(),
    ExtendedSize::U64 => u64::from_be_bytes(take_byte_array(b)?),
  })
}

/// Assuming that the next value is known to be an unsigned integer, read it. May
/// consume 1 to 9 bytes.
///
/// # Example
///
/// ```
/// use std::io::Cursor;
/// use sstables::cbor::read_cbor_u64;
///
/// let mut cursor = Cursor::new([0x18, 0x64]);
/// let value = read_cbor_u64(&mut cursor).unwrap();
/// assert_eq!(value, 100);
/// ```
pub fn read_cbor_u64<R: Read + ?Sized>(b: &mut R) -> io::Result<u64> {
  let byte = take_byte(b)?;
  read_cbor_head_u64(b, byte)
}

/// Assuming that the next value is known to be a byte array, read it.
///
/// # Example
///
/// ```
/// use std::io::Cursor;
/// use sstables::cbor::read_cbor_bytes;
///
/// let mut cursor = Cursor::new([0x44, 0x01, 0x02, 0x03, 0x04]);
/// let bytes = read_cbor_bytes(&mut cursor).unwrap();
/// assert_eq!(bytes, [1, 2, 3, 4]);
/// ```
pub fn read_cbor_bytes<R: Read + ?Sized>(b: &mut R) -> io::Result<Vec<u8>> {
  let byte = take_byte(b)?;
  let len = read_cbor_head_u64(b, byte)?;
  take_byte_slice(b, len as usize)
}

/// Assuming that the next value is known to be a text array, read it.
///
/// # Example
///
/// ```
/// use std::io::Cursor;
/// use sstables::cbor::read_cbor_text;
///
/// let mut cursor = Cursor::new([0x65, 0x68, 0x65, 0x6C, 0x6C, 0x6F]);
/// let text = read_cbor_text(&mut cursor).unwrap();
/// assert_eq!(text, "hello");
/// ```
pub fn read_cbor_text<R: Read + ?Sized>(b: &mut R) -> io::Result<String> {
  let byte = take_byte(b)?;
  let len = read_cbor_head_u64(b, byte)?;
  let bytes = take_byte_slice(b, len as usize)?;
  String::from_utf8(bytes.to_vec()).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}

/// Writes a CBOR head that identifies the bytes that follow.
/// A CBOR head is 1-9 bytes, depending on the size of the value.
///
/// # Example
///
/// ```
/// use std::io::Cursor;
/// use sstables::cbor::{write_cbor_head, MajorType};
///
/// let mut cursor = Cursor::new(Vec::new());
/// write_cbor_head(&mut cursor, MajorType::UnsignedInteger, 100).unwrap();
/// assert_eq!(cursor.into_inner(), vec![0x18, 0x64]);
/// ```
pub fn write_cbor_head<W: Write>(writer: &mut W, major_type: MajorType, value: u64) -> io::Result<()> {
  writer.write_all(&[major_type.as_u8() | get_embedded_value_for_u64(value)])?;
  let num_bytes = get_num_bytes_for_u64(value);
  if num_bytes > 0 {
    let bytes = value.to_be_bytes();
    let start_pos = bytes.len() - num_bytes;
    writer.write_all(&bytes[start_pos..])?;
  };
  Ok(())
}

/// A convenience function for writing text, encoded as UTF8.
///
/// # Example
///
/// ```
/// use std::io::Cursor;
/// use sstables::cbor::write_cbor_text;
///
/// let mut cursor = Cursor::new(Vec::new());
/// write_cbor_text(&mut cursor, "hello").unwrap();
/// assert_eq!(cursor.into_inner(), vec![0x65, 0x68, 0x65, 0x6C, 0x6C, 0x6F]);
/// ```
#[inline]
pub fn write_cbor_text<W: Write>(writer: &mut W, text: &str) -> io::Result<()> {
  let bytes = text.as_bytes();
  write_cbor_head(writer, MajorType::Text, bytes.len() as u64)?;
  writer.write_all(bytes)
}

/// A convenience function for writing bytes.
///
/// # Example
///
/// ```
/// use std::io::Cursor;
/// use sstables::cbor::write_cbor_bytes;
///
/// let mut cursor = Cursor::new(Vec::new());
/// write_cbor_bytes(&mut cursor, &[1, 2, 3, 4]).unwrap();
/// assert_eq!(cursor.into_inner(), vec![0x44, 0x01, 0x02, 0x03, 0x04]);
/// ```
#[inline]
pub fn write_cbor_bytes<W: Write>(writer: &mut W, bytes: &[u8]) -> io::Result<()> {
  write_cbor_head(writer, MajorType::Bytes, bytes.len() as u64)?;
  writer.write_all(bytes)
}

/// A convenience function for writing an unsigned integer.
///
/// # Example
///
/// ```
/// use std::io::Cursor;
/// use sstables::cbor::write_cbor_unsigned_integer;
///
/// let mut cursor = Cursor::new(Vec::new());
/// write_cbor_unsigned_integer(&mut cursor, 100).unwrap();
/// assert_eq!(cursor.into_inner(), vec![0x18, 0x64]);
/// ```
#[inline]
pub fn write_cbor_unsigned_integer<W: Write>(writer: &mut W, value: u64) -> io::Result<()> {
  write_cbor_head(writer, MajorType::UnsignedInteger, value)
}

/// A comparison function for CBOR data items that have already been serialized
/// to bytes.
fn cbor_cmp(a: &Cursor<Vec<u8>>, b: &Cursor<Vec<u8>>) -> std::cmp::Ordering {
  let len_a = a.position() as usize;
  let len_b = b.position() as usize;
  if len_a != len_b {
    return len_a.cmp(&len_b);
  }

  a.get_ref()[0..len_a].cmp(&b.get_ref()[0..len_a])
}

/// Extend common types with a method to search for a key assuming the indices are sorted as per
/// the CBOR spec. If there are duplicates, the index of the first one is returned. If the key is
/// not found, the Result will be an Err with the index where the key would be inserted.
///
/// # Example
///
/// ```
/// use sstables::cbor::CborSearch;
///
/// let indices = vec![
///  (vec![1, 2], 10),
///  (vec![1, 2, 4], 20),
///  (vec![1, 5, 3], 30),
/// ];
/// assert_eq!(indices.cbor_search(&vec![1, 2]), Ok(0));
/// assert_eq!(indices.cbor_search(&vec![1, 2, 4]), Ok(1));
/// assert_eq!(indices.cbor_search(&vec![1, 2, 3]), Err(1));
/// assert_eq!(indices.cbor_search(&vec![1, 2, 4, 5]), Err(3));
/// ```
pub trait CborSearch<T> {
  fn cbor_search(&self, key: T) -> Result<usize, usize>;
}

impl CborSearch<&[u8]> for Vec<(Vec<u8>, u64)> {
  fn cbor_search(&self, key: &[u8]) -> Result<usize, usize> {
    binary_search_bytes_first(self, key)
  }
}

impl CborSearch<&str> for Vec<(String, u64)> {
  fn cbor_search(&self, key: &str) -> Result<usize, usize> {
    binary_search_text_first(self, key)
  }
}

impl CborSearch<&u64> for Vec<(u64, u64)> {
  fn cbor_search(&self, key: &u64) -> Result<usize, usize> {
    binary_search_unsigned_integer_first(self, *key)
  }
}

/// If the key is found, we need to check if there are any duplicates. If there are, we need to
/// return the first one. This function assumes the indices are sorted as per the CBOR spec.
fn step_back_for_duplicates<T>(indices: &[(T, u64)], i: usize) -> usize
where
  T: PartialEq,
{
  let mut i = i;
  while i > 0 && indices[i - 1].0 == indices[i].0 {
    i -= 1;
  }
  i
}

/// Find the index of a byte array assuming the indices are sorted as per the CBOR spec. If there
/// are duplicates, the index of the first one is returned.
///
/// # Example
///
/// ```
/// use sstables::cbor::binary_search_bytes_first;
///
/// let indices = vec![
/// (vec![1, 2], 10),
/// (vec![1, 2, 4], 20),
/// (vec![1, 5, 3], 30),
/// ];
/// assert_eq!(binary_search_bytes_first(&indices, &[1, 2]), Ok(0));
/// ```
pub fn binary_search_bytes_first(indices: &[(Vec<u8>, u64)], seek: &[u8]) -> Result<usize, usize> {
  let mut seek_cur = Cursor::new(Vec::new());
  write_cbor_bytes(&mut seek_cur, seek).unwrap();
  let mut probe_cur = Cursor::new(Vec::new());
  indices
    .binary_search_by(|(probe, _)| {
      probe_cur.set_position(0);
      write_cbor_bytes(&mut probe_cur, probe).unwrap();
      cbor_cmp(&probe_cur, &seek_cur)
    })
    .map(|i| step_back_for_duplicates(indices, i))
}

/// Find the index of a string assuming the indices are sorted as per the CBOR spec. If there
/// are duplicates, the index of the first one is returned.
///
/// # Example
///
/// ```
/// use sstables::cbor::binary_search_text_first;
///
/// let indices = vec![
/// ("a".to_string(), 0),
/// ("a".to_string(), 1),
/// ("a".to_string(), 2),
/// ("a".to_string(), 3),
/// ("b".to_string(), 4),
/// ("cd".to_string(), 5),
/// ];
/// assert_eq!(binary_search_text_first(&indices, "a"), Ok(0));
/// ```
pub fn binary_search_text_first(indices: &[(String, u64)], seek: &str) -> Result<usize, usize> {
  let mut seek_cur = Cursor::new(Vec::new());
  write_cbor_text(&mut seek_cur, seek).unwrap();
  let mut probe_cur = Cursor::new(Vec::new());
  indices
    .binary_search_by(|(probe, _)| {
      probe_cur.set_position(0);
      write_cbor_text(&mut probe_cur, probe).unwrap();
      cbor_cmp(&probe_cur, &seek_cur)
    })
    .map(|i| step_back_for_duplicates(indices, i))
}

/// Find the index of an unsigned integer assuming the indices are sorted as per the CBOR spec. If there
/// are duplicates, the index of the first one is returned.
///
/// # Example
///
/// ```
/// use sstables::cbor::binary_search_unsigned_integer_first;
///
/// let indices = vec![
///  (1, 0),
///  (3, 1),
///  (3, 2),
///  (3, 3),
///  (4, 4),
/// ];
/// assert_eq!(binary_search_unsigned_integer_first(&indices, 3), Ok(1));
/// ```
pub fn binary_search_unsigned_integer_first(indices: &[(u64, u64)], seek: u64) -> Result<usize, usize> {
  let mut seek_cur = Cursor::new(Vec::new());
  write_cbor_unsigned_integer(&mut seek_cur, seek).unwrap();
  let mut probe_cur = Cursor::new(Vec::new());
  indices
    .binary_search_by(|(probe, _)| {
      probe_cur.set_position(0);
      write_cbor_unsigned_integer(&mut probe_cur, *probe).unwrap();
      cbor_cmp(&probe_cur, &seek_cur)
    })
    .map(|i| step_back_for_duplicates(indices, i))
}

/// Depending on the type, sort the indices in place. The indices are sorted by the first value in
/// the tuple. The second value is the offset in the data file. If the bytes are equal, we need to
/// sort by the offset because we'd prefer to read the values in the order they were written.
///
/// # Example
///
/// ```
/// use sstables::cbor::{CborSort, sort_bytes};
///
/// let mut indices = vec![
///  (vec![1, 5, 3], 0),
///  (vec![1, 2, 4], 1),
///  (vec![1, 2], 2),
/// ];
/// indices.cbor_sort();
/// assert_eq!(indices, vec![
///  (vec![1, 2], 2),
///  (vec![1, 2, 4], 1),
///  (vec![1, 5, 3], 0),
/// ]);
/// ```
///
/// # Example
///
/// ```
/// use sstables::cbor::{CborSort, sort_text};
///
/// let mut indices = vec![
///  ("a".to_string(), 0),
///  ("bc".to_string(), 1),
///  ("d".to_string(), 2),
/// ];
/// indices.cbor_sort();
/// assert_eq!(indices, vec![
///  ("a".to_string(), 0),
///  ("d".to_string(), 2),
///  ("bc".to_string(), 1),
/// ]);
/// ```
///
pub trait CborSort<T> {
  fn cbor_sort(&mut self);
}

impl CborSort<Vec<u8>> for Vec<(Vec<u8>, u64)> {
  fn cbor_sort(&mut self) {
    sort_bytes(self);
  }
}

impl CborSort<String> for Vec<(String, u64)> {
  fn cbor_sort(&mut self) {
    sort_text(self);
  }
}

impl CborSort<u64> for Vec<(u64, u64)> {
  fn cbor_sort(&mut self) {
    sort_unsigned_integer(self);
  }
}

/// Sorts the indices in place. The indices are sorted by the first value in the tuple. The second
/// value is the offset in the data file. If the bytes are equal, we need to sort by the offset
/// because we'd prefer to read the values in the order they were written.
///
/// # Performance
///
/// This function is a bit slower than other sort functions because we need to write the bytes
/// to a buffer before we can compare them to be compliant with the CBOR spec. We reuse the same
/// buffers for each comparison, only reallocating to a larger buffer if we encounter a element
/// that needs more space.
pub fn sort_text(indices: &mut [(String, u64)]) {
  let mut a_cur = Cursor::new(Vec::new());
  let mut b_cur = Cursor::new(Vec::new());
  indices.sort_by(|(a_key, a_offset), (b_key, b_offset)| {
    a_cur.set_position(0);
    b_cur.set_position(0);
    write_cbor_text(&mut a_cur, a_key).unwrap();
    write_cbor_text(&mut b_cur, b_key).unwrap();
    let mut r = cbor_cmp(&a_cur, &b_cur);

    if r == std::cmp::Ordering::Equal {
      r = a_offset.cmp(b_offset);
    }

    r
  });
}

/// Sorts the indices in place.
///
/// The indices are sorted by the first value in the tuple. The second
/// value is the offset in the data file. If the bytes are equal, we need to sort by the offset
/// because we'd prefer to read the values in the order they were written.
///
/// # Performance
///
/// This function is a bit slower than other sort functions because we need to write the bytes
/// to a buffer before we can compare them to be compliant with the CBOR spec. We reuse the same
/// buffers for each comparison, only reallocating to a larger buffer if we encounter a element
/// that needs more space.
pub fn sort_bytes(indices: &mut [(Vec<u8>, u64)]) {
  let mut a_cur = Cursor::new(Vec::new());
  let mut b_cur = Cursor::new(Vec::new());
  indices.sort_by(|(a_key, a_offset), (b_key, b_offset)| {
    a_cur.set_position(0);
    b_cur.set_position(0);
    write_cbor_bytes(&mut a_cur, a_key).unwrap();
    write_cbor_bytes(&mut b_cur, b_key).unwrap();
    let mut r = cbor_cmp(&a_cur, &b_cur);

    if r == std::cmp::Ordering::Equal {
      r = a_offset.cmp(b_offset);
    }

    r
  });
}

/// Sorts the indices in place. The indices are sorted by the first value in the tuple. The second
/// value is the offset in the data file. If the bytes are equal, we need to sort by the offset
/// because we'd prefer to read the values in the order they were written.
///
/// # Performance
///
/// This function is a bit slower than other sort functions because we need to write the bytes
/// to a buffer before we can compare them to be compliant with the CBOR spec. We reuse the same
/// buffers for each comparison, only reallocating to a larger buffer if we encounter a element
/// that needs more space.
pub fn sort_unsigned_integer(indices: &mut [(u64, u64)]) {
  let mut a_cur = Cursor::new(Vec::new());
  let mut b_cur = Cursor::new(Vec::new());
  indices.sort_by(|(a_key, a_offset), (b_key, b_offset)| {
    a_cur.set_position(0);
    b_cur.set_position(0);
    write_cbor_unsigned_integer(&mut a_cur, *a_key).unwrap();
    write_cbor_unsigned_integer(&mut b_cur, *b_key).unwrap();
    let mut r = cbor_cmp(&a_cur, &b_cur);

    if r == std::cmp::Ordering::Equal {
      r = a_offset.cmp(b_offset);
    }

    r
  });
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn get_embedded_value_for_u64_works() {
    assert_eq!(get_embedded_value_for_u64(0), 0);
    assert_eq!(get_embedded_value_for_u64(1), 1);
    assert_eq!(get_embedded_value_for_u64(23), 23);
    assert_eq!(get_embedded_value_for_u64(24), 24);
    assert_eq!(get_embedded_value_for_u64(25), 24);
    assert_eq!(get_embedded_value_for_u64(255), 24);
    assert_eq!(get_embedded_value_for_u64(256), 25);
    assert_eq!(get_embedded_value_for_u64(65535), 25);
    assert_eq!(get_embedded_value_for_u64(65536), 26);
    assert_eq!(get_embedded_value_for_u64(4294967295), 26);
    assert_eq!(get_embedded_value_for_u64(4294967296), 27);
    assert_eq!(get_embedded_value_for_u64(u64::MAX), 27);
  }

  #[test]
  fn get_embedded_value_works() {
    assert_eq!(get_embedded_value(0), 0);
    assert_eq!(get_embedded_value(1), 1);
    assert_eq!(get_embedded_value(23), 23);
    assert_eq!(get_embedded_value(24), 24);
    assert_eq!(get_embedded_value(25), 25);
    assert_eq!(get_embedded_value(255), 31);
  }

  #[test]
  fn extended_size_from_u8_works() {
    assert_eq!(ExtendedSize::from_u8(0), ExtendedSize::Embedded);
    assert_eq!(ExtendedSize::from_u8(1), ExtendedSize::Embedded);
    assert_eq!(ExtendedSize::from_u8(23), ExtendedSize::Embedded);
    assert_eq!(ExtendedSize::from_u8(24), ExtendedSize::U8);
    assert_eq!(ExtendedSize::from_u8(25), ExtendedSize::U16);
    assert_eq!(ExtendedSize::from_u8(26), ExtendedSize::U32);
    assert_eq!(ExtendedSize::from_u8(27), ExtendedSize::U64);
    assert_eq!(ExtendedSize::from_u8(28), ExtendedSize::Embedded);
    assert_eq!(ExtendedSize::from_u8(29), ExtendedSize::Embedded);
    assert_eq!(ExtendedSize::from_u8(30), ExtendedSize::Embedded);
    assert_eq!(ExtendedSize::from_u8(31), ExtendedSize::Embedded);
  }

  #[test]
  fn extended_size_as_u8_works() {
    assert_eq!(ExtendedSize::Embedded.as_u8(), 0);
    assert_eq!(ExtendedSize::U8.as_u8(), 24);
    assert_eq!(ExtendedSize::U16.as_u8(), 25);
    assert_eq!(ExtendedSize::U32.as_u8(), 26);
    assert_eq!(ExtendedSize::U64.as_u8(), 27);
  }

  #[test]
  fn major_type_from_u8_works() {
    assert_eq!(MajorType::from_u8(0 << 5), MajorType::UnsignedInteger);
    assert_eq!(MajorType::from_u8(1 << 5), MajorType::NegativeInteger);
    assert_eq!(MajorType::from_u8(2 << 5), MajorType::Bytes);
    assert_eq!(MajorType::from_u8(3 << 5), MajorType::Text);
    assert_eq!(MajorType::from_u8(4 << 5), MajorType::Array);
    assert_eq!(MajorType::from_u8(5 << 5), MajorType::Object);
    assert_eq!(MajorType::from_u8(6 << 5), MajorType::SemanticTag);
    assert_eq!(MajorType::from_u8(7 << 5), MajorType::NoContentType);
  }

  #[test]
  fn write_cbor_head_works() {
    let mut v = Vec::new();

    write_cbor_head(&mut v, MajorType::UnsignedInteger, 0).unwrap();
    assert_eq!(v, vec![0]);
    v.clear();

    write_cbor_head(&mut v, MajorType::NegativeInteger, 0).unwrap();
    assert_eq!(v, vec![32]);
    v.clear();

    write_cbor_head(&mut v, MajorType::Bytes, 0).unwrap();
    assert_eq!(v, vec![64]);
    v.clear();

    write_cbor_head(&mut v, MajorType::Text, 0).unwrap();
    assert_eq!(v, vec![96]);
    v.clear();

    write_cbor_head(&mut v, MajorType::Array, 0).unwrap();
    assert_eq!(v, vec![128]);
    v.clear();

    write_cbor_head(&mut v, MajorType::Object, 0).unwrap();
    assert_eq!(v, vec![160]);
    v.clear();

    write_cbor_head(&mut v, MajorType::SemanticTag, 0).unwrap();
    assert_eq!(v, vec![192]);
    v.clear();

    write_cbor_head(&mut v, MajorType::NoContentType, 0).unwrap();
    assert_eq!(v, vec![224]);
    v.clear();

    write_cbor_head(&mut v, MajorType::Bytes, 40000).unwrap();
    assert_eq!(v, vec![89, 156, 64]);
    v.clear();

    write_cbor_head(&mut v, MajorType::Bytes, 66000).unwrap();
    assert_eq!(v, vec![90, 0, 1, 1, 208]);
    v.clear();

    write_cbor_head(&mut v, MajorType::Bytes, 4294967296).unwrap();
    assert_eq!(v, vec![91, 0, 0, 0, 1, 0, 0, 0, 0]);
    v.clear();

    write_cbor_head(&mut v, MajorType::Bytes, u64::MAX).unwrap();
    assert_eq!(v, vec![91, 255, 255, 255, 255, 255, 255, 255, 255]);
    v.clear();
  }

  #[test]
  fn write_cbor_text_works() {
    let mut v = Vec::new();

    write_cbor_text(&mut v, "").unwrap();
    assert_eq!(v, vec![96]);
    v.clear();

    write_cbor_text(&mut v, "hello").unwrap();
    assert_eq!(v, vec![101, 104, 101, 108, 108, 111]);
    v.clear();

    write_cbor_text(&mut v, "hello world").unwrap();
    assert_eq!(v, vec![107, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100]);
    v.clear();
  }

  #[test]
  fn write_cbor_bytes_works() {
    let mut v = Vec::new();

    write_cbor_bytes(&mut v, &[]).unwrap();
    assert_eq!(v, vec![64]);
    v.clear();

    write_cbor_bytes(&mut v, &[1, 2, 3, 4]).unwrap();
    assert_eq!(v, vec![68, 1, 2, 3, 4]);
    v.clear();
  }

  #[test]
  fn write_cbor_unsigned_integer_works() {
    let mut v = Vec::new();

    write_cbor_unsigned_integer(&mut v, 0).unwrap();
    assert_eq!(v, vec![0]);
    v.clear();

    write_cbor_unsigned_integer(&mut v, 1).unwrap();
    assert_eq!(v, vec![1]);
    v.clear();

    write_cbor_unsigned_integer(&mut v, 23).unwrap();
    assert_eq!(v, vec![23]);
    v.clear();

    write_cbor_unsigned_integer(&mut v, 24).unwrap();
    assert_eq!(v, vec![24, 24]);
    v.clear();

    write_cbor_unsigned_integer(&mut v, 25).unwrap();
    assert_eq!(v, vec![24, 25]);
    v.clear();

    write_cbor_unsigned_integer(&mut v, 255).unwrap();
    assert_eq!(v, vec![24, 255]);
    v.clear();

    write_cbor_unsigned_integer(&mut v, 256).unwrap();
    assert_eq!(v, vec![25, 1, 0]);
    v.clear();

    write_cbor_unsigned_integer(&mut v, 65535).unwrap();
    assert_eq!(v, vec![25, 255, 255]);
    v.clear();

    write_cbor_unsigned_integer(&mut v, 65536).unwrap();
    assert_eq!(v, vec![26, 0, 1, 0, 0]);
    v.clear();

    write_cbor_unsigned_integer(&mut v, 4294967295).unwrap();
    assert_eq!(v, vec![26, 255, 255, 255, 255]);
    v.clear();

    write_cbor_unsigned_integer(&mut v, 4294967296).unwrap();
    assert_eq!(v, vec![27, 0, 0, 0, 1, 0, 0, 0, 0]);
    v.clear();

    write_cbor_unsigned_integer(&mut v, u64::MAX).unwrap();
    assert_eq!(v, vec![27, 255, 255, 255, 255, 255, 255, 255, 255]);
    v.clear();
  }

  #[test]
  fn read_cbor_head_u64_works() {
    let mut cursor = io::Cursor::new([0x18, 0x64]);
    let byte = take_byte(&mut cursor).unwrap();
    let value = read_cbor_head_u64(&mut cursor, byte).unwrap();
    assert_eq!(value, 100);
  }

  #[test]
  fn read_cbor_u64_works() {
    let mut cursor = io::Cursor::new([0x18, 0x64]);
    let value = read_cbor_u64(&mut cursor).unwrap();
    assert_eq!(value, 100);
  }

  #[test]
  fn read_cbor_bytes_works() {
    let mut cursor = io::Cursor::new([0x44, 0x01, 0x02, 0x03, 0x04]);
    let bytes = read_cbor_bytes(&mut cursor).unwrap();
    assert_eq!(bytes, [1, 2, 3, 4]);
  }

  #[test]
  fn read_cbor_text_works() {
    let mut cursor = io::Cursor::new([0x65, 0x68, 0x65, 0x6C, 0x6C, 0x6F]);
    let text = read_cbor_text(&mut cursor).unwrap();
    assert_eq!(text, "hello");
  }

  #[test]
  fn read_cbor_head_u64_works_for_embedded() {
    let mut cursor = io::Cursor::new([0x00]);
    let byte = take_byte(&mut cursor).unwrap();
    let value = read_cbor_head_u64(&mut cursor, byte).unwrap();
    assert_eq!(value, 0);

    let mut cursor = io::Cursor::new([0x17]);
    let byte = take_byte(&mut cursor).unwrap();
    let value = read_cbor_head_u64(&mut cursor, byte).unwrap();
    assert_eq!(value, 23);
  }

  #[test]
  fn read_cbor_head_u64_works_for_u8() {
    let mut cursor = io::Cursor::new([0x18, 0x64]);
    let byte = take_byte(&mut cursor).unwrap();
    let value = read_cbor_head_u64(&mut cursor, byte).unwrap();
    assert_eq!(value, 100);
  }

  #[test]
  fn read_cbor_head_u64_works_for_u16() {
    let mut cursor = io::Cursor::new([0x19, 0x01, 0x00]);
    let byte = take_byte(&mut cursor).unwrap();
    let value = read_cbor_head_u64(&mut cursor, byte).unwrap();
    assert_eq!(value, 256);

    let mut cursor = io::Cursor::new([0x19, 0xFF, 0xFF]);
    let byte = take_byte(&mut cursor).unwrap();
    let value = read_cbor_head_u64(&mut cursor, byte).unwrap();
    assert_eq!(value, 65535);
  }

  #[test]
  fn read_cbor_head_u64_works_for_u32() {
    let mut cursor = io::Cursor::new([0x1A, 0x00, 0x01, 0x00, 0x00]);
    let byte = take_byte(&mut cursor).unwrap();
    let value = read_cbor_head_u64(&mut cursor, byte).unwrap();
    assert_eq!(value, 65536);

    let mut cursor = io::Cursor::new([0x1A, 0xFF, 0xFF, 0xFF, 0xFF]);
    let byte = take_byte(&mut cursor).unwrap();
    let value = read_cbor_head_u64(&mut cursor, byte).unwrap();
    assert_eq!(value, 4294967295);
  }

  #[test]
  fn read_cbor_head_u64_works_for_u64() {
    let mut cursor = io::Cursor::new([0x1B, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00]);
    let byte = take_byte(&mut cursor).unwrap();
    let value = read_cbor_head_u64(&mut cursor, byte).unwrap();
    assert_eq!(value, 4294967296);

    let mut cursor = io::Cursor::new([0x1B, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
    let byte = take_byte(&mut cursor).unwrap();
    let value = read_cbor_head_u64(&mut cursor, byte).unwrap();
    assert_eq!(value, u64::MAX);
  }
}
