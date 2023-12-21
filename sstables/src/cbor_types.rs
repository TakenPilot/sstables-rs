#![allow(dead_code)]
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

use std::io::{self, Write};

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

/// Major types for CBOR data items.
/// Each type corresponds to the high-order 3 bits in the initial byte of a CBOR data item.
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
  #[inline]
  pub fn as_u8(&self) -> u8 {
    *self as u8
  }

  /// Get the MajorType enum from the first three bits of the initial byte.
  #[inline]
  pub fn from_u8(value: u8) -> Self {
    // Use only first three bits. This is safe because we've mapped a value for each possible bit.
    unsafe { ::std::mem::transmute(value & FIRST_THREE_BITS) }
  }
}

/// Large values are stored in the bytes following the initial byte.
/// The initial byte uses this enum to determine how many.
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

/// Get the last five bits of a byte.
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

/// Get the bytes for a large number, including the last five bits of the initial byte.
/// Optimized for the common case of small numbers.
/// See https://tools.ietf.org/html/rfc7049#section-2.1 for the encoding rules.
pub fn get_value_bytes(value: u64) -> Vec<u8> {
  let num_bytes = get_num_bytes_for_u64(value);

  // Pre-allocating vector with exact size needed: 1 byte for the embedded value, plus num_bytes.
  let mut v = Vec::with_capacity(1 + num_bytes);

  // First byte is always the embedded value.
  v.push(get_embedded_value_for_u64(value));

  // If there are additional bytes, add them.
  if num_bytes > 0 {
    // Extracting only the necessary bytes.
    let bytes = value.to_be_bytes();
    let start_pos = 8 - num_bytes;
    v.extend_from_slice(&bytes[start_pos..]);
  }

  v
}

/// Writes a CBOR head that identifies the bytes that follow.
/// A CBOR head is 1-9 bytes, depending on the size of the value.
pub fn write_cbor_head<W: Write>(major_type: MajorType, value: u64, writer: &mut W) -> io::Result<()> {
  writer.write_all(&[major_type.as_u8() | get_embedded_value_for_u64(value)])?;
  let num_bytes = get_num_bytes_for_u64(value);
  if num_bytes > 0 {
    let bytes = value.to_be_bytes();
    let start_pos = bytes.len() - num_bytes;
    writer.write_all(&bytes[start_pos..])?;
  };
  Ok(())
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
  fn get_value_bytes_works() {
    assert_eq!(get_value_bytes(0), vec![0]);
    assert_eq!(get_value_bytes(1), vec![1]);
    assert_eq!(get_value_bytes(23), vec![23]);
    assert_eq!(get_value_bytes(24), vec![24, 24]);
    assert_eq!(get_value_bytes(25), vec![24, 25]);
    assert_eq!(get_value_bytes(255), vec![24, 255]);
    assert_eq!(get_value_bytes(256), vec![25, 1, 0]);
    assert_eq!(get_value_bytes(65535), vec![25, 255, 255]);
    assert_eq!(get_value_bytes(65536), vec![26, 0, 1, 0, 0]);
    assert_eq!(get_value_bytes(4294967295), vec![26, 255, 255, 255, 255]);
    assert_eq!(get_value_bytes(4294967296), vec![27, 0, 0, 0, 1, 0, 0, 0, 0]);
    assert_eq!(
      get_value_bytes(u64::MAX),
      vec![27, 255, 255, 255, 255, 255, 255, 255, 255]
    );
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

    write_cbor_head(MajorType::UnsignedInteger, 0, &mut v).unwrap();
    assert_eq!(v, vec![0]);
    v.clear();

    write_cbor_head(MajorType::NegativeInteger, 0, &mut v).unwrap();
    assert_eq!(v, vec![32]);
    v.clear();

    write_cbor_head(MajorType::Bytes, 0, &mut v).unwrap();
    assert_eq!(v, vec![64]);
    v.clear();

    write_cbor_head(MajorType::Text, 0, &mut v).unwrap();
    assert_eq!(v, vec![96]);
    v.clear();

    write_cbor_head(MajorType::Array, 0, &mut v).unwrap();
    assert_eq!(v, vec![128]);
    v.clear();

    write_cbor_head(MajorType::Object, 0, &mut v).unwrap();
    assert_eq!(v, vec![160]);
    v.clear();

    write_cbor_head(MajorType::SemanticTag, 0, &mut v).unwrap();
    assert_eq!(v, vec![192]);
    v.clear();

    write_cbor_head(MajorType::NoContentType, 0, &mut v).unwrap();
    assert_eq!(v, vec![224]);
    v.clear();

    write_cbor_head(MajorType::Bytes, 40000, &mut v).unwrap();
    assert_eq!(v, vec![89, 156, 64]);
    v.clear();

    write_cbor_head(MajorType::Bytes, 66000, &mut v).unwrap();
    assert_eq!(v, vec![90, 0, 1, 1, 208]);
    v.clear();

    write_cbor_head(MajorType::Bytes, 4294967296, &mut v).unwrap();
    assert_eq!(v, vec![91, 0, 0, 0, 1, 0, 0, 0, 0]);
    v.clear();

    write_cbor_head(MajorType::Bytes, u64::MAX, &mut v).unwrap();
    assert_eq!(v, vec![91, 255, 255, 255, 255, 255, 255, 255, 255]);
    v.clear();
  }
}
