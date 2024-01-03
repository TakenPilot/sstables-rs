use std::{
  cmp::Ordering,
  collections::BinaryHeap,
  io::{self, Seek, SeekFrom},
};

use crate::outputs::{Emit, OutputWriter};
use sstables::{SSTableIndex, SSTableReader};

/// A tuple of (key, SSTableReader, SSTableIndex, index_pos, offset). The key is
/// the first element of the tuple, and is used for ordering. The ordering is the
/// reverse of the natural ordering so that the smallest key is at the top of the
/// heap. The SSTableReader, SSTableIndex, index_pos, and offset are used to
/// retrieve the next key and offset from the heap of SSTables.
#[derive(Debug)]
struct HeapTuple<K: Ord + Clone, V>(K, SSTableReader<(K, V)>, SSTableIndex<K>, usize, u64);

impl<K: Ord + Clone, V> Eq for HeapTuple<K, V> {}

impl<K: Ord + Clone, V> PartialEq for HeapTuple<K, V> {
  fn eq(&self, other: &Self) -> bool {
    self.0 == other.0
  }
}

impl<K: Ord + Clone, V> PartialOrd for HeapTuple<K, V> {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(other.0.cmp(&self.0))
  }
}

impl<K: Ord + Clone, V> Ord for HeapTuple<K, V> {
  fn cmp(&self, other: &Self) -> Ordering {
    // Implement your custom comparison logic here.
    // For example, you can compare based on the first element.
    other.0.cmp(&self.0)
  }
}

/// Initializes a heap with the first key from each SSTable index.
fn initialize_heap<K, V>(sstable_index_pairs: SSTableIndexPairs<K, V>) -> BinaryHeap<HeapTuple<K, V>>
where
  K: Ord + Clone,
{
  let mut heap = BinaryHeap::new();

  for (sstable, sstable_index) in sstable_index_pairs.into_iter() {
    // Move the sstable and sstable index into the heap. Since we only keep each
    // in at most one entry in heap, we can just move them without cloning or Rc.
    if let Some((key, offset)) = clone_index_entry(&sstable_index, 0) {
      heap.push(HeapTuple::<K, V>(key.clone(), sstable, sstable_index, 0, offset));
    }
  }

  heap
}

/// Clone the next index
fn clone_index_entry<K>(sstable_index: &SSTableIndex<K>, index_pos: usize) -> Option<(K, u64)>
where
  K: Ord + Clone,
{
  sstable_index
    .indices
    .get(index_pos)
    .map(|(key, offset)| (key.clone(), *offset))
}

/// Type alias for a tuple of (SSTableReader, SSTableIndex).
pub type SSTableIndexPairs<K, V> = Vec<(SSTableReader<(K, V)>, SSTableIndex<K>)>;

/// Trait for types that can be merged.
pub trait Mergeable {
  /// Returns the key-value pair.
  fn merge(self, emitter: &mut OutputWriter) -> io::Result<()>;
}

impl Mergeable for SSTableIndexPairs<String, String> {
  fn merge(self, emitter: &mut OutputWriter) -> io::Result<()>
  where
    OutputWriter: for<'a> Emit<(&'a str, &'a str)>,
  {
    let sstable_index_pairs = self;
    let mut heap = initialize_heap(sstable_index_pairs);

    // Merge the SSTables by popping the smallest key from the heap and emitting it.
    // Note that we ignore the key so that in the future we can transform it for different
    // kinds of sorts and orderings, such as CBOR vs native, or lexicographic vs numeric.
    while let Some(HeapTuple(_, mut sstable, sstable_index, index_pos, offset)) = heap.pop() {
      sstable.seek(SeekFrom::Start(offset))?;
      let (key, value) = sstable
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "Unexpected EOF"))??;

      emitter.emit((&key, &value))?;

      // Get the next key and offset from this SSTableIndex.
      let next_index_pos = index_pos + 1;
      let next_index_maybe = clone_index_entry(&sstable_index, next_index_pos);

      // If there is a next key, insert it into the heap.
      // Otherwise, we are done with this SSTable so we can let it drop.
      if let Some((next_key, next_offset)) = next_index_maybe {
        heap.push(HeapTuple(next_key, sstable, sstable_index, next_index_pos, next_offset));
      }
    }

    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use crate::outputs::{OutputDestination, OutputWriterBuilder};

  use super::*;
  use common_testing::{assert, setup};
  use sstables::{FromPath, SSTableIndex, SSTableReader, SSTableWriterBuilder};

  fn setup_test_sstables() -> io::Result<()> {
    // Setup the test by removing any existing files.
    {
      setup::create_dir_all(".tmp")?;
      setup::remove_file(".tmp/merge_test")?;
      setup::remove_file(".tmp/merge_test_1")?;
      setup::remove_file(".tmp/merge_test_1.index")?;
      setup::remove_file(".tmp/merge_test_2")?;
      setup::remove_file(".tmp/merge_test_2.index")?;
      setup::remove_file(".tmp/merge_test_3")?;
      setup::remove_file(".tmp/merge_test_3.index")?;
    }

    // Write three SSTables with the following key-value pairs:
    {
      let mut sstable_writer_1 = SSTableWriterBuilder::new(".tmp/merge_test_1").build()?;
      sstable_writer_1.write(("a", "1"))?;
      sstable_writer_1.write(("b", "2"))?;

      let mut sstable_writer_2 = SSTableWriterBuilder::new(".tmp/merge_test_2").build()?;
      sstable_writer_2.write(("c", "3"))?;
      sstable_writer_2.write(("d", "4"))?;

      let mut sstable_writer_3 = SSTableWriterBuilder::new(".tmp/merge_test_3").build()?;
      sstable_writer_3.write(("e", "5"))?;
      sstable_writer_3.write(("f", "6"))?;
    }

    Ok(())
  }

  fn setup_test_sstable_pairs() -> io::Result<SSTableIndexPairs<String, String>> {
    setup_test_sstables()?;

    let sstable_reader_1 = SSTableReader::<(String, String)>::from_path(".tmp/merge_test_1")?;
    let sstable_reader_2 = SSTableReader::<(String, String)>::from_path(".tmp/merge_test_2")?;
    let sstable_reader_3 = SSTableReader::<(String, String)>::from_path(".tmp/merge_test_3")?;

    let sstable_index_1 = SSTableIndex::<String>::from_path(".tmp/merge_test_1.index")?;
    let sstable_index_2 = SSTableIndex::<String>::from_path(".tmp/merge_test_2.index")?;
    let sstable_index_3 = SSTableIndex::<String>::from_path(".tmp/merge_test_3.index")?;

    let sstable_index_pairs = vec![
      (sstable_reader_1, sstable_index_1),
      (sstable_reader_2, sstable_index_2),
      (sstable_reader_3, sstable_index_3),
    ];

    Ok(sstable_index_pairs)
  }

  #[test]
  fn test_initialize_heap_keys() -> io::Result<()> {
    let _lock = setup::sequential();
    let sstable_index_pairs = setup_test_sstable_pairs()?;

    let heap = initialize_heap(sstable_index_pairs);

    // The heap should be ordered by the first element of each tuple.
    let result = heap
      .into_vec()
      .into_iter()
      .map(|HeapTuple(key, _, _, _, _)| key)
      .collect::<Vec<String>>();
    assert::equal(result, vec!["a", "c", "e"]);

    Ok(())
  }

  #[test]
  fn test_heap_tuple_ord() -> io::Result<()> {
    let _lock = setup::sequential();
    let sstable_index_pairs = setup_test_sstable_pairs()?;

    let mut heap = initialize_heap(sstable_index_pairs);

    // The heap should be ordered by the first element of each tuple.
    let HeapTuple(key1, _, _, _, _) = heap.pop().unwrap();
    let HeapTuple(key2, _, _, _, _) = heap.pop().unwrap();
    let HeapTuple(key3, _, _, _, _) = heap.pop().unwrap();
    assert::equal([key1, key2, key3], ["a", "c", "e"]);
    assert::none(&heap.pop());

    Ok(())
  }

  #[test]
  fn test_merge_1() -> io::Result<()> {
    // Merge the SSTables into a single SSTable.
    {
      let _lock = setup::sequential();
      setup_test_sstables()?;

      let sstable_reader_1 = SSTableReader::<(String, String)>::from_path(".tmp/merge_test_1")?;

      let sstable_index_1 = SSTableIndex::<String>::from_path(".tmp/merge_test_1.index")?;

      let sstable_index_pairs = vec![(sstable_reader_1, sstable_index_1)];

      let mut output_writer = OutputWriterBuilder::new(OutputDestination::File(".tmp/merge_test".into())).build()?;
      sstable_index_pairs.merge(&mut output_writer)?;
    }

    // Read the merged SSTable and compare it to the expected key-value pairs.
    {
      let mut sstable_reader = SSTableReader::<(String, String)>::from_path(".tmp/merge_test")?;

      // Write every key-value pair to a string and compare it to the expected string.
      let mut result = String::new();
      while let Some(Ok((key, value))) = sstable_reader.next() {
        result.push_str(&key);
        result.push_str(": ");
        result.push_str(&value);
        result.push('\n');
      }

      assert::equal(result, "a: 1\nb: 2\n");
    }

    Ok(())
  }

  #[test]
  fn test_merge() -> io::Result<()> {
    // Merge the SSTables into a single SSTable.
    {
      let _lock = setup::sequential();
      let sstable_index_pairs = setup_test_sstable_pairs()?;

      let mut output_writer = OutputWriterBuilder::new(OutputDestination::File(".tmp/merge_test".into())).build()?;
      sstable_index_pairs.merge(&mut output_writer)?;
    }

    // Read the merged SSTable and compare it to the expected key-value pairs.
    {
      let mut sstable_reader = SSTableReader::<(String, String)>::from_path(".tmp/merge_test")?;

      // Write every key-value pair to a string and compare it to the expected string.
      let mut result = String::new();
      while let Some(Ok((key, value))) = sstable_reader.next() {
        result.push_str(&key);
        result.push_str(": ");
        result.push_str(&value);
        result.push('\n');
      }

      assert::equal(result, "a: 1\nb: 2\nc: 3\nd: 4\ne: 5\nf: 6\n");
    }

    Ok(())
  }
}
