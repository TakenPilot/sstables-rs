use std::cmp::Ordering;

pub fn is_sorted_by<T, F>(slice: &[T], mut compare: F) -> bool
where
  F: FnMut(&T, &T) -> bool,
{
  slice.windows(2).all(|w| compare(&w[0], &w[1]))
}

/// Returns the min and max of a slice of `Ord` items. If the slice is empty,
/// returns `None`. If the slice is not empty, returns `Some((min, max))`.
///
/// # Examples
///
/// ```
/// use sstable_cli::get_min_max;
///
/// let slice = &[2, 1, 3, 5, 4];
/// let (min, max) = get_min_max(slice).unwrap();
/// assert_eq!(min, &1);
/// assert_eq!(max, &5);
/// ```
pub fn get_min_max<T>(slice: &[T]) -> Option<(&T, &T)>
where
  T: Ord,
{
  if slice.is_empty() {
    return None;
  }

  let mut min = &slice[0];
  let mut max = &slice[0];

  for item in slice.iter().skip(1) {
    if item < min {
      min = item;
    }
    if item > max {
      max = item;
    }
  }

  Some((min, max))
}

/// Returns true if every key is unique in the index
///
/// # Examples
///
/// ```
/// use sstable_cli::is_unique;
///
/// let slice = &[2, 1, 3, 5, 4];
/// let unique = is_unique(slice, |a, b| a == b);
/// assert_eq!(unique, true);
/// ```
///
pub fn is_unique<T>(slice: &[T], compare: fn(&T, &T) -> bool) -> bool {
  let mut unique = true;
  let mut last_key = slice.first().unwrap();
  for key in slice.iter().skip(1) {
    if compare(key, last_key) {
      unique = false;
      break;
    }
    last_key = key;
  }
  unique
}

/// Compares two tuples by their first element.
/// # Examples
///
/// ```
/// use sstable_cli::util::compare_tuples;
///
/// assert_eq!(compare_tuples(&(1, 2), &(1, 3)), std::cmp::Ordering::Equal);
/// assert_eq!(compare_tuples(&(1, 2), &(2, 3)), std::cmp::Ordering::Less);
/// assert_eq!(compare_tuples(&(2, 2), &(1, 3)), std::cmp::Ordering::Greater);
/// ```
pub fn compare_tuples<T: Ord, U>(a: &(T, U), b: &(T, U)) -> Ordering {
  a.0.cmp(&b.0)
}
