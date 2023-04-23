use off64::u8;
use off64::usz;
use std::cmp::Ordering;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::mem::size_of;
use std::ops::Deref;

const WORD_SIZE: usize = size_of::<usize>();
// One byte is taken up by the enum discriminant, and another byte by the length.
const ARRAY_CAP: usize = WORD_SIZE * 4 - 2;

/// Immutable variable-length owned slice of bytes that tries to accept many different types (i.e. generic), similar to `Box<dyn AsRef<[u8]>>` but without boxing (i.e. heap allocation).
pub enum TinyBuf {
  // TODO Could we somehoww reuse the enum discriminant byte for the length, perhaps using `union`?
  Array(u8, [u8; ARRAY_CAP]),
  BoxDyn(Box<dyn AsRef<[u8]> + Send + Sync + 'static>),
  BoxSlice(Box<[u8]>),
  Static(&'static [u8]),
  // This is separate from the Box variants as converting Vec to Box using `into_boxed_slice` will require a reallocation unless capacity is exactly the same as the length.
  Vec(Vec<u8>),
}

// If TinyBuf is larger than the Array variant, then we're wasting unused capacity and should expand Array length.
// NOTE: These work by casting the boolean to usize then subtracting one, which will underflow and fail if 0 (false). Be aware of this as the error message will look strange (something about overflow).
static_assertions::const_assert_eq!(size_of::<Vec<u8>>(), WORD_SIZE * 3);
static_assertions::const_assert_eq!(size_of::<Box<dyn AsRef<[u8]>>>(), WORD_SIZE * 2);
static_assertions::const_assert_eq!(size_of::<TinyBuf>(), WORD_SIZE * 4);

impl TinyBuf {
  pub fn as_slice(&self) -> &[u8] {
    match self {
      TinyBuf::Array(len, v) => &v[..usz!(*len)],
      // The first `as_ref` is on Box, the second is on `AsRef`. Box::as_ref returns a reference to the contained value.
      TinyBuf::BoxDyn(v) => v.as_ref().as_ref(),
      TinyBuf::BoxSlice(v) => v,
      TinyBuf::Static(v) => v,
      TinyBuf::Vec(v) => v.as_slice(),
    }
  }
}

impl AsRef<[u8]> for TinyBuf {
  fn as_ref(&self) -> &[u8] {
    self.as_slice()
  }
}

impl Deref for TinyBuf {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    self.as_slice()
  }
}

impl Clone for TinyBuf {
  fn clone(&self) -> Self {
    Self::Vec(self.as_slice().to_vec())
  }
}

impl PartialEq for TinyBuf {
  fn eq(&self, other: &Self) -> bool {
    self.as_slice() == other.as_slice()
  }
}

impl Eq for TinyBuf {}

impl PartialOrd for TinyBuf {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    self.as_slice().partial_cmp(other.as_slice())
  }
}

impl Ord for TinyBuf {
  fn cmp(&self, other: &Self) -> Ordering {
    self.as_slice().cmp(other.as_slice())
  }
}

impl Hash for TinyBuf {
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.as_slice().hash(state);
  }
}

impl Debug for TinyBuf {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    self.as_slice().fmt(f)
  }
}

impl<const N: usize> From<[u8; N]> for TinyBuf {
  /// NOTE: This will copy the bytes if the array can fit inline; otherwise, the array will be moved into a boxed slice allocated on the heap.
  fn from(value: [u8; N]) -> Self {
    // TODO We could have a branch for `N == ARRAY_CAP` that avoids copying, but Rust will stil think the types mismatch, even with `.try_from`.
    if N <= ARRAY_CAP {
      let mut array = [0u8; ARRAY_CAP];
      array[..N].copy_from_slice(&value);
      Self::Array(u8!(N), array)
    } else {
      Self::BoxSlice(Box::new(value))
    }
  }
}

impl From<Box<dyn AsRef<[u8]> + Send + Sync + 'static>> for TinyBuf {
  fn from(value: Box<dyn AsRef<[u8]> + Send + Sync + 'static>) -> Self {
    Self::BoxDyn(value)
  }
}

impl From<Box<[u8]>> for TinyBuf {
  fn from(value: Box<[u8]>) -> Self {
    Self::BoxSlice(value)
  }
}

impl From<&'static [u8]> for TinyBuf {
  fn from(value: &'static [u8]) -> Self {
    Self::Static(value)
  }
}

impl From<Vec<u8>> for TinyBuf {
  fn from(value: Vec<u8>) -> Self {
    Self::Vec(value)
  }
}
