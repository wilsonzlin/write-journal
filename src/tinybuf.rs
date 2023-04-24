use off64::int::create_u56_le;
use off64::int::Off64ReadInt;
use off64::u64;
use off64::usz;
use std::cmp::Ordering;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::mem::forget;
use std::mem::size_of;
use std::ops::Deref;
use std::sync::Arc;

// TODO We assume that `usize` always represents the word size.
#[allow(unused)]
const WORD_SIZE: usize = size_of::<usize>();
// One byte is taken up by the enum discriminant, and another byte by the length.
// We chose 24 bytes because:
// - It's very common to write 8 or 16 bytes, which would require 17 bytes (1 byte for the length). Due to padding, it becomes 24 bytes anyway.
// - Many widely used types (Arc, Box, HashMap, Rc, Vec) are no more than 24 bytes, so it seems like a good cutoff.
// - Three times the word size should still be fast to copy as a rule of thumb.
#[allow(unused)]
const ARRAY_CAP: usize = WORD_SIZE * 3 - 1;

// TODO Currently we've designed and optimised this library for x86-64. However, keep using abstraction vars/consts/macros/etc. instead of hardcoding numbers/assumptions so that when this assertion is removed everything keeps working. What to change:
// - TinyBuf::Vec `cap` array length and endianness.
// - Amount of TinyBuf::Array* variants.
static_assertions::const_assert_eq!(WORD_SIZE, 8);

/// Immutable variable-length owned slice of bytes that tries to accept many different types (i.e. generic), similar to `Box<dyn AsRef<[u8]>>` but without boxing (i.e. heap allocation).
///
/// Unsupported types:
/// - `Cow`: borrowed data, so requires either owned variant or 'static lifetime, both of which are already supported.
/// - `Rc`: not `Send` and `Sync`.
pub enum TinyBuf {
  Arc(Arc<dyn AsRef<[u8]> + Send + Sync + 'static>),
  BoxDyn(Box<dyn AsRef<[u8]> + Send + Sync + 'static>),
  BoxSlice(Box<[u8]>),
  Static(&'static [u8]),
  // This is separate from the Box variants as converting Vec to Box using `into_boxed_slice` will require a reallocation unless capacity is exactly the same as the length.
  // A Vec takes up 3 * WORD_SIZE, which means if we also add the enum discriminant it becomes 4 * WORD_SIZE due to padding, which is too much (see earlier notes about why we picked 3 * WORD_SIZE). Therefore, we decompose a Vec into its raw parts, which means we have to shave off one byte somewhere; we picked `cap`, as it's the least used field. This limits `cap` to 2^56 on 64-bit systems, which should be more than enough. We store it as the first field, as we assume Rust will place the enum discriminant byte first, so this keeps `ptr` and `len` aligned. (This is an assumption because Rust struct layout is undefined.) We can't just discard `cap` because it's required for Vec to drop, presumably because Rust's allocator requires `Layout` when deallocating (not just the pointer). We don't simply cast `cap` to u32 as it's not entirely unreasonable to have a Vec with more than 4 GiB of data.
  Vec {
    // We must choose an endianness, as otherwise we can't know which byte to discard. We choose little endian since most modern CPUs are.
    cap: [u8; 7],
    ptr: *const u8,
    len: usize,
  },
  // By creating one variant per length, we reuse enum discriminant byte which allows us to have one more length.
  Array0([u8; 0]),
  Array1([u8; 1]),
  Array2([u8; 2]),
  Array3([u8; 3]),
  Array4([u8; 4]),
  Array5([u8; 5]),
  Array6([u8; 6]),
  Array7([u8; 7]),
  Array8([u8; 8]),
  Array9([u8; 9]),
  Array10([u8; 10]),
  Array11([u8; 11]),
  Array12([u8; 12]),
  Array13([u8; 13]),
  Array14([u8; 14]),
  Array15([u8; 15]),
  Array16([u8; 16]),
  Array17([u8; 17]),
  Array18([u8; 18]),
  Array19([u8; 19]),
  Array20([u8; 20]),
  Array21([u8; 21]),
  Array22([u8; 22]),
  Array23([u8; 23]),
}

// We must manually impl as we store a pointer for the Vec variant. We can't extract Vec variant out into own struct buecause it would be padded to 3 * WORD_SIZE, which reverts the entire purpose of it.
unsafe impl Send for TinyBuf {}
unsafe impl Sync for TinyBuf {}

// If TinyBuf is larger than the Array variant, then we're wasting unused capacity and should expand Array length.
// NOTE: These work by casting the boolean to usize then subtracting one, which will underflow and fail if 0 (false). Be aware of this as the error message will look strange (something about overflow).
static_assertions::const_assert_eq!(size_of::<Vec<u8>>(), WORD_SIZE * 3);
static_assertions::const_assert_eq!(size_of::<Box<dyn AsRef<[u8]>>>(), WORD_SIZE * 2);
static_assertions::const_assert_eq!(size_of::<TinyBuf>(), WORD_SIZE * 3);

impl TinyBuf {
  /// This may heap allocate if the length is larger than `ARRAY_CAP`.
  pub fn from_slice(s: &[u8]) -> Self {
    match s.len() {
      0 => TinyBuf::Array0(s.try_into().unwrap()),
      1 => TinyBuf::Array1(s.try_into().unwrap()),
      2 => TinyBuf::Array2(s.try_into().unwrap()),
      3 => TinyBuf::Array3(s.try_into().unwrap()),
      4 => TinyBuf::Array4(s.try_into().unwrap()),
      5 => TinyBuf::Array5(s.try_into().unwrap()),
      6 => TinyBuf::Array6(s.try_into().unwrap()),
      7 => TinyBuf::Array7(s.try_into().unwrap()),
      8 => TinyBuf::Array8(s.try_into().unwrap()),
      9 => TinyBuf::Array9(s.try_into().unwrap()),
      10 => TinyBuf::Array10(s.try_into().unwrap()),
      11 => TinyBuf::Array11(s.try_into().unwrap()),
      12 => TinyBuf::Array12(s.try_into().unwrap()),
      13 => TinyBuf::Array13(s.try_into().unwrap()),
      14 => TinyBuf::Array14(s.try_into().unwrap()),
      15 => TinyBuf::Array15(s.try_into().unwrap()),
      16 => TinyBuf::Array16(s.try_into().unwrap()),
      17 => TinyBuf::Array17(s.try_into().unwrap()),
      18 => TinyBuf::Array18(s.try_into().unwrap()),
      19 => TinyBuf::Array19(s.try_into().unwrap()),
      20 => TinyBuf::Array20(s.try_into().unwrap()),
      21 => TinyBuf::Array21(s.try_into().unwrap()),
      22 => TinyBuf::Array22(s.try_into().unwrap()),
      23 => TinyBuf::Array23(s.try_into().unwrap()),
      // Prefer `Box<[u8]>` over `Vec<u8>` as our Vec variant is a bit slower.
      _ => s.to_vec().into_boxed_slice().into(),
    }
  }

  pub fn as_slice(&self) -> &[u8] {
    match self {
      // The first `as_ref` is on Arc, the second is on `AsRef`. Arc::as_ref returns a reference to the contained value.
      TinyBuf::Arc(v) => v.as_ref().as_ref(),
      // The first `as_ref` is on Box, the second is on `AsRef`. Box::as_ref returns a reference to the contained value.
      TinyBuf::BoxDyn(v) => v.as_ref().as_ref(),
      TinyBuf::BoxSlice(v) => v,
      TinyBuf::Static(v) => v,
      TinyBuf::Vec { len, ptr, .. } => unsafe { std::slice::from_raw_parts(*ptr, *len) },
      TinyBuf::Array0(v) => v.as_slice(),
      TinyBuf::Array1(v) => v.as_slice(),
      TinyBuf::Array2(v) => v.as_slice(),
      TinyBuf::Array3(v) => v.as_slice(),
      TinyBuf::Array4(v) => v.as_slice(),
      TinyBuf::Array5(v) => v.as_slice(),
      TinyBuf::Array6(v) => v.as_slice(),
      TinyBuf::Array7(v) => v.as_slice(),
      TinyBuf::Array8(v) => v.as_slice(),
      TinyBuf::Array9(v) => v.as_slice(),
      TinyBuf::Array10(v) => v.as_slice(),
      TinyBuf::Array11(v) => v.as_slice(),
      TinyBuf::Array12(v) => v.as_slice(),
      TinyBuf::Array13(v) => v.as_slice(),
      TinyBuf::Array14(v) => v.as_slice(),
      TinyBuf::Array15(v) => v.as_slice(),
      TinyBuf::Array16(v) => v.as_slice(),
      TinyBuf::Array17(v) => v.as_slice(),
      TinyBuf::Array18(v) => v.as_slice(),
      TinyBuf::Array19(v) => v.as_slice(),
      TinyBuf::Array20(v) => v.as_slice(),
      TinyBuf::Array21(v) => v.as_slice(),
      TinyBuf::Array22(v) => v.as_slice(),
      TinyBuf::Array23(v) => v.as_slice(),
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
    match self {
      TinyBuf::Arc(v) => TinyBuf::Arc(v.clone()),
      TinyBuf::Static(v) => TinyBuf::Static(v),
      TinyBuf::Array0(v) => TinyBuf::Array0(v.clone()),
      TinyBuf::Array1(v) => TinyBuf::Array1(v.clone()),
      TinyBuf::Array2(v) => TinyBuf::Array2(v.clone()),
      TinyBuf::Array3(v) => TinyBuf::Array3(v.clone()),
      TinyBuf::Array4(v) => TinyBuf::Array4(v.clone()),
      TinyBuf::Array5(v) => TinyBuf::Array5(v.clone()),
      TinyBuf::Array6(v) => TinyBuf::Array6(v.clone()),
      TinyBuf::Array7(v) => TinyBuf::Array7(v.clone()),
      TinyBuf::Array8(v) => TinyBuf::Array8(v.clone()),
      TinyBuf::Array9(v) => TinyBuf::Array9(v.clone()),
      TinyBuf::Array10(v) => TinyBuf::Array10(v.clone()),
      TinyBuf::Array11(v) => TinyBuf::Array11(v.clone()),
      TinyBuf::Array12(v) => TinyBuf::Array12(v.clone()),
      TinyBuf::Array13(v) => TinyBuf::Array13(v.clone()),
      TinyBuf::Array14(v) => TinyBuf::Array14(v.clone()),
      TinyBuf::Array15(v) => TinyBuf::Array15(v.clone()),
      TinyBuf::Array16(v) => TinyBuf::Array16(v.clone()),
      TinyBuf::Array17(v) => TinyBuf::Array17(v.clone()),
      TinyBuf::Array18(v) => TinyBuf::Array18(v.clone()),
      TinyBuf::Array19(v) => TinyBuf::Array19(v.clone()),
      TinyBuf::Array20(v) => TinyBuf::Array20(v.clone()),
      TinyBuf::Array21(v) => TinyBuf::Array21(v.clone()),
      TinyBuf::Array22(v) => TinyBuf::Array22(v.clone()),
      TinyBuf::Array23(v) => TinyBuf::Array23(v.clone()),
      v => Self::from_slice(v.as_slice()),
    }
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

impl Drop for TinyBuf {
  fn drop(&mut self) {
    match self {
      TinyBuf::Vec { cap, ptr, len } => {
        // See comments at TinyBuf::Vec declaration.
        let cap = usz!(cap.read_u56_le_at(0));
        unsafe {
          Vec::from_raw_parts(ptr, *len, cap);
        };
      }
      _ => {}
    }
  }
}

impl From<Arc<dyn AsRef<[u8]> + Send + Sync + 'static>> for TinyBuf {
  fn from(value: Arc<dyn AsRef<[u8]> + Send + Sync + 'static>) -> Self {
    Self::Arc(value)
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
    // See comments at TinyBuf::Vec declaration.
    let ptr = value.as_ptr();
    let cap = u64!(value.capacity());
    assert!(cap < (1 << 56));
    let cap = create_u56_le(u64!(value.capacity()));
    let len = value.len();
    forget(value);
    Self::Vec { cap, ptr, len }
  }
}

/*

FROM ARRAY IMPLS
================

We don't implement for all lengths, as arrays with length greater than ARRAY_CAP must be moved onto the heap, and we'd prefer to make the caller aware and explicitly opt into it.

*/

#[rustfmt::skip] impl From<[u8; 0]> for TinyBuf { fn from(value: [u8; 0]) -> Self { Self::Array0(value) } }
#[rustfmt::skip] impl From<[u8; 1]> for TinyBuf { fn from(value: [u8; 1]) -> Self { Self::Array1(value) } }
#[rustfmt::skip] impl From<[u8; 2]> for TinyBuf { fn from(value: [u8; 2]) -> Self { Self::Array2(value) } }
#[rustfmt::skip] impl From<[u8; 3]> for TinyBuf { fn from(value: [u8; 3]) -> Self { Self::Array3(value) } }
#[rustfmt::skip] impl From<[u8; 4]> for TinyBuf { fn from(value: [u8; 4]) -> Self { Self::Array4(value) } }
#[rustfmt::skip] impl From<[u8; 5]> for TinyBuf { fn from(value: [u8; 5]) -> Self { Self::Array5(value) } }
#[rustfmt::skip] impl From<[u8; 6]> for TinyBuf { fn from(value: [u8; 6]) -> Self { Self::Array6(value) } }
#[rustfmt::skip] impl From<[u8; 7]> for TinyBuf { fn from(value: [u8; 7]) -> Self { Self::Array7(value) } }
#[rustfmt::skip] impl From<[u8; 8]> for TinyBuf { fn from(value: [u8; 8]) -> Self { Self::Array8(value) } }
#[rustfmt::skip] impl From<[u8; 9]> for TinyBuf { fn from(value: [u8; 9]) -> Self { Self::Array9(value) } }
#[rustfmt::skip] impl From<[u8; 10]> for TinyBuf { fn from(value: [u8; 10]) -> Self { Self::Array10(value) } }
#[rustfmt::skip] impl From<[u8; 11]> for TinyBuf { fn from(value: [u8; 11]) -> Self { Self::Array11(value) } }
#[rustfmt::skip] impl From<[u8; 12]> for TinyBuf { fn from(value: [u8; 12]) -> Self { Self::Array12(value) } }
#[rustfmt::skip] impl From<[u8; 13]> for TinyBuf { fn from(value: [u8; 13]) -> Self { Self::Array13(value) } }
#[rustfmt::skip] impl From<[u8; 14]> for TinyBuf { fn from(value: [u8; 14]) -> Self { Self::Array14(value) } }
#[rustfmt::skip] impl From<[u8; 15]> for TinyBuf { fn from(value: [u8; 15]) -> Self { Self::Array15(value) } }
#[rustfmt::skip] impl From<[u8; 16]> for TinyBuf { fn from(value: [u8; 16]) -> Self { Self::Array16(value) } }
#[rustfmt::skip] impl From<[u8; 17]> for TinyBuf { fn from(value: [u8; 17]) -> Self { Self::Array17(value) } }
#[rustfmt::skip] impl From<[u8; 18]> for TinyBuf { fn from(value: [u8; 18]) -> Self { Self::Array18(value) } }
#[rustfmt::skip] impl From<[u8; 19]> for TinyBuf { fn from(value: [u8; 19]) -> Self { Self::Array19(value) } }
#[rustfmt::skip] impl From<[u8; 20]> for TinyBuf { fn from(value: [u8; 20]) -> Self { Self::Array20(value) } }
#[rustfmt::skip] impl From<[u8; 21]> for TinyBuf { fn from(value: [u8; 21]) -> Self { Self::Array21(value) } }
#[rustfmt::skip] impl From<[u8; 22]> for TinyBuf { fn from(value: [u8; 22]) -> Self { Self::Array22(value) } }
#[rustfmt::skip] impl From<[u8; 23]> for TinyBuf { fn from(value: [u8; 23]) -> Self { Self::Array23(value) } }
