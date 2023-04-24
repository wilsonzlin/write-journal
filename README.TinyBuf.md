# TinyBuf

## Types

|Source type|Notes|
|---|---|
|`[u8; N]` where `N <= 23`|Stored inline, no heap allocation or cloning/copying when converting from.|
|`Arc<dyn AsRef<[u8]> + Send + Sync + 'static>`||
|`Box<dyn AsRef<[u8]> + Send + Sync + 'static>`||
|`Box<[u8]>`|This is a separate variant as converting to `Box<dyn AsRef<u8>>` would require further boxing.|
|`&'static [u8]`||
|`Vec<u8>`|This is a separate variant as `into_boxed_slice` may reallocate. Capacity must be less than `2^56`.|

## Cloning

If the data can fit inline, it will be cloned into a new `TinyBuf::Array*` variant. If it's an `Arc`, it will be cheaply cloned. Otherwise, it will be cloned into a new `TinyBuf::Vec`.
