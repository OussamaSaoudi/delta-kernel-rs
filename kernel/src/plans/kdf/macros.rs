//! Macros for KDF implementations.
//!
//! The trivial `Kdf` supertrait methods (`kdf_id` + `finish`) are identical
//! across every concrete KDF — just a distinct string and `Box::new(*self)`.
//! [`impl_kdf!`] collapses them to a single line, leaving each KDF file to
//! contain only the real per-KDF logic (the `apply` body, `KdfOutput`
//! reducer, and `RowVisitor` column list + row loop).

/// Generate `impl Kdf for $Type` with the given `$kdf_id` string and the
/// canonical `finish(self) = Box::new(*self)`.
///
/// ```ignore
/// impl_kdf!(CheckpointHintReader, "consumer.checkpoint_hint");
/// ```
#[macro_export]
macro_rules! impl_kdf {
    ($ty:ty, $id:literal) => {
        impl $crate::plans::kdf::Kdf for $ty {
            fn kdf_id(&self) -> &'static str {
                $id
            }
            fn finish(self: Box<Self>) -> Box<dyn ::std::any::Any + ::std::marker::Send> {
                Box::new(*self)
            }
        }
    };
}
