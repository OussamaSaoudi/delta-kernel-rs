//! Macros for reducing boilerplate in KDF state implementations.

/// Implement FFI conversion methods for a type.
///
/// This generates all the boilerplate for converting to/from raw pointers at FFI boundaries.
/// Methods are implemented as inherent methods (not trait methods) so they don't require
/// importing a trait to use.
///
/// # Example
///
/// ```ignore
/// impl_ffi_convertible!(FilterKdfState);
/// ```
#[macro_export]
macro_rules! impl_ffi_convertible {
    ($type:ty) => {
        impl $type {
            /// Convert to raw pointer for FFI (transfers ownership to the pointer).
            pub fn into_raw(self) -> u64 {
                Box::into_raw(Box::new(self)) as u64
            }

            /// Borrow from raw pointer without taking ownership.
            ///
            /// # Safety
            ///
            /// - `ptr` must have been created by `into_raw()`
            /// - `ptr` must still be valid (not freed)
            /// - The returned reference must not outlive the pointer's validity
            pub unsafe fn borrow_mut_from_raw<'a>(ptr: u64) -> &'a mut Self {
                &mut *(ptr as *mut Self)
            }

            /// Borrow immutably from raw pointer.
            ///
            /// # Safety
            ///
            /// Same requirements as `borrow_mut_from_raw`.
            pub unsafe fn borrow_from_raw<'a>(ptr: u64) -> &'a Self {
                &*(ptr as *const Self)
            }

            /// Reconstruct from raw pointer (takes ownership back).
            ///
            /// # Safety
            ///
            /// - `ptr` must have been created by `into_raw()`
            /// - `ptr` must still be valid (not freed)
            /// - After this call, `ptr` is invalid and must not be used
            pub unsafe fn from_raw(ptr: u64) -> Self {
                *Box::from_raw(ptr as *mut Self)
            }

            /// Free a raw pointer.
            ///
            /// # Safety
            ///
            /// - `ptr` must have been created by `into_raw()`
            /// - `ptr` must not have been freed already
            pub unsafe fn free_raw(ptr: u64) {
                if ptr != 0 {
                    drop(Box::from_raw(ptr as *mut Self));
                }
            }
        }
    };
}

/// Helper macro for declaring column names and types for RowVisitor.
///
/// This reduces boilerplate when declaring which columns a visitor accesses.
///
/// # Example
///
/// ```ignore
/// fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
///     column_names_and_types![
///         DataType::STRING => column_name!("add.path"),
///         DataType::LONG => column_name!("add.size"),
///     ]
/// }
/// ```
#[macro_export]
macro_rules! column_names_and_types {
    ($($dt:expr => $col:expr),+ $(,)?) => {{
        static NAMES_AND_TYPES: std::sync::LazyLock<$crate::schema::ColumnNamesAndTypes> =
            std::sync::LazyLock::new(|| {
                let types_and_names = vec![$(($dt, $col)),+];
                let (types, names) = types_and_names.into_iter().unzip();
                (names, types).into()
            });
        NAMES_AND_TYPES.as_ref()
    }};
}

// Re-export so it's available when using the crate
pub use impl_ffi_convertible;
pub use column_names_and_types;

