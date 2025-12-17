//! Core traits for KDF state management.

use crate::DeltaResult;

/// Base trait for all KDF states.
///
/// Each KDF state implements this trait with appropriate Input/Output types.
pub trait KdfState {
    type Input;
    type Output;
    
    /// Apply the KDF function to the input data.
    fn apply(&mut self, input: Self::Input) -> DeltaResult<Self::Output>;
}

/// Trait for states that can be serialized and distributed to executors.
///
/// This is required for KDFs that need to run in distributed environments.
pub trait Serializable: Sized {
    /// Serialize the state to bytes for distribution.
    fn serialize(&self) -> DeltaResult<Vec<u8>>;
    
    /// Deserialize the state from bytes (received on executor).
    fn deserialize(bytes: &[u8]) -> DeltaResult<Self>;
}

/// Trait for states that can be converted to/from raw pointers for FFI.
///
/// This trait is typically implemented via the `impl_ffi_convertible!` macro.
pub trait FfiConvertible: Sized {
    /// Convert to raw pointer for FFI (transfers ownership).
    fn into_raw(self) -> u64;
    
    /// Borrow mutably from raw pointer without taking ownership.
    ///
    /// # Safety
    ///
    /// - `ptr` must have been created by `into_raw()`
    /// - `ptr` must still be valid (not freed)
    /// - The returned reference must not outlive the pointer's validity
    unsafe fn borrow_mut_from_raw<'a>(ptr: u64) -> &'a mut Self;
    
    /// Borrow immutably from raw pointer.
    ///
    /// # Safety
    ///
    /// Same requirements as `borrow_mut_from_raw`.
    unsafe fn borrow_from_raw<'a>(ptr: u64) -> &'a Self;
    
    /// Reconstruct from raw pointer (takes ownership back).
    ///
    /// # Safety
    ///
    /// - `ptr` must have been created by `into_raw()`
    /// - `ptr` must still be valid (not freed)
    /// - After this call, `ptr` is invalid and must not be used
    unsafe fn from_raw(ptr: u64) -> Self;
    
    /// Free a raw pointer.
    ///
    /// # Safety
    ///
    /// - `ptr` must have been created by `into_raw()`
    /// - `ptr` must not have been freed already
    unsafe fn free_raw(ptr: u64);
}

