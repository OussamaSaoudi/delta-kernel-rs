//! Kernel-Defined Function (KDF) state management.
//!
//! This module provides state types for KDFs - functions that engines must call
//! via FFI because they contain Delta-specific logic that cannot be externalized.
//!
//! KDFs are categorized by their input/output signatures:
//! - **Filter KDFs**: (batch, selection) -> selection - row-level filtering
//! - **Consumer KDFs**: (batch) -> bool - batch consumption (Continue/Break)
//! - **Schema Reader KDFs**: () -> schema - schema extraction

pub mod state_channel;
pub mod traits;
pub mod macros;
pub mod serialization;
pub mod filter;
pub mod consumer;
pub mod schema;

// Re-export state_channel types for state management
pub use state_channel::{
    ConsumerStateReceiver, ConsumerStateSender, FilterStateReceiver, FilterStateSender,
    OwnedFilterState, OwnedState, StateReceiver, StateSender,
};
pub use filter::*;
pub use consumer::*;
pub use schema::*;
pub use traits::*;
pub use serialization::*;

// Re-export macros at crate root for easier access
pub use macros::*;

