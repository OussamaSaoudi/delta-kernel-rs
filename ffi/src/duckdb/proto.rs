//! Generated prost types for the declarative plan IR.
//!
//! Compiled from `ffi/proto/{plan,expressions,schema}.proto` by `build.rs` (prost-build) when the
//! `duckdb` feature is enabled. The module hierarchy mirrors the proto package names
//! (`delta.kernel.{schema,expressions,plan}`) so prost's cross-package `super::` references
//! resolve correctly.
#![allow(clippy::all)]
#![allow(missing_docs)]

pub mod delta {
    pub mod kernel {
        pub mod schema {
            include!(concat!(env!("OUT_DIR"), "/delta.kernel.schema.rs"));
        }
        pub mod expressions {
            include!(concat!(env!("OUT_DIR"), "/delta.kernel.expressions.rs"));
        }
        pub mod plan {
            include!(concat!(env!("OUT_DIR"), "/delta.kernel.plan.rs"));
        }
    }
}

pub use delta::kernel::{expressions, plan, schema};

/// Re-export of [`prost::Message`] so consumers (tests, decoders) can call `decode`/`encode`
/// without taking a direct prost dependency.
pub use prost::Message;
