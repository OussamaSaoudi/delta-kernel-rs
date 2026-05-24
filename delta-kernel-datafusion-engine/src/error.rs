//! Error helpers for the DataFusion engine.
//!
//! Engine internals operate in [`DataFusionError`] space: every helper here produces a
//! [`DataFusionError`] variant, so engine code can use bare `?` to propagate errors. Conversion
//! into kernel-flavored errors ([`DeltaError`], `EngineError`) happens at the engine ->
//! kernel boundary methods on the executor (which lives in a downstream module of this
//! workspace slice), and is exposed as the [`DfResultIntoDelta`] extension trait so boundary
//! call sites can write `.into_delta()` instead of `.map_err(df_to_delta)`.

use datafusion_common::error::DataFusionError;
use delta_kernel::delta_error;
use delta_kernel::plans::errors::{DeltaError, DeltaErrorCode};

/// Wrap an arbitrary error chain into a [`DataFusionError::External`].
///
/// Bridges kernel-side errors (e.g. [`DeltaError`], kernel `Error`) into the
/// engine's native [`DataFusionError`] flow.
pub fn wrap_delta_err<E>(err: E) -> DataFusionError
where
    E: std::error::Error + Send + Sync + 'static,
{
    DataFusionError::External(Box::new(err))
}

/// Typed plan-compilation failure for the DataFusion engine path.
pub fn plan_compilation(detail: impl Into<String>) -> DataFusionError {
    DataFusionError::Plan(format!("PlanCompilation: {}", detail.into()))
}

/// Explicitly unsupported IR for this scaffold / engine slice.
pub fn unsupported(detail: impl Into<String>) -> DataFusionError {
    DataFusionError::NotImplemented(format!("Unsupported: {}", detail.into()))
}

/// Engine-internal invariant violation.
pub fn internal_error(detail: impl Into<String>) -> DataFusionError {
    DataFusionError::Internal(format!("Internal: {}", detail.into()))
}

/// Convert a [`DataFusionError`] produced by engine internals into a [`DeltaError`] at the
/// engine -> kernel boundary. [`DataFusionError::External`] values that already wrap a
/// [`DeltaError`] are unwrapped so callers receive the original typed error instead of a nested
/// wrapper.
///
/// The kernel's [`DeltaErrorCode`] surface is Delta-domain-specific (no `Unsupported` /
/// `Plan` etc.), so every non-Delta DataFusion error is tagged
/// [`DeltaErrorCode::DeltaCommandInvariantViolation`] with the DataFusion variant name
/// prepended to the message. The original [`DataFusionError`] is preserved as the error
/// source so callers can walk `std::error::Error::source()` for the original variant.
///
/// This is intentionally a catch-all today: kernel does not yet carry typed codes for
/// "unsupported" or "plan compilation" failures (only `DeltaCommandInvariantViolation`
/// exists as a generic catch-all). When the kernel adds finer-grained codes, this match
/// should be widened so that `DataFusionError::NotImplemented` -> "unsupported" and
/// `DataFusionError::Plan` -> "plan compilation" map to dedicated codes rather than
/// collapsing into an invariant-violation umbrella.
///
/// Both the orphan rule (foreign-on-foreign forbids `impl From<DataFusionError> for DeltaError`)
/// and the lift-typed-`External` semantics keep this as a free function; the
/// [`DfResultIntoDelta`] trait is the call-site sugar.
pub fn df_to_delta(e: DataFusionError) -> DeltaError {
    match e {
        DataFusionError::External(inner) => match inner.downcast::<DeltaError>() {
            Ok(delta_err) => *delta_err,
            Err(orig) => {
                let wrapped = DataFusionError::External(orig);
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    source = wrapped,
                    "DF(External)",
                )
            }
        },
        other => {
            let variant = df_variant_name(&other);
            delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                source = other,
                "DF({variant})",
            )
        }
    }
}

/// Short tag for a [`DataFusionError`] variant. Used by [`df_to_delta`] to surface the
/// original variant name in the [`DeltaError`] message without dropping it into the source
/// chain only.
fn df_variant_name(e: &DataFusionError) -> &'static str {
    match e {
        DataFusionError::ArrowError(..) => "ArrowError",
        DataFusionError::ParquetError(..) => "ParquetError",
        DataFusionError::ObjectStore(..) => "ObjectStore",
        DataFusionError::IoError(..) => "IoError",
        DataFusionError::SQL(..) => "SQL",
        DataFusionError::NotImplemented(..) => "NotImplemented",
        DataFusionError::Internal(..) => "Internal",
        DataFusionError::Plan(..) => "Plan",
        DataFusionError::Configuration(..) => "Configuration",
        DataFusionError::SchemaError(..) => "SchemaError",
        DataFusionError::Execution(..) => "Execution",
        DataFusionError::ExecutionJoin(..) => "ExecutionJoin",
        DataFusionError::ResourcesExhausted(..) => "ResourcesExhausted",
        DataFusionError::External(..) => "External",
        DataFusionError::Context(..) => "Context",
        DataFusionError::Substrait(..) => "Substrait",
        _ => "Other",
    }
}

/// Convert a `Result<T, DataFusionError>` into `Result<T, DeltaError>` via `.into_delta()?`.
/// Concentrates the engine -> kernel error transition at engine API boundaries into a single
/// fluent method, so call sites don't repeat `.map_err(df_to_delta)`.
pub(crate) trait DfResultIntoDelta<T> {
    fn into_delta(self) -> Result<T, DeltaError>;
}

impl<T> DfResultIntoDelta<T> for Result<T, DataFusionError> {
    fn into_delta(self) -> Result<T, DeltaError> {
        self.map_err(df_to_delta)
    }
}
