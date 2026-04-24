//! Phase surface: the async API coroutine bodies use to drive
//! [`super::engine::CoroutineSM`].
//!
//! Three pieces, all kernel-internal:
//!
//! - [`PhaseYield`] / [`PhaseResume`] / [`PhaseCo`] — the typed protocol
//!   flowing through the coroutine: yield plans and receive either a task
//!   ack (on dispatch) or a completed [`PhaseKdfState`] / [`EngineError`]
//!   (on await).
//! - [`Handle`] — typed receipt for a dispatched plan; produced by
//!   [`Phase::dispatch`] and consumed by [`Phase::await_handle`]. Carries
//!   the KDF token so the correct per-partition payloads are extracted.
//! - [`Phase`] — ergonomic wrappers around the raw yields (`dispatch`,
//!   `execute`, `try_execute`, `run_schema_query`, ...). SM authors
//!   write their phase body as `phase.execute(prepared).await`.
//!
//! Panics in this module are reserved for internal coroutine-protocol
//! invariants — the driver assigns task IDs and matches `PhaseYield` /
//! `PhaseResume` variants in lockstep, so a mismatched pair is a kernel
//! bug. The `expect_used` / `panic` clippy lints are allowed for that
//! reason; user-facing failures still flow through [`DeltaError`].
#![allow(clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use crate::delta_error;
use crate::plans::errors::{DeltaError, DeltaErrorCode};
use crate::plans::ir::{Plan, Prepared};
use crate::plans::kdf::typed::ExtractFn;
use crate::plans::kdf::KdfStateToken;
use crate::plans::state_machines::framework::engine_error::EngineError;
use crate::plans::state_machines::framework::phase_kdf_state::PhaseKdfState;
use crate::plans::state_machines::framework::phase_operation::SchemaQueryNode;
use crate::schema::{SchemaRef, StructType};

use super::generator::Co;

/// Value yielded by a coroutine at each phase boundary. The driver drains
/// [`Dispatch`](Self::Dispatch) yields into a single batch until an
/// [`Await`](Self::Await) or [`SchemaQuery`](Self::SchemaQuery), then
/// presents the batch to the engine.
pub(crate) enum PhaseYield {
    /// Dispatch a plan to the engine. The coroutine receives
    /// [`PhaseResume::Dispatched`] with an assigned task ID and continues
    /// without blocking.
    Dispatch {
        plan: Plan,
        phase_name: &'static str,
    },
    /// Await a previously dispatched task. Blocks until the engine returns
    /// merged [`PhaseKdfState`] for this batch.
    Await { task_id: u64 },
    /// Request a metadata-only file-schema read.
    SchemaQuery {
        node: SchemaQueryNode,
        phase_name: &'static str,
    },
}

/// Value the driver passes back to the coroutine on resume.
pub(crate) enum PhaseResume {
    /// Ack for a [`PhaseYield::Dispatch`] — carries the assigned task ID.
    Dispatched { task_id: u64 },
    /// Result for an [`Await`](PhaseYield::Await) or
    /// [`SchemaQuery`](PhaseYield::SchemaQuery).
    Completed(Result<PhaseKdfState, EngineError>),
}

impl std::fmt::Debug for PhaseResume {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dispatched { task_id } => {
                write!(f, "Dispatched {{ task_id: {task_id} }}")
            }
            Self::Completed(Ok(_)) => f.write_str("Completed(Ok(..))"),
            Self::Completed(Err(e)) => write!(f, "Completed(Err({:?}))", e.kind),
        }
    }
}

/// Coroutine handle used inside phase bodies. Thin alias over the generic
/// [`Co`] from the [`generator`](super::generator) shim.
pub(crate) type PhaseCo = Co<PhaseYield, PhaseResume>;

/// Typed receipt for a dispatched plan. Returned by [`Phase::dispatch`]
/// and resolved by [`Phase::await_handle`].
///
/// Carries the task ID assigned by the driver, the KDF token so the
/// extractor knows which handles to pull from the returned
/// [`PhaseKdfState`], and the typed extractor itself. The
/// `phase_name` is only used in error-context strings.
pub(crate) struct Handle<R> {
    task_id: u64,
    phase_name: &'static str,
    token: KdfStateToken,
    extract: ExtractFn<R>,
}

/// Async surface SM authors call. Holds a mutable borrow of the coroutine
/// handle; lives only for the duration of one phase body.
pub(crate) struct Phase<'a>(pub(crate) &'a mut PhaseCo);

impl<'a> Phase<'a> {
    /// Dispatch a [`Prepared`] plan (non-blocking). The coroutine receives
    /// a task ID immediately and continues running; multiple dispatches
    /// before the first await are batched by the driver into one
    /// `PhaseOperation::Plans` for the engine.
    pub(crate) async fn dispatch<R>(
        &mut self,
        prepared: Prepared<R>,
        name: &'static str,
    ) -> Handle<R>
    where
        R: Send + 'static,
    {
        let token = prepared.token().clone();
        let (plan, extract) = prepared.into_parts();
        let resume = self
            .0
            .yield_(PhaseYield::Dispatch {
                plan,
                phase_name: name,
            })
            .await;
        let task_id = match resume {
            PhaseResume::Dispatched { task_id } => task_id,
            PhaseResume::Completed(_) => {
                panic!("dispatch: expected Dispatched resume, got Completed")
            }
        };
        Handle {
            task_id,
            phase_name: name,
            token,
            extract,
        }
    }

    /// Await a previously dispatched [`Handle`], returning the typed
    /// output.
    ///
    /// Failure policy: any [`EngineError`] or extraction failure is lifted
    /// into a [`DeltaError`] tagged
    /// [`DeltaCommandInvariantViolation`](DeltaErrorCode::DeltaCommandInvariantViolation).
    /// SMs that need to branch on a specific
    /// [`EngineErrorKind`](super::super::engine_error::EngineErrorKind)
    /// (e.g. `FileNotFound`) MUST use
    /// [`try_await_handle`](Self::try_await_handle) instead.
    pub(crate) async fn await_handle<R>(&mut self, handle: Handle<R>) -> Result<R, DeltaError> {
        let phase_name = handle.phase_name;
        let resume = self
            .0
            .yield_(PhaseYield::Await {
                task_id: handle.task_id,
            })
            .await;
        match resume {
            PhaseResume::Completed(Ok(kdf)) => {
                let parts = kdf.take_by_token(&handle.token);
                (handle.extract)(parts).map_err(|e| {
                    delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        operation = format!("Phase::await_handle::extraction[phase={phase_name}]"),
                        detail = e.to_string(),
                        source = e,
                    )
                })
            }
            // `detail` MUST render the full source chain: wire-
            // reconstructed `EngineErrorKind::Internal` carries its
            // payload exclusively in `source`, so a plain `e.to_string()`
            // collapses to "internal engine error" and the underlying
            // engine-side cause is silently swallowed at the rendered
            // `DeltaError` boundary. The `source = e` binding preserves
            // programmatic access for callers that downcast the chain.
            PhaseResume::Completed(Err(e)) => Err(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                operation = format!("Phase::await_handle::execution[phase={phase_name}]"),
                detail = e.display_with_source_chain(),
                source = e,
            )),
            PhaseResume::Dispatched { .. } => {
                panic!("await_handle: expected Completed resume, got Dispatched")
            }
        }
    }

    /// Like [`await_handle`](Self::await_handle) but exposes the raw
    /// [`EngineError`] for pattern matching on its typed kind.
    ///
    /// Extraction failures (post-engine, kernel-side typed projection
    /// errors) are wrapped as
    /// [`EngineErrorKind::Internal`](super::super::engine_error::EngineErrorKind::Internal)
    /// with the originating [`DeltaError`] attached as `source`.
    pub(crate) async fn try_await_handle<R>(
        &mut self,
        handle: Handle<R>,
    ) -> Result<R, EngineError> {
        let resume = self
            .0
            .yield_(PhaseYield::Await {
                task_id: handle.task_id,
            })
            .await;
        match resume {
            PhaseResume::Completed(Ok(kdf)) => {
                let parts = kdf.take_by_token(&handle.token);
                (handle.extract)(parts).map_err(EngineError::internal)
            }
            PhaseResume::Completed(Err(e)) => Err(e),
            PhaseResume::Dispatched { .. } => {
                panic!("try_await_handle: expected Completed resume, got Dispatched")
            }
        }
    }

    /// Execute a [`Prepared`] phase: dispatch + immediate await.
    ///
    /// The primary API for single-plan phases. Sugar for
    /// `self.dispatch(prepared, name).await` followed by
    /// `self.await_handle(handle).await`.
    pub(crate) async fn execute<R>(
        &mut self,
        prepared: Prepared<R>,
        name: &'static str,
    ) -> Result<R, DeltaError>
    where
        R: Send + 'static,
    {
        let handle = self.dispatch(prepared, name).await;
        self.await_handle(handle).await
    }

    /// Like [`execute`](Self::execute) but exposes the raw [`EngineError`]
    /// for pattern matching. Use when the caller needs to handle specific
    /// variants (e.g., `FileNotFound`) without mapping to `DeltaResult`
    /// first.
    pub(crate) async fn try_execute<R>(
        &mut self,
        prepared: Prepared<R>,
        name: &'static str,
    ) -> Result<R, EngineError>
    where
        R: Send + 'static,
    {
        let handle = self.dispatch(prepared, name).await;
        self.try_await_handle(handle).await
    }

    /// Yield a schema-query op, execute it, and return the stored schema
    /// (if any). Errors from the engine are surfaced as a [`DeltaError`]
    /// tagged `DeltaCommandInvariantViolation` with the originating
    /// [`EngineError`] attached as `source`.
    pub(crate) async fn run_schema_query(
        &mut self,
        file_path: impl Into<String>,
        name: &'static str,
    ) -> Result<Option<SchemaRef>, DeltaError> {
        // Mint a token matching the SchemaQueryNode's expected key.
        let token = KdfStateToken::new("schema_query");
        let node = SchemaQueryNode::new(file_path, token.to_string());
        let resume = self
            .0
            .yield_(PhaseYield::SchemaQuery {
                node,
                phase_name: name,
            })
            .await;
        match resume {
            PhaseResume::Completed(Ok(kdf)) => {
                let parts = kdf.take_by_token(&token);
                match parts.len() {
                    0 => Ok(None),
                    1 => {
                        let boxed = parts.into_iter().next().expect("len==1");
                        let schema: StructType = *boxed.downcast::<StructType>().map_err(|_| {
                            delta_error!(
                                DeltaErrorCode::DeltaCommandInvariantViolation,
                                operation = "Phase::run_schema_query::extract",
                                phase = name,
                                detail = "schema-query result was not a StructType".to_string(),
                            )
                        })?;
                        Ok(Some(Arc::new(schema)))
                    }
                    n => Err(delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        operation = "Phase::run_schema_query::extract",
                        phase = name,
                        detail = format!("expected 0 or 1 schema result, got {n}"),
                    )),
                }
            }
            PhaseResume::Completed(Err(e)) => Err(delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                operation = "Phase::run_schema_query::execution",
                phase = name,
                detail = e.display_with_source_chain(),
                source = e,
            )),
            PhaseResume::Dispatched { .. } => {
                panic!("run_schema_query: expected Completed resume, got Dispatched")
            }
        }
    }
}
