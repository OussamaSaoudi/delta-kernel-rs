//! Phase-level KDF state accumulator.
//!
//! `PhaseKdfState` is the thread-safe container the executor fills as it
//! finalizes per-partition KDF state, and that the SM consumes after each
//! phase completes. It holds [`FinishedHandle`]s keyed by their
//! [`KdfStateToken`] — the typetag-erased `Box<dyn Any + Send>` payloads the
//! KDF framework v2 produces via [`Kdf::finish`](crate::plans::kdf::Kdf::finish).
//!
//! ## Lifecycle
//!
//! 1. Executor calls [`PhaseKdfState::empty`] at the start of a phase.
//! 2. Each per-partition KDF iterator calls
//!    [`Handle::finish`](crate::plans::kdf::Handle::finish) to produce a
//!    [`FinishedHandle`] and hands it to [`PhaseKdfState::submit`].
//! 3. When the phase terminates, the executor returns the (cheap-to-clone)
//!    `PhaseKdfState` to the SM as part of `PhaseResume::Completed`.
//! 4. The SM's `Phase::await_handle` call extracts the erased `Vec<Box<dyn
//!    Any + Send>>` via [`PhaseKdfState::take_by_token`] and feeds it to the
//!    typed `Prepared<O>` extractor.
//!
//! ## Design notes
//!
//! Unlike the source prototype, no serde round-trip happens at submit /
//! extract time — the `Box<dyn Any + Send>` erased payload is handed
//! directly to [`downcast_all`](crate::plans::kdf::downcast_all) /
//! [`take_single`](crate::plans::kdf::take_single). Typetag-based
//! serialization, when needed for FFI, happens separately on the
//! `Box<dyn FilterKdf>` / `Box<dyn ConsumerKdf>` side before `.finish()`.
//!
//! `expect_used` is allowed below for `Mutex::lock().expect("...")`: a
//! poisoned mutex is a kernel bug (some earlier thread panicked while
//! holding it), not a recoverable error.

#![allow(clippy::expect_used)]

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::plans::kdf::{FinishedHandle, KdfStateToken};

/// Internal storage. All public methods take the outer [`Mutex`] briefly.
#[derive(Debug, Default)]
struct PhaseKdfStateInner {
    entries: HashMap<KdfStateToken, Vec<FinishedHandle>>,
}

/// Thread-safe KDF handle accumulator for one phase execution.
///
/// Cheap to [`Clone`] — inner state is shared through an `Arc<Mutex<_>>`.
/// Cloning is expected: the executor clones the container into each KDF
/// iterator's worker so partition finalization can happen concurrently.
#[derive(Clone, Debug, Default)]
pub struct PhaseKdfState {
    inner: Arc<Mutex<PhaseKdfStateInner>>,
}

impl PhaseKdfState {
    /// Construct an empty accumulator.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Submit a finalized per-partition KDF state. Thread-safe; callers
    /// typically hold a cloned `PhaseKdfState` inside their iterator.
    pub fn submit(&self, handle: FinishedHandle) {
        let mut inner = self.inner.lock().expect("PhaseKdfState poisoned");
        inner
            .entries
            .entry(handle.token.clone())
            .or_default()
            .push(handle);
    }

    /// Remove and return all erased per-partition payloads for `token`.
    ///
    /// Returns an empty `Vec` if the token was never submitted to. The
    /// typed `Prepared<O>` extractor feeds this directly into
    /// [`downcast_all`](crate::plans::kdf::downcast_all).
    pub fn take_by_token(&self, token: &KdfStateToken) -> Vec<Box<dyn Any + Send>> {
        let mut inner = self.inner.lock().expect("PhaseKdfState poisoned");
        inner
            .entries
            .remove(token)
            .map(|handles| handles.into_iter().map(|h| h.erased).collect())
            .unwrap_or_default()
    }

    /// Merge `other` into `self`. Entries under the same token are appended
    /// in iteration order. Used when an executor shards work across
    /// sub-executors and recombines their outputs.
    pub fn merge(&self, other: &PhaseKdfState) {
        let mut mine = self.inner.lock().expect("PhaseKdfState poisoned");
        let mut theirs = other.inner.lock().expect("PhaseKdfState poisoned");
        for (token, handles) in theirs.entries.drain() {
            mine.entries.entry(token).or_default().extend(handles);
        }
    }

    /// `true` if no handles have been submitted.
    pub fn is_empty(&self) -> bool {
        self.inner
            .lock()
            .expect("PhaseKdfState poisoned")
            .entries
            .is_empty()
    }

    /// Number of tokens with at least one submitted handle.
    pub fn token_count(&self) -> usize {
        self.inner
            .lock()
            .expect("PhaseKdfState poisoned")
            .entries
            .len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plans::kdf::TraceContext;

    fn finished_handle(token: &KdfStateToken, partition: u32, payload: i64) -> FinishedHandle {
        FinishedHandle {
            token: token.clone(),
            ctx: TraceContext::new("test", "phase"),
            partition,
            erased: Box::new(payload),
        }
    }

    #[test]
    fn empty_accumulator_has_no_tokens() {
        let s = PhaseKdfState::empty();
        assert!(s.is_empty());
        assert_eq!(s.token_count(), 0);
    }

    #[test]
    fn submit_and_take_roundtrips_payloads() {
        let s = PhaseKdfState::empty();
        let t = KdfStateToken::new("test.one");

        s.submit(finished_handle(&t, 0, 10));
        s.submit(finished_handle(&t, 1, 20));

        assert_eq!(s.token_count(), 1);

        let payloads = s.take_by_token(&t);
        assert_eq!(payloads.len(), 2);
        let v0 = *payloads[0].downcast_ref::<i64>().unwrap();
        let v1 = *payloads[1].downcast_ref::<i64>().unwrap();
        assert_eq!(v0 + v1, 30);

        // take_by_token drains: a second call returns empty.
        assert!(s.take_by_token(&t).is_empty());
        assert!(s.is_empty());
    }

    #[test]
    fn take_by_unknown_token_returns_empty() {
        let s = PhaseKdfState::empty();
        let t = KdfStateToken::new("test.never_submitted");
        assert!(s.take_by_token(&t).is_empty());
    }

    #[test]
    fn merge_appends_handles_for_same_token() {
        let a = PhaseKdfState::empty();
        let b = PhaseKdfState::empty();
        let t = KdfStateToken::new("test.shared");

        a.submit(finished_handle(&t, 0, 1));
        b.submit(finished_handle(&t, 1, 2));
        a.merge(&b);

        assert!(b.is_empty(), "merge drains `other`");
        let payloads = a.take_by_token(&t);
        assert_eq!(payloads.len(), 2);
    }

    #[test]
    fn clone_shares_inner_state() {
        let a = PhaseKdfState::empty();
        let b = a.clone();
        let t = KdfStateToken::new("test.shared");

        a.submit(finished_handle(&t, 0, 99));

        let via_b = b.take_by_token(&t);
        assert_eq!(via_b.len(), 1);
        assert!(a.is_empty(), "take through clone drained the shared state");
    }
}
