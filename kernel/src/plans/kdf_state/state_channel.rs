//! State channel infrastructure for zero per-batch locking.
//!
//! This module provides [`StateSender`], [`StateReceiver`], and [`OwnedState`] for managing
//! KDF state across parallel partition execution. The sender/receiver split enables:
//!
//! - **Plans hold `StateSender`**: Creates owned states for each partition
//! - **Phases hold `StateReceiver`**: Collects results after execution completes
//!
//! This pattern supports both local and (future) remote execution while maintaining
//! zero locking on the hot path.
//!
//! # Example
//!
//! ```ignore
//! // Build sender/receiver pair from a template
//! let (sender, receiver) = StateSender::build(MyState::default());
//!
//! // Plan contains sender, create owned states for partitions
//! let owned1 = sender.create_owned();
//! let owned2 = sender.create_owned();
//!
//! // Use states (zero locking on hot path)
//! owned1.state_mut().process(batch);
//! owned2.state_mut().process(batch);
//!
//! // Drop deposits states to channel
//! drop(owned1);
//! drop(owned2);
//!
//! // Phase contains receiver, collect all states
//! let states = receiver.take_all()?;
//! assert_eq!(states.len(), 2);
//! ```

use crate::{DeltaResult, Error};
use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::Arc;

// =============================================================================
// StateSender - Holds template, creates owned states for partitions
// =============================================================================

/// Sender side of the state channel - holds template, creates owned states.
///
/// The sender is stored in the plan and cloned when the plan is distributed.
/// Each call to [`create_owned`](Self::create_owned) creates a fresh state by
/// cloning the template, and registers it for tracking.
///
/// # Thread Safety
///
/// `StateSender` is `Clone` and `Send + Sync`. Multiple threads can call
/// `create_owned()` concurrently - each gets an independent [`OwnedState`].
#[derive(Debug)]
pub struct StateSender<S: Clone + Send + 'static> {
    /// Template state to clone for each partition
    template: S,
    /// Channel sender for collecting finished states
    sender: mpsc::Sender<S>,
    /// Counter shared with receiver to verify all states are collected
    created: Arc<AtomicUsize>,
}

impl<S: Clone + Send + 'static> Clone for StateSender<S> {
    fn clone(&self) -> Self {
        Self {
            template: self.template.clone(),
            sender: self.sender.clone(),
            created: self.created.clone(),
        }
    }
}

impl<S: Clone + Send + 'static> StateSender<S> {
    /// Build a sender/receiver pair from a template state.
    ///
    /// The template is cloned for each partition when [`create_owned`](Self::create_owned)
    /// is called. The receiver should be stored in the phase for later collection.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let (sender, receiver) = StateSender::build(FilterKdfState::AddRemoveDedup(
    ///     AddRemoveDedupState::new()
    /// ));
    ///
    /// // sender goes into the plan
    /// // receiver goes into the phase
    /// ```
    pub fn build(template: S) -> (Self, StateReceiver<S>) {
        let (tx, rx) = mpsc::channel();
        let created = Arc::new(AtomicUsize::new(0));
        (
            Self {
                template,
                sender: tx,
                created: created.clone(),
            },
            StateReceiver {
                receiver: rx,
                created,
            },
        )
    }

    /// Create owned state for a partition (clones template).
    ///
    /// Each call:
    /// 1. Increments the created counter
    /// 2. Clones the template state
    /// 3. Returns an [`OwnedState`] that will auto-send on drop
    ///
    /// The returned state provides zero-locking access during stream execution.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut owned = sender.create_owned();
    /// owned.state_mut().apply(batch, selection)?;
    /// // State is sent to receiver when owned is dropped
    /// ```
    pub fn create_owned(&self) -> OwnedState<S> {
        self.created.fetch_add(1, Ordering::SeqCst);
        OwnedState {
            state: ManuallyDrop::new(self.template.clone()),
            sender: self.sender.clone(),
        }
    }

    /// Get a reference to the template state.
    ///
    /// Useful for inspecting the initial state configuration.
    pub fn template(&self) -> &S {
        &self.template
    }

    /// Get the number of owned states created so far.
    ///
    /// Note: This may be called concurrently with `create_owned()`, so the
    /// count may increase between reads.
    pub fn created_count(&self) -> usize {
        self.created.load(Ordering::SeqCst)
    }
}

// =============================================================================
// StateReceiver - Held by phase, collects results
// =============================================================================

/// Receiver side of the state channel - held by phase, collects results.
///
/// The receiver is stored in the phase (e.g., `CommitPhase`, `CheckpointPhase`)
/// and used to collect all partition states after execution completes.
///
/// # Single Use
///
/// [`take_all`](Self::take_all) consumes the receiver and verifies that
/// the number of collected states matches the number of created states.
#[derive(Debug)]
pub struct StateReceiver<S: Send + 'static> {
    /// Channel receiver for collecting finished states
    receiver: mpsc::Receiver<S>,
    /// Counter shared with sender to verify all states are collected
    created: Arc<AtomicUsize>,
}

impl<S: Send + 'static> StateReceiver<S> {
    /// Drain all states and verify count matches created.
    ///
    /// This method consumes the receiver and:
    /// 1. Drains all states from the channel
    /// 2. Verifies the count matches the number of `create_owned()` calls
    ///
    /// # Errors
    ///
    /// Returns an error if the number of collected states doesn't match
    /// the number of created states. This indicates streams are still running
    /// or states were lost.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // After all partitions complete...
    /// let states = receiver.take_all()?;
    ///
    /// // Merge states
    /// let mut merged = State::new();
    /// for state in states {
    ///     merged.merge(state);
    /// }
    /// ```
    pub fn take_all(self) -> DeltaResult<Vec<S>> {
        let expected = self.created.load(Ordering::SeqCst);
        let states: Vec<S> = self.receiver.try_iter().collect();

        if states.len() != expected {
            return Err(Error::generic(format!(
                "State count mismatch: expected {}, got {} (streams may still be running)",
                expected,
                states.len()
            )));
        }
        Ok(states)
    }

    /// Get the expected count of states.
    ///
    /// Returns the number of `create_owned()` calls made on the sender.
    pub fn expected_count(&self) -> usize {
        self.created.load(Ordering::SeqCst)
    }
}

// =============================================================================
// OwnedState - Held by stream, sends on Drop
// =============================================================================

/// Owned state handle - sends to channel on drop.
///
/// Provides zero-locking access to KDF state during stream execution.
/// When dropped, the state is automatically sent back to the [`StateReceiver`]
/// via the channel.
///
/// # Usage
///
/// ```ignore
/// let mut owned = sender.create_owned();
///
/// // Zero-locking access
/// owned.state_mut().apply(batch, selection)?;
///
/// // State is sent when owned goes out of scope
/// ```
///
/// # Panic Safety
///
/// Even if the stream panics, the state will be sent on drop (though the
/// channel may be disconnected if the receiver was also affected).
#[derive(Debug)]
pub struct OwnedState<S: Send + 'static> {
    /// The state itself - wrapped in ManuallyDrop to prevent double-free on Drop
    state: ManuallyDrop<S>,
    /// Channel sender for returning state when done
    sender: mpsc::Sender<S>,
}

impl<S: Send + 'static> OwnedState<S> {
    /// Get a reference to the state.
    pub fn state(&self) -> &S {
        &self.state
    }

    /// Get a mutable reference to the state.
    ///
    /// This is the primary access method during stream execution.
    /// No locking is required - the state is fully owned.
    pub fn state_mut(&mut self) -> &mut S {
        &mut self.state
    }

    /// Consume the owned state and return the inner state without sending.
    ///
    /// **Warning**: This bypasses the automatic send-on-drop behavior.
    /// Use only when you need to manually control when/if the state is sent.
    /// The receiver's count will not match if you call this.
    pub fn into_inner(self) -> S {
        // Prevent Drop from running by forgetting self
        let mut this = ManuallyDrop::new(self);
        // SAFETY: We're taking ownership and forgetting self, so this is safe
        unsafe { ManuallyDrop::take(&mut this.state) }
    }
}

impl<S: Send + 'static> Drop for OwnedState<S> {
    fn drop(&mut self) {
        // SAFETY: Drop runs exactly once, state is never accessed after
        let state = unsafe { ManuallyDrop::take(&mut self.state) };
        // Ignore send errors - receiver may have been dropped
        let _ = self.sender.send(state);
    }
}

impl<S: Send + 'static> AsRef<S> for OwnedState<S> {
    fn as_ref(&self) -> &S {
        self.state()
    }
}

impl<S: Send + 'static> AsMut<S> for OwnedState<S> {
    fn as_mut(&mut self) -> &mut S {
        self.state_mut()
    }
}

// =============================================================================
// Specialized implementations for KDF state types
// =============================================================================

use crate::arrow::array::BooleanArray;
use crate::EngineData;

use super::filter::FilterKdfState;
use super::ConsumerKdfState;

/// Specialized implementation for filter KDF states.
///
/// Provides a convenient `apply` method that delegates to the inner state,
/// avoiding the need to call `state_mut().apply(...)`.
impl OwnedState<FilterKdfState> {
    /// Apply the filter to a batch - zero-locking, direct state access.
    ///
    /// This is a convenience method that delegates to the inner
    /// [`FilterKdfState::apply`] method.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut owned = sender.create_owned();
    /// let filtered = owned.apply(&batch, selection)?;
    /// ```
    #[inline]
    pub fn apply(
        &mut self,
        batch: &dyn EngineData,
        selection: BooleanArray,
    ) -> DeltaResult<BooleanArray> {
        self.state.apply(batch, selection)
    }
}

/// Type alias for filter KDF sender.
///
/// Used in plans that need to distribute filter state to partitions.
pub type FilterStateSender = StateSender<FilterKdfState>;

/// Convenience constructors for filter KDF senders.
///
/// These create sender/receiver pairs with pre-configured filter states.
/// The receiver is typically stored in the phase for later collection.
impl StateSender<FilterKdfState> {
    /// Create an AddRemoveDedup filter sender (for commit phase).
    ///
    /// Returns both the sender (goes into the plan) and receiver (goes into the phase).
    /// The receiver can be discarded if state collection is not needed.
    pub fn add_remove_dedup() -> (Self, StateReceiver<FilterKdfState>) {
        Self::build(FilterKdfState::AddRemoveDedup(
            super::filter::AddRemoveDedupState::new(),
        ))
    }

    /// Create a CheckpointDedup filter sender (for checkpoint phase).
    ///
    /// Returns both the sender (goes into the plan) and receiver (goes into the phase).
    /// The receiver can be discarded if state collection is not needed.
    pub fn checkpoint_dedup() -> (Self, StateReceiver<FilterKdfState>) {
        Self::build(FilterKdfState::CheckpointDedup(
            super::filter::CheckpointDedupState::new(),
        ))
    }
}

/// Type alias for filter KDF receiver.
///
/// Used in phases that need to collect filter state after execution.
pub type FilterStateReceiver = StateReceiver<FilterKdfState>;

/// Type alias for filter KDF owned state.
///
/// Used in execution streams for zero-lock access to filter state.
/// Created via `FilterStateSender::create_owned()`.
pub type OwnedFilterState = OwnedState<FilterKdfState>;

/// Type alias for consumer KDF sender.
///
/// Used in plans that need to distribute consumer state to partitions.
pub type ConsumerStateSender = StateSender<ConsumerKdfState>;

/// Type alias for consumer KDF receiver.
///
/// Used in phases that need to collect consumer state after execution.
pub type ConsumerStateReceiver = StateReceiver<ConsumerKdfState>;

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_partition() {
        let (sender, receiver) = StateSender::build(vec![1, 2, 3]);

        let mut owned = sender.create_owned();
        owned.state_mut().push(4);

        drop(owned);

        let states = receiver.take_all().unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0], vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_multiple_partitions() {
        let (sender, receiver) = StateSender::build(0i32);

        let mut owned1 = sender.create_owned();
        let mut owned2 = sender.create_owned();
        let mut owned3 = sender.create_owned();

        *owned1.state_mut() = 10;
        *owned2.state_mut() = 20;
        *owned3.state_mut() = 30;

        drop(owned1);
        drop(owned2);
        drop(owned3);

        let states = receiver.take_all().unwrap();
        assert_eq!(states.len(), 3);
        assert_eq!(states.iter().sum::<i32>(), 60);
    }

    #[test]
    fn test_sender_is_clone() {
        let (sender, receiver) = StateSender::build(0i32);

        let sender2 = sender.clone();

        let mut owned1 = sender.create_owned();
        let mut owned2 = sender2.create_owned();

        *owned1.state_mut() = 10;
        *owned2.state_mut() = 20;

        drop(owned1);
        drop(owned2);

        let states = receiver.take_all().unwrap();
        assert_eq!(states.len(), 2);
        assert_eq!(states.iter().sum::<i32>(), 30);
    }

    #[test]
    fn test_mismatch_count_fails() {
        let (sender, receiver) = StateSender::build(());

        let _owned = sender.create_owned(); // Not dropped

        let result = receiver.take_all();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("State count mismatch"));
    }

    #[test]
    fn test_created_count() {
        let (sender, _receiver) = StateSender::build(0i32);

        assert_eq!(sender.created_count(), 0);

        let _owned1 = sender.create_owned();
        assert_eq!(sender.created_count(), 1);

        let _owned2 = sender.create_owned();
        assert_eq!(sender.created_count(), 2);
    }

    #[test]
    fn test_template_access() {
        let (sender, _receiver) = StateSender::build(vec![1, 2, 3]);
        assert_eq!(sender.template(), &vec![1, 2, 3]);
    }

    #[test]
    fn test_into_inner_bypasses_send() {
        let (sender, receiver) = StateSender::build(42i32);

        let owned = sender.create_owned();
        let value = owned.into_inner();

        assert_eq!(value, 42);

        // Should fail because count is 1 but nothing was sent
        let result = receiver.take_all();
        assert!(result.is_err());
    }

    #[test]
    fn test_concurrent_create_owned() {
        use std::thread;

        let (sender, receiver) = StateSender::build(0i32);
        let mut handles = vec![];

        for i in 0..10 {
            let sender = sender.clone();
            handles.push(thread::spawn(move || {
                let mut owned = sender.create_owned();
                *owned.state_mut() = i;
                // owned is dropped here, sending to channel
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let states = receiver.take_all().unwrap();
        assert_eq!(states.len(), 10);
        assert_eq!(states.iter().sum::<i32>(), (0..10).sum::<i32>());
    }

    #[test]
    fn test_as_ref_as_mut() {
        let (sender, receiver) = StateSender::build(vec![1, 2]);

        let mut owned = sender.create_owned();

        // Test AsRef
        assert_eq!(owned.as_ref(), &vec![1, 2]);

        // Test AsMut
        owned.as_mut().push(3);
        assert_eq!(owned.as_ref(), &vec![1, 2, 3]);

        drop(owned);
        let states = receiver.take_all().unwrap();
        assert_eq!(states[0], vec![1, 2, 3]);
    }
}

