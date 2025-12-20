//! State collection infrastructure for zero per-batch locking.
//!
//! This module provides [`StateCollector`] and [`OwnedState`] for managing KDF state
//! across parallel partition execution without per-batch locking overhead.

use std::mem::ManuallyDrop;
use std::sync::mpsc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use crate::{DeltaResult, Error};

/// Collects states from multiple partitions via channel.
///
/// Single-use: after `take_all()` is called, no more states can be created
/// and `take_all()` cannot be called again.
///
/// # Example
///
/// ```ignore
/// let collector = StateCollector::new(|| MyState::new());
///
/// // Create owned states for partitions
/// let owned1 = collector.create_owned();
/// let owned2 = collector.create_owned();
///
/// // Use states (zero locking on hot path)
/// owned1.as_mut().process(batch);
/// owned2.as_mut().process(batch);
///
/// // Drop deposits states to channel
/// drop(owned1);
/// drop(owned2);
///
/// // Collect all states
/// let states = collector.take_all()?;
/// ```
pub struct StateCollector<S: Send + 'static> {
    factory: Arc<dyn Fn() -> S + Send + Sync>,
    sender: mpsc::Sender<S>,
    receiver: Mutex<mpsc::Receiver<S>>,
    created: AtomicUsize,
    finalized: AtomicBool,
}

impl<S: Send + 'static> StateCollector<S> {
    /// Create a new state collector with the given factory function.
    ///
    /// The factory is called each time `create_owned()` is invoked to produce
    /// a fresh state instance for a partition.
    pub fn new(factory: impl Fn() -> S + Send + Sync + 'static) -> Self {
        let (sender, receiver) = mpsc::channel();
        Self {
            factory: Arc::new(factory),
            sender,
            receiver: Mutex::new(receiver),
            created: AtomicUsize::new(0),
            finalized: AtomicBool::new(false),
        }
    }

    /// Create owned state for a partition. Auto-deposits on drop.
    ///
    /// Each call creates a fresh state instance using the factory function.
    /// The returned `OwnedState` provides zero-locking access to the state
    /// and automatically deposits the state back to the collector when dropped.
    ///
    /// # Panics
    ///
    /// Panics if called after `take_all()`.
    pub fn create_owned(&self) -> OwnedState<S> {
        if self.finalized.load(Ordering::SeqCst) {
            panic!("StateCollector: cannot create state after take_all()");
        }
        self.created.fetch_add(1, Ordering::SeqCst);
        OwnedState {
            state: ManuallyDrop::new((self.factory)()),
            sender: self.sender.clone(),
        }
    }

    /// Drain all states. Verifies created == deposited.
    ///
    /// This method can only be called once. It drains all deposited states
    /// from the channel and verifies that the number of deposited states
    /// matches the number of created states.
    ///
    /// # Errors
    ///
    /// - If called before all `OwnedState`s have been dropped
    /// - If called more than once
    pub fn take_all(&self) -> DeltaResult<Vec<S>> {
        if self.finalized.swap(true, Ordering::SeqCst) {
            return Err(Error::generic("StateCollector: take_all() already called"));
        }

        let created = self.created.load(Ordering::SeqCst);
        let receiver = self.receiver.lock()
            .map_err(|_| Error::generic("StateCollector: receiver lock poisoned"))?;

        let states: Vec<S> = receiver.try_iter().collect();

        if states.len() != created {
            return Err(Error::generic(format!(
                "StateCollector: created {}, deposited {} (streams may still be running)",
                created, states.len()
            )));
        }
        Ok(states)
    }
}

/// Owned state handle. Auto-deposits via channel on drop.
///
/// Provides zero-locking access to KDF state during stream execution.
/// When dropped, the state is automatically sent back to the [`StateCollector`]
/// via the channel.
///
/// # Usage
///
/// ```ignore
/// let mut owned = collector.create_owned();
///
/// // Zero-locking access via AsMut
/// owned.as_mut().apply(batch, selection)?;
///
/// // State is deposited when owned goes out of scope
/// ```
pub struct OwnedState<S: Send + 'static> {
    state: ManuallyDrop<S>,
    sender: mpsc::Sender<S>,
}

impl<S: Send + 'static> AsRef<S> for OwnedState<S> {
    fn as_ref(&self) -> &S {
        &self.state
    }
}

impl<S: Send + 'static> AsMut<S> for OwnedState<S> {
    fn as_mut(&mut self) -> &mut S {
        &mut self.state
    }
}

impl<S: Send + 'static> Drop for OwnedState<S> {
    fn drop(&mut self) {
        // SAFETY: Drop runs exactly once, state is never accessed after
        let state = unsafe { ManuallyDrop::take(&mut self.state) };
        let _ = self.sender.send(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_partition() {
        let collector = StateCollector::new(|| vec![1, 2, 3]);

        let mut owned = collector.create_owned();
        owned.as_mut().push(4);

        drop(owned);

        let states = collector.take_all().unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0], vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_multiple_partitions() {
        let collector = StateCollector::new(|| 0i32);

        let mut owned1 = collector.create_owned();
        let mut owned2 = collector.create_owned();
        let mut owned3 = collector.create_owned();

        *owned1.as_mut() = 10;
        *owned2.as_mut() = 20;
        *owned3.as_mut() = 30;

        drop(owned1);
        drop(owned2);
        drop(owned3);

        let states = collector.take_all().unwrap();
        assert_eq!(states.len(), 3);
        assert_eq!(states.iter().sum::<i32>(), 60);
    }

    #[test]
    fn test_take_all_twice_fails() {
        let collector = StateCollector::new(|| ());

        let owned = collector.create_owned();
        drop(owned);

        let _ = collector.take_all().unwrap();
        let result = collector.take_all();

        assert!(result.is_err());
    }

    #[test]
    #[should_panic(expected = "cannot create state after take_all")]
    fn test_create_after_take_all_panics() {
        let collector = StateCollector::new(|| ());

        let owned = collector.create_owned();
        drop(owned);

        let _ = collector.take_all().unwrap();
        let _ = collector.create_owned(); // Should panic
    }

    #[test]
    fn test_mismatch_count_fails() {
        let collector = StateCollector::new(|| ());

        let _owned = collector.create_owned(); // Not dropped

        let result = collector.take_all();
        assert!(result.is_err());
    }
}

