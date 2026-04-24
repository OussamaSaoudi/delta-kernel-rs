//! Minimal hand-rolled stackless coroutine shim.
//!
//! Replaces the `genawaiter` crate with a narrow, no-deps implementation that
//! covers exactly the shape the [`super::engine::CoroutineSM`] driver needs:
//!
//! - [`Co<Y, R>`] — the coroutine-side handle. An `async fn` given a `Co`
//!   calls [`Co::yield_`] to surrender control with a value of type `Y`,
//!   and receives back a value of type `R` when the driver resumes it.
//! - [`Gen<Y, R, O>`] — the driver-side generator. Wraps a boxed future
//!   produced by the user's `FnOnce(Co<Y, R>) -> F` closure. [`Gen::resume_with`]
//!   advances the future until it yields or completes, returning
//!   [`GeneratorState<Y, O>`].
//!
//! ## Mechanics
//!
//! A [`Channel`] shared between `Co` and `Gen` (via `Arc<Mutex<_>>`) holds one
//! yielded value and one resume value at a time. [`Co::yield_`] stores the
//! yield into the channel and awaits a [`YieldFuture`] that returns
//! `Poll::Pending` on its first poll (handing control back to the driver),
//! then retrieves the stashed resume value on its second poll. [`Gen::resume_with`]
//! stores the resume value, polls the boxed future with a no-op waker, and
//! interprets `Poll::Pending` as "yielded" (pulling the value from the channel)
//! and `Poll::Ready(o)` as "completed with output `o`".
//!
//! ## Constraints
//!
//! - The producer must not hold the channel lock across an `.await` boundary —
//!   [`Co::yield_`] and [`YieldFuture::poll`] take the lock only to load/store
//!   the single word of state, then drop it before returning. This is enforced
//!   by keeping the critical sections trivial.
//! - [`Gen::resume_with`] is `&mut self`: only one thread advances the
//!   coroutine at a time. The `Send` marker applies to futures that don't hold
//!   non-`Send` state across await points.
//! - The no-op waker never schedules a re-poll; the driver polls explicitly
//!   in response to external events (engine results), never speculatively.
//!
//! Panics in this module are reserved for internal coroutine-protocol
//! invariants — the driver and body communicate in lockstep through
//! [`Channel`], so any `None` where a yield or resume value was expected is
//! a kernel bug. The `expect_used` / `panic` clippy lints are allowed for
//! that reason; user-facing failures never flow through this module.

#![allow(clippy::expect_used, clippy::panic)]

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

/// State stored between the coroutine body and the driver.
struct Channel<Y, R> {
    /// Value produced by the most recent [`Co::yield_`], waiting for the
    /// driver to pick it up.
    yielded: Option<Y>,
    /// Value the driver handed to [`Gen::resume_with`], waiting for the
    /// coroutine body to pick it up from its [`YieldFuture`].
    resume: Option<R>,
}

impl<Y, R> Channel<Y, R> {
    fn new() -> Self {
        Self {
            yielded: None,
            resume: None,
        }
    }
}

/// Coroutine-side handle. Passed into the producer closure.
pub struct Co<Y, R> {
    channel: Arc<Mutex<Channel<Y, R>>>,
}

impl<Y, R> Co<Y, R> {
    /// Yield control to the driver with value `y` and await the corresponding
    /// resume value `R`.
    pub async fn yield_(&self, y: Y) -> R {
        {
            let mut ch = self.channel.lock().expect("coroutine channel poisoned");
            debug_assert!(ch.yielded.is_none(), "previous yield not consumed");
            ch.yielded = Some(y);
        }
        YieldFuture {
            channel: self.channel.clone(),
            armed: true,
        }
        .await
    }
}

/// Future returned by [`Co::yield_`]. First poll hands control back to the
/// driver by returning `Poll::Pending`; second poll (after the driver has
/// stored a resume value) returns `Poll::Ready` with that value.
struct YieldFuture<Y, R> {
    channel: Arc<Mutex<Channel<Y, R>>>,
    armed: bool,
}

impl<Y, R> Future for YieldFuture<Y, R> {
    type Output = R;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<R> {
        if self.armed {
            // First poll — yielded value is already in the channel; surrender
            // control to the driver.
            self.armed = false;
            Poll::Pending
        } else {
            // Second poll — driver has stored a resume value; take it.
            let mut ch = self.channel.lock().expect("coroutine channel poisoned");
            let r = ch.resume.take().expect(
                "YieldFuture polled after first Pending without a resume value \
                 — the driver must call Gen::resume_with before re-polling",
            );
            Poll::Ready(r)
        }
    }
}

/// Result of [`Gen::resume_with`].
pub enum GeneratorState<Y, O> {
    /// The coroutine yielded a value and is waiting for the next resume.
    Yielded(Y),
    /// The coroutine completed with the given output and cannot be resumed.
    Complete(O),
}

/// Driver-side generator. Owns the boxed future produced by the user's
/// producer closure.
///
/// Use [`Gen::new`] to create a generator, then drive it by repeatedly
/// calling [`Gen::resume_with`] until you see [`GeneratorState::Complete`].
pub struct Gen<Y, R, O> {
    channel: Arc<Mutex<Channel<Y, R>>>,
    future: Option<Pin<Box<dyn Future<Output = O> + Send>>>,
}

impl<Y, R, O> Gen<Y, R, O>
where
    Y: Send + 'static,
    R: Send + 'static,
    O: Send + 'static,
{
    /// Construct a generator from a producer closure. The closure receives a
    /// [`Co<Y, R>`] and returns the body future; the body is not polled until
    /// the first [`Gen::resume_with`].
    pub fn new<F, Fut>(producer: F) -> Self
    where
        F: FnOnce(Co<Y, R>) -> Fut,
        Fut: Future<Output = O> + Send + 'static,
    {
        let channel = Arc::new(Mutex::new(Channel::<Y, R>::new()));
        let co = Co {
            channel: channel.clone(),
        };
        let future: Pin<Box<dyn Future<Output = O> + Send>> = Box::pin(producer(co));
        Self {
            channel,
            future: Some(future),
        }
    }

    /// Advance the coroutine: store `r` as the resume value the currently-
    /// pending [`Co::yield_`] will observe, poll the body future once, and
    /// report whether it yielded or completed.
    ///
    /// The very first call on a fresh `Gen` is the "prime" — the body has
    /// not yet awaited anything, so the first [`Co::yield_`] it emits does
    /// not read `r`. In that case `r` is simply discarded. Matches the
    /// behavior of `genawaiter::sync::Gen::resume_with`.
    ///
    /// Panics if called after the generator has already completed.
    pub fn resume_with(&mut self, r: R) -> GeneratorState<Y, O> {
        let future = self
            .future
            .as_mut()
            .expect("Gen::resume_with called after completion");
        {
            let mut ch = self.channel.lock().expect("coroutine channel poisoned");
            ch.resume = Some(r);
        }
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let poll_result = future.as_mut().poll(&mut cx);
        // Discard any unread resume value. Happens exclusively on the priming
        // call when the body yields before ever awaiting a `YieldFuture`; in
        // all other cases the second-poll path already consumed `resume`.
        self.channel
            .lock()
            .expect("coroutine channel poisoned")
            .resume = None;
        match poll_result {
            Poll::Pending => {
                let mut ch = self.channel.lock().expect("coroutine channel poisoned");
                let y = ch.yielded.take().expect(
                    "coroutine returned Pending without producing a yield value \
                     — bodies must only await futures produced by Co::yield_",
                );
                GeneratorState::Yielded(y)
            }
            Poll::Ready(o) => {
                self.future = None;
                GeneratorState::Complete(o)
            }
        }
    }
}

// === no-op Waker ===
//
// The driver polls explicitly in response to external events (engine results),
// so the waker never needs to wake anything. A no-op waker satisfies the
// `Context` API without allocating or scheduling.

const NOOP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    |_| noop_raw_waker(), // clone
    |_| {},               // wake
    |_| {},               // wake_by_ref
    |_| {},               // drop
);

fn noop_raw_waker() -> RawWaker {
    RawWaker::new(std::ptr::null(), &NOOP_WAKER_VTABLE)
}

fn noop_waker() -> Waker {
    // SAFETY: the vtable's clone/wake/wake_by_ref/drop are all no-ops and the
    // data pointer is never dereferenced, so the waker is trivially sound.
    unsafe { Waker::from_raw(noop_raw_waker()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn yields_then_completes() {
        let mut g = Gen::<u32, &'static str, i64>::new(|co| async move {
            let a = co.yield_(1).await;
            let b = co.yield_(2).await;
            (a.len() + b.len()) as i64
        });

        match g.resume_with("prime") {
            GeneratorState::Yielded(y) => assert_eq!(y, 1),
            _ => panic!("expected first yield"),
        }
        match g.resume_with("hello") {
            GeneratorState::Yielded(y) => assert_eq!(y, 2),
            _ => panic!("expected second yield"),
        }
        match g.resume_with("world!") {
            GeneratorState::Complete(o) => assert_eq!(o, 5 + 6),
            _ => panic!("expected completion"),
        }
    }

    #[test]
    fn empty_body_completes_on_first_poll() {
        let mut g = Gen::<(), (), &'static str>::new(|_co| async move { "done" });
        match g.resume_with(()) {
            GeneratorState::Complete(o) => assert_eq!(o, "done"),
            GeneratorState::Yielded(_) => panic!("unexpected yield"),
        }
    }

    #[test]
    #[should_panic(expected = "Gen::resume_with called after completion")]
    fn resume_after_complete_panics() {
        let mut g = Gen::<(), (), ()>::new(|_co| async move {});
        let _ = g.resume_with(());
        // Second call panics.
        let _ = g.resume_with(());
    }

    #[test]
    fn propagates_resume_values_through_multiple_yields() {
        // Three yields; the body completes with `a + b + c` where each of
        // a/b/c is the resume value fed to the corresponding yield's
        // `.await`. The first `resume_with` call primes the generator and
        // its argument is discarded (genawaiter-compatible semantics).
        let mut g = Gen::<(), i32, i32>::new(|co| async move {
            let a = co.yield_(()).await;
            let b = co.yield_(()).await;
            let c = co.yield_(()).await;
            a + b + c
        });

        assert!(matches!(g.resume_with(-1), GeneratorState::Yielded(())));
        assert!(matches!(g.resume_with(10), GeneratorState::Yielded(()))); // a = 10
        assert!(matches!(g.resume_with(20), GeneratorState::Yielded(()))); // b = 20
        match g.resume_with(30) {
            // c = 30 — body returns 10 + 20 + 30 = 60.
            GeneratorState::Complete(o) => assert_eq!(o, 60),
            _ => panic!("expected complete"),
        }
    }
}
