//! Flow control signal (`utils.Signal`).
//!
//! A Buffer with `signal: true` from the peer toggles the state: the first one
//! closes (stop sending), the next one opens again. The sender waits while the
//! state is closed.
//!
//! The wait is level-triggered: [`Signal::wait_open`] returns only when the
//! state is open. A waiter checks the state again after each wake, so a wake
//! while the state is closed (for example, the signal opened and closed again
//! before the waiter ran) keeps it waiting.

use std::future::poll_fn;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Poll, Waker};

/// The flow control state shared by the parser (which toggles it) and the
/// serializer (which waits on it). Clone it to share it.
#[derive(Clone, Debug)]
pub struct Signal {
    state: Arc<Mutex<State>>,
}

#[derive(Debug)]
struct State {
    open: bool,
    /// The next id for a waiter.
    next_id: u64,
    /// The waiters, by id. A waiter replaces its waker at each poll and removes
    /// it when it is dropped.
    waiters: Vec<(u64, Waker)>,
}

impl Signal {
    /// A new signal, open.
    pub fn new() -> Self {
        Signal {
            state: Arc::new(Mutex::new(State {
                open: true,
                next_id: 0,
                waiters: Vec::new(),
            })),
        }
    }

    /// Toggle the state (`Signal.change`).
    pub fn change(&self) {
        let wake = {
            let mut state = self.lock();
            if state.open {
                state.open = false;
                Vec::new()
            } else {
                state.open = true;
                std::mem::take(&mut state.waiters)
            }
        };
        for (_, waker) in wake {
            waker.wake();
        }
    }

    /// True when the sender can send.
    pub fn is_open(&self) -> bool {
        self.lock().open
    }

    /// Wait until the state is open. Returns at once when it is open.
    ///
    /// Level-triggered: it returns only when the state is open. After each wake
    /// it checks the state again, and it waits on if the state is closed. The
    /// check and the registration of the waker are done under one lock, and
    /// [`Signal::change`] wakes the waiters after it releases the lock, so a
    /// change cannot be lost between the check and the registration.
    pub async fn wait_open(&self) {
        let mut waiter = Waiter {
            signal: self,
            id: None,
        };
        poll_fn(|cx| {
            let mut state = waiter.signal.lock();
            if state.open {
                return Poll::Ready(());
            }
            match waiter.id {
                Some(id) => match state.waiters.iter_mut().find(|(i, _)| *i == id) {
                    Some((_, waker)) => waker.clone_from(cx.waker()),
                    // `change` took the waker when it woke us: register again.
                    None => state.waiters.push((id, cx.waker().clone())),
                },
                None => {
                    let id = state.next_id;
                    state.next_id = state.next_id.wrapping_add(1);
                    waiter.id = Some(id);
                    state.waiters.push((id, cx.waker().clone()));
                }
            }
            Poll::Pending
        })
        .await
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // The state is a bool, a counter and a list: a panic in another thread cannot
        // leave it half changed.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Default for Signal {
    fn default() -> Self {
        Self::new()
    }
}

/// A pending [`Signal::wait_open`]. Removes its waker when the wait ends or is
/// cancelled.
struct Waiter<'a> {
    signal: &'a Signal,
    /// The id of the waiter, once it has registered a waker.
    id: Option<u64>,
}

impl Drop for Waiter<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            self.signal.lock().waiters.retain(|(i, _)| *i != id);
        }
    }
}
