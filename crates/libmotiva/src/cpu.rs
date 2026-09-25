//! Dedicated thread pool for CPU-bound work.
//!
//! Decoding index responses and scoring candidates can hold a thread for
//! several milliseconds. Running that on the async runtime's worker threads
//! stalls every other task scheduled on them (I/O, health checks, other
//! requests), so it is dispatched to this pool, sized to the number of
//! available cores, and awaited from the async side.

use std::{
  panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
  sync::LazyLock,
};

use tokio::sync::oneshot;
use tracing::{Dispatch, Span, dispatcher};

static POOL: LazyLock<rayon::ThreadPool> = LazyLock::new(|| {
  rayon::ThreadPoolBuilder::new()
    .num_threads(std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1))
    .thread_name(|index| format!("motiva-cpu-{index}"))
    .build()
    .expect("could not build CPU thread pool")
});

/// Run a CPU-bound closure on the dedicated pool and await its result.
///
/// Jobs are started in submission order. The closure runs within the caller's
/// tracing subscriber and span, and a panic inside it is propagated to the
/// awaiting task, as if the closure had run inline.
pub(crate) async fn run<F, R>(f: F) -> R
where
  F: FnOnce() -> R + Send + 'static,
  R: Send + 'static,
{
  let (tx, rx) = oneshot::channel();
  let dispatch = dispatcher::get_default(Dispatch::clone);
  let span = Span::current();

  POOL.spawn_fifo(move || {
    let result = dispatcher::with_default(&dispatch, || span.in_scope(|| catch_unwind(AssertUnwindSafe(f))));
    let _ = tx.send(result);
  });

  match rx.await.expect("CPU pool job was dropped") {
    Ok(result) => result,
    Err(panic) => resume_unwind(panic),
  }
}

#[cfg(test)]
mod tests {
  #[tokio::test]
  async fn returns_result() {
    assert_eq!(super::run(|| 21 * 2).await, 42);
  }

  #[tokio::test]
  async fn runs_in_caller_span() {
    use tracing::{Instrument, Span};

    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry());
    let span = tracing::info_span!("caller");
    let expected = span.id();

    assert!(expected.is_some());
    assert_eq!(super::run(|| Span::current().id()).instrument(span).await, expected);
  }

  #[tokio::test]
  async fn propagates_panics() {
    let result = tokio::spawn(super::run(|| panic!("boom"))).await;

    assert!(result.unwrap_err().is_panic());
  }
}
