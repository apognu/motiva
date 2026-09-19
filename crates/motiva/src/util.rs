use std::{sync::LazyLock, thread::available_parallelism};

use crate::api::config::parse_env;

pub static AVAILABLE_CORES: LazyLock<usize> = LazyLock::new(|| available_parallelism().map(usize::from).unwrap_or(4).max(1));

pub fn runtime_thread_counts() -> anyhow::Result<(usize, usize)> {
  let defaults = match *AVAILABLE_CORES {
    1 => (1, 0),
    n if n < 4 => (n, 0),
    n => {
      let tokio_threads = match n {
        4..=5 => 1,
        6..=12 => 2,
        13..=24 => 3,
        _ => 4,
      };
      let rayon_threads = n.saturating_sub(tokio_threads);

      (tokio_threads, rayon_threads)
    }
  };

  Ok((parse_env("TOKIO_THREADS", defaults.0).unwrap(), parse_env("RAYON_THREADS", defaults.1).unwrap()))
}
