//! Server implementations.
//!
//! TCP servers for accepting RESP-speaking client connections. Both servers share
//! the same command-dispatch logic via [`process_command_into`] and
//! [`parse_command_bounds`] in the [`tcp`] module.
//!
//! Both [`TokioServer`] and [`IoUringServer`] are generic over `const N: usize`
//! (the per-side inline storage size for keys and values). The default is
//! [`DEFAULT_INLINE_SIZE`] (64 bytes).

pub mod tcp;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub mod io_uring;

pub use tcp::TokioServer;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use io_uring::IoUringServer;
