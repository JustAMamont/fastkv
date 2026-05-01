//! Server implementations.
//!
//! TCP servers for accepting Redis client connections. Both servers share
//! the same command-dispatch logic via [`process_command_into`] and
//! [`parse_command_bounds`] in the [`tcp`] module.

pub mod tcp;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub mod io_uring;

pub use tcp::TokioServer;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use io_uring::IoUringServer;
