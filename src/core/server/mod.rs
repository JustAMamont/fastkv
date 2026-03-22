//! Server implementations
//!
//! TCP servers for accepting Redis client connections.

pub mod tcp;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub mod io_uring;

pub use tcp::TokioServer;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use io_uring::IoUringServer;
