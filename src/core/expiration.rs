//! Key expiration / TTL management.
//!
//! # Design
//!
//! Expiration state is kept in a secondary data structure
//! ([`ExpirationManager`]) that lives alongside the in-memory KV store.
//! Two complementary strategies are employed:
//!
//! * **Lazy expiration** — before returning a value from the store, the
//!   server checks [`ExpirationManager::check_and_purge_if_expired`]; if
//!   the key has passed its deadline the entry is deleted on the spot and
//!   `(nil)` is returned.
//! * **Active expiration** — a background thread wakes every
//!   [`ACTIVE_EXPIRY_INTERVAL`], samples up to [`ACTIVE_EXPIRY_MAX_KEYS`]
//!   random keys with TTLs, and purges those that have expired. This
//!   prevents unaccessed stale keys from accumulating.
//!
//! Both strategies combined guarantee that no expired key survives longer
//! than roughly `ACTIVE_EXPIRY_INTERVAL + scan_time` even if it is never
//! accessed, while keeping CPU overhead below a configurable ceiling.
//!
//! # Concurrency
//!
//! The internal deadline map is protected by a [`RwLock`] instead of a
//! `Mutex`.  This allows multiple concurrent readers (e.g. `has_deadline`,
//! `len`, `is_expired` fast-path, active-expiry scan) to proceed without
//! blocking each other, while writers (`expire`, `remove`, `persist`,
//! `purge`) still acquire exclusive access.
//!
//! # Usage
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use fast_kv::core::kv::KvStore;
//! use fast_kv::core::expiration::ExpirationManager;
//!
//! let store = Arc::new(KvStore::with_capacity(100_000));
//! let exp = Arc::new(ExpirationManager::new(Arc::clone(&store)));
//!
//! store.set(b"key", b"value");
//! exp.expire(b"key", std::time::Duration::from_secs(60));
//!
//! // Start background purging:
//! let _handle = fast_kv::core::expiration::spawn_active_expiration(Arc::clone(&exp));
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::core::kv::{DEFAULT_INLINE_SIZE, KvStoreLockFree};

// ---------------------------------------------------------------------------
// Tunables
// ---------------------------------------------------------------------------

/// How often the active-expiration background thread wakes up.
pub const ACTIVE_EXPIRY_INTERVAL: Duration = Duration::from_millis(100);

/// Maximum number of keys examined per active-expiration cycle.
pub const ACTIVE_EXPIRY_MAX_KEYS: usize = 200;

// ---------------------------------------------------------------------------
// ExpirationManager
// ---------------------------------------------------------------------------

/// Manages per-key TTL deadlines.
///
/// The internal [`HashMap`] is protected by a [`RwLock`] so that the many
/// read-only operations (`has_deadline`, `len`, the scan phase of active
/// expiry, and the fast-path of `check_and_purge_if_expired`) can proceed
/// concurrently.  Writers (`expire`, `remove`, `persist`) acquire exclusive
/// access only when actually mutating the map.
pub struct ExpirationManager<const N: usize = DEFAULT_INLINE_SIZE> {
    /// Maps `key -> absolute deadline (Instant)`.
    deadlines: RwLock<HashMap<Vec<u8>, Instant>>,
    /// Signals the background thread to shut down.
    shutdown: AtomicBool,
    /// Reference to the KV store so we can delete expired keys.
    store: Arc<KvStoreLockFree<N>>,
    /// Optional callback invoked when a key is expired and deleted.
    on_expire: Option<Arc<dyn Fn(&[u8]) + Send + Sync>>,
}

// SAFETY: all fields are `Send + Sync`.
unsafe impl<const N: usize> Send for ExpirationManager<N> {}
unsafe impl<const N: usize> Sync for ExpirationManager<N> {}

impl<const N: usize> ExpirationManager<N> {
    /// Create a new expiration manager backed by *store*.
    ///
    /// The background active-expiration thread is **not** started
    /// automatically; call [`spawn_active_expiration`] explicitly.
    pub fn new(store: Arc<KvStoreLockFree<N>>) -> Self {
        Self {
            deadlines: RwLock::new(HashMap::new()),
            shutdown: AtomicBool::new(false),
            store,
            on_expire: None,
        }
    }

    /// Create an expiration manager with a cleanup callback.
    ///
    /// The callback is invoked whenever a key is purged (either lazily
    /// via [`check_and_purge_if_expired`], actively via
    /// [`active_expire_cycle`], or via [`ttl`]). This allows external
    /// components (e.g. `ListManager`) to clean up auxiliary data when a
    /// key expires.
    pub fn with_on_expire(
        store: Arc<KvStoreLockFree<N>>,
        on_expire: Arc<dyn Fn(&[u8]) + Send + Sync>,
    ) -> Self {
        Self {
            deadlines: RwLock::new(HashMap::new()),
            shutdown: AtomicBool::new(false),
            store,
            on_expire: Some(on_expire),
        }
    }

    // -------------------------------------------------------------------
    // Combined check + purge (hot path — called on every GET)
    // -------------------------------------------------------------------

    /// If *key* has a TTL and has expired, atomically delete it from both
    /// the expiration index and the KV store.
    ///
    /// Returns `true` if the key was purged.
    ///
    /// This is a single-operation replacement for the old pattern of
    /// calling `is_expired()` followed by `purge_if_expired()`, avoiding
    /// a redundant second lock acquisition in the common (not-expired)
    /// case.
    ///
    /// # Concurrency
    ///
    /// The fast path (key has no TTL or is not yet expired) only takes a
    /// **read lock**, which does not block other concurrent readers.  The
    /// slow path (expired) falls through to a write lock.
    pub fn check_and_purge_if_expired(&self, key: &[u8]) -> bool {
        // Fast path: read lock.
        {
            let map = self.deadlines.read().unwrap_or_else(|e| e.into_inner());
            if let Some(&deadline) = map.get(key) {
                if deadline > Instant::now() {
                    return false; // Not expired yet.
                }
                // Deadline exists and is in the past → fall through.
            } else {
                return false; // No deadline at all.
            }
        }

        // Slow path: write lock to remove.
        let mut map = self.deadlines.write().unwrap_or_else(|e| e.into_inner());
        if let Some(&deadline) = map.get(key) {
            if deadline <= Instant::now() {
                map.remove(key);
                drop(map);
                self.store.del(key);
                self.fire_on_expire(key);
                return true;
            }
        }
        false
    }

    /// Check whether *key* has expired **without** deleting it.
    ///
    /// This is a read-only operation (uses a read lock).
    /// Use [`check_and_purge_if_expired`](Self::check_and_purge_if_expired)
    /// to actually remove the key when this returns `true`.
    pub fn is_expired(&self, key: &[u8]) -> bool {
        let map = self.deadlines.read().unwrap_or_else(|e| e.into_inner());
        map.get(key).is_some_and(|&deadline| deadline <= Instant::now())
    }

    /// If *key* has a TTL and has expired, delete it from both the
    /// expiration index and the KV store.
    ///
    /// Returns `true` if the key was purged.
    ///
    /// **Prefer [`check_and_purge_if_expired`](Self::check_and_purge_if_expired)**
    /// when you need to both check and purge in one step — it avoids the
    /// double-lock pattern.
    pub fn purge_if_expired(&self, key: &[u8]) -> bool {
        self.check_and_purge_if_expired(key)
    }

    // -------------------------------------------------------------------
    // Write operations (require write lock)
    // -------------------------------------------------------------------

    /// Register (or update) a TTL for *key*.
    ///
    /// `ttl` is measured from **now**. If the key already has a TTL the
    /// deadline is replaced.
    ///
    /// Returns `true` if the key exists in the KV store and the TTL was
    /// successfully set.
    pub fn expire(&self, key: &[u8], ttl: Duration) -> bool {
        if self.store.get(key).is_none() {
            return false;
        }
        let deadline = Instant::now() + ttl;
        let mut map = self.deadlines.write().unwrap_or_else(|e| e.into_inner());
        map.insert(key.to_vec(), deadline);
        true
    }

    /// Remove the TTL from *key*, making it persistent again.
    ///
    /// Returns `true` if a TTL was actually removed.
    pub fn persist(&self, key: &[u8]) -> bool {
        let mut map = self.deadlines.write().unwrap_or_else(|e| e.into_inner());
        map.remove(key).is_some()
    }

    /// Remove *key* from the expiration index (e.g. after a `DEL`
    /// command). No-op if the key has no TTL.
    pub fn remove(&self, key: &[u8]) {
        let mut map = self.deadlines.write().unwrap_or_else(|e| e.into_inner());
        map.remove(key);
    }

    // -------------------------------------------------------------------
    // Read-only operations (use read lock)
    // -------------------------------------------------------------------

    /// Check whether *key* has an associated deadline (whether expired or not).
    pub fn has_deadline(&self, key: &[u8]) -> bool {
        let map = self.deadlines.read().unwrap_or_else(|e| e.into_inner());
        map.contains_key(key)
    }

    /// Return the remaining time-to-live for *key*.
    ///
    /// * `Some(remaining)` — the key has a TTL and has not yet expired.
    /// * `None` — no TTL is set, or the key does not exist.
    ///
    /// If the key has expired it is **lazily deleted** from both the
    /// expiration index and the KV store, and `None` is returned.
    ///
    /// The fast path (not expired) only takes a read lock.
    pub fn ttl(&self, key: &[u8]) -> Option<Duration> {
        // Fast path: read lock.
        {
            let map = self.deadlines.read().unwrap_or_else(|e| e.into_inner());
            if let Some(&deadline) = map.get(key) {
                if let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
                    return Some(remaining);
                }
                // Expired — fall through to write lock.
            } else {
                return None;
            }
        }

        // Slow path: write lock to purge.
        let mut map = self.deadlines.write().unwrap_or_else(|e| e.into_inner());
        if let Some(&deadline) = map.get(key) {
            if deadline <= Instant::now() {
                map.remove(key);
                drop(map);
                self.store.del(key);
                self.fire_on_expire(key);
                return None;
            }
            Some(deadline - Instant::now())
        } else {
            None
        }
    }

    /// Number of keys that currently have a TTL.
    pub fn len(&self) -> usize {
        let map = self.deadlines.read().unwrap_or_else(|e| e.into_inner());
        map.len()
    }

    /// `true` if no keys have a TTL.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return all keys with TTLs and their absolute deadlines as milliseconds
    /// since UNIX epoch.
    ///
    /// This is used by the checkpoint/BGSAVE mechanism to persist TTL state
    /// into the compact WAL. Expired entries are filtered out and purged.
    pub fn get_all_deadlines_ms(&self) -> Vec<(Vec<u8>, u64)> {
        let now = Instant::now();
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let map = self.deadlines.read().unwrap_or_else(|e| e.into_inner());
        let mut result = Vec::with_capacity(map.len());
        for (key, deadline) in map.iter() {
            if let Some(remaining) = deadline.checked_duration_since(now) {
                let deadline_ms = now_ms.saturating_add(remaining.as_millis() as u64);
                result.push((key.clone(), deadline_ms));
            }
            // If the deadline is in the past, we skip it — the active expiry
            // thread or lazy check will clean it up.
        }
        result
    }

    /// Signal the active-expiration thread (if any) to stop.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    // -------------------------------------------------------------------
    // WAL persistence helpers
    // -------------------------------------------------------------------

    /// Set a TTL and return the absolute deadline as milliseconds since
    /// UNIX epoch. The returned value can be persisted to WAL and later
    /// passed to [`expire_at_deadline_ms`] during recovery.
    ///
    /// Returns `None` if the key does not exist.
    pub fn expire_with_deadline(&self, key: &[u8], ttl: Duration) -> Option<u64> {
        if self.store.get(key).is_none() {
            return None;
        }
        let deadline = Instant::now() + ttl;
        let mut map = self.deadlines.write().unwrap_or_else(|e| e.into_inner());
        map.insert(key.to_vec(), deadline);

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Some(now_ms.saturating_add(ttl.as_millis() as u64))
    }

    /// Restore a TTL from a WAL entry.
    ///
    /// *deadline_ms* is an absolute timestamp (milliseconds since UNIX epoch)
    /// that was previously returned by [`expire_with_deadline`].
    ///
    /// If the deadline has already passed the key is deleted immediately.
    /// Returns `true` if the key exists and a TTL was set.
    pub fn expire_at_deadline_ms(&self, key: &[u8], deadline_ms: u64) -> bool {
        if self.store.get(key).is_none() {
            return false;
        }
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        if deadline_ms <= now_ms {
            // Already expired — purge immediately.
            self.store.del(key);
            self.fire_on_expire(key);
            return true;
        }

        let remaining = Duration::from_millis(deadline_ms - now_ms);
        let deadline = Instant::now() + remaining;
        let mut map = self.deadlines.write().unwrap_or_else(|e| e.into_inner());
        map.insert(key.to_vec(), deadline);
        true
    }

    // -------------------------------------------------------------------
    // Internal: on-expire callback
    // -------------------------------------------------------------------

    #[inline]
    fn fire_on_expire(&self, key: &[u8]) {
        if let Some(ref cb) = self.on_expire {
            cb(key);
        }
    }

    // -------------------------------------------------------------------
    // Internal: one active-expiration cycle
    // -------------------------------------------------------------------

    /// Run one cycle of active expiration: collect up to
    /// [`ACTIVE_EXPIRY_MAX_KEYS`] expired entries and purge them.
    ///
    /// Phase 1 (scan) uses a **read lock** so it does not block
    /// concurrent `is_expired` / `has_deadline` calls.
    fn active_expire_cycle(&self, buf: &mut Vec<Vec<u8>>) {
        buf.clear();

        // Phase 1: collect expired candidates (read lock).
        {
            let map = self.deadlines.read().unwrap_or_else(|e| e.into_inner());
            let now = Instant::now();
            for (key, &deadline) in map.iter() {
                if buf.len() >= ACTIVE_EXPIRY_MAX_KEYS {
                    break;
                }
                if deadline <= now {
                    buf.push(key.clone());
                }
            }
        }

        // Phase 2: remove from map (write lock, re-check deadline for TOCTOU).
        if !buf.is_empty() {
            let mut map = self.deadlines.write().unwrap_or_else(|e| e.into_inner());
            let now = Instant::now();
            buf.retain(|key| {
                if let Some(&deadline) = map.get(key) {
                    if deadline <= now {
                        map.remove(key);
                        return true; // confirmed expired — KEEP in buf for Phase 3
                    }
                }
                false // not expired or already removed — DROP from buf
            });
        }

        // Phase 3: delete from KV store (no lock held).
        for key in buf.drain(..) {
            self.store.del(&key);
            self.fire_on_expire(&key);
        }
    }
}

impl<const N: usize> Drop for ExpirationManager<N> {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Free function: spawn the active-expiration thread
// ---------------------------------------------------------------------------

/// Spawn a background thread that periodically purges expired keys.
///
/// The thread holds an [`Arc`] to *mgr*, keeping it alive.  The returned
/// [`JoinHandle`] can be used to wait for the thread to finish (it exits
/// only when [`ExpirationManager::shutdown`] is called or the manager is
/// dropped).
pub fn spawn_active_expiration<const N: usize>(mgr: Arc<ExpirationManager<N>>) -> thread::JoinHandle<()> {
    mgr.shutdown.store(false, Ordering::Relaxed);
    thread::Builder::new()
        .name("fastkv-expiry".into())
        .spawn(move || {
            let mut buf = Vec::with_capacity(ACTIVE_EXPIRY_MAX_KEYS);
            loop {
                thread::sleep(ACTIVE_EXPIRY_INTERVAL);
                if mgr.shutdown.load(Ordering::Relaxed) {
                    break;
                }
                mgr.active_expire_cycle(&mut buf);
            }
        })
        .expect("failed to spawn fastkv-expiry thread")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::kv::DEFAULT_INLINE_SIZE;
    use std::thread;
    use std::time::Duration;

    type Store = KvStoreLockFree<DEFAULT_INLINE_SIZE>;

    #[test]
    fn test_expire_and_ttl() {
        let store = Arc::new(Store::new());
        let mgr = ExpirationManager::new(Arc::clone(&store));

        store.set(b"hello", b"world");
        assert!(mgr.expire(b"hello", Duration::from_secs(10)));
        let ttl = mgr.ttl(b"hello").unwrap();
        assert!(ttl <= Duration::from_secs(10));
        assert!(ttl > Duration::from_secs(9));
    }

    #[test]
    fn test_expire_nonexistent_key() {
        let store = Arc::new(Store::new());
        let mgr = ExpirationManager::new(Arc::clone(&store));

        assert!(!mgr.expire(b"nope", Duration::from_secs(5)));
    }

    #[test]
    fn test_persist_removes_ttl() {
        let store = Arc::new(Store::new());
        let mgr = ExpirationManager::new(Arc::clone(&store));

        store.set(b"key", b"val");
        mgr.expire(b"key", Duration::from_secs(60));
        assert!(mgr.persist(b"key"));
        assert!(mgr.ttl(b"key").is_none());
    }

    #[test]
    fn test_lazy_expiry() {
        let store = Arc::new(Store::new());
        let mgr = ExpirationManager::new(Arc::clone(&store));

        store.set(b"ephemeral", b"data");
        mgr.expire(b"ephemeral", Duration::from_millis(50));
        thread::sleep(Duration::from_millis(80));

        // Key still in store before explicit TTL check.
        assert!(store.get(b"ephemeral").is_some());

        // ttl() triggers lazy deletion.
        assert!(mgr.ttl(b"ephemeral").is_none());
        assert!(store.get(b"ephemeral").is_none());
    }

    #[test]
    fn test_check_and_purge_if_expired() {
        let store = Arc::new(Store::new());
        let mgr = ExpirationManager::new(Arc::clone(&store));

        store.set(b"temp", b"value");
        mgr.expire(b"temp", Duration::from_millis(30));
        thread::sleep(Duration::from_millis(60));

        assert!(mgr.check_and_purge_if_expired(b"temp"));
        assert!(store.get(b"temp").is_none());
    }

    #[test]
    fn test_check_and_purge_not_expired() {
        let store = Arc::new(Store::new());
        let mgr = ExpirationManager::new(Arc::clone(&store));

        store.set(b"fresh", b"value");
        mgr.expire(b"fresh", Duration::from_secs(60));
        assert!(!mgr.check_and_purge_if_expired(b"fresh"));
        assert!(store.get(b"fresh").is_some());
    }

    #[test]
    fn test_purge_if_expired_compat() {
        let store = Arc::new(Store::new());
        let mgr = ExpirationManager::new(Arc::clone(&store));

        store.set(b"temp", b"value");
        mgr.expire(b"temp", Duration::from_millis(30));
        thread::sleep(Duration::from_millis(60));

        // purge_if_expired delegates to check_and_purge_if_expired.
        assert!(mgr.purge_if_expired(b"temp"));
        assert!(store.get(b"temp").is_none());
    }

    #[test]
    fn test_remove_on_del() {
        let store = Arc::new(Store::new());
        let mgr = ExpirationManager::new(Arc::clone(&store));

        store.set(b"key", b"val");
        mgr.expire(b"key", Duration::from_secs(100));
        mgr.remove(b"key");
        assert!(mgr.ttl(b"key").is_none());
    }

    #[test]
    fn test_len_and_is_empty() {
        let store = Arc::new(Store::new());
        let mgr = ExpirationManager::new(Arc::clone(&store));

        assert!(mgr.is_empty());
        store.set(b"a", b"1");
        mgr.expire(b"a", Duration::from_secs(10));
        assert_eq!(mgr.len(), 1);
        store.set(b"b", b"2");
        mgr.expire(b"b", Duration::from_secs(10));
        assert_eq!(mgr.len(), 2);
    }

    #[test]
    fn test_active_expiration_purges_expired_keys() {
        let store = Arc::new(Store::new());
        let mgr = Arc::new(ExpirationManager::new(Arc::clone(&store)));

        for i in 0..50 {
            let key = format!("exp_key_{}", i);
            store.set(key.as_bytes(), b"val");
            mgr.expire(key.as_bytes(), Duration::from_millis(30));
        }

        // Also add some persistent keys.
        for i in 0..50 {
            let key = format!("perm_key_{}", i);
            store.set(key.as_bytes(), b"val");
        }

        // Wait for expiration.
        thread::sleep(Duration::from_millis(80));

        // Run a manual cycle (equivalent to what the background thread does).
        let mut buf = Vec::new();
        mgr.active_expire_cycle(&mut buf);

        // All expired keys should be gone.
        for i in 0..50 {
            let key = format!("exp_key_{}", i);
            assert!(store.get(key.as_bytes()).is_none(), "expired key {} still present", i);
        }
        // Persistent keys must remain.
        for i in 0..50 {
            let key = format!("perm_key_{}", i);
            assert!(store.get(key.as_bytes()).is_some(), "persistent key {} missing", i);
        }
    }

    #[test]
    fn test_expire_overwrite_ttl() {
        let store = Arc::new(Store::new());
        let mgr = ExpirationManager::new(Arc::clone(&store));

        store.set(b"k", b"v");
        mgr.expire(b"k", Duration::from_secs(60));
        // Overwrite with a shorter TTL.
        assert!(mgr.expire(b"k", Duration::from_secs(5)));
        let ttl = mgr.ttl(b"k").unwrap();
        assert!(ttl <= Duration::from_secs(5));
        assert!(ttl > Duration::from_secs(4));
    }

    #[test]
    fn test_persist_nonexistent_key() {
        let store = Arc::new(Store::new());
        let mgr = ExpirationManager::new(Arc::clone(&store));
        assert!(!mgr.persist(b"nope")); // no TTL set
    }

    #[test]
    fn test_remove_nonexistent_key() {
        let store = Arc::new(Store::new());
        let mgr = ExpirationManager::new(Arc::clone(&store));
        mgr.remove(b"nope"); // should not panic
    }

    #[test]
    fn test_is_expired_nonexistent() {
        let store = Arc::new(Store::new());
        let mgr = ExpirationManager::new(Arc::clone(&store));
        assert!(!mgr.is_expired(b"nope"));
    }

    #[test]
    fn test_check_and_purge_nonexistent() {
        let store = Arc::new(Store::new());
        let mgr = ExpirationManager::new(Arc::clone(&store));
        assert!(!mgr.check_and_purge_if_expired(b"nope"));
    }

    #[test]
    fn test_shutdown_flag() {
        let store = Arc::new(Store::new());
        let mgr = ExpirationManager::new(Arc::clone(&store));
        mgr.shutdown();
        // After shutdown, spawning should still work (it checks the flag in loop).
    }
}
