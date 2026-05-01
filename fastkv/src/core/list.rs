//! List data type operations.
//!
//! Lists are stored in a separate `HashMap` (protected by `Mutex`) since they
//! can grow beyond the KV store's `INLINE_SIZE` (64 bytes). A sentinel value
//! (`0xFE` magic byte) is stored in the KV store to mark list-type keys so
//! that `GET` / `SET` on a list key returns `WRONGTYPE`.
//!
//! Lists are **not** persisted to WAL — they are purely in-memory. This is
//! suitable for ephemeral data such as matchmaking queues.
//!
//! ## Commands
//!
//! `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`,
//! `LREM`, `LTRIM`, `LSET`.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use crate::core::kv::KvStore;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Magic byte prefix identifying a list-type value in the KV store.
pub const LIST_MAGIC: u8 = 0xFE;

// ---------------------------------------------------------------------------
// ListManager
// ---------------------------------------------------------------------------

/// In-memory list storage.
///
/// Each list is a `VecDeque<Vec<u8>>` keyed by the KV-store key.  A sentinel
/// byte (`0xFE`) is written into the KV store so that `GET` on a list key
/// correctly returns `WRONGTYPE`.
///
/// Thread safety is provided by a `Mutex` around the inner `HashMap`.  List
/// operations are orders of magnitude less frequent than raw `GET`/`SET`, so
/// contention is negligible.
pub struct ListManager {
    lists: Mutex<HashMap<Vec<u8>, VecDeque<Vec<u8>>>>,
    store: Arc<KvStore>,
}

// SAFETY: all fields are `Send + Sync`.
unsafe impl Send for ListManager {}
unsafe impl Sync for ListManager {}

impl ListManager {
    /// Create a new list manager backed by *store*.
    pub fn new(store: Arc<KvStore>) -> Self {
        Self {
            lists: Mutex::new(HashMap::new()),
            store,
        }
    }

    // -----------------------------------------------------------------------
    // Query helpers
    // -----------------------------------------------------------------------

    /// Check whether a raw KV-store value is a list sentinel.
    #[inline]
    pub fn is_list_value(data: &[u8]) -> bool {
        data.len() == 1 && data[0] == LIST_MAGIC
    }

    /// Return the number of keys that currently hold a list.
    pub fn len(&self) -> usize {
        self.lists.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// `true` if no lists exist.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // -----------------------------------------------------------------------
    // Mutation: LPUSH / RPUSH
    // -----------------------------------------------------------------------

    /// Insert *elements* at the **head** of the list stored at *key*.
    ///
    /// Each element is pushed to the head one-by-one (Redis semantics),
    /// so the **rightmost** element in the argument list ends up at the head.
    /// Example: `LPUSH key c b a` → `[a, b, c]`.
    ///
    /// If the key does not yet hold a list, one is created (a sentinel is
    /// written to the KV store).  If the key holds a non-list value,
    /// [`ListError::WrongType`] is returned.
    ///
    /// Returns the new length of the list.
    pub fn lpush(&self, key: &[u8], elements: &[&[u8]]) -> Result<usize, ListError> {
        self.check_type(key)?;
        self.ensure_sentinel(key);

        let mut lists = self.lists.lock().unwrap_or_else(|e| e.into_inner());
        let list = lists.entry(key.to_vec()).or_default();
        for elem in elements {
            list.push_front(elem.to_vec());
        }
        Ok(list.len())
    }

    /// Insert *elements* at the **tail** of the list stored at *key*.
    ///
    /// Returns the new length of the list.
    pub fn rpush(&self, key: &[u8], elements: &[&[u8]]) -> Result<usize, ListError> {
        self.check_type(key)?;
        self.ensure_sentinel(key);

        let mut lists = self.lists.lock().unwrap_or_else(|e| e.into_inner());
        let list = lists.entry(key.to_vec()).or_default();
        for elem in elements {
            list.push_back(elem.to_vec());
        }
        Ok(list.len())
    }

    // -----------------------------------------------------------------------
    // Mutation: LPOP / RPOP
    // -----------------------------------------------------------------------

    /// Remove and return up to *count* elements from the **head** of the list.
    ///
    /// Returns a vector of elements (may be empty).  If the list becomes empty
    /// after the operation both the list data and the KV-store sentinel are
    /// removed.
    pub fn lpop(&self, key: &[u8], count: usize) -> Vec<Vec<u8>> {
        self.pop_inner(key, count, false)
    }

    /// Remove and return up to *count* elements from the **tail** of the list.
    pub fn rpop(&self, key: &[u8], count: usize) -> Vec<Vec<u8>> {
        self.pop_inner(key, count, true)
    }

    // -----------------------------------------------------------------------
    // Query: LRANGE / LLEN / LINDEX
    // -----------------------------------------------------------------------

    /// Return elements in the range `[start, stop]` (inclusive, Redis semantics).
    ///
    /// Negative indices count from the end.  Out-of-range indices are clamped.
    /// If `start` is beyond the list length, an empty vector is returned.
    pub fn lrange(&self, key: &[u8], start: i64, stop: i64) -> Vec<Vec<u8>> {
        let lists = self.lists.lock().unwrap_or_else(|e| e.into_inner());
        match lists.get(key) {
            Some(list) => {
                let len = list.len() as i64;
                // For start: clamp to [0, len].  If start >= len → empty.
                let s = if start < 0 { (len + start).max(0) } else { start.min(len) };
                if s >= len {
                    return Vec::new();
                }
                let e = normalize_index(stop, len);
                if s > e {
                    return Vec::new();
                }
                list.iter()
                    .skip(s as usize)
                    .take((e - s + 1) as usize)
                    .cloned()
                    .collect()
            }
            None => Vec::new(),
        }
    }

    /// Return the length of the list at *key*.
    ///
    /// Returns 0 if the key does not exist.
    pub fn llen(&self, key: &[u8]) -> usize {
        let lists = self.lists.lock().unwrap_or_else(|e| e.into_inner());
        lists.get(key).map_or(0, VecDeque::len)
    }

    /// Return the element at *index*, or `None` if out of range.
    ///
    /// Negative indices count from the end.
    pub fn lindex(&self, key: &[u8], index: i64) -> Option<Vec<u8>> {
        let lists = self.lists.lock().unwrap_or_else(|e| e.into_inner());
        let list = lists.get(key)?;
        let len = list.len() as i64;
        let idx = if index < 0 { len + index } else { index };
        if idx < 0 || idx >= len {
            return None;
        }
        list.get(idx as usize).cloned()
    }

    // -----------------------------------------------------------------------
    // Mutation: LREM / LTRIM / LSET
    // -----------------------------------------------------------------------

    /// Remove the first / last / all occurrences of *element*.
    ///
    /// * `count > 0` — remove the first *count* matches (from head).
    /// * `count < 0` — remove the last |count| matches (from tail).
    /// * `count == 0` — remove all matches.
    ///
    /// Returns the number of elements removed.
    pub fn lrem(&self, key: &[u8], count: i64, element: &[u8]) -> usize {
        let mut lists = self.lists.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(list) = lists.get_mut(key) {
            let original_len = list.len();
            if count > 0 {
                let mut removed = 0usize;
                list.retain(|e| {
                    if removed >= count as usize {
                        true
                    } else if e == element {
                        removed += 1;
                        false
                    } else {
                        true
                    }
                });
            } else if count < 0 {
                let mut removed = 0usize;
                let abs_count = (-count) as usize;
                let mut i = list.len();
                while i > 0 && removed < abs_count {
                    i -= 1;
                    if &list[i] == element.as_ref() {
                        list.remove(i);
                        removed += 1;
                    }
                }
            } else {
                list.retain(|e| e != element);
            }
            let removed = original_len - list.len();
            if list.is_empty() {
                drop(lists);
                self.remove_key(key);
            }
            removed
        } else {
            0
        }
    }

    /// Trim the list to the range `[start, stop]` (inclusive).
    ///
    /// Elements outside the range are removed.  If the resulting list is
    /// empty the key is deleted entirely.  If `start` is beyond the list
    /// length, the list is emptied (Redis semantics).
    pub fn ltrim(&self, key: &[u8], start: i64, stop: i64) {
        let mut lists = self.lists.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(list) = lists.get_mut(key) {
            let len = list.len() as i64;
            // If start is beyond the list length, empty the list.
            let s_raw = if start < 0 { (len + start).max(0) } else { start };
            if s_raw >= len {
                drop(lists);
                self.remove_key(key);
                return;
            }
            let s = normalize_index(start, len);
            let e = normalize_index(stop, len);
            if s > e {
                drop(lists);
                self.remove_key(key);
                return;
            }
            let keep_start = s as usize;
            let keep_end = (e + 1) as usize;
            // Drain elements outside the range.
            let keep: VecDeque<Vec<u8>> = list
                .iter()
                .skip(keep_start)
                .take(keep_end - keep_start)
                .cloned()
                .collect();
            *list = keep;
            if list.is_empty() {
                drop(lists);
                self.remove_key(key);
            }
        }
    }

    /// Set the element at *index* to *element*.
    ///
    /// Returns `true` on success, `false` if the index is out of range.
    pub fn lset(&self, key: &[u8], index: i64, element: &[u8]) -> bool {
        let mut lists = self.lists.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(list) = lists.get_mut(key) {
            let len = list.len() as i64;
            let idx = if index < 0 { len + index } else { index };
            if idx < 0 || idx >= len {
                return false;
            }
            list[idx as usize] = element.to_vec();
            true
        } else {
            false
        }
    }

    // -----------------------------------------------------------------------
    // Cleanup
    // -----------------------------------------------------------------------

    /// Remove the list data and the KV-store sentinel for *key*.
    ///
    /// Called by `DEL`, `SET` (overwrite), and expiration callbacks.
    pub fn remove_key(&self, key: &[u8]) {
        {
            let mut lists = self.lists.lock().unwrap_or_else(|e| e.into_inner());
            lists.remove(key);
        }
        self.store.del(key);
    }

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    /// Check that *key* either doesn't exist or already holds a list.
    fn check_type(&self, key: &[u8]) -> Result<(), ListError> {
        if let Some(val) = self.store.get(key) {
            if !Self::is_list_value(&val) {
                return Err(ListError::WrongType);
            }
        }
        Ok(())
    }

    /// Write the list sentinel into the KV store (idempotent).
    fn ensure_sentinel(&self, key: &[u8]) {
        self.store.set(key, &[LIST_MAGIC]);
    }

    /// Shared implementation for `lpop` and `rpop`.
    fn pop_inner(&self, key: &[u8], count: usize, from_tail: bool) -> Vec<Vec<u8>> {
        let mut lists = self.lists.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(list) = lists.get_mut(key) {
            let n = count.min(list.len());
            let mut result = Vec::with_capacity(n);
            for _ in 0..n {
                if from_tail {
                    result.push(list.pop_back().unwrap());
                } else {
                    // pop_front with swap_remove_back for O(1)
                    let elem = list.pop_front().unwrap();
                    result.push(elem);
                }
            }
            if list.is_empty() {
                lists.remove(key); // remove while lock is held
                drop(lists);
                self.store.del(key); // remove sentinel
            }
            result
        } else {
            Vec::new()
        }
    }
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors returned by list operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListError {
    /// The key holds a non-list value.
    WrongType,
}

impl std::fmt::Display for ListError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ListError::WrongType => write!(f, "WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }
}

impl std::error::Error for ListError {}

// ---------------------------------------------------------------------------
// Index normalisation (Redis semantics)
// ---------------------------------------------------------------------------

/// Convert a possibly-negative Redis-style index to a non-negative offset.
///
/// * `idx < 0` → counts from end (`len + idx`).
/// * `idx >= len` → clamped to `len - 1` (inclusive upper bound).
/// * `idx < -len` → clamped to `0`.
#[inline]
fn normalize_index(idx: i64, len: i64) -> i64 {
    if idx < 0 {
        (len + idx).max(0)
    } else {
        idx.min(len - 1).max(0)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_manager() -> ListManager {
        ListManager::new(Arc::new(KvStore::with_capacity(100)))
    }

    #[test]
    fn test_is_list_value() {
        assert!(ListManager::is_list_value(&[0xFE]));
        assert!(!ListManager::is_list_value(&[0xFF]));
        assert!(!ListManager::is_list_value(b"hello"));
        assert!(!ListManager::is_list_value(b""));
    }

    #[test]
    fn test_lpush_rpush_basic() {
        let mgr = make_manager();
        assert_eq!(mgr.lpush(b"mylist", &[b"c", b"b", b"a"]).unwrap(), 3);
        assert_eq!(mgr.rpush(b"mylist", &[b"d", b"e"]).unwrap(), 5);
        assert_eq!(mgr.lrange(b"mylist", 0, -1), vec![
            b"a".to_vec(), b"b".to_vec(), b"c".to_vec(),
            b"d".to_vec(), b"e".to_vec(),
        ]);
    }

    #[test]
    fn test_lpush_wrong_type() {
        let mgr = make_manager();
        mgr.store.set(b"str", b"hello");
        let err = mgr.lpush(b"str", &[b"x"]);
        assert_eq!(err, Err(ListError::WrongType));
    }

    #[test]
    fn test_lpush_on_hash_wrong_type() {
        let mgr = make_manager();
        mgr.store.set(b"hash", &[0xFF, 0x00, 0x00]);
        let err = mgr.lpush(b"hash", &[b"x"]);
        assert_eq!(err, Err(ListError::WrongType));
    }

    #[test]
    fn test_lpop_rpop() {
        let mgr = make_manager();
        mgr.lpush(b"q", &[b"3", b"2", b"1"]).unwrap();

        assert_eq!(mgr.lpop(b"q", 1), vec![b"1".to_vec()]);
        assert_eq!(mgr.rpop(b"q", 1), vec![b"3".to_vec()]);
        assert_eq!(mgr.lpop(b"q", 10), vec![b"2".to_vec()]);
        // Key should be removed when empty.
        assert_eq!(mgr.lpop(b"q", 1), Vec::<Vec<u8>>::new());
    }

    #[test]
    fn test_llen() {
        let mgr = make_manager();
        assert_eq!(mgr.llen(b"missing"), 0);
        mgr.rpush(b"lst", &[b"a", b"b"]).unwrap();
        assert_eq!(mgr.llen(b"lst"), 2);
    }

    #[test]
    fn test_lindex() {
        let mgr = make_manager();
        mgr.rpush(b"lst", &[b"a", b"b", b"c"]).unwrap();
        assert_eq!(mgr.lindex(b"lst", 0), Some(b"a".to_vec()));
        assert_eq!(mgr.lindex(b"lst", 1), Some(b"b".to_vec()));
        assert_eq!(mgr.lindex(b"lst", -1), Some(b"c".to_vec()));
        assert_eq!(mgr.lindex(b"lst", -4), None);
        assert_eq!(mgr.lindex(b"lst", 99), None);
    }

    #[test]
    fn test_lrem() {
        let mgr = make_manager();
        mgr.rpush(b"lst", &[b"a", b"b", b"a", b"c", b"a"]).unwrap();

        // Remove first 2 "a"
        assert_eq!(mgr.lrem(b"lst", 2, b"a"), 2);
        assert_eq!(mgr.lrange(b"lst", 0, -1), vec![
            b"b".to_vec(), b"c".to_vec(), b"a".to_vec(),
        ]);

        // Remove last 1 "a"
        assert_eq!(mgr.lrem(b"lst", -1, b"a"), 1);
        assert_eq!(mgr.lrange(b"lst", 0, -1), vec![
            b"b".to_vec(), b"c".to_vec(),
        ]);
    }

    #[test]
    fn test_lrem_all() {
        let mgr = make_manager();
        mgr.rpush(b"lst", &[b"x", b"a", b"x"]).unwrap();
        assert_eq!(mgr.lrem(b"lst", 0, b"x"), 2);
        assert_eq!(mgr.lrange(b"lst", 0, -1), vec![b"a".to_vec()]);
    }

    #[test]
    fn test_ltrim() {
        let mgr = make_manager();
        mgr.rpush(b"lst", &[b"a", b"b", b"c", b"d", b"e"]).unwrap();
        mgr.ltrim(b"lst", 1, 3);
        assert_eq!(mgr.lrange(b"lst", 0, -1), vec![
            b"b".to_vec(), b"c".to_vec(), b"d".to_vec(),
        ]);
    }

    #[test]
    fn test_lset() {
        let mgr = make_manager();
        mgr.rpush(b"lst", &[b"a", b"b", b"c"]).unwrap();
        assert!(mgr.lset(b"lst", 1, b"B"));
        assert_eq!(mgr.lindex(b"lst", 1), Some(b"B".to_vec()));
        assert!(!mgr.lset(b"lst", 99, b"x"));
    }

    #[test]
    fn test_remove_key() {
        let mgr = make_manager();
        mgr.rpush(b"tmp", &[b"x"]).unwrap();
        assert!(mgr.store.exists(b"tmp"));
        mgr.remove_key(b"tmp");
        assert!(!mgr.store.exists(b"tmp"));
        assert_eq!(mgr.llen(b"tmp"), 0);
    }

    #[test]
    fn test_lrange_negative() {
        let mgr = make_manager();
        mgr.rpush(b"lst", &[b"a", b"b", b"c"]).unwrap();
        assert_eq!(mgr.lrange(b"lst", -2, -1), vec![
            b"b".to_vec(), b"c".to_vec(),
        ]);
    }

    #[test]
    fn test_lrange_out_of_range() {
        let mgr = make_manager();
        mgr.rpush(b"lst", &[b"a", b"b"]).unwrap();
        assert_eq!(mgr.lrange(b"lst", 5, 10), Vec::<Vec<u8>>::new());
    }

    #[test]
    fn test_wrong_type_display() {
        let err = ListError::WrongType;
        assert!(err.to_string().contains("WRONGTYPE"));
    }

    #[test]
    fn test_normalize_index() {
        assert_eq!(normalize_index(0, 5), 0);
        assert_eq!(normalize_index(4, 5), 4);
        assert_eq!(normalize_index(10, 5), 4);
        assert_eq!(normalize_index(-1, 5), 4);
        assert_eq!(normalize_index(-5, 5), 0);
        assert_eq!(normalize_index(-10, 5), 0);
    }

    #[test]
    fn test_list_error_display() {
        let err = ListError::WrongType;
        assert!(err.to_string().contains("WRONGTYPE"));
    }
}
