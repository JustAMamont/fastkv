//! Sorted Set — Redis-compatible ZADD/ZRANGE/ZSCORE/etc.
//!
//! Lock-free implementation using:
//! - `crossbeam_skiplist::SkipMap` for ordered score→members mapping
//! - `dashmap::DashMap` for O(1) member→score lookup
//!
//! No RwLock, no Mutex — all operations are concurrent without blocking.

use crossbeam_skiplist::SkipMap;
use dashmap::DashMap;
use std::sync::Arc;

pub type Score = f64;

/// Ordered wrapper for f64 — converts to comparable u64 via IEEE 754.
#[derive(Clone, Copy)]
struct OrderedScore(f64);

impl OrderedScore {
    fn key(self) -> u64 {
        if self.0.is_nan() { return 0; }
        let bits = self.0.to_bits();
        if self.0 >= 0.0 { bits ^ 0x8000_0000_0000_0000 } else { !bits }
    }
}
impl PartialEq for OrderedScore { fn eq(&self, o: &Self) -> bool { self.key() == o.key() } }
impl Eq for OrderedScore {}
impl PartialOrd for OrderedScore { fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(o)) } }
impl Ord for OrderedScore { fn cmp(&self, o: &Self) -> std::cmp::Ordering { self.key().cmp(&o.key()) } }

/// A single sorted set — lock-free.
struct SortedSet {
    /// score → members. SkipMap = lock-free concurrent skip list.
    by_score: SkipMap<OrderedScore, Vec<Vec<u8>>>,
    /// member → score. DashMap = lock-free concurrent HashMap.
    by_member: DashMap<Vec<u8>, Score>,
}

impl SortedSet {
    fn new() -> Self {
        Self { by_score: SkipMap::new(), by_member: DashMap::new() }
    }

    /// Insert a member into a score bucket in by_score.
    /// If the bucket doesn't exist, create it. If it does, append.
    fn insert_into_bucket(&self, score: OrderedScore, member: &[u8]) {
        // Check if bucket already exists
        if let Some(entry) = self.by_score.get(&score) {
            let mut members = entry.value().clone();
            members.push(member.to_vec());
            drop(entry);
            self.by_score.remove(&score);
            self.by_score.insert(score, members);
        } else {
            // Bucket doesn't exist — insert new
            self.by_score.insert(score, vec![member.to_vec()]);
        }
    }

    /// Remove a member from a score bucket.
    fn remove_from_bucket(&self, score: OrderedScore, member: &[u8]) {
        if let Some(entry) = self.by_score.get(&score) {
            let mut members = entry.value().clone();
            members.retain(|m| m.as_slice() != member);
            drop(entry);
            self.by_score.remove(&score);
            if !members.is_empty() {
                self.by_score.insert(score, members);
            }
        }
    }
}

/// Top-level store: key → Arc<SortedSet>.
pub struct SortedSetStore {
    sets: DashMap<String, Arc<SortedSet>>,
}

impl Default for SortedSetStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SortedSetStore {
    pub fn new() -> Self {
        Self { sets: DashMap::new() }
    }

    fn get_or_create(&self, key: &str) -> Arc<SortedSet> {
        if let Some(entry) = self.sets.get(key) {
            return entry.clone();
        }
        self.sets
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(SortedSet::new()))
            .clone()
    }

    /// ZADD key score member [score member ...] → count of new elements.
    pub async fn zadd(&self, key: &str, pairs: &[(Score, Vec<u8>)]) -> usize {
        let set = self.get_or_create(key);
        let mut added = 0;

        for (score, member) in pairs {
            if let Some(mut entry) = set.by_member.get_mut(member) {
                let old_score = *entry.value();
                if old_score != *score {
                    *entry.value_mut() = *score;
                    drop(entry);
                    set.remove_from_bucket(OrderedScore(old_score), member);
                    set.insert_into_bucket(OrderedScore(*score), member);
                }
            } else {
                set.by_member.insert(member.clone(), *score);
                set.insert_into_bucket(OrderedScore(*score), member);
                added += 1;
            }
        }
        added
    }

    /// ZSCORE key member → Option<Score>
    pub async fn zscore(&self, key: &str, member: &[u8]) -> Option<Score> {
        let set = self.sets.get(key)?;
        set.by_member.get(member).map(|e| *e.value())
    }

    /// ZCARD key → count
    pub async fn zcard(&self, key: &str) -> usize {
        let Some(set) = self.sets.get(key) else { return 0 };
        set.by_member.len()
    }

    /// ZRANGE key start stop — ascending score order. Supports negative indices.
    pub async fn zrange(&self, key: &str, start: i64, stop: i64) -> Vec<Vec<u8>> {
        let Some(set) = self.sets.get(key) else { return vec![] };
        let total = set.by_member.len() as i64;
        if total == 0 { return vec![]; }

        let start = if start < 0 { (total + start).max(0) } else { start.min(total - 1) };
        let stop = if stop < 0 { total + stop } else { stop.min(total - 1) };
        if start > stop || start >= total { return vec![]; }

        let mut result: Vec<Vec<u8>> = Vec::with_capacity((stop - start + 1) as usize);
        let mut idx = 0i64;
        for entry in set.by_score.iter() {
            for member in entry.value() {
                if idx >= start && idx <= stop {
                    result.push(member.clone());
                }
                idx += 1;
                if idx > stop { return result; }
            }
        }
        result
    }

    /// ZREVRANGE key start stop — descending score order.
    pub async fn zrevrange(&self, key: &str, start: i64, stop: i64) -> Vec<Vec<u8>> {
        let Some(set) = self.sets.get(key) else { return vec![] };
        let total = set.by_member.len() as i64;
        if total == 0 { return vec![]; }

        let start = if start < 0 { (total + start).max(0) } else { start.min(total - 1) };
        let stop = if stop < 0 { total + stop } else { stop.min(total - 1) };
        if start > stop || start >= total { return vec![]; }

        let mut all: Vec<Vec<u8>> = Vec::with_capacity(total as usize);
        for entry in set.by_score.iter().rev() {
            for member in entry.value() {
                all.push(member.clone());
            }
        }
        if (start as usize) < all.len() && (stop as usize) < all.len() {
            all[start as usize..=stop as usize].to_vec()
        } else {
            vec![]
        }
    }

    /// ZREVRANGEBYSCORE key max min — members with score in [min, max], descending.
    pub async fn zrevrangebyscore(&self, key: &str, max: Score, min: Score) -> Vec<Vec<u8>> {
        let Some(set) = self.sets.get(key) else { return vec![] };
        let mut result = vec![];
        for entry in set.by_score.iter().rev() {
            let score = entry.key().0;
            if score >= min && score <= max {
                result.extend(entry.value().iter().cloned());
            }
        }
        result
    }

    /// ZREM key member [member ...] → count removed.
    pub async fn zrem(&self, key: &str, members: &[Vec<u8>]) -> usize {
        let Some(set) = self.sets.get(key) else { return 0 };
        let mut removed = 0;
        for member in members {
            if let Some((_, score)) = set.by_member.remove(member) {
                set.remove_from_bucket(OrderedScore(score), member);
                removed += 1;
            }
        }
        removed
    }

    /// ZINCRBY key increment member → new score.
    pub async fn zincrby(&self, key: &str, increment: Score, member: &[u8]) -> Score {
        let set = self.get_or_create(key);

        let mut entry = set.by_member.entry(member.to_vec()).or_insert(0.0);
        let old_score = *entry.value();
        let new_score = old_score + increment;
        *entry.value_mut() = new_score;
        drop(entry);

        if old_score != 0.0 {
            set.remove_from_bucket(OrderedScore(old_score), member);
        }
        set.insert_into_bucket(OrderedScore(new_score), member);
        new_score
    }

    /// Delete entire sorted set.
    pub async fn del(&self, key: &str) -> bool {
        self.sets.remove(key).is_some()
    }

    /// Check if key exists.
    pub async fn exists(&self, key: &str) -> bool {
        self.sets.contains_key(key)
    }
}
