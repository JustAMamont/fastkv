//! SimHash — locality-sensitive hashing for near-duplicate detection.
//!
//! SimHash produces a 64-bit fingerprint of a document/profile such that
//! similar inputs produce similar (small Hamming distance) hashes.  This
//! enables O(1) near-duplicate detection when combined with LSH banding.
//!
//! ## Algorithm
//!
//! 1. For each feature (key-value pair) in the input, compute a 64-bit hash.
//! 2. Maintain a 64-element vector of weights, initialised to zero.
//! 3. For each bit *i* of the feature hash:
//!    - If bit *i* is 1, add the feature weight to `v[i]`.
//!    - If bit *i* is 0, subtract the feature weight from `v[i]`.
//! 4. The final SimHash has bit *i* = 1 iff `v[i] > 0`.
//!
//! ## Hamming distance
//!
//! Two SimHashes are considered "similar" when their Hamming distance
//! (number of differing bits) is ≤ *threshold* (default 3).  On x86,
//! Hamming distance is computed with a single `popcnt(xor(a, b))`
//! instruction.
//!
//! ## Weighted features
//!
//! For browser profile comparison, fields have different importance:
//! - `user_agent` (weight 4) — most distinctive
//! - `webgl_renderer` (weight 3)
//! - `screen_resolution` (weight 2)
//! - `os` (weight 2)
//! - `fonts` (weight 1) — many shared fonts are common
//! - `timezone` (weight 1)

use std::collections::HashMap;

/// A feature key and its weight for profile fingerprinting.
#[derive(Debug, Clone)]
pub struct FeatureWeight {
    /// The feature key (e.g. b"user_agent").
    pub key: &'static [u8],
    /// The weight (higher = more distinctive).
    pub weight: i32,
}

/// Default feature weights for browser profile comparison.
///
/// These weights reflect how distinctive each field is for
/// fingerprinting purposes:
/// - `user_agent` (4) — most distinctive, varies per browser/version/OS
/// - `webgl_renderer` (3) — GPU-specific
/// - `screen_resolution` (2) — moderate variation
/// - `os` (2) — moderate variation
/// - `fonts` (1) — many shared fonts across profiles
/// - `timezone` (1) — low variation within regions
pub const DEFAULT_PROFILE_WEIGHTS: &[FeatureWeight] = &[
    FeatureWeight { key: b"user_agent", weight: 4 },
    FeatureWeight { key: b"webgl_renderer", weight: 3 },
    FeatureWeight { key: b"screen_resolution", weight: 2 },
    FeatureWeight { key: b"os", weight: 2 },
    FeatureWeight { key: b"fonts", weight: 1 },
    FeatureWeight { key: b"timezone", weight: 1 },
    FeatureWeight { key: b"locale", weight: 1 },
    FeatureWeight { key: b"platform", weight: 2 },
    FeatureWeight { key: b"webgl_vendor", weight: 2 },
    FeatureWeight { key: b"browser_version", weight: 3 },
];

/// Build a HashMap from DEFAULT_PROFILE_WEIGHTS for use with simhash_kv.
pub fn default_profile_weights_map() -> HashMap<Vec<u8>, i32> {
    DEFAULT_PROFILE_WEIGHTS.iter()
        .map(|fw| (fw.key.to_vec(), fw.weight))
        .collect()
}

// ---------------------------------------------------------------------------
// Hashing — wyhash-inspired 64-bit (same as kv.rs for zero dependencies)
// ---------------------------------------------------------------------------

/// Compute a 64-bit hash of *data* using a wyhash-inspired mixer.
///
/// This is the same hash function used in `kv.rs` for consistency
/// and zero external dependencies.
#[inline]
pub fn hash64(data: &[u8]) -> u64 {
    const SECRET0: u64 = 0xa0761d6478bd642f;
    const SECRET1: u64 = 0xe7037ed1a0b428db;
    let mut hash = 0xcbf29ce484222325;
    let chunks = data.chunks_exact(8);
    let remainder = chunks.remainder();
    for chunk in chunks {
        let v = u64::from_le_bytes(chunk.try_into().unwrap());
        hash ^= v.wrapping_mul(SECRET0);
        hash = hash.rotate_left(31).wrapping_mul(SECRET1);
    }
    let mut v = hash;
    for &byte in remainder {
        v ^= (byte as u64).wrapping_mul(SECRET0);
        v = v.rotate_left(31).wrapping_mul(SECRET1);
    }
    v ^ (v >> 31)
}

// ---------------------------------------------------------------------------
// SimHash computation
// ---------------------------------------------------------------------------

/// Default Hamming distance threshold for near-duplicate detection.
pub const DEFAULT_HAMMING_THRESHOLD: u32 = 3;

/// Compute a SimHash from a set of weighted features.
///
/// Each feature is a (key, weight) pair. The key is hashed to produce a
/// 64-bit value, and the weight determines how strongly that feature
/// influences the final hash.
///
/// Returns a 64-bit SimHash fingerprint.
pub fn simhash(features: &[(Vec<u8>, i32)]) -> u64 {
    let mut v = [0i64; 64];

    for (data, weight) in features {
        let h = hash64(data);
        for i in 0..64 {
            if (h >> i) & 1 == 1 {
                v[i] += *weight as i64;
            } else {
                v[i] -= *weight as i64;
            }
        }
    }

    let mut result: u64 = 0;
    for i in 0..64 {
        if v[i] > 0 {
            result |= 1u64 << i;
        }
    }
    result
}

/// Compute a SimHash from a set of byte slices with default weights.
///
/// Each feature is assigned weight 1. Useful for simple document
/// fingerprinting where all features are equally important.
pub fn simhash_uniform(features: &[&[u8]]) -> u64 {
    let weighted: Vec<(Vec<u8>, i32)> = features
        .iter()
        .map(|f| (f.to_vec(), 1))
        .collect();
    simhash(&weighted)
}

/// Compute a SimHash from key-value pairs (e.g. a JSON profile).
///
/// Each key-value pair is combined into a single byte string
/// `key\x00value` before hashing. The weight is determined by
/// a provided weight table, defaulting to 1 for unknown keys.
pub fn simhash_kv(
    pairs: &HashMap<Vec<u8>, Vec<u8>>,
    weights: &HashMap<Vec<u8>, i32>,
) -> u64 {
    let features: Vec<(Vec<u8>, i32)> = pairs
        .iter()
        .map(|(k, v)| {
            let mut data = Vec::with_capacity(k.len() + 1 + v.len());
            data.extend_from_slice(k);
            data.push(0x00); // separator
            data.extend_from_slice(v);
            let w = weights.get(k).copied().unwrap_or(1);
            (data, w)
        })
        .collect();
    simhash(&features)
}

// ---------------------------------------------------------------------------
// Hamming distance
// ---------------------------------------------------------------------------

/// Compute the Hamming distance between two 64-bit hashes.
///
/// On x86/x86_64 this compiles to a single `popcnt` instruction.
/// On other architectures it uses a portable bit-counting loop.
#[inline]
pub fn hamming_distance(a: u64, b: u64) -> u32 {
    (a ^ b).count_ones()
}

/// Check if two SimHashes are "similar" (Hamming distance ≤ threshold).
#[inline]
pub fn is_similar(a: u64, b: u64, threshold: u32) -> bool {
    hamming_distance(a, b) <= threshold
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simhash_identical_inputs() {
        let features: Vec<(Vec<u8>, i32)> = vec![
            (b"hello".to_vec(), 1),
            (b"world".to_vec(), 1),
        ];
        let h1 = simhash(&features);
        let h2 = simhash(&features);
        assert_eq!(h1, h2);
        assert_eq!(hamming_distance(h1, h2), 0);
    }

    #[test]
    fn test_simhash_similar_inputs() {
        let features1: Vec<(Vec<u8>, i32)> = vec![
            (b"user_agent:Chrome/148".to_vec(), 4),
            (b"screen:1920x1080".to_vec(), 2),
            (b"os:Windows".to_vec(), 2),
        ];
        let features2: Vec<(Vec<u8>, i32)> = vec![
            (b"user_agent:Chrome/148".to_vec(), 4),
            (b"screen:1920x1080".to_vec(), 2),
            (b"os:MacOS".to_vec(), 2),
        ];
        let h1 = simhash(&features1);
        let h2 = simhash(&features2);
        // Similar inputs should have small Hamming distance.
        assert!(hamming_distance(h1, h2) <= 10, "Hamming distance too large: {}", hamming_distance(h1, h2));
    }

    #[test]
    fn test_simhash_different_inputs() {
        let features1: Vec<(Vec<u8>, i32)> = vec![
            (b"aaaa".to_vec(), 4),
            (b"bbbb".to_vec(), 4),
        ];
        let features2: Vec<(Vec<u8>, i32)> = vec![
            (b"zzzz".to_vec(), 4),
            (b"yyyy".to_vec(), 4),
        ];
        let h1 = simhash(&features1);
        let h2 = simhash(&features2);
        // Very different inputs should have large Hamming distance.
        assert!(hamming_distance(h1, h2) > 5, "Hamming distance unexpectedly small: {}", hamming_distance(h1, h2));
    }

    #[test]
    fn test_hamming_distance() {
        assert_eq!(hamming_distance(0, 0), 0);
        assert_eq!(hamming_distance(0, 1), 1);
        assert_eq!(hamming_distance(0, u64::MAX), 64);
        assert_eq!(hamming_distance(0b1010, 0b0101), 4);
    }

    #[test]
    fn test_is_similar() {
        let h = 0xABCD_EF01_2345_6789u64;
        assert!(is_similar(h, h, 0));
        assert!(is_similar(h, h ^ 1, 1));
        assert!(!is_similar(h, h ^ 0xFF, 3));
        assert!(is_similar(h, h ^ 0x7, 3));
    }

    #[test]
    fn test_simhash_uniform() {
        let features: Vec<&[u8]> = vec![b"foo", b"bar", b"baz"];
        let h1 = simhash_uniform(&features);
        let h2 = simhash_uniform(&features);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_simhash_kv() {
        let mut pairs = HashMap::new();
        pairs.insert(b"user_agent".to_vec(), b"Chrome/148".to_vec());
        pairs.insert(b"os".to_vec(), b"Windows".to_vec());

        let weights = default_profile_weights_map();
        let h1 = simhash_kv(&pairs, &weights);
        let h2 = simhash_kv(&pairs, &weights);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_simhash_empty_features() {
        let features: Vec<(Vec<u8>, i32)> = vec![];
        let h = simhash(&features);
        // Empty features should produce a deterministic hash.
        assert_eq!(h, 0);
    }

    #[test]
    fn test_simhash_single_feature() {
        let features: Vec<(Vec<u8>, i32)> = vec![
            (b"test".to_vec(), 1),
        ];
        let h = simhash(&features);
        // Single feature: bits should follow the hash of the feature.
        let feature_hash = hash64(b"test");
        // With weight 1, bit i = 1 iff the hash has bit i = 1 (since v[i] = +1 or -1).
        assert_eq!(h, feature_hash);
    }

    #[test]
    fn test_weight_influence() {
        let features_low: Vec<(Vec<u8>, i32)> = vec![
            (b"a".to_vec(), 1),
            (b"b".to_vec(), 1),
        ];
        let features_high: Vec<(Vec<u8>, i32)> = vec![
            (b"a".to_vec(), 10),
            (b"b".to_vec(), 10),
        ];
        // Same relative weights should produce same hash.
        let h_low = simhash(&features_low);
        let h_high = simhash(&features_high);
        assert_eq!(h_low, h_high);
    }
}
