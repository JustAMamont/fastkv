//! MinHash — Jaccard similarity estimation for set-based fields.
//!
//! MinHash provides a compact signature that approximates the Jaccard
//! similarity between two sets without storing the full sets. This is
//! particularly useful for comparing browser fingerprint fields like
//! fonts, plugins, and URL histories.
//!
//! ## Algorithm
//!
//! 1. For each of *k* hash functions h₁, h₂, …, hₖ:
//!    - Compute hᵢ(element) for every element in the set.
//!    - Record the minimum value as signature component *i*.
//! 2. The Jaccard similarity of two sets is estimated by the fraction
//!    of matching components in their MinHash signatures.
//!
//! ## Hash functions
//!
//! Instead of using *k* independent hash functions, we use a single
//! hash function with coefficient permutation:
//!
//! ```text
//! hᵢ(x) = (aᵢ * hash(x) + bᵢ) mod 2^64
//! ```
//!
//! where `aᵢ` and `bᵢ` are pre-generated odd random constants.
//! This is equivalent to using *k* independent hash functions but
//! much faster (one hash per element, then cheap permutations).
//!
//! ## Configuration
//!
//! - **128 hashes** (default) — gives ~2.8% error in Jaccard estimation
//! - Each component is a `u32` (4 bytes) → 512 bytes per signature
//! - Feature-gated behind `similarity`

/// Default number of hash functions (signature length).
pub const DEFAULT_NUM_HASHES: usize = 128;

/// Maximum number of hash functions supported.
pub const MAX_NUM_HASHES: usize = 256;

// ---------------------------------------------------------------------------
// Hash permutation coefficients
// ---------------------------------------------------------------------------

/// Pre-generated coefficients for hash permutation.
///
/// `a[i]` and `b[i]` are used to compute:
/// `h_i(x) = (a[i] * hash(x) + b[i]) mod 2^64`
///
/// The `a` values are odd to ensure they are coprime with 2^64.
/// Generated from a fixed seed for deterministic results.
struct HashCoefficients {
    a: Vec<u64>,
    b: Vec<u64>,
}

impl HashCoefficients {
    /// Generate coefficients for *k* hash functions.
    ///
    /// Uses a simple LCG (linear congruential generator) with a fixed
    /// seed to produce deterministic coefficients.
    fn new(k: usize) -> Self {
        let mut a = Vec::with_capacity(k);
        let mut b = Vec::with_capacity(k);

        // LCG parameters (same as glibc).
        let mut state: u64 = 0x1234_5678_9ABC_DEF0;
        let lcg_a: u64 = 6_364_136_223_846_793_005;
        let lcg_c: u64 = 1_442_695_040_888_963_407;

        for _ in 0..k {
            state = state.wrapping_mul(lcg_a).wrapping_add(lcg_c);
            // Ensure a is odd (coprime with 2^64).
            let ai = state | 1;
            state = state.wrapping_mul(lcg_a).wrapping_add(lcg_c);
            let bi = state;
            a.push(ai);
            b.push(bi);
        }

        Self { a, b }
    }
}

// ---------------------------------------------------------------------------
// MinHash signature
// ---------------------------------------------------------------------------

/// A MinHash signature — a compact representation of a set for
/// Jaccard similarity estimation.
///
/// The signature consists of *k* `u32` values, where each value is
/// the minimum hash value across all set elements for one of *k*
/// hash functions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinHashSig {
    /// The signature components (k values).
    pub values: Vec<u32>,
}

impl MinHashSig {
    /// Compute a MinHash signature for a set of byte strings.
    ///
    /// Each element is hashed and the minimum across all elements is
    /// recorded for each of *k* hash functions.
    pub fn new(elements: &[&[u8]], k: usize) -> Self {
        let k = k.clamp(1, MAX_NUM_HASHES);
        let coeffs = HashCoefficients::new(k);

        // Initialise with maximum values.
        let mut sig = vec![u32::MAX; k];

        for element in elements {
            let h = crate::core::simhash::hash64(element);

            for i in 0..k {
                // Permutation: h_i(x) = (a[i] * h + b[i]) mod 2^64
                let perm = coeffs.a[i].wrapping_mul(h).wrapping_add(coeffs.b[i]);
                // Truncate to u32 for compact storage.
                let val = (perm >> 32) as u32;
                if val < sig[i] {
                    sig[i] = val;
                }
            }
        }

        Self { values: sig }
    }

    /// Compute a MinHash signature for a set of byte strings with
    /// a custom hash seed.
    ///
    /// The seed is mixed into the initial hash to produce different
    /// signatures for the same set (useful for ensemble methods).
    pub fn new_seeded(elements: &[&[u8]], k: usize, seed: u64) -> Self {
        let k = k.clamp(1, MAX_NUM_HASHES);
        let coeffs = HashCoefficients::new(k);
        let mut sig = vec![u32::MAX; k];

        for element in elements {
            // Mix seed into hash.
            let h = crate::core::simhash::hash64(element) ^ seed;

            for i in 0..k {
                let perm = coeffs.a[i].wrapping_mul(h).wrapping_add(coeffs.b[i]);
                let val = (perm >> 32) as u32;
                if val < sig[i] {
                    sig[i] = val;
                }
            }
        }

        Self { values: sig }
    }

    /// Estimate the Jaccard similarity between this signature and another.
    ///
    /// Returns a value in [0.0, 1.0] where:
    /// - 1.0 = identical sets (all components match)
    /// - 0.0 = completely disjoint sets (no components match)
    ///
    /// The estimate has standard error ≈ 1/√k. For k=128, this is ~2.8%.
    pub fn jaccard_similarity(&self, other: &MinHashSig) -> f64 {
        if self.values.len() != other.values.len() {
            return 0.0;
        }
        if self.values.is_empty() {
            return 1.0;
        }
        let matches = self.values.iter().zip(other.values.iter())
            .filter(|(a, b)| a == b)
            .count();
        matches as f64 / self.values.len() as f64
    }

    /// Number of hash functions used (signature length).
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Whether the signature is empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Serialize the signature to a byte vector.
    ///
    /// Each component is stored as 4 bytes (little-endian).
    /// Total size: 4 * k bytes (512 bytes for k=128).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.values.len() * 4);
        for &v in &self.values {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        buf
    }

    /// Deserialize a signature from a byte slice.
    ///
    /// Returns `None` if the input length is not a multiple of 4.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if !data.len().is_multiple_of(4) {
            return None;
        }
        let values: Vec<u32> = data.chunks_exact(4)
            .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
            .collect();
        Some(Self { values })
    }

    /// Get the band value at the given band index.
    ///
    /// The 64-bit SimHash is split into `num_bands` equal-width bands.
    /// Each band is a contiguous chunk of bits used as an LSH bucket key.
    /// This method extracts the bits for band `band_idx` out of `num_bands`.
    ///
    /// Returns `None` if `band_idx >= num_bands`.
    pub fn band_value(&self, band_idx: usize, num_bands: usize) -> Option<u32> {
        if band_idx >= num_bands || num_bands == 0 || num_bands > self.values.len() {
            return None;
        }
        let band_size = self.values.len() / num_bands;
        let start = band_idx * band_size;
        let end = start + band_size;
        if end > self.values.len() {
            return None;
        }
        // Combine the values in this band into a single u32 hash.
        let mut h: u32 = 0;
        for &v in &self.values[start..end] {
            h = h.wrapping_add(v);
            h = h.rotate_left(7);
            h ^= v;
        }
        Some(h)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_minhash_identical_sets() {
        let set: Vec<&[u8]> = vec![b"Arial", b"Helvetica", b"Times New Roman"];
        let sig1 = MinHashSig::new(&set, DEFAULT_NUM_HASHES);
        let sig2 = MinHashSig::new(&set, DEFAULT_NUM_HASHES);
        assert_eq!(sig1, sig2);
        let sim = sig1.jaccard_similarity(&sig2);
        assert!((sim - 1.0).abs() < 0.001, "Expected ~1.0, got {}", sim);
    }

    #[test]
    fn test_minhash_similar_sets() {
        let set1: Vec<&[u8]> = vec![b"Arial", b"Helvetica", b"Times New Roman", b"Courier"];
        let set2: Vec<&[u8]> = vec![b"Arial", b"Helvetica", b"Times New Roman", b"Verdana"];
        // 3/4 overlap → Jaccard ≈ 0.6
        let sig1 = MinHashSig::new(&set1, DEFAULT_NUM_HASHES);
        let sig2 = MinHashSig::new(&set2, DEFAULT_NUM_HASHES);
        let sim = sig1.jaccard_similarity(&sig2);
        // MinHash estimate should be in reasonable range.
        assert!(sim > 0.2 && sim < 0.9, "Expected ~0.6, got {}", sim);
    }

    #[test]
    fn test_minhash_disjoint_sets() {
        let set1: Vec<&[u8]> = vec![b"aaaa", b"bbbb", b"cccc"];
        let set2: Vec<&[u8]> = vec![b"xxxx", b"yyyy", b"zzzz"];
        let sig1 = MinHashSig::new(&set1, DEFAULT_NUM_HASHES);
        let sig2 = MinHashSig::new(&set2, DEFAULT_NUM_HASHES);
        let sim = sig1.jaccard_similarity(&sig2);
        // Disjoint sets should have low similarity.
        assert!(sim < 0.3, "Expected low similarity, got {}", sim);
    }

    #[test]
    fn test_minhash_serialization() {
        let set: Vec<&[u8]> = vec![b"foo", b"bar", b"baz"];
        let sig = MinHashSig::new(&set, DEFAULT_NUM_HASHES);
        let bytes = sig.to_bytes();
        assert_eq!(bytes.len(), DEFAULT_NUM_HASHES * 4);

        let restored = MinHashSig::from_bytes(&bytes).unwrap();
        assert_eq!(sig, restored);
    }

    #[test]
    fn test_minhash_from_bytes_invalid() {
        assert!(MinHashSig::from_bytes(&[1, 2, 3]).is_none()); // Not multiple of 4
    }

    #[test]
    fn test_minhash_empty_set() {
        let set: Vec<&[u8]> = vec![];
        let sig = MinHashSig::new(&set, DEFAULT_NUM_HASHES);
        // All values should be u32::MAX for empty set.
        assert!(sig.values.iter().all(|&v| v == u32::MAX));
    }

    #[test]
    fn test_minhash_single_element() {
        let set: Vec<&[u8]> = vec![b"only_one"];
        let sig = MinHashSig::new(&set, 32);
        assert_eq!(sig.len(), 32);
        // All values should be deterministic.
        let sig2 = MinHashSig::new(&set, 32);
        assert_eq!(sig, sig2);
    }

    #[test]
    fn test_band_value() {
        let set: Vec<&[u8]> = vec![b"Arial", b"Helvetica", b"Times"];
        let sig = MinHashSig::new(&set, 128);
        // With 4 bands, each band covers 32 values.
        let b0 = sig.band_value(0, 4);
        let b1 = sig.band_value(1, 4);
        assert!(b0.is_some());
        assert!(b1.is_some());
        // Different bands should generally produce different values.
        // (Not guaranteed but very likely with 32-bit output.)
    }

    #[test]
    fn test_band_value_out_of_range() {
        let set: Vec<&[u8]> = vec![b"test"];
        let sig = MinHashSig::new(&set, 128);
        assert!(sig.band_value(4, 4).is_none());
        assert!(sig.band_value(0, 0).is_none());
    }

    #[test]
    fn test_seeded_minhash() {
        let set: Vec<&[u8]> = vec![b"foo", b"bar"];
        let sig1 = MinHashSig::new_seeded(&set, 64, 42);
        let sig2 = MinHashSig::new_seeded(&set, 64, 42);
        let sig3 = MinHashSig::new_seeded(&set, 64, 99);
        // Same seed → same signature.
        assert_eq!(sig1, sig2);
        // Different seed → different signature (very likely).
        assert_ne!(sig1, sig3);
    }
}
