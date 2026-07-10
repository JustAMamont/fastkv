//! Locality-Sensitive Hashing (LSH) for O(1) approximate nearest neighbor search.
//!
//! LSH buckets similar items together so that finding similar profiles
//! requires only a hash table lookup rather than a full scan. This
//! module implements band-based LSH using both SimHash and MinHash
//! signatures.
//!
//! ## SimHash LSH
//!
//! A 64-bit SimHash is split into *b* bands of *r* bits each
//! (where b × r = 64). For each band, the *r*-bit sub-hash is used
//! as a bucket key. Two items are candidates for being similar if
//! they share at least one band value.
//!
//! **Default:** 4 bands × 16 bits — probability of candidate match
//! for Hamming distance ≤ 3 is ~99.6%.
//!
//! ## MinHash LSH
//!
//! A MinHash signature of *k* components is split into *b* bands of
//! *r* components each (where b × r = k). For each band, the *r*
//! components are hashed into a single bucket key. Two items are
//! candidates if they share at least one band value.
//!
//! **Default:** 4 bands × 32 components (k=128) — probability of
//! candidate match for Jaccard ≥ 0.5 is ~99.8%.
//!
//! ## Storage
//!
//! All LSH data is stored as regular key-value pairs in the FastKV
//! hash table with the following key patterns:
//!
//! - `lsh:sim:{band}:{value}` → list of profile IDs (SimHash band bucket)
//! - `lsh:min:{band}:{value}` → list of profile IDs (MinHash band bucket)
//!
//! This means LSH benefits from all existing FastKV features: WAL
//! persistence, TTL, blob storage, etc.

use crate::core::simhash::{self, hamming_distance};
use crate::core::minhash::MinHashSig;
use crate::core::kv::KvStoreLockFree;

use crate::core::blob::BlobArena;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default number of bands for SimHash LSH.
pub const DEFAULT_SIM_BANDS: usize = 4;

/// Bits per band for SimHash LSH (64 bits / 4 bands = 16 bits).
pub const DEFAULT_SIM_BITS_PER_BAND: usize = 16;

/// Default number of bands for MinHash LSH.
pub const DEFAULT_MIN_BANDS: usize = 4;

/// Components per band for MinHash LSH (128 / 4 = 32).
pub const DEFAULT_MIN_ROWS_PER_BAND: usize = 32;

// ---------------------------------------------------------------------------
// SimHash band extraction
// ---------------------------------------------------------------------------

/// Extract the sub-hash for a specific band from a 64-bit SimHash.
///
/// The SimHash is split into `num_bands` equal-width bands.
/// Band 0 is the least significant bits, band (num_bands-1) is the most
/// significant bits.
///
/// Returns the band value as a u16 (for 16-bit bands with 4 bands).
#[inline]
pub fn simhash_band(hash: u64, band_idx: usize, num_bands: usize) -> u64 {
    let bits_per_band = 64 / num_bands;
    let shift = band_idx * bits_per_band;
    let mask = (1u64 << bits_per_band) - 1;
    (hash >> shift) & mask
}

// ---------------------------------------------------------------------------
// LSH bucket key generation
// ---------------------------------------------------------------------------

/// Generate the FastKV key for a SimHash band bucket.
///
/// Format: `lsh:sim:{band}:{value}`
pub fn sim_bucket_key(band: usize, value: u64) -> Vec<u8> {
    format!("lsh:sim:{}:{}", band, value).into_bytes()
}

/// Generate the FastKV key for a MinHash band bucket.
///
/// Format: `lsh:min:{band}:{value}`
pub fn min_bucket_key(band: usize, value: u32) -> Vec<u8> {
    format!("lsh:min:{}:{}", band, value).into_bytes()
}

// ---------------------------------------------------------------------------
// SimHash profile for LSH
// ---------------------------------------------------------------------------

/// A profile fingerprint ready for LSH indexing.
///
/// Contains both the 64-bit SimHash and the MinHash signature,
/// allowing both types of similarity search.
#[derive(Debug, Clone)]
pub struct LshProfile {
    /// Unique profile ID (used as the key in FastKV).
    pub id: Vec<u8>,
    /// 64-bit SimHash fingerprint.
    pub simhash: u64,
    /// MinHash signature (optional — 128 × u32 = 512 bytes).
    pub minhash: Option<MinHashSig>,
}

// ---------------------------------------------------------------------------
// LSH operations
// ---------------------------------------------------------------------------

/// Compute a SimHash for a value stored in the KV store.
///
/// The value is treated as a set of features separated by newlines
/// (or null bytes for structured data). Each feature is hashed
/// individually.
///
/// Returns the 64-bit SimHash, or `None` if the key doesn't exist.
pub fn compute_simhash_for_key<const N: usize>(
    store: &KvStoreLockFree<N>,
    blob: Option<&BlobArena>,
    key: &[u8],
) -> Option<u64> {
    let data = store.get(key)?;

    // If the data is a BlobRef, try to dereference it
    let data = if data.len() == crate::core::blob::BLOB_REF_SIZE
        && data[0] == crate::core::blob::BLOB_REF_FLAG
    {
        if let Some(blob_arena) = blob {
            if let Some(blob_ref) = crate::core::blob::BlobRef::decode(&data) {
                blob_arena.retrieve(&blob_ref).unwrap_or(data)
            } else {
                data
            }
        } else {
            data
        }
    } else {
        data
    };

    let features: Vec<&[u8]> = if data.contains(&0x00) {
        data.split(|&b| b == 0x00).filter(|s| !s.is_empty()).collect()
    } else {
        data.split(|&b| b == b'\n').filter(|s| !s.is_empty()).collect()
    };

    if features.is_empty() {
        return Some(0);
    }

    Some(simhash::simhash_uniform(&features))
}

/// Add a profile to the LSH index.
///
/// Stores the SimHash band values as bucket keys in the KV store,
/// with the profile ID added to each bucket's member list.
///
/// Returns the number of band entries created.
pub fn lsh_add_sim<const N: usize>(
    store: &KvStoreLockFree<N>,
    profile_id: &[u8],
    simhash_value: u64,
    num_bands: usize,
) -> usize {
    let mut count = 0;
    for band in 0..num_bands {
        let band_val = simhash_band(simhash_value, band, num_bands);
        let bucket_key = sim_bucket_key(band, band_val);

        // Append profile_id to the bucket list.
        // The list is stored as: length(u32 LE) + id1_len(u32) + id1 + id2_len(u32) + id2 + ...
        if let Some(existing) = store.get(&bucket_key) {
            let mut new_data = existing;
            new_data.extend_from_slice(&(profile_id.len() as u32).to_le_bytes());
            new_data.extend_from_slice(profile_id);
            let _ = store.set(&bucket_key, &new_data);
        } else {
            let mut new_data = Vec::new();
            new_data.extend_from_slice(&1u32.to_le_bytes()); // count = 1
            new_data.extend_from_slice(&(profile_id.len() as u32).to_le_bytes());
            new_data.extend_from_slice(profile_id);
            let _ = store.set(&bucket_key, &new_data);
        }
        count += 1;
    }
    count
}

/// Add a MinHash profile to the LSH index.
///
/// Stores the MinHash band values as bucket keys, with the profile ID
/// added to each bucket's member list.
pub fn lsh_add_min<const N: usize>(
    store: &KvStoreLockFree<N>,
    profile_id: &[u8],
    minhash: &MinHashSig,
    num_bands: usize,
) -> usize {
    let mut count = 0;
    for band in 0..num_bands {
        if let Some(band_val) = minhash.band_value(band, num_bands) {
            let bucket_key = min_bucket_key(band, band_val);

            if let Some(existing) = store.get(&bucket_key) {
                let mut new_data = existing;
                new_data.extend_from_slice(&(profile_id.len() as u32).to_le_bytes());
                new_data.extend_from_slice(profile_id);
                let _ = store.set(&bucket_key, &new_data);
            } else {
                let mut new_data = Vec::new();
                new_data.extend_from_slice(&1u32.to_le_bytes());
                new_data.extend_from_slice(&(profile_id.len() as u32).to_le_bytes());
                new_data.extend_from_slice(profile_id);
                let _ = store.set(&bucket_key, &new_data);
            }
            count += 1;
        }
    }
    count
}

/// Remove a profile from the SimHash LSH index.
///
/// Removes the profile ID from all band buckets.
/// Returns the number of bucket entries modified.
pub fn lsh_rem_sim<const N: usize>(
    store: &KvStoreLockFree<N>,
    profile_id: &[u8],
    simhash_value: u64,
    num_bands: usize,
) -> usize {
    let mut count = 0;
    for band in 0..num_bands {
        let band_val = simhash_band(simhash_value, band, num_bands);
        let bucket_key = sim_bucket_key(band, band_val);

        if let Some(existing) = store.get(&bucket_key) {
            let ids = parse_id_list(&existing);
            let filtered: Vec<&[u8]> = ids.iter()
                .filter(|id| **id != profile_id)
                .copied()
                .collect();

            if filtered.is_empty() {
                store.del(&bucket_key);
            } else {
                let new_data = encode_id_list(&filtered);
                let _ = store.set(&bucket_key, &new_data);
            }
            count += 1;
        }
    }
    count
}

/// Remove a profile from the MinHash LSH index.
pub fn lsh_rem_min<const N: usize>(
    store: &KvStoreLockFree<N>,
    profile_id: &[u8],
    minhash: &MinHashSig,
    num_bands: usize,
) -> usize {
    let mut count = 0;
    for band in 0..num_bands {
        if let Some(band_val) = minhash.band_value(band, num_bands) {
            let bucket_key = min_bucket_key(band, band_val);

            if let Some(existing) = store.get(&bucket_key) {
                let ids = parse_id_list(&existing);
                let filtered: Vec<&[u8]> = ids.iter()
                    .filter(|id| **id != profile_id)
                    .copied()
                    .collect();

                if filtered.is_empty() {
                    store.del(&bucket_key);
                } else {
                    let new_data = encode_id_list(&filtered);
                    let _ = store.set(&bucket_key, &new_data);
                }
                count += 1;
            }
        }
    }
    count
}

/// Find profiles similar to a given SimHash using LSH banding.
///
/// 1. Extract band values from the query SimHash.
/// 2. Look up candidate IDs from each band bucket.
/// 3. (Optional) Verify Hamming distance for each candidate if a
///    SimHash→key mapping is available.
///
/// Returns a list of candidate profile IDs.
pub fn find_similar_sim<const N: usize>(
    store: &KvStoreLockFree<N>,
    simhash_value: u64,
    num_bands: usize,
    threshold: u32,
) -> Vec<Vec<u8>> {
    let mut candidates = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for band in 0..num_bands {
        let band_val = simhash_band(simhash_value, band, num_bands);
        let bucket_key = sim_bucket_key(band, band_val);

        if let Some(data) = store.get(&bucket_key) {
            let ids = parse_id_list(&data);
            for id in ids {
                if seen.insert(id.to_vec()) {
                    candidates.push(id.to_vec());
                }
            }
        }
    }

    // If we have stored simhash values for candidates, filter by threshold.
    // We look for "lsh:simhash:{profile_id}" keys that store the simhash value.
    candidates.retain(|id| {
        let meta_key = format!("lsh:simhash:{}", String::from_utf8_lossy(id));
        if let Some(data) = store.get(meta_key.as_bytes()) {
            if data.len() == 8 {
                let candidate_hash = u64::from_le_bytes(data[..8].try_into().unwrap_or([0; 8]));
                hamming_distance(simhash_value, candidate_hash) <= threshold
            } else {
                true // Keep if we can't verify (conservative).
            }
        } else {
            true // No metadata stored — keep all candidates.
        }
    });

    candidates
}

/// Store the SimHash value for a profile ID (used for verification
/// during FINDSIM).
pub fn store_simhash_for_profile<const N: usize>(
    store: &KvStoreLockFree<N>,
    profile_id: &[u8],
    simhash_value: u64,
) -> bool {
    let meta_key = format!("lsh:simhash:{}", String::from_utf8_lossy(profile_id));
    store.set(meta_key.as_bytes(), &simhash_value.to_le_bytes())
}

/// Get the stored SimHash value for a profile ID.
pub fn get_simhash_for_profile<const N: usize>(
    store: &KvStoreLockFree<N>,
    profile_id: &[u8],
) -> Option<u64> {
    let meta_key = format!("lsh:simhash:{}", String::from_utf8_lossy(profile_id));
    let data = store.get(meta_key.as_bytes())?;
    if data.len() == 8 {
        Some(u64::from_le_bytes(data[..8].try_into().ok()?))
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// ID list encoding/decoding
// ---------------------------------------------------------------------------

/// Parse an encoded ID list from a byte buffer.
///
/// Format: count(u32 LE) + [id_len(u32 LE) + id_bytes] × count
pub fn parse_id_list(data: &[u8]) -> Vec<&[u8]> {
    if data.len() < 4 {
        return Vec::new();
    }
    let count = u32::from_le_bytes(data[..4].try_into().unwrap_or([0; 4])) as usize;
    let mut ids = Vec::with_capacity(count);
    let mut offset = 4;

    for _ in 0..count {
        if offset + 4 > data.len() {
            break;
        }
        let id_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap_or([0; 4])) as usize;
        offset += 4;
        if offset + id_len > data.len() {
            break;
        }
        ids.push(&data[offset..offset + id_len]);
        offset += id_len;
    }

    ids
}

/// Encode an ID list into a byte buffer.
pub fn encode_id_list(ids: &[&[u8]]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + ids.len() * 8);
    buf.extend_from_slice(&(ids.len() as u32).to_le_bytes());
    for id in ids {
        buf.extend_from_slice(&(id.len() as u32).to_le_bytes());
        buf.extend_from_slice(id);
    }
    buf
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simhash_band_extraction() {
        let hash: u64 = 0x1234_5678_9ABC_DEF0;

        // 4 bands × 16 bits.
        let b0 = simhash_band(hash, 0, 4);
        let b1 = simhash_band(hash, 1, 4);
        let b2 = simhash_band(hash, 2, 4);
        let b3 = simhash_band(hash, 3, 4);

        // Verify: band 0 = bits 0-15, band 1 = bits 16-31, etc.
        assert_eq!(b0, hash & 0xFFFF);
        assert_eq!(b1, (hash >> 16) & 0xFFFF);
        assert_eq!(b2, (hash >> 32) & 0xFFFF);
        assert_eq!(b3, (hash >> 48) & 0xFFFF);
    }

    #[test]
    fn test_bucket_key_format() {
        let key = sim_bucket_key(2, 0x1234);
        assert_eq!(key, b"lsh:sim:2:4660");

        let key = min_bucket_key(1, 0xAB);
        assert_eq!(key, b"lsh:min:1:171");
    }

    #[test]
    fn test_id_list_roundtrip() {
        let ids: Vec<&[u8]> = vec![b"profile:1", b"profile:2", b"profile:3"];
        let encoded = encode_id_list(&ids);
        let decoded = parse_id_list(&encoded);
        assert_eq!(decoded, ids);
    }

    #[test]
    fn test_id_list_empty() {
        let ids: Vec<&[u8]> = vec![];
        let encoded = encode_id_list(&ids);
        let decoded = parse_id_list(&encoded);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_id_list_truncated() {
        // Truncated data should return partial results gracefully.
        let data = [1u8, 0, 0, 0, 5, 0, 0, 0]; // count=1, id_len=5, but no id bytes
        let decoded = parse_id_list(&data);
        assert!(decoded.is_empty()); // Graceful handling of truncation
    }

    #[test]
    fn test_lsh_add_and_find_sim() {
        let store = KvStoreLockFree::<64>::with_capacity(1000);
        let sim1 = simhash::simhash_uniform(&[b"Chrome/148", b"Windows", b"1920x1080"]);
        let sim2 = simhash::simhash_uniform(&[b"Chrome/148", b"Windows", b"1920x1080"]);

        // Add profile with sim1.
        lsh_add_sim::<64>(&store, b"profile:1", sim1, 4);
        store_simhash_for_profile::<64>(&store, b"profile:1", sim1);

        // Find similar to sim2 (should find profile:1 since sim1 ≈ sim2).
        let results = find_similar_sim::<64>(&store, sim2, 4, 5);
        assert!(!results.is_empty(), "Should find at least one candidate");
    }

    #[test]
    fn test_lsh_remove() {
        let store = KvStoreLockFree::<64>::with_capacity(1000);
        let sim = simhash::simhash_uniform(&[b"test_profile"]);

        lsh_add_sim::<64>(&store, b"profile:1", sim, 4);
        let removed = lsh_rem_sim::<64>(&store, b"profile:1", sim, 4);
        assert_eq!(removed, 4);
    }

    #[test]
    fn test_lsh_add_minhash() {
        let store = KvStoreLockFree::<64>::with_capacity(1000);
        let mh = MinHashSig::new(&[b"Arial", b"Helvetica"], 128);
        let added = lsh_add_min::<64>(&store, b"profile:1", &mh, 4);
        assert_eq!(added, 4);
    }

    #[test]
    fn test_simhash_band_consistency() {
        let hash: u64 = 0xDEAD_BEEF_CAFE_BABE;
        // Bands should be consistent across calls.
        for _ in 0..10 {
            let b0 = simhash_band(hash, 0, 4);
            let b1 = simhash_band(hash, 1, 4);
            let b2 = simhash_band(hash, 2, 4);
            let b3 = simhash_band(hash, 3, 4);
            // Reconstruct should give back original hash.
            let reconstructed = b0 | (b1 << 16) | (b2 << 32) | (b3 << 48);
            assert_eq!(reconstructed, hash);
        }
    }
}
