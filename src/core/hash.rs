//! Hash data type operations.
//!
//! Hash fields are stored encoded in the value of a regular key using a
//! compact binary format. A value starting with byte `0xFF` is treated as
//! a hash; everything else is a plain string.
//!
//! ## Encoding format
//!
//! ```text
//! 0xFF <num_fields: u16 LE> [<field_len: u16 LE><field><val_len: u16 LE><value>]*
//! ```
//!
//! This design means:
//! - No new data structures needed — hash data lives in the existing inline value.
//! - All existing operations (GET, SET, DEL, TTL, WAL) work unchanged at the
//!   storage layer.
//! - Hash operations only need to encode / decode the value format.
//! - Fields are packed contiguously for cache efficiency.
//!
//! ## Size constraints
//!
//! The total encoded value must fit within the KV store's `INLINE_SIZE` (64 bytes).
//! Individual limits:
//! - Max field name: 32 bytes
//! - Max field value: 28 bytes

/// Magic byte prefix that identifies a hash-type value.
pub const HASH_MAGIC: u8 = 0xFF;

/// Maximum encoded hash size (must fit in the KV store's inline value).
pub const MAX_ENCODED_SIZE: usize = 64;

/// Maximum length of a single field name.
pub const MAX_FIELD_NAME: usize = 32;

/// Maximum length of a single field value.
pub const MAX_FIELD_VALUE: usize = 28;

/// WRONGTYPE error message (matches Redis exactly).
pub const WRONGTYPE_ERR: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

/// Errors returned by hash operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HashError {
    /// The encoded value would exceed `MAX_ENCODED_SIZE`.
    ValueTooLong,
    /// A field name exceeds `MAX_FIELD_NAME`.
    FieldTooLong,
    /// Adding a field would exceed `MAX_ENCODED_SIZE` (too many fields / data).
    TooManyFields,
    /// The operation was attempted on a non-hash (string) value.
    NotAHash,
}

impl std::fmt::Display for HashError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HashError::ValueTooLong => write!(f, "hash value too long"),
            HashError::FieldTooLong => write!(f, "hash field name too long"),
            HashError::TooManyFields => write!(f, "hash has too many fields or data exceeds limit"),
            HashError::NotAHash => write!(f, "value is not a hash"),
        }
    }
}

impl std::error::Error for HashError {}

// ---------------------------------------------------------------------------
// Encoding / Decoding
// ---------------------------------------------------------------------------

/// Encode a hash (field-value pairs) into a compact byte vector.
///
/// Format: `0xFF <num_fields: u16 LE> [<field_len: u16 LE><field><val_len: u16 LE><value>]*`
///
/// Returns an error if any field name exceeds [`MAX_FIELD_NAME`], any value
/// exceeds [`MAX_FIELD_VALUE`], or the total encoded size exceeds
/// [`MAX_ENCODED_SIZE`].
pub fn encode_hash(fields: &[(&[u8], &[u8])]) -> Result<Vec<u8>, HashError> {
    // Header: 1 byte magic + 2 bytes num_fields
    let mut total_size: usize = 3;

    for (field, value) in fields {
        if field.len() > MAX_FIELD_NAME {
            return Err(HashError::FieldTooLong);
        }
        if value.len() > MAX_FIELD_VALUE {
            return Err(HashError::ValueTooLong);
        }
        // Per field: 2 (field_len) + field_data + 2 (val_len) + val_data
        total_size += 2 + field.len() + 2 + value.len();
    }

    if total_size > MAX_ENCODED_SIZE {
        return Err(HashError::TooManyFields);
    }

    let mut buf = Vec::with_capacity(total_size);
    buf.push(HASH_MAGIC);
    buf.extend_from_slice(&(fields.len() as u16).to_le_bytes());

    for (field, value) in fields {
        buf.extend_from_slice(&(field.len() as u16).to_le_bytes());
        buf.extend_from_slice(field);
        buf.extend_from_slice(&(value.len() as u16).to_le_bytes());
        buf.extend_from_slice(value);
    }

    Ok(buf)
}

/// Decode a hash from its binary representation.
///
/// Returns `None` if the data is empty, does not start with `0xFF`, or is
/// malformed (truncated fields, etc.).
pub fn decode_hash(data: &[u8]) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
    if data.is_empty() || data[0] != HASH_MAGIC {
        return None;
    }
    if data.len() < 3 {
        return None;
    }

    let num_fields = u16::from_le_bytes([data[1], data[2]]) as usize;
    let mut pos = 3;
    let mut fields = Vec::with_capacity(num_fields);

    for _ in 0..num_fields {
        // Read field length.
        if pos + 2 > data.len() {
            return None;
        }
        let field_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;

        // Read field data.
        if pos + field_len > data.len() {
            return None;
        }
        let field = data[pos..pos + field_len].to_vec();
        pos += field_len;

        // Read value length.
        if pos + 2 > data.len() {
            return None;
        }
        let val_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;

        // Read value data.
        if pos + val_len > data.len() {
            return None;
        }
        let value = data[pos..pos + val_len].to_vec();
        pos += val_len;

        fields.push((field, value));
    }

    Some(fields)
}

// ---------------------------------------------------------------------------
// Query operations
// ---------------------------------------------------------------------------

/// Find a field's value in encoded hash data.
///
/// Returns `None` if the data is not a valid hash or the field does not exist.
pub fn hash_get(data: &[u8], field: &[u8]) -> Option<Vec<u8>> {
    let fields = decode_hash(data)?;
    for (f, v) in &fields {
        if f.as_slice() == field {
            return Some(v.clone());
        }
    }
    None
}

/// Check if *field* exists in the encoded hash *data*.
///
/// Returns `false` if the data is not a valid hash or the field is absent.
pub fn hash_exists(data: &[u8], field: &[u8]) -> bool {
    let fields = match decode_hash(data) {
        Some(f) => f,
        None => return false,
    };
    fields.iter().any(|(f, _)| f.as_slice() == field)
}

/// Return the number of fields in the encoded hash.
///
/// Returns `0` if the data is not a valid hash.
pub fn hash_len(data: &[u8]) -> usize {
    decode_hash(data).map_or(0, |f| f.len())
}

/// Return all field names in the encoded hash.
pub fn hash_keys(data: &[u8]) -> Vec<Vec<u8>> {
    match decode_hash(data) {
        Some(fields) => fields.into_iter().map(|(f, _)| f).collect(),
        None => Vec::new(),
    }
}

/// Return all values in the encoded hash.
pub fn hash_values(data: &[u8]) -> Vec<Vec<u8>> {
    match decode_hash(data) {
        Some(fields) => fields.into_iter().map(|(_, v)| v).collect(),
        None => Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Mutation operations
// ---------------------------------------------------------------------------

/// Set a field in encoded hash data, returning the new encoded data.
///
/// If *data* is empty the hash is created from scratch. If *data* starts with
/// the [`HASH_MAGIC`] the field is inserted or updated. If *data* is non-empty
/// but does not start with `HASH_MAGIC`, [`HashError::NotAHash`] is returned.
pub fn hash_set(
    data: &[u8],
    field: &[u8],
    value: &[u8],
) -> Result<Vec<u8>, HashError> {
    if field.len() > MAX_FIELD_NAME {
        return Err(HashError::FieldTooLong);
    }
    if value.len() > MAX_FIELD_VALUE {
        return Err(HashError::ValueTooLong);
    }

    let mut fields: Vec<(Vec<u8>, Vec<u8>)>;

    if data.is_empty() {
        fields = Vec::new();
    } else if data[0] == HASH_MAGIC {
        fields = decode_hash(data).unwrap_or_default();
    } else {
        return Err(HashError::NotAHash);
    }

    // Update existing field or insert a new one.
    let mut found = false;
    for (f, v) in &mut fields {
        if f.as_slice() == field {
            *v = value.to_vec();
            found = true;
            break;
        }
    }
    if !found {
        fields.push((field.to_vec(), value.to_vec()));
    }

    let refs: Vec<(&[u8], &[u8])> = fields
        .iter()
        .map(|(f, v)| (f.as_slice(), v.as_slice()))
        .collect();
    encode_hash(&refs)
}

/// Delete a field from encoded hash data.
///
/// Returns a [`HashDelResult`] indicating what happened:
///
/// | Variant | Meaning |
/// |---------|---------|
/// | `FieldNotFound` | The field does not exist in the hash (or data is not a hash). |
/// | `Updated(data)` | The field was removed; `data` is the remaining encoded hash. |
/// | `HashEmpty` | The field was the last one; the key should be deleted from the store. |
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HashDelResult {
    /// The field was not found in the hash.
    FieldNotFound,
    /// The field was removed. Contains the new encoded hash data.
    Updated(Vec<u8>),
    /// The field was the last one; the hash is now empty and the key
    /// should be deleted from the store.
    HashEmpty,
}

/// Delete a field from encoded hash data.
///
/// See [`HashDelResult`] for the return value semantics.
pub fn hash_del(data: &[u8], field: &[u8]) -> HashDelResult {
    let mut fields = match decode_hash(data) {
        Some(f) => f,
        None => return HashDelResult::FieldNotFound,
    };

    let idx = match fields.iter().position(|(f, _)| f.as_slice() == field) {
        Some(i) => i,
        None => return HashDelResult::FieldNotFound,
    };

    fields.remove(idx);

    if fields.is_empty() {
        HashDelResult::HashEmpty
    } else {
        let refs: Vec<(&[u8], &[u8])> = fields
            .iter()
            .map(|(f, v)| (f.as_slice(), v.as_slice()))
            .collect();
        match encode_hash(&refs) {
            Ok(encoded) => HashDelResult::Updated(encoded),
            Err(_) => HashDelResult::FieldNotFound, // Should not happen on removal.
        }
    }
}

/// Check whether a raw value (as stored in the KV store) is a hash type.
#[inline]
pub fn is_hash_value(data: &[u8]) -> bool {
    !data.is_empty() && data[0] == HASH_MAGIC
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ----- Encode / Decode round-trip -----

    #[test]
    fn test_encode_decode_roundtrip() {
        let pairs: Vec<(&[u8], &[u8])> = vec![
            (b"name", b"Alice"),
            (b"age", b"30"),
        ];
        let encoded = encode_hash(&pairs).unwrap();
        assert_eq!(encoded[0], HASH_MAGIC);

        let decoded = decode_hash(&encoded).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0], (b"name".to_vec(), b"Alice".to_vec()));
        assert_eq!(decoded[1], (b"age".to_vec(), b"30".to_vec()));
    }

    #[test]
    fn test_encode_empty_hash() {
        let pairs: Vec<(&[u8], &[u8])> = vec![];
        let encoded = encode_hash(&pairs).unwrap();
        assert_eq!(encoded, vec![0xFF, 0x00, 0x00]);
        let decoded = decode_hash(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    // ----- hash_set -----

    #[test]
    fn test_hash_set_new_field() {
        // Setting on empty data creates a new hash.
        let result = hash_set(b"", b"color", b"red").unwrap();
        assert_eq!(result[0], HASH_MAGIC);
        assert_eq!(hash_get(&result, b"color"), Some(b"red".to_vec()));
        assert_eq!(hash_len(&result), 1);
    }

    #[test]
    fn test_hash_set_existing_field() {
        // Create a hash, then update an existing field.
        let data = hash_set(b"", b"color", b"red").unwrap();
        let updated = hash_set(&data, b"color", b"blue").unwrap();
        assert_eq!(hash_get(&updated, b"color"), Some(b"blue".to_vec()));
        assert_eq!(hash_len(&updated), 1);
    }

    #[test]
    fn test_hash_set_not_a_hash() {
        // Trying to hash_set on a plain string returns NotAHash.
        let err = hash_set(b"plain_string", b"f", b"v").unwrap_err();
        assert_eq!(err, HashError::NotAHash);
    }

    #[test]
    fn test_hash_set_multiple_fields() {
        let data = hash_set(b"", b"a", b"1").unwrap();
        let data = hash_set(&data, b"b", b"2").unwrap();
        let data = hash_set(&data, b"c", b"3").unwrap();
        assert_eq!(hash_len(&data), 3);
        assert_eq!(hash_get(&data, b"a"), Some(b"1".to_vec()));
        assert_eq!(hash_get(&data, b"b"), Some(b"2".to_vec()));
        assert_eq!(hash_get(&data, b"c"), Some(b"3".to_vec()));
    }

    // ----- hash_del -----

    #[test]
    fn test_hash_del_field() {
        let data = hash_set(b"", b"a", b"1").unwrap();
        let data = hash_set(&data, b"b", b"2").unwrap();
        let result = hash_del(&data, b"a");
        assert_eq!(result, HashDelResult::Updated(hash_set(b"", b"b", b"2").unwrap()));
        assert_eq!(hash_get(&hash_set(b"", b"b", b"2").unwrap(), b"a"), None);
    }

    #[test]
    fn test_hash_del_last_field() {
        // Deleting the only field should signal HashEmpty.
        let data = hash_set(b"", b"only", b"val").unwrap();
        let result = hash_del(&data, b"only");
        assert_eq!(result, HashDelResult::HashEmpty);
    }

    #[test]
    fn test_hash_del_missing_field() {
        let data = hash_set(b"", b"a", b"1").unwrap();
        let result = hash_del(&data, b"missing");
        assert_eq!(result, HashDelResult::FieldNotFound);
    }

    // ----- hash_get -----

    #[test]
    fn test_hash_get_missing() {
        let data = hash_set(b"", b"a", b"1").unwrap();
        assert_eq!(hash_get(&data, b"nonexistent"), None);
    }

    #[test]
    fn test_hash_get_non_hash_data() {
        assert_eq!(hash_get(b"plain", b"field"), None);
    }

    // ----- hash_exists / hash_len / hash_keys / hash_values -----

    #[test]
    fn test_hash_exists() {
        let data = hash_set(b"", b"key1", b"val1").unwrap();
        assert!(hash_exists(&data, b"key1"));
        assert!(!hash_exists(&data, b"missing"));
        assert!(!hash_exists(b"", b"anything"));
    }

    #[test]
    fn test_hash_len() {
        let data = hash_set(b"", b"a", b"1").unwrap();
        assert_eq!(hash_len(&data), 1);
        let data = hash_set(&data, b"b", b"2").unwrap();
        assert_eq!(hash_len(&data), 2);
        assert_eq!(hash_len(b""), 0);
        assert_eq!(hash_len(b"not_a_hash"), 0);
    }

    #[test]
    fn test_hash_keys_values() {
        let data = hash_set(b"", b"name", b"Alice").unwrap();
        let data = hash_set(&data, b"age", b"30").unwrap();
        let keys = hash_keys(&data);
        let vals = hash_values(&data);
        assert_eq!(keys.len(), 2);
        assert_eq!(vals.len(), 2);
        assert!(keys.contains(&b"name".to_vec()));
        assert!(keys.contains(&b"age".to_vec()));
        assert!(vals.contains(&b"Alice".to_vec()));
        assert!(vals.contains(&b"30".to_vec()));
    }

    // ----- Size limits -----

    #[test]
    fn test_hash_max_size() {
        // A field name of 33 bytes should be rejected.
        let long_field = vec![b'x'; 33];
        let err = hash_set(b"", &long_field, b"v").unwrap_err();
        assert_eq!(err, HashError::FieldTooLong);

        // A field value of 29 bytes should be rejected.
        let long_val = vec![b'y'; 29];
        let err = hash_set(b"", b"f", &long_val).unwrap_err();
        assert_eq!(err, HashError::ValueTooLong);

        // Filling up to the 64-byte limit should succeed for small fields.
        let mut data = Vec::new();
        for i in 0u8..7 {
            // Each pair: 2+1 + 2+1 = 6 bytes, total ~3 + 6*7 = 45 bytes → fits
            let field = [b'k', i];
            let value = [b'v', i];
            data = hash_set(&data, &field, &value).unwrap();
        }
        assert_eq!(hash_len(&data), 7);
        assert!(data.len() <= MAX_ENCODED_SIZE);
    }

    // ----- Edge cases -----

    #[test]
    fn test_non_hash_data_returns_none() {
        assert_eq!(decode_hash(b""), None);
        assert_eq!(decode_hash(b"hello"), None);
        assert_eq!(decode_hash(&[0xFE, 0x01, 0x00]), None); // wrong magic
    }

    #[test]
    fn test_empty_hash() {
        let encoded = encode_hash(&[]).unwrap();
        assert_eq!(encoded.len(), 3); // magic + num_fields
        let decoded = decode_hash(&encoded).unwrap();
        assert!(decoded.is_empty());
        assert_eq!(hash_len(&encoded), 0);
        assert_eq!(hash_get(&encoded, b"any"), None);
        assert!(!hash_exists(&encoded, b"any"));
    }

    #[test]
    fn test_is_hash_value() {
        assert!(is_hash_value(&[0xFF, 0x00, 0x00]));
        assert!(is_hash_value(&[0xFF, 0x01, 0x00, 0x01, 0x00, b'a', 0x01, 0x00, b'b']));
        assert!(!is_hash_value(b""));
        assert!(!is_hash_value(b"hello"));
        assert!(!is_hash_value(&[0x00]));
    }

    #[test]
    fn test_hash_error_display() {
        assert!(HashError::ValueTooLong.to_string().contains("too long"));
        assert!(HashError::FieldTooLong.to_string().contains("field"));
        assert!(HashError::TooManyFields.to_string().contains("too many"));
        assert!(HashError::NotAHash.to_string().contains("not a hash"));
    }

    #[test]
    fn test_truncated_hash_data() {
        // Only magic + one byte of num_fields → should fail to decode.
        assert_eq!(decode_hash(&[0xFF, 0x01]), None);
        // Magic + num_fields but missing field data.
        assert_eq!(decode_hash(&[0xFF, 0x01, 0x00, 0x02]), None);
    }
}
