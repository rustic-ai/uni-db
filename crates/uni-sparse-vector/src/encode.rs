use crate::error::SparseError;
use crate::sparse::SparseVector;

/// Canonical binary form of a [`SparseVector`].
///
/// Layout (little-endian, variable length):
/// ```text
/// [count: u32] [indices: count × u32] [values: count × f32]
/// ```
/// Indices and values are stored in separate runs (not interleaved) to match
/// the Arrow `Struct{indices: List<UInt32>, values: List<Float32>}` lowering
/// and to keep each run contiguous for scoring. Weights are lossless `f32`;
/// quantization is applied by the storage engine at the postings boundary, not
/// here.
pub fn encode(sv: &SparseVector) -> Vec<u8> {
    let n = sv.len();
    let mut buf = Vec::with_capacity(4 + n * 8);
    buf.extend_from_slice(&(n as u32).to_le_bytes());
    for &idx in sv.indices() {
        buf.extend_from_slice(&idx.to_le_bytes());
    }
    for &val in sv.values() {
        buf.extend_from_slice(&val.to_le_bytes());
    }
    buf
}

/// Decode a [`SparseVector`] from its canonical binary form, re-validating all
/// invariants (so a corrupted or hand-built buffer cannot smuggle in unsorted
/// indices or NaN weights).
pub fn decode_slice(bytes: &[u8]) -> Result<SparseVector, SparseError> {
    if bytes.len() < 4 {
        return Err(SparseError::Truncated {
            need: 4,
            got: bytes.len(),
        });
    }
    let count = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let need = 4 + count * 8;
    if bytes.len() < need {
        return Err(SparseError::Truncated {
            need,
            got: bytes.len(),
        });
    }
    if bytes.len() > need {
        return Err(SparseError::TrailingBytes {
            trailing: bytes.len() - need,
        });
    }

    // The payload is two contiguous runs of `count` little-endian 4-byte words:
    // the indices first, then the values. `need` was checked above, so each run
    // splits cleanly into 4-byte chunks.
    let (index_bytes, value_bytes) = bytes[4..need].split_at(count * 4);
    let indices = index_bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|c| u32::from_le_bytes(*c))
        .collect();
    let values = value_bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|c| f32::from_le_bytes(*c))
        .collect();

    SparseVector::new(indices, values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_basic() {
        let original = SparseVector::new(vec![1, 7, 42], vec![0.25, -1.5, 3.0]).unwrap();
        let bytes = encode(&original);
        let decoded = decode_slice(&bytes).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn roundtrip_empty() {
        let original = SparseVector::new(vec![], vec![]).unwrap();
        let bytes = encode(&original);
        assert_eq!(bytes.len(), 4);
        assert_eq!(decode_slice(&bytes).unwrap(), original);
    }

    #[test]
    fn truncated_header_rejected() {
        assert!(matches!(
            decode_slice(&[0u8; 3]).unwrap_err(),
            SparseError::Truncated { .. }
        ));
    }

    #[test]
    fn truncated_payload_rejected() {
        let original = SparseVector::new(vec![1, 2], vec![1.0, 2.0]).unwrap();
        let mut bytes = encode(&original);
        bytes.truncate(bytes.len() - 1);
        assert!(matches!(
            decode_slice(&bytes).unwrap_err(),
            SparseError::Truncated { .. }
        ));
    }

    #[test]
    fn trailing_bytes_rejected() {
        let original = SparseVector::new(vec![1], vec![1.0]).unwrap();
        let mut bytes = encode(&original);
        bytes.push(0xFF);
        assert!(matches!(
            decode_slice(&bytes).unwrap_err(),
            SparseError::TrailingBytes { .. }
        ));
    }

    #[test]
    fn decode_revalidates_invariants() {
        // Hand-build a buffer with unsorted indices [5, 1]; decode must reject.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(&5u32.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&1.0f32.to_le_bytes());
        bytes.extend_from_slice(&2.0f32.to_le_bytes());
        assert!(matches!(
            decode_slice(&bytes).unwrap_err(),
            SparseError::UnsortedIndices { .. }
        ));
    }
}
