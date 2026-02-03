// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::{Result, anyhow};
use multibase::Base;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Internal Vertex ID (64 bits) - pure auto-increment
///
/// VIDs are dense, sequential identifiers assigned on vertex creation.
/// Unlike the previous design, VIDs no longer embed label information.
/// Label lookups are done via the VidLabelsIndex.
///
/// For O(1) array indexing during query execution, use DenseIdx via VidRemapper.
#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Vid(u64);

impl Vid {
    /// Creates a new vertex ID from a raw u64 value.
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the raw u64 value of this VID.
    pub fn as_u64(&self) -> u64 {
        self.0
    }

    /// Sentinel value representing an invalid/null VID.
    pub const INVALID: Vid = Vid(u64::MAX);

    /// Check if this VID is the invalid sentinel.
    pub fn is_invalid(&self) -> bool {
        self.0 == u64::MAX
    }
}

impl From<u64> for Vid {
    fn from(val: u64) -> Self {
        Self(val)
    }
}

impl From<Vid> for u64 {
    fn from(vid: Vid) -> Self {
        vid.0
    }
}

impl Default for Vid {
    fn default() -> Self {
        Self::INVALID
    }
}

impl fmt::Debug for Vid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_invalid() {
            write!(f, "Vid(INVALID)")
        } else {
            write!(f, "Vid({})", self.0)
        }
    }
}

impl fmt::Display for Vid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for Vid {
    type Err = anyhow::Error;

    /// Parses a Vid from a numeric string.
    fn from_str(s: &str) -> Result<Self> {
        let id: u64 = s
            .parse()
            .map_err(|e| anyhow!("Invalid Vid '{}': {}", s, e))?;
        Ok(Self::new(id))
    }
}

/// Internal Edge ID (64 bits) - pure auto-increment
///
/// EIDs are dense, sequential identifiers assigned on edge creation.
/// Unlike the previous design, EIDs no longer embed type information.
/// Edge type lookups are done via the edge tables.
#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Eid(u64);

impl Eid {
    /// Creates a new edge ID from a raw u64 value.
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the raw u64 value of this EID.
    pub fn as_u64(&self) -> u64 {
        self.0
    }

    /// Sentinel value representing an invalid/null EID.
    pub const INVALID: Eid = Eid(u64::MAX);

    /// Check if this EID is the invalid sentinel.
    pub fn is_invalid(&self) -> bool {
        self.0 == u64::MAX
    }
}

impl From<u64> for Eid {
    fn from(val: u64) -> Self {
        Self(val)
    }
}

impl From<Eid> for u64 {
    fn from(eid: Eid) -> Self {
        eid.0
    }
}

impl Default for Eid {
    fn default() -> Self {
        Self::INVALID
    }
}

impl fmt::Debug for Eid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_invalid() {
            write!(f, "Eid(INVALID)")
        } else {
            write!(f, "Eid({})", self.0)
        }
    }
}

impl fmt::Display for Eid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for Eid {
    type Err = anyhow::Error;

    /// Parses an Eid from a numeric string.
    fn from_str(s: &str) -> Result<Self> {
        let id: u64 = s
            .parse()
            .map_err(|e| anyhow!("Invalid Eid '{}': {}", s, e))?;
        Ok(Self::new(id))
    }
}

/// Dense index for O(1) array access during query execution.
///
/// During query execution, we load subgraphs into memory with dense arrays.
/// DenseIdx provides efficient indexing into these arrays, while VidRemapper
/// handles the bidirectional mapping between sparse VIDs and dense indices.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct DenseIdx(pub u32);

impl DenseIdx {
    /// Creates a new dense index.
    pub fn new(idx: u32) -> Self {
        Self(idx)
    }

    /// Returns the index as usize for array indexing.
    pub fn as_usize(&self) -> usize {
        self.0 as usize
    }

    /// Returns the raw u32 value.
    pub fn as_u32(&self) -> u32 {
        self.0
    }

    /// Sentinel value for invalid index.
    pub const INVALID: DenseIdx = DenseIdx(u32::MAX);

    /// Check if this is the invalid sentinel.
    pub fn is_invalid(&self) -> bool {
        self.0 == u32::MAX
    }
}

impl From<u32> for DenseIdx {
    fn from(val: u32) -> Self {
        Self(val)
    }
}

impl From<usize> for DenseIdx {
    fn from(val: usize) -> Self {
        Self(val as u32)
    }
}

impl fmt::Display for DenseIdx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// UniId: 44-character base32 multibase string (SHA3-256)
#[derive(Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct UniId([u8; 32]);

impl UniId {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Parses a UniId from a multibase-encoded string.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The string is not valid multibase
    /// - The encoding is not Base32Lower (the canonical format for UniId)
    /// - The decoded length is not exactly 32 bytes
    ///
    /// # Security
    ///
    /// **CWE-345 (Insufficient Verification)**: Validates that the input uses
    /// the expected Base32Lower encoding, rejecting other multibase formats
    /// that could cause interoperability issues or confusion.
    pub fn from_multibase(s: &str) -> Result<Self> {
        let (base, bytes) =
            multibase::decode(s).map_err(|e| anyhow!("Multibase decode error: {}", e))?;

        // Validate encoding matches our canonical format
        if base != Base::Base32Lower {
            return Err(anyhow!(
                "UniId must use Base32Lower encoding, got {:?}",
                base
            ));
        }

        let inner: [u8; 32] = bytes.try_into().map_err(|v: Vec<u8>| {
            anyhow!("Invalid UniId length: expected 32 bytes, got {}", v.len())
        })?;

        Ok(Self(inner))
    }

    pub fn to_multibase(&self) -> String {
        multibase::encode(Base::Base32Lower, self.0)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for UniId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UniId({})", self.to_multibase())
    }
}

impl fmt::Display for UniId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_multibase())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vid_basic() {
        let vid = Vid::new(12345);
        assert_eq!(vid.as_u64(), 12345);
        assert!(!vid.is_invalid());
    }

    #[test]
    fn test_vid_invalid() {
        let vid = Vid::INVALID;
        assert!(vid.is_invalid());
        assert_eq!(vid.as_u64(), u64::MAX);
    }

    #[test]
    fn test_vid_from_str() {
        let vid: Vid = "42".parse().unwrap();
        assert_eq!(vid.as_u64(), 42);

        // Round-trip through Display and FromStr
        let original = Vid::new(12345678);
        let s = original.to_string();
        let parsed: Vid = s.parse().unwrap();
        assert_eq!(original, parsed);

        // Error cases
        assert!("invalid".parse::<Vid>().is_err());
        assert!("".parse::<Vid>().is_err());
    }

    #[test]
    fn test_eid_basic() {
        let eid = Eid::new(67890);
        assert_eq!(eid.as_u64(), 67890);
        assert!(!eid.is_invalid());
    }

    #[test]
    fn test_eid_invalid() {
        let eid = Eid::INVALID;
        assert!(eid.is_invalid());
        assert_eq!(eid.as_u64(), u64::MAX);
    }

    #[test]
    fn test_eid_from_str() {
        let eid: Eid = "100".parse().unwrap();
        assert_eq!(eid.as_u64(), 100);

        // Round-trip through Display and FromStr
        let original = Eid::new(0xABCDEF);
        let s = original.to_string();
        let parsed: Eid = s.parse().unwrap();
        assert_eq!(original, parsed);

        // Error cases
        assert!("invalid".parse::<Eid>().is_err());
    }

    #[test]
    fn test_dense_idx() {
        let idx = DenseIdx::new(100);
        assert_eq!(idx.as_usize(), 100);
        assert_eq!(idx.as_u32(), 100);
        assert!(!idx.is_invalid());

        let invalid = DenseIdx::INVALID;
        assert!(invalid.is_invalid());
    }

    #[test]
    fn test_uni_id_multibase() {
        let bytes = [0u8; 32];
        let uid = UniId(bytes);
        let s = uid.to_multibase();
        let decoded = UniId::from_multibase(&s).unwrap();
        assert_eq!(uid, decoded);
    }

    /// Security tests for CWE-345 (Insufficient Verification).
    mod security_tests {
        use super::*;

        /// CWE-345: UniId should reject non-Base32Lower encodings.
        #[test]
        fn test_uni_id_rejects_wrong_encoding() {
            // Create a Base58Btc encoded string (different from our Base32Lower)
            let bytes = [0u8; 32];
            let base58_encoded = multibase::encode(multibase::Base::Base58Btc, bytes);

            let result = UniId::from_multibase(&base58_encoded);
            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("Base32Lower encoding")
            );
        }

        /// CWE-345: UniId should reject wrong length.
        #[test]
        fn test_uni_id_rejects_wrong_length() {
            // Encode only 16 bytes instead of 32
            let short_bytes = [0u8; 16];
            let encoded = multibase::encode(Base::Base32Lower, short_bytes);

            let result = UniId::from_multibase(&encoded);
            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("expected 32 bytes")
            );
        }
    }
}
