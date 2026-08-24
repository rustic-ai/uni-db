// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Hex codec for the guest/host wire boundary.
//!
//! Every loader that hands raw bytes across an untrusted boundary (Extism's
//! JSON host-fn wire, Rhai's script boundary) needs the same lowercase-hex
//! encode/decode pair. They were implemented independently and byte-identically
//! in `uni-plugin-extism` and `uni-plugin-rhai`; they live here so the
//! panic-safety property below is stated and tested once.

use std::fmt::Write as _;

/// Lowercase hex encoding for the guest/host wire boundary.
#[must_use]
pub fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Decode lowercase/uppercase hex; errors on odd length or non-hex digits.
///
/// Operates on raw bytes (`chunks_exact(2)`), NOT `&s[i..i+2]` string slicing.
/// The guest or script controls this string, and byte-index slicing panics on a
/// multibyte UTF-8 codepoint that happens to make the byte length even. A
/// non-ASCII byte simply fails the hex-digit test and returns `Err`, so a
/// hostile input cannot take down the host thread.
///
/// # Errors
///
/// Returns `Err` if `s` has odd byte length or contains a non-hex digit.
pub fn from_hex(s: &str) -> Result<Vec<u8>, String> {
    let bytes = s.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return Err("odd-length hex string".to_owned());
    }
    fn nibble(b: u8) -> Result<u8, String> {
        match b {
            b'0'..=b'9' => Ok(b - b'0'),
            b'a'..=b'f' => Ok(b - b'a' + 10),
            b'A'..=b'F' => Ok(b - b'A' + 10),
            _ => Err("invalid hex digit".to_owned()),
        }
    }
    bytes
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| Ok((nibble(pair[0])? << 4) | nibble(pair[1])?))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_round_trips() {
        let bytes = vec![0x00, 0x0f, 0x10, 0xff, 0xa5];
        let hex = to_hex(&bytes);
        assert_eq!(hex, "000f10ffa5");
        assert_eq!(from_hex(&hex).unwrap(), bytes);
    }

    #[test]
    fn from_hex_accepts_uppercase() {
        assert_eq!(from_hex("A5FF").unwrap(), vec![0xa5, 0xff]);
    }

    #[test]
    fn from_hex_errors_on_odd_length() {
        assert!(from_hex("abc").is_err());
    }

    #[test]
    fn from_hex_errors_on_invalid_digit() {
        assert!(from_hex("zz").is_err());
    }

    /// A multibyte codepoint can make `s.len()` even while `&s[0..2]` is not a
    /// char boundary. Byte-wise decoding must return `Err`, never panic.
    #[test]
    fn from_hex_errors_on_even_byte_multibyte_input() {
        // "é" is 2 bytes in UTF-8, so this string has even byte length.
        let res = from_hex("é");
        assert!(res.is_err(), "multibyte input must error, not panic");
    }
}
