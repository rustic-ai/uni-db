//! Compatibility facade; all implementation comes from the patched release.
#![no_std]
pub use patched::*;

#[cfg(test)]
extern crate std;

// Regression cases adapted from upstream zstd-rs; see ../LICENSE.
#[cfg(test)]
mod tests {
    use super::*;
    use std::vec::Vec;

    #[test]
    fn corrupt_legacy_frame_does_not_expose_uninitialized_output() {
        let corrupt = [0x27, 0xb5, 0x2f, 0xfd, 0x08, 0x00];
        let mut context = DCtx::create();
        let mut buffer = Vec::with_capacity(1024);
        let mut input = InBuffer::around(&corrupt);
        let mut output = OutBuffer::around(&mut buffer);
        assert!(context.decompress_stream(&mut output, &mut input).is_err());
        assert_eq!(output.pos(), 0);
        assert_eq!(input.pos(), 0);
        assert!(buffer.is_empty());
    }

    #[test]
    fn seekable_offsets_past_end_leave_vector_empty() {
        let data = b"graph properties and identifiers";
        let mut storage = [0; 512];
        let mut encoder = seekable::SeekableCStream::create();
        encoder.init(3, true, 64).unwrap();
        let mut input = InBuffer::around(data);
        let mut output = OutBuffer::around(&mut storage[..]);
        while input.pos() < data.len() {
            encoder.compress_stream(&mut output, &mut input).unwrap();
        }
        while encoder.end_stream(&mut output).unwrap() != 0 {}
        let mut decoder = seekable::Seekable::create();
        decoder.init_buff(output.as_slice()).unwrap();
        let mut decoded = Vec::with_capacity(128);
        assert_eq!(decoder.decompress(&mut decoded, 0).unwrap(), data.len());
        assert_eq!(decoded, data);
        for offset in [data.len() as u64, data.len() as u64 + 1, u64::MAX] {
            decoded.clear();
            assert_eq!(decoder.decompress(&mut decoded, offset), Ok(0));
            assert!(decoded.is_empty());
            assert!(decoded.len() <= decoded.capacity());
        }
    }
}
