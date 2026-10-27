//! Compatibility facade; all implementation comes from the patched release.
pub use patched::*;

// Regression cases adapted from upstream zstd-rs; see ../LICENSE.
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    #[test]
    fn reused_encoder_does_not_leak_abandoned_input() {
        let mut context = zstd_safe::CCtx::create();
        stream::write::Encoder::with_context(Vec::new(), &mut context)
            .write_all(b"private abandoned stream")
            .unwrap();
        let mut encoder = stream::write::Encoder::with_context(Vec::new(), &mut context);
        encoder.write_all(b"public next stream").unwrap();
        let output = encoder.finish().unwrap();
        assert_eq!(decode_all(&output[..]).unwrap(), b"public next stream");
    }

    #[test]
    fn reused_decoder_discards_previous_output() {
        let first = encode_all(&b"private abandoned stream"[..], 1).unwrap();
        let second = encode_all(&b"public next stream"[..], 1).unwrap();
        let mut context = zstd_safe::DCtx::create();
        stream::read::Decoder::with_context(&first[..], &mut context)
            .read_exact(&mut [0; 5])
            .unwrap();
        let mut output = Vec::new();
        stream::read::Decoder::with_context(&second[..], &mut context)
            .read_to_end(&mut output)
            .unwrap();
        assert_eq!(output, b"public next stream");
    }

    #[test]
    fn bulk_and_dictionary_round_trips() {
        let dictionary = b"shared dictionary for graph properties and identifiers";
        let data = b"graph properties and identifiers: repeated graph properties";
        let mut compressor = bulk::Compressor::with_dictionary(3, dictionary).unwrap();
        let compressed = compressor.compress(data).unwrap();
        let mut decompressor = bulk::Decompressor::with_dictionary(dictionary).unwrap();
        assert_eq!(
            decompressor.decompress(&compressed, data.len()).unwrap(),
            data
        );
        let compressed = bulk::compress(data, 3).unwrap();
        assert_eq!(bulk::decompress(&compressed, data.len()).unwrap(), data);
    }
}
