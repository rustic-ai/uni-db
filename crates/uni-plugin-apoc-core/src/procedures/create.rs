// Rust guideline compliant
//! `apoc.create.*` analogue — synthesis helpers.
//!
//! Mirrors a subset of Neo4j's `apoc.create.*` namespace. The
//! virtual-node procedures (`apoc.create.vNode` / `vRelationship`) need
//! the ephemeral-entity Value-model extension from proposal §4.13.1
//! and are deferred to that milestone. This file ships the trivial
//! synthesizers that don't touch the graph: UUID generation, random
//! integer/float, current timestamp helpers (deferred — depend on
//! `Value::Temporal` plumbing).

use std::sync::{Arc, OnceLock};

use arrow_array::{Array, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use uni_plugin::traits::procedure::{
    NamedArgType, ProcedureContext, ProcedureMode, ProcedurePlugin, ProcedureSignature,
};
use uni_plugin::traits::scalar::ArgType;
use uni_plugin::{FnError, PluginError, PluginRegistrar, QName, SideEffects};

use super::support::{self, ApocProc, MAX_SYNTHESIZED_LEN, batch_err};

/// Register `uni.create.*` procedures into `r`.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a qname is taken.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    support::register_all::<CreateProc>(r)
}

fn uuid_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![],
        yields: vec![Field::new("result", DataType::Utf8, false)],
        mode: ProcedureMode::Read,
        // Side effect: marks this as nondeterministic so the planner
        // doesn't cache call results.
        side_effects: SideEffects::ExternalIo,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

fn uuids_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![NamedArgType {
            name: smol_str::SmolStr::new("count"),
            ty: ArgType::Primitive(DataType::Int64),
            default: None,
            doc: "Number of UUIDs to generate.".to_owned(),
        }],
        yields: vec![Field::new("result", DataType::Utf8, false)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ExternalIo,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

/// All `apoc.create.*` procedures via one discriminant.
#[derive(Debug, Clone, Copy)]
enum CreateProc {
    Uuid,
    Uuids,
}

impl CreateProc {
    /// Canonical docstring per variant. The `register_into` strings
    /// were descriptive; the `OnceLock` ones were just "uuid"/"uuids".
    /// We keep the descriptive form.
    fn docs(&self) -> &'static str {
        match self {
            Self::Uuid => "Generate a fresh random UUIDv4 as a hyphenated string.",
            Self::Uuids => "Generate N fresh random UUIDv4 strings, one per row.",
        }
    }
}

impl ApocProc for CreateProc {
    const ALL: &'static [Self] = &[Self::Uuid, Self::Uuids];

    fn qname(&self) -> QName {
        match self {
            Self::Uuid => QName::new("apoc-core", "create.uuid"),
            Self::Uuids => QName::new("apoc-core", "create.uuids"),
        }
    }

    fn index(&self) -> usize {
        *self as usize
    }

    fn build_signature(&self) -> ProcedureSignature {
        match self {
            Self::Uuid => uuid_sig(self.docs()),
            Self::Uuids => uuids_sig(self.docs()),
        }
    }
}

impl ProcedurePlugin for CreateProc {
    fn signature(&self) -> &ProcedureSignature {
        static CACHE: OnceLock<Vec<ProcedureSignature>> = OnceLock::new();
        support::cached_signature(&CACHE, self)
    }

    fn invoke(
        &self,
        _ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "result",
            DataType::Utf8,
            false,
        )]));

        let values: Vec<String> = match self {
            Self::Uuid => vec![generate_uuid_v4()],
            Self::Uuids => {
                let count = args
                    .first()
                    .and_then(|a| match a {
                        ColumnarValue::Scalar(ScalarValue::Int64(Some(n))) => Some(*n),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        FnError::new(
                            FnError::CODE_TYPE_COERCION,
                            "create.uuids: integer count required",
                        )
                    })?;
                if count < 0 {
                    return Err(FnError::new(
                        FnError::CODE_TYPE_COERCION,
                        "create.uuids: count must be non-negative",
                    ));
                }
                // Cap at a reasonable upper bound to prevent OOM in
                // pathological cases; users wanting larger batches can
                // call repeatedly or open a feature request.
                let capped = (count as usize).min(MAX_SYNTHESIZED_LEN);
                (0..capped).map(|_| generate_uuid_v4()).collect()
            }
        };

        // `create` emits N rows, so it builds its own batch rather than using
        // the single-row `support::one_row_stream`.
        let arr = Arc::new(StringArray::from(values)) as Arc<dyn Array>;
        support::one_row_stream(schema, arr, batch_err::CREATE, "create")
    }
}

/// Generate a UUIDv4 as a hyphenated lowercase string.
///
/// Uses 16 bytes of process-time + thread-id + counter entropy mixed
/// through xorshift. This is **not** cryptographic — adequate for
/// transient identifiers, opaque keys, and de-duplication, but not
/// for security-sensitive uses. A future variant can route through the
/// `Kms` capability for crypto-strength UUIDs.
fn generate_uuid_v4() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    let tid = std::thread::current().id();
    // Use the thread-id's debug representation hash as entropy.
    let tid_hash = {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        format!("{tid:?}").hash(&mut h);
        h.finish()
    };
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut state = nanos ^ tid_hash ^ counter.wrapping_mul(0x9E37_79B9_7F4A_7C15);

    // xorshift64* — generate 16 bytes of pseudo-random output.
    let mut bytes = [0u8; 16];
    for chunk in bytes.as_chunks_mut::<8>().0 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let mixed = state.wrapping_mul(0x2545_F491_4F6C_DD1D);
        chunk.copy_from_slice(&mixed.to_le_bytes());
    }

    // Apply UUIDv4 version/variant bits.
    bytes[6] = (bytes[6] & 0x0F) | 0x40; // version 4
    bytes[8] = (bytes[8] & 0x3F) | 0x80; // variant 10xx

    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn uuid_returns_36_char_hyphenated_string() {
        let mut stream = CreateProc::Uuid
            .invoke(ProcedureContext::default(), &[])
            .unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let uuid = col.value(0);
        assert_eq!(uuid.len(), 36, "UUID should be 36 chars; got {uuid:?}");
        assert_eq!(uuid.chars().nth(14).unwrap(), '4', "UUID v4 marker");
        // Variant nibble at position 19 should be 8, 9, a, or b.
        assert!(
            matches!(uuid.chars().nth(19).unwrap(), '8' | '9' | 'a' | 'b'),
            "UUID v4 variant marker; got {}",
            uuid.chars().nth(19).unwrap()
        );
    }

    #[tokio::test]
    async fn uuids_are_unique_across_calls() {
        async fn make() -> String {
            let mut s = CreateProc::Uuid
                .invoke(ProcedureContext::default(), &[])
                .unwrap();
            let b = s.next().await.unwrap().unwrap();
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .to_owned()
        }
        let a = make().await;
        let b = make().await;
        assert_ne!(a, b);
    }
}
