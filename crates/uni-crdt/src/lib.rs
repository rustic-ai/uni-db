// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod gcounter;
pub mod gset;
pub mod lww_map;
pub mod lww_register;
pub mod orset;
pub mod rga;
pub mod vc_register;
pub mod vector_clock;

pub use gcounter::GCounter;
pub use gset::GSet;
pub use lww_map::LWWMap;
pub use lww_register::LWWRegister;
pub use orset::ORSet;
pub use rga::Rga;
pub use vc_register::VCRegister;
pub use vector_clock::VectorClock;

#[derive(Error, Debug)]
pub enum CrdtError {
    #[error("Type mismatch: cannot merge {0} with {1}")]
    TypeMismatch(String, String),
    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Trait for state-based CRDTs (CvRDTs)
pub trait CrdtMerge {
    /// Merge another instance into self.
    /// Must satisfy: commutativity, associativity, idempotency.
    fn merge(&mut self, other: &Self);
}

/// Dynamic CRDT wrapper for storage and query layers.
/// Using MessagePack for binary serialization in the storage layer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "t", content = "d")]
pub enum Crdt {
    #[serde(rename = "gc")]
    GCounter(GCounter),
    #[serde(rename = "gs")]
    GSet(GSet<String>),
    #[serde(rename = "os")]
    ORSet(ORSet<String>),
    #[serde(rename = "lr")]
    LWWRegister(LWWRegister<serde_json::Value>),
    #[serde(rename = "lm")]
    LWWMap(LWWMap<String, serde_json::Value>),
    #[serde(rename = "rg")]
    Rga(Rga<String>),
    #[serde(rename = "vc")]
    VectorClock(VectorClock),
    #[serde(rename = "vr")]
    VCRegister(VCRegister<serde_json::Value>),
}

impl Crdt {
    /// Try to merge another CRDT into this one.
    /// Returns an error if the types don't match.
    /// This is the safe, non-panicking version of merge.
    pub fn try_merge(&mut self, other: &Self) -> Result<(), CrdtError> {
        match (self, other) {
            (Crdt::GCounter(a), Crdt::GCounter(b)) => {
                a.merge(b);
                Ok(())
            }
            (Crdt::GSet(a), Crdt::GSet(b)) => {
                a.merge(b);
                Ok(())
            }
            (Crdt::ORSet(a), Crdt::ORSet(b)) => {
                a.merge(b);
                Ok(())
            }
            (Crdt::LWWRegister(a), Crdt::LWWRegister(b)) => {
                a.merge(b);
                Ok(())
            }
            (Crdt::LWWMap(a), Crdt::LWWMap(b)) => {
                a.merge(b);
                Ok(())
            }
            (Crdt::Rga(a), Crdt::Rga(b)) => {
                a.merge(b);
                Ok(())
            }
            (Crdt::VectorClock(a), Crdt::VectorClock(b)) => {
                a.merge(b);
                Ok(())
            }
            (Crdt::VCRegister(a), Crdt::VCRegister(b)) => {
                a.merge(b);
                Ok(())
            }
            (a, b) => Err(CrdtError::TypeMismatch(
                format!("{:?}", std::mem::discriminant(a)),
                format!("{:?}", std::mem::discriminant(b)),
            )),
        }
    }

    /// Returns the type name of this CRDT variant for error messages.
    pub fn type_name(&self) -> &'static str {
        match self {
            Crdt::GCounter(_) => "GCounter",
            Crdt::GSet(_) => "GSet",
            Crdt::ORSet(_) => "ORSet",
            Crdt::LWWRegister(_) => "LWWRegister",
            Crdt::LWWMap(_) => "LWWMap",
            Crdt::Rga(_) => "Rga",
            Crdt::VectorClock(_) => "VectorClock",
            Crdt::VCRegister(_) => "VCRegister",
        }
    }
}

impl CrdtMerge for Crdt {
    /// Merge another CRDT into this one.
    /// Panics if the types don't match. For a non-panicking version, use `try_merge`.
    fn merge(&mut self, other: &Self) {
        if let Err(e) = self.try_merge(other) {
            panic!(
                "Cannot merge different CRDT types: {} and {} ({e})",
                self.type_name(),
                other.type_name()
            );
        }
    }
}

impl Crdt {
    /// Serialize the CRDT to MessagePack bytes.
    pub fn to_msgpack(&self) -> Result<Vec<u8>, CrdtError> {
        rmp_serde::to_vec_named(self).map_err(|e| CrdtError::Serialization(e.to_string()))
    }

    /// Deserialize a CRDT from MessagePack bytes.
    pub fn from_msgpack(bytes: &[u8]) -> Result<Self, CrdtError> {
        rmp_serde::from_slice(bytes).map_err(|e| CrdtError::Serialization(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crdt_serialization() {
        let mut gc = GCounter::new();
        gc.increment("actor1", 42);
        let crdt = Crdt::GCounter(gc);

        let bytes = crdt.to_msgpack().unwrap();
        let decoded = Crdt::from_msgpack(&bytes).unwrap();

        assert_eq!(crdt, decoded);
    }
}
