//! Plugin manifest — TOML and JSON (de)serialization.
//!
//! The manifest is the *typed contract* between a plugin and the host. It
//! declares the plugin's identity, the ABI range it targets, capabilities it
//! requests, declarative dependencies on other plugins, the determinism /
//! side-effect / scope characterizations, and a summary of what surfaces it
//! plans to register.
//!
//! Manifests are persisted in TOML for human authoring and on-the-wire JSON
//! for programmatic exchange (WASM plugins return JSON from their
//! `manifest-json` export). Both forms round-trip through `serde`.

use std::collections::BTreeMap;

use semver::{Version, VersionReq};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::capability::{CapabilitySet, Determinism, Scope, SideEffects};
use crate::errors::PluginError;
use crate::plugin::PluginId;

/// A semver range expressing which ABI majors this plugin supports.
///
/// Stored as the original requirement string so manifests round-trip
/// losslessly through serialization. Use [`AbiRange::matches`] to test
/// against a host major.
///
/// # Examples
///
/// ```
/// use uni_plugin::AbiRange;
/// let r = AbiRange::parse("^1.2").unwrap();
/// assert!(r.matches(1));
/// assert!(!r.matches(2));
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct AbiRange(String);

// #233 Tier 1: `Deserialize` used to be derived alongside `Serialize` with
// `#[serde(transparent)]`, so a manifest read from TOML or JSON never went
// through `parse` and ANY string became an `AbiRange`. `matches` then fell
// back to `VersionReq::STAR`, which matches every version — so a plugin
// declaring `abi = "not-a-range"` loaded against any host ABI. Validating in
// `Deserialize` makes the unvalidated state unrepresentable rather than
// leaving each consumer to re-check.
impl<'de> Deserialize<'de> for AbiRange {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw).map_err(serde::de::Error::custom)
    }
}

impl AbiRange {
    /// Parse an ABI range from a semver requirement string.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::ManifestParse`] if the input is not a valid
    /// semver `VersionReq`.
    pub fn parse(s: impl AsRef<str>) -> Result<Self, PluginError> {
        let s = s.as_ref();
        VersionReq::parse(s)
            .map_err(|e| PluginError::ManifestParse(format!("invalid abi range `{s}`: {e}")))?;
        Ok(Self(s.to_owned()))
    }

    /// Check whether a host ABI major satisfies this range.
    ///
    /// The check passes if any version with major == `host_major` falls
    /// within the requirement. We probe with a high minor / patch so that
    /// ranges like `^1.2` (which excludes `1.0.0`) still recognize the
    /// host's major as compatible.
    #[must_use]
    pub fn matches(&self, host_major: u64) -> bool {
        // Fail closed. The range is validated in both `parse` and
        // `Deserialize`, so this branch is unreachable; if it is ever reached
        // the safe answer is "does not match", never `VersionReq::STAR`,
        // which matched every host major and is what let an unvalidated
        // range load (#233).
        let Ok(req) = VersionReq::parse(&self.0) else {
            return false;
        };
        // The question is "does ANY version with major == host_major satisfy the
        // requirement?". A single high `(major, MAX, MAX)` probe answers that only
        // for caret / lower-bounded ranges — it wrongly fails an upper-bounded
        // minor/patch range such as `~1.2` (`>=1.2.0, <1.3.0`) or `=1.2.3`, whose
        // in-range points sit at the comparators' own coordinates. So probe those
        // coordinates (and just above each, for `>x.y.z` lower bounds) plus the
        // extremes, and accept if any candidate at this major satisfies the req.
        let mut candidates: Vec<(u64, u64)> = vec![(0, 0), (u64::MAX / 2, u64::MAX / 2)];
        for c in &req.comparators {
            let minor = c.minor.unwrap_or(0);
            let patch = c.patch.unwrap_or(0);
            candidates.push((minor, patch));
            candidates.push((minor, patch.saturating_add(1)));
        }
        candidates
            .into_iter()
            .any(|(minor, patch)| req.matches(&Version::new(host_major, minor, patch)))
    }

    /// Returns the underlying range string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A dependency on another plugin.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginDep {
    /// Required dependency plugin id.
    pub id: PluginId,
    /// Version requirement (semver range).
    pub version_req: String,
    /// If `true`, the dependency is best-effort (init still runs even if missing).
    #[serde(default)]
    pub optional: bool,
}

impl PluginDep {
    /// Construct a required dependency.
    #[must_use]
    pub fn new(id: PluginId, version_req: impl Into<String>) -> Self {
        Self {
            id,
            version_req: version_req.into(),
            optional: false,
        }
    }

    /// Check whether the supplied `version` satisfies this dependency.
    #[must_use]
    pub fn satisfied_by(&self, version: &Version) -> bool {
        VersionReq::parse(&self.version_req).is_ok_and(|r| r.matches(version))
    }
}

/// Declarative summary of what surfaces a plugin's `register()` will add.
///
/// Built by the plugin author and serialized into the manifest. The host can
/// use this to validate registrations against the manifest and to build a
/// fast pre-registration routing table.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ProvidedSurfaces {
    /// Scalar function locals (un-namespaced, joined with manifest id at
    /// registration).
    pub scalar_fns: Vec<SmolStr>,
    /// Aggregate function locals.
    pub aggregate_fns: Vec<SmolStr>,
    /// Window function locals.
    pub window_fns: Vec<SmolStr>,
    /// Procedure locals.
    pub procedures: Vec<SmolStr>,
    /// Locy aggregate locals.
    pub locy_aggregates: Vec<SmolStr>,
    /// Locy predicate locals.
    pub locy_predicates: Vec<SmolStr>,
    /// Algorithm locals.
    pub algorithms: Vec<SmolStr>,
    /// Storage backends declared (by URI scheme).
    pub storage_backends: Vec<SmolStr>,
    /// Index kinds declared.
    pub index_kinds: Vec<SmolStr>,
    /// CRDT kinds declared.
    pub crdt_kinds: Vec<SmolStr>,
    /// Logical (Arrow extension) types declared.
    pub logical_types: Vec<SmolStr>,
    /// Whether the plugin contributes phased hooks.
    pub hooks: bool,
    /// Whether the plugin contributes triggers.
    pub triggers: bool,
    /// Whether the plugin contributes background jobs.
    pub background_jobs: bool,
    /// Wire-protocol connectors declared.
    pub connectors: Vec<SmolStr>,
}

/// Top-level plugin manifest.
///
/// Authored as TOML, exchanged as JSON (the WASM `manifest-json` export
/// returns this serialized to JSON). Round-trips through `serde` in either
/// format.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginManifest {
    /// Reverse-DNS plugin id.
    pub id: PluginId,
    /// Plugin semantic version.
    pub version: Version,
    /// ABI range this plugin supports.
    pub abi: AbiRange,
    /// Plugins this depends on. May be empty.
    #[serde(default)]
    pub depends_on: Vec<PluginDep>,
    /// Capability requests; granted set is intersection with host grants.
    #[serde(default)]
    pub capabilities: CapabilitySet,
    /// Determinism characterization.
    #[serde(default)]
    pub determinism: Determinism,
    /// Side-effect characterization.
    #[serde(default)]
    pub side_effects: SideEffects,
    /// Lifetime scope.
    #[serde(default)]
    pub scope: Scope,
    /// Optional hash pin (blake3 hex string of the plugin payload).
    #[serde(default)]
    pub hash: Option<String>,
    /// Optional Ed25519 signature over canonical-JSON manifest + payload hash.
    #[serde(default)]
    pub signature: Option<ManifestSignature>,
    /// Declarative surface summary.
    #[serde(default)]
    pub provides: ProvidedSurfaces,
    /// Markdown docs surfaced via `uni plugin help <qname>` and
    /// `CALL uni.plugin.help('qname')`.
    #[serde(default)]
    pub docs: String,
    /// Free-form metadata (author, license, repo, etc.).
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

impl PluginManifest {
    /// Parse a manifest from a TOML string.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::ManifestParse`] if the input fails to parse.
    pub fn from_toml(s: impl AsRef<str>) -> Result<Self, PluginError> {
        toml::from_str(s.as_ref()).map_err(|e| PluginError::ManifestParse(format!("toml: {e}")))
    }

    /// Parse a manifest from a JSON string.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::ManifestParse`] if the input fails to parse.
    pub fn from_json(s: impl AsRef<str>) -> Result<Self, PluginError> {
        serde_json::from_str(s.as_ref())
            .map_err(|e| PluginError::ManifestParse(format!("json: {e}")))
    }

    /// Serialize this manifest to TOML.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::ManifestParse`] if serialization fails
    /// (unusual — only happens with non-stringifiable map keys, which the
    /// manifest doesn't produce).
    pub fn to_toml(&self) -> Result<String, PluginError> {
        toml::to_string_pretty(self)
            .map_err(|e| PluginError::ManifestParse(format!("toml serialize: {e}")))
    }

    /// Serialize this manifest to JSON (compact).
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::ManifestParse`] if serialization fails.
    pub fn to_json(&self) -> Result<String, PluginError> {
        serde_json::to_string(self)
            .map_err(|e| PluginError::ManifestParse(format!("json serialize: {e}")))
    }
}

/// Manifest signature material.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestSignature {
    /// Algorithm identifier — `"ed25519"` for v1.
    pub algorithm: String,
    /// Key identifier (key fingerprint or human-readable name).
    pub key_id: String,
    /// Base64-encoded signature bytes.
    pub value: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_manifest() -> PluginManifest {
        PluginManifest {
            id: PluginId::new("ai.dragonscale.geo"),
            version: Version::parse("0.3.1").unwrap(),
            abi: AbiRange::parse("^1").unwrap(),
            depends_on: vec![],
            capabilities: CapabilitySet::new(),
            determinism: Determinism::Pure,
            side_effects: SideEffects::ReadOnly,
            scope: Scope::Instance,
            hash: None,
            signature: None,
            provides: ProvidedSurfaces::default(),
            docs: String::new(),
            metadata: BTreeMap::new(),
        }
    }

    #[test]
    fn abi_range_parse_and_match() {
        let r = AbiRange::parse("^1.2").unwrap();
        assert!(r.matches(1));
        assert!(!r.matches(2));
    }

    #[test]
    fn abi_range_rejects_garbage() {
        assert!(AbiRange::parse("not-semver").is_err());
    }

    /// An invalid ABI range must be rejected when a manifest is *deserialized*,
    /// not only when it is built through `parse`.
    ///
    /// #233 Tier 1. `AbiRange` derived `Deserialize` under
    /// `#[serde(transparent)]`, so a manifest loaded from TOML or JSON skipped
    /// `parse` entirely and carried an arbitrary string. `matches` then fell
    /// back to `VersionReq::STAR` and returned `true` for every host major, so
    /// an ABI-incompatible plugin loaded successfully. Both parse entry points
    /// are covered because the extism loader uses the JSON one and the on-disk
    /// manifest format is TOML.
    #[test]
    fn a_manifest_carrying_an_invalid_abi_range_is_rejected() {
        let good = sample_manifest().to_json().expect("sample serializes");
        assert!(
            good.contains("\"^1\""),
            "test needs the abi range to appear verbatim in the JSON, got {good}"
        );
        let bad_json = good.replace("\"^1\"", "\"not-semver\"");

        let err = PluginManifest::from_json(&bad_json)
            .expect_err("a manifest with an invalid abi range must not deserialize");
        let msg = err.to_string();
        assert!(
            msg.contains("not-semver") || msg.contains("abi range"),
            "the error should name the offending range, got: {msg}"
        );

        let bad_toml = toml::to_string(&sample_manifest())
            .expect("sample serializes to toml")
            .replace("\"^1\"", "\"not-semver\"");
        assert!(
            PluginManifest::from_toml(&bad_toml).is_err(),
            "the TOML path must reject it too"
        );
    }

    /// A range that cannot be parsed must not match every host major.
    #[test]
    fn matches_fails_closed_rather_than_matching_everything() {
        // Reachable only through the crate-private field, which is the point:
        // if a future change reintroduces an unvalidated construction path,
        // the answer must be "no match", not `VersionReq::STAR`.
        let unvalidated = AbiRange("not-semver".to_owned());
        assert!(
            !unvalidated.matches(1) && !unvalidated.matches(2),
            "an unparseable range must fail closed"
        );
    }

    #[test]
    fn manifest_round_trip_json() {
        let m = sample_manifest();
        let s = m.to_json().unwrap();
        let parsed = PluginManifest::from_json(&s).unwrap();
        assert_eq!(parsed, m);
    }

    #[test]
    fn manifest_round_trip_toml() {
        let m = sample_manifest();
        let s = m.to_toml().unwrap();
        let parsed = PluginManifest::from_toml(&s).unwrap();
        assert_eq!(parsed, m);
    }

    #[test]
    fn plugin_dep_version_satisfaction() {
        let dep = PluginDep::new(PluginId::new("units"), "^0.4".to_owned());
        assert!(dep.satisfied_by(&Version::parse("0.4.2").unwrap()));
        assert!(!dep.satisfied_by(&Version::parse("0.3.0").unwrap()));
    }
}
