//! What names a generated dataset: the values its rows are a pure function of. The engine owns the
//! identity's encoding and its SHA-256, so the library takes both from here and computes neither.

use std::collections::BTreeMap;

use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;

use crate::profile::provenance;
use crate::rng::STREAM_VERSION;

/// A dataset's identity. The library computes the profile's SHA-256 and the scenario's and passes
/// them in, each as `sha256:` and 64 lowercase hex digits. The scenario's is null where no
/// scenario applies. In a bundle the profile's is the bundle's, and `scale` and `table` name
/// what of it the rows are; both are null outside one. `overrides_sha256` names the columns a
/// sample pins and their values, and is absent where none is pinned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetIdentity {
    stream_version: u32,
    profile_sha256: String,
    scenario_sha256: Option<String>,
    overrides_sha256: Option<String>,
    seed: u64,
    rows: u64,
    scale: Option<String>,
    table: Option<String>,
}

/// The identity's key for a value: its provenance key without the `doppeldata.` prefix, so a file's
/// metadata and the identity name each value the same way.
fn key(provenance_key: &'static str) -> &'static str {
    provenance_key
        .strip_prefix("doppeldata.")
        .expect("every provenance key starts doppeldata.")
}

impl DatasetIdentity {
    /// The identity of a dataset this engine generates.
    pub fn new(profile_sha256: impl Into<String>, seed: u64, rows: u64) -> Self {
        Self {
            stream_version: STREAM_VERSION,
            profile_sha256: profile_sha256.into(),
            scenario_sha256: None,
            overrides_sha256: None,
            seed,
            rows,
            scale: None,
            table: None,
        }
    }

    /// The identity of the same dataset with the scenario whose SHA-256 is `scenario_sha256` applied.
    pub fn with_scenario(self, scenario_sha256: impl Into<String>) -> Self {
        Self {
            scenario_sha256: Some(scenario_sha256.into()),
            ..self
        }
    }

    /// The identity of the same dataset with columns pinned by the overrides whose SHA-256 is
    /// `overrides_sha256`.
    pub fn with_overrides(self, overrides_sha256: impl Into<String>) -> Self {
        Self {
            overrides_sha256: Some(overrides_sha256.into()),
            ..self
        }
    }

    /// The identity of a bundle generated at `scale`, whose SHA-256 is `profile_sha256`, or of its
    /// table `table` where one is named.
    pub fn in_bundle(self, scale: f64, table: Option<&str>) -> Self {
        Self {
            scale: Some(scale_text(scale)),
            table: table.map(String::from),
            ..self
        }
    }

    /// The scale in decimal, the shortest text reading back as the same double, or None outside a
    /// bundle.
    pub fn scale(&self) -> Option<&str> {
        self.scale.as_deref()
    }

    /// The bundle table these rows are of, or None outside a bundle and for the bundle's own identity.
    pub fn table(&self) -> Option<&str> {
        self.table.as_deref()
    }

    pub fn stream_version(&self) -> u32 {
        self.stream_version
    }

    pub fn profile_sha256(&self) -> &str {
        &self.profile_sha256
    }

    pub fn scenario_sha256(&self) -> Option<&str> {
        self.scenario_sha256.as_deref()
    }

    pub fn overrides_sha256(&self) -> Option<&str> {
        self.overrides_sha256.as_deref()
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// The values as RFC 8785 canonical JSON, the bytes [`Self::sha256`] hashes. The map sorts its
    /// keys. The 64-bit integers and the scale are decimal strings, because RFC 8785 writes a number
    /// as an IEEE 754 double in a form only ECMAScript fixes. An absent value is written as null,
    /// except `overrides_sha256`, which is left out where nothing is pinned. So an unpinned
    /// dataset's identity holds no key for overrides.
    pub fn canonical_json(&self) -> String {
        let mut canonical = BTreeMap::from([
            (key(provenance::DATASET_ROWS), Value::from(self.rows.to_string())),
            (key(provenance::PROFILE_SHA256), Value::from(self.profile_sha256.as_str())),
            (key(provenance::SCALE), Value::from(self.scale.as_deref())),
            (key(provenance::SCENARIO_SHA256), Value::from(self.scenario_sha256.as_deref())),
            (key(provenance::SEED), Value::from(self.seed.to_string())),
            (key(provenance::STREAM_VERSION), Value::from(self.stream_version)),
            (key(provenance::TABLE), Value::from(self.table.as_deref())),
        ]);
        if let Some(overrides) = &self.overrides_sha256 {
            canonical.insert(key(provenance::OVERRIDES_SHA256), Value::from(overrides.as_str()));
        }
        serde_json::to_string(&canonical).expect("strings, null and an integer always serialize")
    }

    /// `sha256:` and the hex SHA-256 of [`Self::canonical_json`].
    pub fn sha256(&self) -> String {
        sha256_of(self.canonical_json().as_bytes())
    }
}

/// `sha256:` and the lowercase hex SHA-256 of `bytes`.
pub(crate) fn sha256_of(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(7 + 64);
    text.push_str("sha256:");
    for byte in Sha256::digest(bytes) {
        text.push_str(&format!("{byte:02x}"));
    }
    text
}

/// `scale` as the identity writes it: Rust's shortest decimal that reads back as the same double.
pub fn scale_text(scale: f64) -> String {
    format!("{scale}")
}

/// Whether `text` is `sha256:` and 64 lowercase hex digits, the form the library writes.
pub(crate) fn is_sha256(text: &str) -> bool {
    text.strip_prefix("sha256:")
        .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')))
}
