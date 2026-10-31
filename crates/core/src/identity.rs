//! What names a generated dataset: the values its rows are a pure function of. The engine owns the
//! identity's encoding and its digest, so the library takes both from here and computes neither.

use std::collections::BTreeMap;

use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;

use crate::profile::provenance;
use crate::rng::STREAM_VERSION;

/// A dataset's identity. The library computes the profile digest and the scenario digest and passes
/// them in, each as `sha256:` and 64 lowercase hex digits. The scenario digest is null where no
/// scenario applies. In a bundle the profile digest is the bundle's, and `scale` and `table` name
/// what of it the rows are; both are null outside one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetIdentity {
    stream_version: u32,
    profile_digest: String,
    scenario_digest: Option<String>,
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
    pub fn new(profile_digest: impl Into<String>, seed: u64, rows: u64) -> Self {
        Self {
            stream_version: STREAM_VERSION,
            profile_digest: profile_digest.into(),
            scenario_digest: None,
            seed,
            rows,
            scale: None,
            table: None,
        }
    }

    /// The identity of the same dataset with the scenario whose digest is `scenario_digest` applied.
    pub fn with_scenario(self, scenario_digest: impl Into<String>) -> Self {
        Self {
            scenario_digest: Some(scenario_digest.into()),
            ..self
        }
    }

    /// The identity of a bundle generated at `scale`, whose digest is the profile digest, or of its
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

    pub fn profile_digest(&self) -> &str {
        &self.profile_digest
    }

    pub fn scenario_digest(&self) -> Option<&str> {
        self.scenario_digest.as_deref()
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// The values as RFC 8785 canonical JSON, the bytes [`Self::digest`] hashes. The map sorts its
    /// keys. The 64-bit integers and the scale are decimal strings, because RFC 8785 writes a number
    /// as an IEEE 754 double in a form only ECMAScript fixes. An absent value is written as null.
    pub fn canonical_json(&self) -> String {
        let canonical = BTreeMap::from([
            (key(provenance::DATASET_ROWS), Value::from(self.rows.to_string())),
            (key(provenance::PROFILE_DIGEST), Value::from(self.profile_digest.as_str())),
            (key(provenance::SCALE), Value::from(self.scale.as_deref())),
            (key(provenance::SCENARIO_DIGEST), Value::from(self.scenario_digest.as_deref())),
            (key(provenance::SEED), Value::from(self.seed.to_string())),
            (key(provenance::STREAM_VERSION), Value::from(self.stream_version)),
            (key(provenance::TABLE), Value::from(self.table.as_deref())),
        ]);
        serde_json::to_string(&canonical).expect("strings, null and an integer always serialize")
    }

    /// `sha256:` and the hex SHA-256 of [`Self::canonical_json`].
    pub fn digest(&self) -> String {
        let hash = Sha256::digest(self.canonical_json().as_bytes());
        let mut text = String::with_capacity(7 + 64);
        text.push_str("sha256:");
        for byte in hash {
            text.push_str(&format!("{byte:02x}"));
        }
        text
    }
}

/// `scale` as the identity writes it: Rust's shortest decimal that reads back as the same double.
pub fn scale_text(scale: f64) -> String {
    format!("{scale}")
}

/// Whether `digest` is `sha256:` and 64 lowercase hex digits, the form the library writes.
pub(crate) fn is_sha256_digest(digest: &str) -> bool {
    digest
        .strip_prefix("sha256:")
        .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')))
}
