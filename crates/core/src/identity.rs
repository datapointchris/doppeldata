//! What names a generated dataset: the five values its rows are a pure function of. The engine owns
//! the identity's encoding and its digest, so the library takes both from here and computes neither.

use std::collections::BTreeMap;

use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;

use crate::profile::provenance;
use crate::rng::STREAM_VERSION;

/// A dataset's identity. The library computes the profile digest and passes it in, as `sha256:`
/// and 64 lowercase hex digits. This engine applies no scenario, so the scenario digest is null.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetIdentity {
    stream_version: u32,
    profile_digest: String,
    seed: u64,
    rows: u64,
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
            seed,
            rows,
        }
    }

    pub fn stream_version(&self) -> u32 {
        self.stream_version
    }

    pub fn profile_digest(&self) -> &str {
        &self.profile_digest
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// The five values as RFC 8785 canonical JSON, the bytes [`Self::digest`] hashes. The map sorts
    /// its keys. The 64-bit integers are decimal strings, because RFC 8785 writes a number as an
    /// IEEE 754 double.
    pub fn canonical_json(&self) -> String {
        let canonical = BTreeMap::from([
            (key(provenance::DATASET_ROWS), Value::from(self.rows.to_string())),
            (key(provenance::PROFILE_DIGEST), Value::from(self.profile_digest.as_str())),
            (key(provenance::SCENARIO_DIGEST), Value::Null),
            (key(provenance::SEED), Value::from(self.seed.to_string())),
            (key(provenance::STREAM_VERSION), Value::from(self.stream_version)),
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

/// Whether `digest` is `sha256:` and 64 lowercase hex digits, the form the library writes.
pub(crate) fn is_sha256_digest(digest: &str) -> bool {
    digest
        .strip_prefix("sha256:")
        .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')))
}
