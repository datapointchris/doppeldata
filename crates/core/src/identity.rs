//! What names a generated dataset: the five values its rows are a pure function of.

use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;

use crate::rng::STREAM_VERSION;

/// A dataset's identity. The library computes the profile and scenario digests and passes them in,
/// each as `sha256:` and 64 lowercase hex digits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetIdentity {
    pub stream_version: u32,
    pub profile_digest: String,
    /// `None` when no scenario applies.
    pub scenario_digest: Option<String>,
    pub seed: u64,
    pub rows: u64,
}

/// The identity's RFC 8785 form. Fields are declared in key order, and the 64-bit integers are
/// decimal strings, because RFC 8785 writes a number as an IEEE 754 double.
#[derive(Serialize)]
struct Canonical<'a> {
    profile_digest: &'a str,
    rows: String,
    scenario_digest: Option<&'a str>,
    seed: String,
    stream_version: u32,
}

impl DatasetIdentity {
    /// The identity of a dataset this engine generates with no scenario.
    pub fn new(profile_digest: impl Into<String>, seed: u64, rows: u64) -> Self {
        Self {
            stream_version: STREAM_VERSION,
            profile_digest: profile_digest.into(),
            scenario_digest: None,
            seed,
            rows,
        }
    }

    /// The five values as RFC 8785 canonical JSON, the bytes [`Self::digest`] hashes.
    pub fn canonical_json(&self) -> String {
        let canonical = Canonical {
            profile_digest: &self.profile_digest,
            rows: self.rows.to_string(),
            scenario_digest: self.scenario_digest.as_deref(),
            seed: self.seed.to_string(),
            stream_version: self.stream_version,
        };
        serde_json::to_string(&canonical).expect("strings and an integer always serialize")
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
