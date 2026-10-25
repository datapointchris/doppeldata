use std::borrow::Cow;
use std::collections::BTreeMap;

use schemars::JsonSchema;
use schemars::Schema;
use schemars::SchemaGenerator;
use schemars::json_schema;
use serde::Deserialize;

/// The format version this engine reads. It changes only on a breaking format change.
pub const FORMAT_VERSION: u64 = 1;

/// `profile_version`, which must equal [`FORMAT_VERSION`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(try_from = "u64")]
pub struct ProfileVersion;

impl TryFrom<u64> for ProfileVersion {
    type Error = String;

    fn try_from(version: u64) -> Result<Self, Self::Error> {
        if version == FORMAT_VERSION {
            Ok(Self)
        } else {
            Err(format!(
                "profile_version {version} is not {FORMAT_VERSION}, the version this engine reads"
            ))
        }
    }
}

impl JsonSchema for ProfileVersion {
    fn schema_name() -> Cow<'static, str> {
        "ProfileVersion".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "The format version, which changes only on a breaking format change.",
            "const": FORMAT_VERSION,
        })
    }
}

/// A share of rows, from 0 to 1.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Deserialize)]
#[serde(try_from = "f64")]
pub struct Rate(pub f64);

impl TryFrom<f64> for Rate {
    type Error = String;

    fn try_from(rate: f64) -> Result<Self, Self::Error> {
        if (0.0..=1.0).contains(&rate) {
            Ok(Self(rate))
        } else {
            Err(format!("{rate} is not a rate between 0 and 1"))
        }
    }
}

impl JsonSchema for Rate {
    fn schema_name() -> Cow<'static, str> {
        "Rate".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "A share of rows, from 0 to 1.",
            "type": "number",
            "minimum": 0,
            "maximum": 1,
        })
    }
}

/// Values at named quantiles: `p05` is the 5th percentile, `p50` the median, `p999` the 99.9th. A
/// name's digits after `p` are the level's decimal digits, so `p5` and `p100` are refused.
#[derive(Debug, Clone, PartialEq, Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct Quantiles(pub BTreeMap<String, f64>);

impl JsonSchema for Quantiles {
    fn schema_name() -> Cow<'static, str> {
        "Quantiles".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "Values at named quantiles: p05 is the 5th percentile, p50 the median, p999 the 99.9th. The digits after p are the level's decimal digits, at least two and never ending in 0 past the second.",
            "type": "object",
            "propertyNames": { "pattern": "^p[0-9]{2}([0-9]*[1-9])?$" },
            "additionalProperties": { "type": "number" },
        })
    }
}
