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

/// A share, from 0 to 1.
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
            "description": "A share, from 0 to 1.",
            "type": "number",
            "minimum": 0,
            "maximum": 1,
        })
    }
}

/// The microseconds in a length of time written as a whole number and a unit, `-` before it where
/// `signed` allows one.
fn duration_microseconds(text: &str, signed: bool) -> Result<i64, String> {
    let (sign, unsigned) = match text.strip_prefix('-') {
        Some(rest) if signed => (-1, rest),
        _ => (1, text),
    };
    let split = unsigned.find(|c: char| !c.is_ascii_digit()).unwrap_or(unsigned.len());
    let (count, unit) = unsigned.split_at(split);
    let per_unit: i64 = match unit {
        "ms" => 1_000,
        "s" => 1_000_000,
        "m" => 60_000_000,
        "h" => 3_600_000_000,
        "d" => 86_400_000_000,
        "w" => 604_800_000_000,
        _ => {
            return Err(format!(
                "{text:?} is not a duration such as 30s, 6h or 2d; the units are ms, s, m, h, d and w"
            ));
        }
    };
    let count: i64 = count
        .parse()
        .map_err(|_| format!("{text:?} is not a duration: it needs a whole number before its unit"))?;
    count
        .checked_mul(per_unit)
        .map(|total| sign * total)
        .ok_or_else(|| format!("{text:?} is not a duration under 292,000 years"))
}

/// A length of time written as a whole number above zero and a unit: `250ms`, `30s`, `15m`, `6h`,
/// `2d` or `1w`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(try_from = "String")]
pub struct Duration {
    pub microseconds: i64,
}

impl TryFrom<String> for Duration {
    type Error = String;

    fn try_from(text: String) -> Result<Self, String> {
        let microseconds = duration_microseconds(&text, false)?;
        if microseconds == 0 {
            return Err(format!("{text:?} is not a duration above zero"));
        }
        Ok(Self { microseconds })
    }
}

impl JsonSchema for Duration {
    fn schema_name() -> Cow<'static, str> {
        "Duration".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "A length of time as a whole number above zero and a unit: ms, s, m, h, d or w, such as 2d.",
            "type": "string",
            "pattern": "^0*[1-9][0-9]*(ms|s|m|h|d|w)$",
        })
    }
}

/// A length of time a value is offset by, as [`Duration`] writes one, which may also be zero or
/// negative: `0d`, `-3d` or `690s`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(try_from = "String")]
pub struct SignedDuration {
    pub microseconds: i64,
}

impl TryFrom<String> for SignedDuration {
    type Error = String;

    fn try_from(text: String) -> Result<Self, String> {
        duration_microseconds(&text, true).map(|microseconds| Self { microseconds })
    }
}

impl JsonSchema for SignedDuration {
    fn schema_name() -> Cow<'static, str> {
        "SignedDuration".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "A length of time as a whole number, `-` before it where negative, and a unit: ms, s, m, h, d or w, such as 2d or -690s.",
            "type": "string",
            "pattern": "^-?[0-9]+(ms|s|m|h|d|w)$",
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
