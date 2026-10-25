use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::Deserialize;

use super::values::ProfileVersion;
use super::values::Quantiles;
use super::values::Rate;

/// An object whose fields format v1 leaves open. Its fields are kept as written and checked by nothing.
pub type OpenObject = serde_json::Map<String, serde_json::Value>;

/// One table's profile. Every section is optional, and the generator falls back to defaults.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct TableProfile {
    pub profile_version: ProfileVersion,
    pub name: String,
    pub generated: Option<Generated>,
    pub privacy: Option<Privacy>,
    pub source: Option<Source>,
    pub dataset: Option<Dataset>,
    pub columns: Option<Vec<Column>>,
}

/// How and when the profile was made. `at` and `tool` are left out of the profile's digest.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct Generated {
    /// The tool and its version, as `doppeldata <version>`.
    pub tool: Option<String>,
    /// When profiling finished, in RFC 3339.
    pub at: Option<String>,
    pub engine: Option<Engine>,
    /// A hash of the source's paths, sizes and modification times, never its content.
    pub source_fingerprint: Option<String>,
    pub sample: Option<Sample>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Engine {
    Polars,
    Duckdb,
    Spark,
    /// Read from a catalog or a data contract rather than computed from data.
    Import,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct Sample {
    pub strategy: SampleStrategy,
    pub rows_read: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SampleStrategy {
    Full,
    Head,
    Reservoir,
}

/// The privacy policy the profile was written under, and every literal it kept or withheld.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct Privacy {
    pub policy: Option<PrivacyPolicy>,
    /// The fewest rows any stored literal or cell may rest on.
    pub k_min: Option<u64>,
    pub dp: Option<DifferentialPrivacy>,
    /// Columns whose literals were withheld, and why.
    pub suppressed: Option<Vec<Suppression>>,
    /// Every literal kept, and the rule that admitted it.
    pub audit: Option<Vec<AuditEntry>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PrivacyPolicy {
    Default,
    Strict,
    Healthcare,
    Dp,
    None,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct DifferentialPrivacy {
    pub epsilon: f64,
    pub delta: f64,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct Suppression {
    pub column: String,
    pub reason: String,
    pub semantic: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct AuditEntry {
    pub column: String,
    /// How many literals of the column the profile keeps, where the rule admits a count of them.
    pub values: Option<u64>,
    pub rule: String,
}

/// The files the data came from.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct Source {
    pub format: Option<SourceFormat>,
    pub compression: Option<Compression>,
    pub layout: Option<Layout>,
    pub csv: Option<OpenObject>,
    pub parquet: Option<OpenObject>,
    pub json: Option<OpenObject>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceFormat {
    Csv,
    Tsv,
    Json,
    Ndjson,
    Parquet,
    Arrow,
    Orc,
    Avro,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Compression {
    None,
    Gzip,
    Bzip2,
    Zstd,
    Snappy,
    Lz4,
    Lzo,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct Layout {
    pub files: Option<FileCounts>,
    pub rows_per_file: Option<Quantiles>,
    pub partitioning: Option<Partitioning>,
    pub sort_order: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct FileCounts {
    pub count: Option<u64>,
    pub size_bytes: Option<FileSizes>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct FileSizes {
    pub min: Option<f64>,
    pub p50: Option<f64>,
    pub max: Option<f64>,
    pub total: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct Partitioning {
    pub style: Option<PartitionStyle>,
    pub keys: Option<Vec<PartitionKey>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PartitionStyle {
    /// `key=value` directories.
    Hive,
    /// Bare value directories, one level per key.
    Directory,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct PartitionKey {
    pub name: String,
    /// An Arrow type string.
    #[serde(rename = "type")]
    pub physical_type: Option<String>,
    pub values: Option<ValueCount>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct ValueCount {
    pub count: Option<u64>,
}

/// Facts about the table as a whole.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct Dataset {
    pub row_count: Option<u64>,
    pub duplicate_row_rate: Option<Rate>,
    pub keys: Option<Keys>,
    pub correlations: Option<Vec<Correlation>>,
    pub conditionals: Option<Vec<Conditional>>,
    pub dependencies: Option<Vec<Dependency>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct Keys {
    pub primary: Option<Key>,
    pub candidates: Option<Vec<Key>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct Key {
    pub columns: Vec<String>,
    /// The share of rows whose key value no other row holds.
    pub uniqueness: Option<Rate>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct Correlation {
    pub a: String,
    pub b: String,
    pub method: CorrelationMethod,
    pub value: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CorrelationMethod {
    Pearson,
    Spearman,
    Kendall,
    CramersV,
}

/// The distribution of `column` within each value of `given`. Each row of `table` sums to 1.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct Conditional {
    pub given: String,
    pub column: String,
    pub table: BTreeMap<String, BTreeMap<String, Rate>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct Dependency {
    pub kind: DependencyKind,
    pub lhs: Vec<String>,
    pub rhs: String,
    pub violation_rate: Option<Rate>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DependencyKind {
    /// `lhs` determines `rhs`.
    Functional,
}

/// One column, or one field nested in a list or struct column.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct Column {
    /// The column's name. A list's element is named `element`.
    pub name: String,
    /// The dotted path to a nested field.
    pub path: Option<String>,
    /// An Arrow type string, such as `decimal128(12, 2)`.
    pub physical_type: Option<String>,
    pub logical_type: Option<LogicalType>,
    pub nullable: Option<bool>,
    pub null_rate: Option<Rate>,
    /// Tokens a text source uses for a missing value, with the share of rows holding each.
    pub missing_tokens: Option<BTreeMap<String, Rate>>,
    /// The share of raw values parsing as each type. The shares sum to 1.
    pub type_mix: Option<BTreeMap<RawValueType, Rate>>,
    pub distinct: Option<Distinct>,
    pub numeric: Option<NumericStats>,
    pub string: Option<StringStats>,
    pub temporal: Option<TemporalStats>,
    pub boolean: Option<BooleanStats>,
    pub list: Option<ListStats>,
    #[serde(rename = "struct")]
    pub struct_fields: Option<StructStats>,
    pub semantic: Option<Semantic>,
    pub quality: Option<Quality>,
}

/// A type a raw value in a text source parses as.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RawValueType {
    String,
    Integer,
    Decimal,
    Boolean,
    Date,
    Timestamp,
}

/// The logical types of the Open Data Contract Standard v3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LogicalType {
    String,
    Date,
    Timestamp,
    Time,
    Number,
    Integer,
    Object,
    Array,
    Boolean,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct Distinct {
    pub approx: Option<u64>,
    pub ratio: Option<Rate>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct NumericStats {
    pub integral: Option<bool>,
    pub precision: Option<u32>,
    pub scale: Option<i32>,
    pub extremes: Option<NumericExtremes>,
    pub mean: Option<f64>,
    pub std: Option<f64>,
    pub skew: Option<f64>,
    pub kurtosis: Option<f64>,
    pub zero_rate: Option<Rate>,
    pub negative_rate: Option<Rate>,
    pub quantiles: Option<Quantiles>,
    pub histogram: Option<Histogram>,
    pub fit: Option<Fit>,
    pub monotonic: Option<Monotonic>,
    pub sequence: Option<Sequence>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct NumericExtremes {
    pub low: Option<f64>,
    pub high: Option<f64>,
    pub method: Option<ExtremesMethod>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExtremesMethod {
    /// The true minimum and maximum, written only under a policy that allows them.
    Exact,
    /// The values at rank `k_min` from each end.
    TrimmedK,
}

/// Bin edges and the rows in each bin. `counts` has one entry fewer than `edges`.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct Histogram {
    pub kind: HistogramKind,
    pub edges: Vec<f64>,
    pub counts: Vec<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HistogramKind {
    EqualDepth,
    EqualWidth,
}

/// A fitted distribution, named as scipy.stats names it, with its parameters.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct Fit {
    pub family: String,
    pub params: BTreeMap<String, f64>,
    /// The Kolmogorov-Smirnov statistic of the fit.
    pub ks: Option<f64>,
    pub aic: Option<f64>,
    /// The rows the fit was made on.
    pub sample: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Monotonic {
    Increasing,
    Decreasing,
    None,
}

/// An integer column counting from `start` by `step` with no gaps.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct Sequence {
    pub start: i64,
    pub step: i64,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct StringStats {
    pub length: Option<LengthStats>,
    pub charset: Option<Charset>,
    /// The share of values in each case: upper, lower, title or mixed.
    pub case: Option<BTreeMap<String, Rate>>,
    pub whitespace_edge_rate: Option<Rate>,
    pub shapes: Option<Vec<Shape>>,
    /// A generalized regular expression, never an alternation of literals.
    pub pattern: Option<String>,
    /// Values at least `k_min` rows hold, in a column that is not an identifier.
    pub top_values: Option<Vec<TopValue>>,
    /// The share of rows holding a value outside `top_values`.
    pub other_rate: Option<Rate>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct LengthStats {
    pub min: Option<u64>,
    pub max: Option<u64>,
    pub mean: Option<f64>,
    pub quantiles: Option<Quantiles>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct Charset {
    pub ascii: Option<bool>,
    pub classes: Option<Vec<CharClass>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CharClass {
    Upper,
    Lower,
    Digit,
    Space,
    Punct,
    Other,
}

/// A value's characters mapped to their class: `A` for an ASCII upper-case letter, `a` for an
/// ASCII lower-case one, `9` for an ASCII digit, and any other character as itself.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct Shape {
    pub shape: String,
    pub rate: Rate,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct TopValue {
    pub value: String,
    pub rate: Rate,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct TemporalStats {
    /// The strftime format of a textual date or time.
    pub format: Option<String>,
    pub timezone: Option<String>,
    pub extremes: Option<TemporalExtremes>,
    /// The share of rows in each month, January first.
    #[schemars(length(equal = 12))]
    pub by_month: Option<Vec<Rate>>,
    /// The share of rows on each day of the week, Monday first.
    #[schemars(length(equal = 7))]
    pub by_dow: Option<Vec<Rate>>,
    /// The share of rows in each hour of the day, from 00.
    #[schemars(length(equal = 24))]
    pub by_hour: Option<Vec<Rate>>,
    /// The share of days in the range holding no rows.
    pub gap_rate: Option<Rate>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct TemporalExtremes {
    pub low: Option<String>,
    pub high: Option<String>,
    pub granularity: Option<Granularity>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Granularity {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct BooleanStats {
    pub true_rate: Option<Rate>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct ListStats {
    pub length: Option<Quantiles>,
    pub empty_rate: Option<Rate>,
    pub element: Option<Box<Column>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct StructStats {
    pub fields: Option<Vec<Column>>,
    /// The share of rows in which each field is present.
    pub presence: Option<BTreeMap<String, Rate>>,
}

/// What the column holds, as a semantic detector judged it.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct Semantic {
    /// A type from the detector pack's vocabulary, such as `email`.
    #[serde(rename = "type")]
    pub semantic_type: Option<String>,
    pub confidence: Option<Rate>,
    pub detectors: Option<Vec<Detector>>,
    pub sensitivity: Option<Sensitivity>,
    /// What the generator should draw from, such as `{provider: email}`.
    pub generator_hint: Option<GeneratorHint>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct GeneratorHint {
    pub provider: Option<Provider>,
    /// A regular expression every value matches. It takes the place of `string.pattern`.
    pub pattern: Option<String>,
    /// The check digit a value's last digit holds. `quality.invalid_rate` of values hold a wrong one.
    pub check_digit: Option<CheckDigit>,
}

/// A source of realistic values. The names are doppeldata's, so a change of faker breaks no profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    Email,
    FreeEmail,
    Username,
    Ipv4,
    Ipv6,
    MacAddress,
    UserAgent,
    Name,
    FirstName,
    LastName,
    PhoneNumber,
    CellNumber,
    Company,
    JobTitle,
    StreetAddress,
    StreetName,
    BuildingNumber,
    City,
    State,
    StateAbbr,
    ZipCode,
    PostCode,
    Country,
    CountryCode,
    CreditCard,
    Uuid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CheckDigit {
    /// The mod-10 digit of payment card numbers.
    Luhn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Detector {
    Pattern,
    CheckDigit,
    Presidio,
    Glue,
    NameHint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Sensitivity {
    Identifier,
    QuasiIdentifier,
    Sensitive,
    None,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
pub struct Quality {
    /// The share of values failing the semantic validator, such as a bad check digit.
    pub invalid_rate: Option<Rate>,
}
