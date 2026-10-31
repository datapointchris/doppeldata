use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use super::values::Quantiles;
use super::values::Rate;
use super::values::SignedDuration;
use crate::format_settings::DelimitedSettings;
use crate::format_settings::JsonSettings;
use crate::format_settings::LineTerminator;
use crate::format_settings::ParquetSettings;

/// One table's profile. Every section is optional, and the generator falls back to defaults.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TableProfile {
    pub kind: TableKind,
    pub name: String,
    pub generated: Option<Generated>,
    pub privacy: Option<Privacy>,
    pub source: Option<Source>,
    pub dataset: Option<Dataset>,
    pub columns: Option<Vec<Column>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[schemars(inline)]
pub enum TableKind {
    Table,
}

/// How and when the profile was made. `GENERATED_FIELDS` in the library's
/// `doppeldata.profile_format` says which fields the profile's digest leaves out.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Generated {
    /// The tool and its version, as `doppeldata <version>`.
    pub tool: Option<String>,
    /// The package that fits each numeric column's family, and its version, as `scipy <version>`.
    /// Left out when scipy was not installed, so no column was fitted.
    pub fitter: Option<String>,
    /// Each detector pack the profiler typed columns with, by name, with the distribution that
    /// provides it and its version, as `doppeldata <version>`.
    pub detector_packs: Option<BTreeMap<String, String>>,
    /// Each code list the caller supplied a detector, by the name the detector reads it by, with
    /// the sha256 of the file's content, as `sha256:<hex>`. The file's path is not recorded.
    pub code_lists: Option<BTreeMap<String, String>>,
    /// Each package that read or sniffed the source files, by its distribution name, with its
    /// installed version.
    pub readers: Option<BTreeMap<String, String>>,
    /// When profiling finished, in RFC 3339.
    pub at: Option<String>,
    pub engine: Option<Engine>,
    /// A hash of each source file's path, size and revision as listed: its modification time on
    /// disk, or its S3 ETag. It tells two listings apart, and never hashes the content.
    pub source_fingerprint: Option<String>,
    pub sample: Option<Sample>,
    /// The schema an `import` profile was read from, as `<kind>:<name>[#<table>]`. The kind names
    /// what read it, such as `odcs`, `glue` or `pandera`. A file's name is the file's alone, with
    /// no directory, such as `odcs:orders.odcs.yaml#orders`, and `#<table>` names one table of
    /// several. A Glue table is `<region>/<database>/<table>`, and `@<version>` the catalog's
    /// version of it where the catalog gives one. A schema class is its module and class, such as
    /// `pandera:shop.Orders`, and an object is its own name, else its type, such as
    /// `spark:StructType`.
    pub imported_from: Option<String>,
    /// The facts an `import` profile's schema declared, by column path: a column's name, or the
    /// dotted path of a nested field, such as `shipping.city`. Every other bound, length or format
    /// the profile states is a draw default, such as its date window, which no export carries.
    pub declared: Option<BTreeMap<String, Vec<DeclaredFact>>>,
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

/// A fact a schema declared, named for the profile field it marks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeclaredFact {
    /// The low end of `numeric.extremes` or `temporal.extremes`.
    Low,
    /// The high end of `numeric.extremes` or `temporal.extremes`.
    High,
    /// `numeric.scale`.
    Scale,
    /// `string.length.min`.
    MinLength,
    /// `string.length.max`.
    MaxLength,
    /// The least of `list.length`.
    MinItems,
    /// The most of `list.length`.
    MaxItems,
    /// `semantic.type`, read from a format such as `email`.
    Format,
    /// `string.pattern`.
    Pattern,
    /// `top_values`.
    TopValues,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
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

/// The fewest rows a stored literal or cell rests on, where a profile states no `privacy.k_min`.
pub const DEFAULT_K_MIN: u64 = 11;

/// The privacy policy the profile was written under, and every literal it kept or withheld.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Privacy {
    pub policy: Option<PrivacyPolicy>,
    /// The fewest rows any stored literal or cell may rest on.
    pub k_min: Option<u64>,
    pub dp: Option<DifferentialPrivacy>,
    /// Columns whose literals were withheld, and why.
    pub suppressed: Option<Vec<Suppression>>,
    /// Every literal kept, and the rule that admitted it.
    pub audit: Option<Vec<AuditEntry>>,
    /// Every dataset-level field a floor holds, such as a conditional table's cells or a dependency's left-hand
    /// values, and the rule it met.
    pub dataset_audit: Option<Vec<DatasetAuditEntry>>,
    /// Every dataset-level field left out because it failed a floor, and the rule it failed.
    pub withheld: Option<Vec<WithheldField>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WithheldField {
    /// The RFC 6901 pointer to the field from the profile's root, such as `/dataset/keys`.
    pub field: String,
    pub rule: WithheldRule,
}

/// The floor a withheld field failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
pub enum WithheldRule {
    /// The table holds at least `k_min` rows, as a key is stated only over that many.
    #[serde(rename = "rows >= k_min")]
    RowsAtLeastKMin,
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
#[serde(deny_unknown_fields)]
pub struct DifferentialPrivacy {
    pub epsilon: f64,
    pub delta: f64,
}

/// A column some of whose literals the policy withheld.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Suppression {
    pub column: String,
    pub reason: SuppressionReason,
    /// The column's `semantic.type`, where it has one.
    pub semantic: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SuppressionReason {
    /// Typed `identifier`, so the column keeps no literal, extreme or top value.
    Identifier,
    /// Typed `quasi_identifier` under `strict`, which treats it as an identifier.
    QuasiIdentifier,
    /// Typed `free_text`, so the column keeps its length statistics only.
    FreeText,
    /// A ZIP code under `healthcare`, which keeps three-digit prefixes and writes a restricted one as `000`.
    ZipPrefix,
    /// An age under `healthcare`, which collapses every age over 89 into one bin at 90.
    #[serde(rename = "age_over_89")]
    AgeOver89,
    /// A date or timestamp under `healthcare`, whose extremes keep only their years.
    DateToYear,
}

/// One field holding literals, and the rule that admitted them.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuditEntry {
    /// The RFC 6901 pointer to the field within `column`, such as `/string/top_values`.
    pub field: Option<String>,
    pub column: String,
    /// How many literals or cells the field holds.
    pub values: Option<u64>,
    pub rule: AuditRule,
    /// The fewest rows any literal or cell of the field rests on.
    pub min_count: Option<u64>,
}

/// One dataset-level field a floor holds, and the rule it met.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DatasetAuditEntry {
    /// The RFC 6901 pointer to the field from the profile's root, such as `/dataset/conditionals/0`.
    pub field: String,
    /// How many cells or counted values the field holds.
    pub values: Option<u64>,
    pub rule: AuditRule,
    /// The fewest rows any cell or counted value of the field rests on.
    pub min_count: Option<u64>,
}

/// The floor a field's literals met to be kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
pub enum AuditRule {
    /// Each listed value or shape is held by at least `k_min` rows, and the remainder by none or `k_min`.
    #[serde(rename = "count >= k_min")]
    CountAtLeastKMin,
    /// Each histogram bin, overflow count or calendar share holds no rows or at least `k_min`.
    #[serde(rename = "cell >= k_min")]
    CellAtLeastKMin,
    /// The extremes are the values at rank `k_min` from each end.
    #[serde(rename = "trimmed_k")]
    TrimmedK,
    /// Each quantile sits at least `k_min` rows from both ends.
    #[serde(rename = "k_min rows from each end")]
    KMinRowsFromEachEnd,
    /// The pattern's every literal is shared by all the values it was generalized from.
    #[serde(rename = "generalized")]
    Generalized,
    /// The family was fitted to the values between the trimmed extremes.
    #[serde(rename = "fitted within the trimmed extremes")]
    FittedWithinTrimmedExtremes,
    /// The sequence's start follows from the trimmed extreme at its end and its step.
    #[serde(rename = "follows the trimmed extreme")]
    FollowsTrimmedExtreme,
    /// Each left-hand value a dependency is counted over is held by at least `k_min` rows.
    #[serde(rename = "lhs >= k_min")]
    LhsAtLeastKMin,
}

/// The files the data came from. `csv`, `parquet` and `json` each hold the format's `writer`, as a
/// manifest's `writer` holds it, so a generated dataset can be written the way its source was.
/// Beside it each holds what the profiler observed of the files that no writer setting reproduces.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub format: Option<SourceFormat>,
    pub compression: Option<Compression>,
    pub layout: Option<Layout>,
    pub csv: Option<CsvSource>,
    pub parquet: Option<ParquetSource>,
    pub json: Option<JsonSource>,
}

/// A delimited source's dialect, as a writer takes it, and how it was found.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CsvSource {
    /// The dialect a writer writes the files in. A setting left out takes the writer's default.
    #[serde(default)]
    pub writer: DelimitedSettings,
    /// The encoding charset-normalizer named for the first block, such as `ascii`, `utf_8` or
    /// `cp1252`. `encoding` holds the nearest one a writer writes.
    pub detected_encoding: Option<String>,
    /// The share of the first block's line breaks that end in each terminator.
    pub line_terminator_mix: Option<BTreeMap<LineTerminator, Rate>>,
    /// Whether every sampled line ends in a delimiter, which a reader takes as one more empty column.
    pub trailing_delimiter: Option<bool>,
    pub detector: Option<DialectDetection>,
    pub malformed: Option<MalformedRows>,
}

/// How the dialect was found. DuckDB's sniffer reads the files first, and CleverCSV confirms its
/// dialect on the first block. DuckDB's is read where the two agree or CleverCSV names none, and
/// CleverCSV's where they disagree.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DialectDetection {
    pub primary: Option<DialectSniffer>,
    pub confirm: Option<DialectSniffer>,
    /// Whether the two found the same delimiter and quote.
    pub agreed: Option<bool>,
    pub chosen: Option<DialectSniffer>,
    /// The share of the first block's records that split into the most common number of fields
    /// under the chosen dialect.
    pub confidence: Option<Rate>,
    /// What each sniffer named, the one read first leading. So a quote read where neither named
    /// one shows as the format's own rather than a sniffer's.
    pub sniffed: Option<Vec<SniffedDialect>>,
}

/// One sniffer's dialect. Each part it named none of is null, and all are where it found no
/// dialect.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SniffedDialect {
    pub sniffer: Option<DialectSniffer>,
    pub delimiter: Option<String>,
    pub quote: Option<String>,
    pub escape: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DialectSniffer {
    Duckdb,
    Clevercsv,
    /// Neither sniffer named a dialect, so the format's own was read.
    Default,
}

/// The data rows a reader rejects, as shares of every data row the files hold. A type held by
/// fewer than `k_min` rows is left out, and `rate` is the sum of the rest.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MalformedRows {
    pub rate: Option<Rate>,
    /// Each rejected row is counted once, under its first error.
    pub by_type: Option<BTreeMap<MalformedRow, Rate>>,
}

/// Why a reader rejects a row, as DuckDB's rejects table names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MalformedRow {
    /// A value that does not parse as its column's type.
    Cast,
    MissingColumns,
    TooManyColumns,
    /// A quoted value whose closing quote is not followed by a delimiter or a line break.
    UnquotedValue,
    LineSizeOverMaximum,
    InvalidEncoding,
    InvalidState,
}

/// A Parquet source's writer settings, read from its files' footers, and what the footers say of
/// the writer. No value statistic is read from a footer.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParquetSource {
    /// The settings a writer reproduces the files' layout with. A setting left out takes the
    /// writer's default.
    #[serde(default)]
    pub writer: ParquetSettings,
    /// The `created_by` most footers name, such as `parquet-mr version 1.13.1 (build ...)`.
    pub created_by: Option<String>,
    /// `org.apache.spark.version` from the key-value metadata, where Spark wrote the files.
    pub spark_version: Option<String>,
    /// The format version most footers declare, such as `1.0` or `2.6`.
    pub format_version: Option<String>,
    pub row_groups: Option<RowGroups>,
    /// Each timestamp column's unit as stored. `int96` is the legacy encoding no writer reproduces.
    pub timestamp_units: Option<BTreeMap<String, StoredTimestampUnit>>,
    /// Whether a decimal column's scale differs between files.
    pub decimal_scale_drift: Option<bool>,
    /// How many files' footers were read: every file's, or a seeded sample's.
    pub footers_read: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RowGroups {
    /// Row groups in each file.
    pub per_file: Option<Quantiles>,
    pub rows: Option<Quantiles>,
    /// Each row group's bytes on disk, the sum of its column chunks' compressed sizes.
    pub compressed_bytes: Option<Quantiles>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StoredTimestampUnit {
    Ms,
    Us,
    Ns,
    Int96,
}

/// A JSON source's writer settings and its nesting.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JsonSource {
    /// The settings a writer writes the files with. A setting left out takes the writer's default.
    #[serde(default)]
    pub writer: JsonSettings,
    /// The deepest nesting of objects and arrays, counting the record as 1.
    pub max_depth: Option<u32>,
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
#[serde(deny_unknown_fields)]
pub struct Layout {
    pub files: Option<FileCounts>,
    pub rows_per_file: Option<Quantiles>,
    pub partitioning: Option<Partitioning>,
    pub sort_order: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FileCounts {
    pub count: Option<u64>,
    pub size_bytes: Option<FileSizes>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FileSizes {
    pub min: Option<f64>,
    pub p50: Option<f64>,
    pub max: Option<f64>,
    pub total: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Partitioning {
    pub style: Option<PartitionStyle>,
    pub keys: Option<Vec<PartitionKey>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, serde::Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PartitionStyle {
    /// `key=value` directories.
    #[default]
    Hive,
    /// Bare value directories, one level per key.
    Directory,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PartitionKey {
    pub name: String,
    /// An Arrow type string.
    #[serde(rename = "type")]
    pub physical_type: Option<String>,
    pub values: Option<ValueCount>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ValueCount {
    pub count: Option<u64>,
    /// The share of rows under each value at least `k_min` rows hold, written where the key takes
    /// no more values than a string column's top values list.
    pub shares: Option<BTreeMap<String, Rate>>,
    /// The share of rows under a non-null value `shares` does not list.
    pub other_rate: Option<Rate>,
}

/// Facts about the table as a whole.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Dataset {
    pub row_count: Option<u64>,
    /// The share of rows repeating an earlier row in every column, written as 0 where fewer than `k_min` rows do.
    pub duplicate_row_rate: Option<Rate>,
    pub keys: Option<Keys>,
    pub correlations: Option<Vec<Correlation>>,
    /// Each correlation ratio η between a numeric column and a categorical one: the square root of the share of the
    /// variance of the numeric column's ranks that the categorical column's values explain, corrected for the share
    /// they explain by chance. `a` and `b` follow the columns' order, and the column kinds tell which is the category.
    pub correlation_ratios: Option<Vec<CorrelationRatio>>,
    pub conditionals: Option<Vec<Conditional>>,
    pub conditional_quantiles: Option<Vec<ConditionalQuantiles>>,
    pub conditional_rates: Option<Vec<ConditionalRates>>,
    pub dependencies: Option<Vec<Dependency>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Keys {
    pub primary: Option<Key>,
    pub candidates: Option<Vec<Key>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Key {
    pub columns: Vec<String>,
    /// The share of rows whose key value no other row holds.
    pub uniqueness: Option<Rate>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Correlation {
    pub a: String,
    pub b: String,
    pub method: CorrelationMethod,
    pub value: f64,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CorrelationRatio {
    pub a: String,
    pub b: String,
    pub value: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CorrelationMethod {
    Pearson,
    Spearman,
    Kendall,
    /// Bias-corrected Cramér's V between two categorical columns.
    CramersV,
}

/// The distribution of `column` within each value of `given`. Each row of `table` sums to 1 at most, and the share it
/// leaves is the rows of values it does not state. Each value `column` lists takes as much of that share as no row
/// states of it, and the share of values it lists none of draws `column`'s own values.
///
/// Both columns are strings, integers or booleans, and each value is a key as its text: a string as
/// itself, an integer in decimal, and a boolean as `true` or `false`.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Conditional {
    pub given: String,
    pub column: String,
    pub table: BTreeMap<String, BTreeMap<String, Rate>>,
    /// The distribution of `column` over every value of `given` that `table` does not list, summing to 1 at most as a
    /// row does.
    pub other: Option<BTreeMap<String, Rate>>,
}

/// The quantiles of the numeric `column` within each value of the categorical `given`, each value a key as a
/// conditional table's is. Each row takes the same names `column`'s own quantiles do, and holds its zeros as they
/// do: a zero rate stated for the value given the same column, else `column`'s own, places its run of zeros.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConditionalQuantiles {
    pub given: String,
    pub column: String,
    pub quantiles: BTreeMap<String, Quantiles>,
    /// `column`'s quantiles over every value of `given` that `quantiles` does not list.
    pub other: Option<Quantiles>,
}

/// The share of `column`'s values that are zero, and of its rows that are null, within each value of the categorical
/// `given`, each value a key as a conditional table's is.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConditionalRates {
    pub given: String,
    pub column: String,
    /// The share of `column`'s values that are zero, among the rows holding each value of `given`.
    pub zero_rate: Option<BTreeMap<String, Rate>>,
    /// The share of rows where `column` is null, among the rows holding each value of `given`.
    pub null_rate: Option<BTreeMap<String, Rate>>,
    /// Both rates over every value of `given` that the two maps do not list.
    pub other: Option<OtherRates>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OtherRates {
    pub zero_rate: Option<Rate>,
    pub null_rate: Option<Rate>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Dependency {
    pub kind: DependencyKind,
    pub lhs: Vec<String>,
    pub rhs: String,
    /// The share of rows whose `lhs` value holds more than one `rhs` value, of the rows whose `lhs` value at least
    /// `k_min` rows hold.
    pub violation_rate: Option<Rate>,
    /// Each value of a one-column `lhs` at least `k_min` rows hold, with the `rhs` value it determines, each a key as
    /// a conditional table's is. The generator looks `rhs` up here, and draws it from its own statistics for an
    /// `lhs` value the table does not list.
    pub table: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DependencyKind {
    /// `lhs` determines `rhs`.
    Functional,
}

/// One column, or one field nested in a list or struct column.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
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
    /// How the column is computed from others, in place of being drawn from its own statistics.
    pub derived: Option<Derived>,
}

/// How a column is computed from other columns of its table.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Derived {
    /// A temporal column offset from `base` by a lag drawn from its quantiles.
    Timedelta(Timedelta),
    /// A numeric column computed from others by an arithmetic expression.
    Expression(Expression),
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Timedelta {
    /// The temporal column the lag is added to.
    pub base: String,
    pub lag: Lag,
}

/// The lag between the base column and the column, at named quantiles as [`Quantiles`] names them, between its
/// trimmed extremes. A date's lag is written in days, and a timestamp's in seconds.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Lag {
    pub quantiles: Option<BTreeMap<String, SignedDuration>>,
    /// The lag at rank `k_min` from the low end.
    pub min: Option<SignedDuration>,
    /// The lag at rank `k_min` from the high end.
    pub max: Option<SignedDuration>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Expression {
    /// Column names, numbers, `+ - * /`, parentheses and `round(x)` or `round(x, places)`. A name holding a character
    /// other than a letter, a digit or `_` is quoted in backticks.
    pub expr: String,
    /// The decimal places the result is rounded to, else the column's own `numeric.scale`.
    pub round: Option<i32>,
    /// The share of rows the expression does not hold for, which draw the column from its own statistics instead.
    pub violation_rate: Option<Rate>,
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
#[serde(deny_unknown_fields)]
pub struct Distinct {
    pub approx: Option<u64>,
    pub ratio: Option<Rate>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NumericStats {
    pub integral: Option<bool>,
    pub precision: Option<u32>,
    /// Decimal places: a decimal type's scale, or the fewest places every value of a float column
    /// equals itself rounded to, where the profiler finds one.
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
    /// Values at least `k_min` rows hold, in a column with few distinct values that is not an
    /// identifier.
    pub top_values: Option<Vec<NumericTopValue>>,
    /// The share of values outside `top_values`.
    pub other_rate: Option<Rate>,
    /// Written when the value list was frozen from an earlier profile.
    pub overflow: Option<ValueOverflow>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NumericTopValue {
    pub value: f64,
    pub rate: Rate,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
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
    /// The least and greatest integers with as many digits as the trimmed extremes, so an
    /// identifier's range shows its length and none of its values.
    DigitBounds,
}

impl ExtremesMethod {
    /// The most rows that lie beyond each extreme of a profile whose floor is `k_min`. Digit bounds
    /// widen the trimmed extremes they are written from, so no more lie beyond them.
    pub fn rows_beyond_each(self, k_min: u64) -> u64 {
        match self {
            Self::Exact => 0,
            Self::TrimmedK | Self::DigitBounds => k_min.saturating_sub(1),
        }
    }
}

/// Bin edges and the rows in each bin. `counts` has one entry fewer than `edges`. Each bin holds
/// the rows from its lower edge up to its upper one, and the last bin holds its upper edge too.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Histogram {
    pub kind: HistogramKind,
    pub edges: Vec<f64>,
    pub counts: Vec<u64>,
    /// The rows outside the edges, written when the edges were frozen from an earlier profile.
    pub overflow: Option<HistogramOverflow>,
}

/// Rows below the first edge and above the last. A count of fewer than `k_min` rows is written as
/// 0, and its rows are counted in the outermost bin on their side.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HistogramOverflow {
    pub below: u64,
    pub above: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HistogramKind {
    EqualDepth,
    EqualWidth,
}

/// A fitted distribution, named as scipy.stats names it, with its parameters.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Fit {
    pub family: String,
    pub params: BTreeMap<String, f64>,
    /// The Kolmogorov-Smirnov statistic of the fit.
    pub ks: Option<f64>,
    pub aic: Option<f64>,
    /// The rows the fit was made on.
    pub sample: Option<u64>,
    /// The share of the fitting sample's values that were zero and were left out of the fit, so
    /// the column is the family mixed with this share of zeros.
    pub zero_inflation: Option<Rate>,
    /// Whether the KS statistic is above the profiler's poor-fit threshold, so the family describes
    /// the column's tail only loosely.
    pub poor: Option<bool>,
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
#[serde(deny_unknown_fields)]
pub struct Sequence {
    pub start: i64,
    pub step: i64,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
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
    /// The values' leading characters, counted in place of `top_values`, as a policy keeps a ZIP
    /// code's first three digits.
    pub prefixes: Option<Prefixes>,
    /// Written when the value list was frozen from an earlier profile.
    pub overflow: Option<ValueOverflow>,
}

/// A column's values counted by their leading characters, each generated value a prefix followed
/// by a value of `suffix`.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Prefixes {
    /// Each prefix at least `k_min` rows begin with, and its share of the column's values.
    pub values: Vec<TopValue>,
    /// The share of values beginning with a prefix `values` does not list.
    pub other_rate: Option<Rate>,
    /// A regular expression drawn after each prefix, such as `\d{2}` after a ZIP code's first three
    /// digits.
    pub suffix: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ValueOverflow {
    /// The rows `other_rate` holds beyond the share it held when the list was first frozen: the
    /// earlier profile's `other_rate`, less its own `new_value_rows`. So a column whose values did
    /// not change writes about 0. A count of fewer than `k_min` rows is written as 0.
    pub new_value_rows: u64,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LengthStats {
    pub min: Option<u64>,
    pub max: Option<u64>,
    pub mean: Option<f64>,
    pub quantiles: Option<Quantiles>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct Shape {
    pub shape: String,
    pub rate: Rate,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TopValue {
    pub value: String,
    pub rate: Rate,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
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
    /// The share of days between the extremes holding no rows.
    pub gap_rate: Option<Rate>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct BooleanStats {
    pub true_rate: Option<Rate>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListStats {
    pub length: Option<Quantiles>,
    pub empty_rate: Option<Rate>,
    pub element: Option<Box<Column>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StructStats {
    pub fields: Option<Vec<Column>>,
    /// The share of rows in which each field is present.
    pub presence: Option<BTreeMap<String, Rate>>,
}

/// What the column holds, as a semantic detector judged it.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Semantic {
    /// A type from the detector pack's vocabulary, such as `email`.
    #[serde(rename = "type")]
    pub semantic_type: Option<String>,
    /// The detector pack whose detector assigned the type, such as `general`.
    pub pack: Option<String>,
    pub confidence: Option<Rate>,
    pub detectors: Option<Vec<Detector>>,
    pub sensitivity: Option<Sensitivity>,
    /// What the generator should draw from, such as `{provider: email}`.
    pub generator_hint: Option<GeneratorHint>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
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
    /// A code of the ICD-10-CM list the engine ships, drawn uniformly.
    Icd10cm,
    /// A code of the HCPCS Level II list the engine ships, drawn uniformly.
    HcpcsLevel2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CheckDigit {
    /// The mod-10 digit of payment card numbers.
    Luhn,
    /// A National Provider Identifier's: the Luhn digit over `80840` and the digits before it.
    Npi,
    /// A DEA registration number's: the last digit of the first, third and fifth digits plus twice
    /// the second, fourth and sixth.
    Dea,
}

impl std::str::FromStr for CheckDigit {
    type Err = String;

    fn from_str(name: &str) -> Result<Self, String> {
        serde_json::from_value(serde_json::Value::from(name)).map_err(|_| format!("{name:?} is not a check digit a profile names"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Detector {
    /// A regular expression matched the column's values.
    Pattern,
    /// A check of the value other than a regular expression, such as a lookup or a parse.
    Validator,
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
#[serde(deny_unknown_fields)]
pub struct Quality {
    /// The share of values failing the semantic validator, such as a bad check digit.
    pub invalid_rate: Option<Rate>,
}
