//! The scenario format: a layer over a profile that bends the generated data on purpose.
//!
//! A merge patch changes what a profile can express as a value. An ordered list of perturbations
//! applies faults a profile cannot express. `expect` declares what the scenario promises, which a
//! check certifies. The engine applies the patch and the perturbations, so the stream version
//! governs both. The library computes the scenario's digest.

use std::borrow::Cow;
use std::collections::BTreeMap;

use schemars::JsonSchema;
use schemars::Schema;
use schemars::SchemaGenerator;
use schemars::json_schema;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Map;
use serde_json::Value;

use crate::format_settings::Output;
use crate::perturbations::ScenarioFault;
use crate::perturbations::pointer_token;
use crate::plan::PlanError;
pub use crate::profile::Duration;
use crate::profile::Rate;

/// A scenario file: a layer over a profile that bends the generated data on purpose.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    /// The scenario's name.
    pub scenario: String,
    pub description: Option<String>,
    pub patch: Option<ProfilePatch>,
    /// Faults applied in order to each block of generated rows. Each draws from its own stream,
    /// keyed by its `id`, so inserting one never moves another's rows.
    #[serde(default)]
    pub perturbations: Vec<Perturbation>,
    /// What the scenario promises of the generated data, which `data check --scenario` certifies.
    #[serde(default)]
    pub expect: Vec<Expectation>,
}

/// An RFC 7396 JSON Merge Patch onto the profile, with one extension: `columns` is a map keyed by
/// column name rather than the profile's list, so a patch names only the columns it changes. A
/// column patched to null is removed. A column the profile does not hold is refused.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(transparent)]
pub struct ProfilePatch(pub Map<String, Value>);

impl JsonSchema for ProfilePatch {
    fn schema_name() -> Cow<'static, str> {
        "ProfilePatch".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "An RFC 7396 JSON Merge Patch onto the profile. `columns` is a map keyed by column name, an extension of RFC 7396, whose arrays replace wholesale, so a patch names only the columns it changes. A column patched to null is removed, and one the profile does not hold is refused.",
            "type": "object",
            "properties": {
                "columns": {
                    "type": "object",
                    "additionalProperties": { "type": ["object", "null"] },
                },
            },
        })
    }
}

/// One fault, named by `op`. `id` keys its random stream, and defaults to
/// `<op>:<column or "table">:<n>`, where n counts earlier entries of the same op and column.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Perturbation {
    HotKeys(HotKeys),
    Duplicates(Duplicates),
    LateArrival(LateArrival),
    Missingness(Missingness),
    OutOfRange(OutOfRange),
    SwappedValues(SwappedValues),
    CategoricalShift(CategoricalShift),
    Noise(Noise),
    Scaling(Scaling),
    BrokenCharacters(BrokenCharacters),
    Outliers(Outliers),
    SchemaDrift(SchemaDrift),
    SmallFiles(SmallFiles),
    PartitionSkew(PartitionSkew),
    MalformedRows(MalformedRows),
    Cdc(Cdc),
}

/// A change stream in the layout a change-data-capture tool writes, beside `_expected`, the table a
/// correct merge of it produces. The full load is the table the profile generates. The changes are
/// drawn across `period`, each commit time after its start, so the same seed writes the same
/// bytes on any day.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Cdc {
    pub id: Option<String>,
    pub style: CdcStyle,
    /// The schema in the `<schema>/<table>/` prefix every file is written under.
    pub schema: String,
    /// The key column, which every change of a row holds as it is. Without it, the profile's
    /// `dataset.keys.primary`, which must name one column.
    pub key: Option<String>,
    /// New rows, as a share of the full load's.
    pub inserts: Option<Rate>,
    /// The share of the full load's rows updated at least once.
    pub updates: Option<Rate>,
    /// The share of the full load's rows deleted, each after its updates.
    pub deletes: Option<Rate>,
    /// How many times an updated row is updated. Without it, once.
    pub updates_per_row: Option<CountSpread>,
    pub period: Period,
    /// One change file per interval with any changes. Without it, `1m`, as DMS's `CdcMaxBatchInterval`.
    pub file_interval: Option<Duration>,
    /// Write `I` as the first field of every full-load record, as DMS's `IncludeOpForFullLoad` does.
    #[serde(default)]
    pub include_op_for_full_load: bool,
    /// The column holding each record's commit time, as DMS's `TimestampColumnName` names it.
    /// Without it, `dms_commit_ts`.
    pub timestamp_column: Option<String>,
    /// Without it, every change file lies directly under the prefix.
    pub date_partition: Option<DatePartition>,
    /// The unit every timestamp column of a Parquet write is stored at. Without it, `us`.
    pub timestamp_unit: Option<CdcTimestampUnit>,
    /// The chance an eligible change is written a second time, in the first change file after the
    /// one holding its key's last change. A change is eligible when its key's last change comes
    /// after it, and deletes the row or leaves it different from this change's.
    pub duplicate_delivery_rate: Option<Rate>,
    /// The chance an eligible change is written late instead, in the first change file after the
    /// one holding its key's last change. Eligible as for `duplicate_delivery_rate`.
    pub out_of_order_rate: Option<Rate>,
}

impl Cdc {
    /// The column every record's commit time is written in.
    pub fn timestamp_column(&self) -> &str {
        self.timestamp_column.as_deref().unwrap_or(DEFAULT_TIMESTAMP_COLUMN)
    }
}

/// The commit-time column a `cdc` with no `timestamp_column` writes.
pub const DEFAULT_TIMESTAMP_COLUMN: &str = "dms_commit_ts";

/// The column a change record's operation is written in: `I`, `U` or `D`.
pub const OPERATION_COLUMN: &str = "Op";

/// Which tool's layout a change stream takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CdcStyle {
    /// AWS DMS writing to Amazon S3: `LOAD` files, then timestamp-named change files.
    Dms,
}

/// A count of one or more, drawn from a family by its mean.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CountSpread {
    pub family: CountFamily,
    #[schemars(range(min = 1))]
    pub mean: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CountFamily {
    /// 1, 2, 3 and on, each `1 − 1/mean` as likely as the one before.
    Geometric,
}

/// The span the changes are committed across.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Period {
    /// The full load's time, which every commit falls after.
    pub start: UtcInstant,
    pub duration: Duration,
}

/// An instant in RFC 3339, such as `2026-10-01T00:00:00Z`, held as microseconds since the epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(try_from = "String")]
pub struct UtcInstant {
    pub microseconds: i64,
}

impl TryFrom<String> for UtcInstant {
    type Error = String;

    fn try_from(text: String) -> Result<Self, String> {
        chrono::DateTime::parse_from_rfc3339(&text)
            .map(|instant| Self {
                microseconds: instant.timestamp_micros(),
            })
            .map_err(|error| format!("{text:?} is not an RFC 3339 instant such as 2026-10-01T00:00:00Z: {error}"))
    }
}

impl JsonSchema for UtcInstant {
    fn schema_name() -> Cow<'static, str> {
        "UtcInstant".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "An instant in RFC 3339, such as 2026-10-01T00:00:00Z.",
            "type": "string",
            "format": "date-time",
        })
    }
}

/// Change files filed under folders named for their commit date, as DMS's `DatePartitionEnabled`
/// files them. Full-load files are never partitioned.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DatePartition {
    #[serde(default = "enabled_by_default")]
    pub enabled: bool,
    #[serde(default)]
    pub sequence: DateSequence,
    #[serde(default)]
    pub delimiter: DateDelimiter,
}

fn enabled_by_default() -> bool {
    true
}

/// The date parts a folder is named for, in order, as DMS's `DatePartitionSequence` names them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, JsonSchema)]
pub enum DateSequence {
    #[default]
    #[serde(rename = "YYYYMMDD")]
    YearMonthDay,
    #[serde(rename = "YYYYMMDDHH")]
    YearMonthDayHour,
    #[serde(rename = "YYYYMM")]
    YearMonth,
    #[serde(rename = "MMYYYYDD")]
    MonthYearDay,
    #[serde(rename = "DDMMYYYY")]
    DayMonthYear,
}

/// What separates a folder name's date parts, as DMS's `DatePartitionDelimiter` names it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DateDelimiter {
    /// One folder per part, as `2026/10/01/`.
    #[default]
    Slash,
    Dash,
    Underscore,
    None,
}

/// The unit DMS stores Parquet timestamps at: `ms` with `ParquetTimestampInMillisecond`, else `us`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CdcTimestampUnit {
    Us,
    Ms,
}

/// A fault `cdc` places in a change stream, listed in `_faults.jsonl` beside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StreamFault {
    /// A change written a second time, after its key's last change.
    DuplicateDelivery,
    /// A change written after its key's last change, in a later file than its commit time.
    OutOfOrder,
}

impl StreamFault {
    pub fn name(self) -> &'static str {
        match self {
            Self::DuplicateDelivery => "duplicate_delivery",
            Self::OutOfOrder => "out_of_order",
        }
    }
}

/// A fault a fault list names: one `malformed_rows` writes, or one `cdc` places.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum ListedFault {
    Malformed(FaultKind),
    Stream(StreamFault),
}

impl ListedFault {
    pub fn name(self) -> &'static str {
        match self {
            Self::Malformed(kind) => kind.name(),
            Self::Stream(kind) => kind.name(),
        }
    }

    /// Whether the fault breaks a whole file.
    pub fn is_file_kind(self) -> bool {
        matches!(self, Self::Malformed(kind) if kind.is_file_kind())
    }
}

impl From<FaultKind> for ListedFault {
    fn from(kind: FaultKind) -> Self {
        Self::Malformed(kind)
    }
}

/// Bytes a reader rejects or misreads, written into the files and listed in `_faults.jsonl` beside
/// them. With no `rate`, `kinds` or `files_share`, it reproduces the malformed rows and type mix the
/// profile records of its CSV source. Otherwise `rate` of rows each take one of the row
/// kinds named, and `files_share` of files each take one of the file kinds named.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MalformedRows {
    pub id: Option<String>,
    /// The share of rows given a row fault, each kind named equally often.
    pub rate: Option<Rate>,
    pub kinds: Option<Vec<FaultKind>>,
    /// The share of files given a file fault, each kind named equally often.
    pub files_share: Option<Rate>,
}

/// One way a file is broken. A row kind breaks one record, and a file kind a whole file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FaultKind {
    /// A record ending before its last 1 to n − 1 fields.
    RaggedShort,
    /// A record carrying 1 or 2 fields past the last.
    RaggedLong,
    /// A lone quote inside a quoted field.
    UnescapedQuote,
    /// A line break inside a quoted string field, which is legal CSV.
    EmbeddedNewline,
    /// `N/A`, `-`, `?` or a unit suffix in place of a typed value, or text of another type.
    TypeInvalid,
    /// A lone continuation byte or the bytes C3 28 inside a field.
    InvalidUtf8,
    /// A record past DuckDB's default `max_line_size`.
    LineTooLong,
    /// The file's last record cut mid-field, with no line break, as a cut-off upload leaves it.
    TruncatedLine,
    /// A file starting with the UTF-8 byte order mark.
    Bom,
    /// A file whose records end in LF, CRLF and CR mixed.
    MixedLineEndings,
    /// A file written with another delimiter than the rest.
    WrongDelimiter,
    /// A file whose header renames a column or swaps two, its values swapped with it.
    HeaderDrift,
    /// A file encoded windows-1252 among UTF-8 ones.
    Latin1File,
    /// A JSON array among NDJSON files, or an NDJSON file among JSON arrays.
    JsonLayoutMix,
    /// A record missing its closing brace.
    JsonUnbalanced,
    /// A number or boolean written as a string.
    JsonTypeFlip,
    /// A file whose timestamp columns are stored at another unit than the rest.
    ParquetTimestampUnitDrift,
    /// A file whose timestamp columns are stored as INT96, as Hive and Impala wrote them.
    ParquetInt96,
    /// A file whose decimal columns are stored one digit of scale wider than the rest.
    ParquetDecimalScaleDrift,
}

/// What breaks, as a fault kind places it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Breaks {
    Row,
    File,
}

/// The formats whose bytes can hold a fault kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultFormats {
    /// CSV and TSV.
    Delimited,
    /// CSV, TSV, JSON and NDJSON.
    Text,
    /// JSON and NDJSON.
    Json,
    Parquet,
}

impl FaultFormats {
    /// Whether a file `output` writes can hold the fault.
    pub fn hold(self, output: &Output) -> bool {
        match output {
            Output::Csv(_) | Output::Tsv(_) => matches!(self, Self::Delimited | Self::Text),
            Output::Json(_) | Output::Ndjson(_) => matches!(self, Self::Text | Self::Json),
            Output::Parquet(_) => self == Self::Parquet,
            Output::Arrow(_) => false,
        }
    }
}

impl FaultKind {
    /// The kind's name as `kinds` spells it, what it breaks, the formats that hold it, and the
    /// formats whose files `data check` reads back as one table while they hold it. Every fact
    /// about a kind is decided here, so a kind added fails to compile until it is described.
    const fn facts(self) -> (&'static str, Breaks, FaultFormats, Option<FaultFormats>) {
        use Breaks::File;
        use Breaks::Row;
        use FaultFormats::Delimited;
        use FaultFormats::Json;
        use FaultFormats::Parquet;
        use FaultFormats::Text;
        match self {
            Self::RaggedShort => ("ragged_short", Row, Delimited, Some(Delimited)),
            Self::RaggedLong => ("ragged_long", Row, Delimited, Some(Delimited)),
            Self::UnescapedQuote => ("unescaped_quote", Row, Delimited, Some(Delimited)),
            Self::EmbeddedNewline => ("embedded_newline", Row, Delimited, Some(Delimited)),
            Self::TypeInvalid => ("type_invalid", Row, Text, Some(Text)),
            Self::InvalidUtf8 => ("invalid_utf8", Row, Delimited, Some(Delimited)),
            Self::LineTooLong => ("line_too_long", Row, Delimited, Some(Delimited)),
            // DuckDB rejects a cut CSV record and reads the rest, and refuses a cut JSON file.
            Self::TruncatedLine => ("truncated_line", File, Text, Some(Delimited)),
            Self::Bom => ("bom", File, Delimited, Some(Delimited)),
            // The profiler reads every file in the first one's dialect and header.
            Self::MixedLineEndings => ("mixed_line_endings", File, Delimited, None),
            Self::WrongDelimiter => ("wrong_delimiter", File, Delimited, None),
            Self::HeaderDrift => ("header_drift", File, Delimited, None),
            Self::Latin1File => ("latin1_file", File, Delimited, Some(Delimited)),
            Self::JsonLayoutMix => ("json_layout_mix", File, Json, None),
            Self::JsonUnbalanced => ("json_unbalanced", Row, Json, None),
            Self::JsonTypeFlip => ("json_type_flip", Row, Json, Some(Json)),
            // The profiler reads every file under one Arrow schema.
            Self::ParquetTimestampUnitDrift => ("parquet_timestamp_unit_drift", File, Parquet, None),
            Self::ParquetInt96 => ("parquet_int96", File, Parquet, None),
            Self::ParquetDecimalScaleDrift => ("parquet_decimal_scale_drift", File, Parquet, None),
        }
    }

    /// The kind's name, as `kinds` spells it.
    pub fn name(self) -> &'static str {
        self.facts().0
    }

    /// Whether the kind breaks a whole file, so `files_share` places it rather than `rate`.
    pub fn is_file_kind(self) -> bool {
        self.facts().1 == Breaks::File
    }

    /// The formats whose files can hold the kind.
    pub fn formats(self) -> FaultFormats {
        self.facts().2
    }

    /// Whether `data check` reads files `output` writes back as one table while they hold the kind.
    pub fn check_reads_back(self, output: &Output) -> bool {
        self.facts().3.is_some_and(|formats| formats.hold(output))
    }
}

/// `share` of rows take one of `keys` values: the column's most frequent ones, or values its own
/// sampler draws where it lists none.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HotKeys {
    pub id: Option<String>,
    pub column: String,
    #[schemars(range(min = 1))]
    pub keys: u32,
    pub share: Rate,
}

/// Copies of existing rows, so that `rate` of the rows written are copies. Each block's count is
/// planned up front, so block boundaries never move.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Duplicates {
    pub id: Option<String>,
    pub rate: Rate,
    #[serde(default)]
    pub scope: DuplicateScope,
    /// False varies one non-key column of each copy, taking that column's value from another row.
    #[serde(default = "exact_by_default")]
    pub exact: bool,
}

fn exact_by_default() -> bool {
    true
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DuplicateScope {
    /// Each copy is written to the file its source row is written to.
    WithinFile,
    /// Each copy comes from a block half the dataset away, so from another file.
    #[default]
    AcrossFiles,
}

/// `rate` of rows have their event time shifted earlier by a drawn lag.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LateArrival {
    pub id: Option<String>,
    /// The event time.
    pub column: String,
    pub rate: Rate,
    pub lag: LagSpread,
    /// A column to hold each row's unshifted time, as when it arrived. It is written whether or not
    /// the profile holds it, and added with the event column's type when it does not. A date
    /// column takes the time truncated to its day. `late_arrival.rate` is measured against it.
    pub ingest: Option<String>,
}

/// How far a late row's time moves, as a lognormal by its median and 99th percentile.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LagSpread {
    pub family: SpreadFamily,
    pub median: Duration,
    /// Without it, the lognormal's sigma is 1, which puts the 99th percentile about ten times the
    /// median.
    pub p99: Option<Duration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SpreadFamily {
    Lognormal,
}

/// Nulls written into `column`: MCAR uniformly, MAR more often for higher values of `depends_on`,
/// and MNAR more often for the column's own higher values. Each averages `rate` over the rows.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Missingness {
    pub id: Option<String>,
    pub column: String,
    pub mechanism: Mechanism,
    pub rate: Rate,
    /// The column MAR's chance of a null rises with. Only MAR takes it.
    pub depends_on: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
pub enum Mechanism {
    /// Missing completely at random.
    #[serde(rename = "MCAR")]
    Mcar,
    /// Missing at random, given another column.
    #[serde(rename = "MAR")]
    Mar,
    /// Missing not at random: the column's own value decides.
    #[serde(rename = "MNAR")]
    Mnar,
}

/// `rate` of rows take a sentinel from `values`, or a value past the column's extreme on the side
/// `beyond` names. Exactly one of the two is given.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OutOfRange {
    pub id: Option<String>,
    pub column: String,
    pub rate: Rate,
    /// Sentinels, each cast to the column's type.
    pub values: Option<Vec<serde_json::Value>>,
    pub beyond: Option<Side>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Low,
    High,
}

/// Two columns' values exchanged in `rate` of rows. Both must have one type.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SwappedValues {
    pub id: Option<String>,
    pub columns: [String; 2],
    pub rate: Rate,
}

/// The category mix moved: each value named takes its share, and every other value keeps its
/// proportion of what is left. As few rows change as reach the new mix.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CategoricalShift {
    pub id: Option<String>,
    pub column: String,
    pub to: BTreeMap<String, Rate>,
}

/// Gaussian noise of standard deviation `sigma`, in the column's units, added to `rate` of rows.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Noise {
    pub id: Option<String>,
    pub column: String,
    pub rate: Rate,
    #[schemars(range(min = 0))]
    pub sigma: f64,
}

/// `rate` of rows multiplied by `factor`, a unit error such as cents written as dollars.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Scaling {
    pub id: Option<String>,
    pub column: String,
    pub rate: Rate,
    pub factor: f64,
}

/// `rate` of strings broken: one holding a character past ASCII is mojibake, its UTF-8 read as
/// windows-1252, and any other has one character replaced by U+FFFD.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BrokenCharacters {
    pub id: Option<String>,
    pub column: String,
    pub rate: Rate,
}

/// `rate` of rows set far past the trimmed extremes: between `multiplier` and `multiplier + 1`
/// times the span between them, beyond the high extreme, or beyond the low one in a column that
/// goes below zero.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Outliers {
    pub id: Option<String>,
    pub column: String,
    pub rate: Rate,
    #[schemars(range(min = 0))]
    pub multiplier: f64,
}

/// Files whose first row lies at or past `after_fraction` of the rows carry a new shape. Every
/// name but `add`'s and `reorder`'s is a column as the profile names it.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SchemaDrift {
    pub id: Option<String>,
    pub after_fraction: Rate,
    /// Columns that appear, generated from their type alone.
    #[serde(default)]
    pub add: Vec<AddedColumn>,
    #[serde(default)]
    pub drop: Vec<String>,
    #[serde(default)]
    pub rename: BTreeMap<String, String>,
    /// Columns cast to a wider Arrow type, such as int32 to int64.
    #[serde(default)]
    pub widen: BTreeMap<String, String>,
    /// Columns moved to the front in this order, by the names they end with. The rest follow in
    /// their order.
    #[serde(default)]
    pub reorder: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AddedColumn {
    pub name: String,
    /// An Arrow type string, as a profile's `physical_type` spells it.
    #[serde(rename = "type")]
    pub physical_type: String,
}

/// File sizes drawn from `file_rows` in place of the layout's. A caller's own file size still
/// overrides it.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SmallFiles {
    pub id: Option<String>,
    pub file_rows: RowsSpread,
}

/// A count of rows, as a lognormal by its median and 99th percentile.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RowsSpread {
    pub family: SpreadFamily,
    #[schemars(range(min = 1))]
    pub median: f64,
    /// Without it, the lognormal's sigma is 1, which puts the 99th percentile about ten times the
    /// median.
    pub p99: Option<f64>,
}

/// Each value named takes its share of rows, written as the column's type. Every other row keeps
/// its own value, so a partitioned write's directories hold those shares.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PartitionSkew {
    pub id: Option<String>,
    pub column: String,
    pub shares: BTreeMap<String, Rate>,
}

/// A statistic the scenario promises of the generated data, within its bounds.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Expectation {
    pub metric: ExpectedMetric,
    /// The column measured. A metric of the whole table, such as `malformed.rate`, takes none.
    pub column: Option<String>,
    pub at_least: Option<f64>,
    pub at_most: Option<f64>,
}

/// The statistics an expectation can name. Each is measured on the generated data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
pub enum ExpectedMetric {
    /// The share of rows holding the `keys` most frequent values of `column`, with `keys` from the
    /// column's `hot_keys` perturbation.
    #[serde(rename = "hot_keys.share")]
    HotKeysShare,
    /// The share of rows whose `column`, a key each copy shares with its source, repeats an earlier
    /// row's. A later perturbation writing that column is refused, because a copy's key would then
    /// differ from its source's.
    #[serde(rename = "duplicates.rate")]
    DuplicatesRate,
    /// The share of rows whose `column` is earlier than the `ingest` column of its `late_arrival`
    /// perturbation.
    #[serde(rename = "late_arrival.rate")]
    LateArrivalRate,
    /// The share of rows whose `column` is null, the profile's own nulls included.
    #[serde(rename = "missingness.rate")]
    MissingnessRate,
    /// The share of a CSV or TSV dataset's data rows a reader rejects, as a profile's
    /// `source.csv.malformed.rate` records it. It takes no column.
    #[serde(rename = "malformed.rate")]
    MalformedRate,
}

impl ExpectedMetric {
    /// The op whose perturbation the metric measures.
    pub fn op(self) -> &'static str {
        match self {
            Self::HotKeysShare => "hot_keys",
            Self::DuplicatesRate => "duplicates",
            Self::LateArrivalRate => "late_arrival",
            Self::MissingnessRate => "missingness",
            Self::MalformedRate => "malformed_rows",
        }
    }

    /// Whether the metric measures one column, so an expectation of it names `column`, rather than
    /// the whole table.
    pub fn takes_column(self) -> bool {
        match self {
            Self::HotKeysShare | Self::DuplicatesRate | Self::LateArrivalRate | Self::MissingnessRate => true,
            Self::MalformedRate => false,
        }
    }
}

impl Perturbation {
    /// The op's name, as `op` spells it.
    pub fn op_name(&self) -> &'static str {
        match self {
            Self::HotKeys(_) => "hot_keys",
            Self::Duplicates(_) => "duplicates",
            Self::LateArrival(_) => "late_arrival",
            Self::Missingness(_) => "missingness",
            Self::OutOfRange(_) => "out_of_range",
            Self::SwappedValues(_) => "swapped_values",
            Self::CategoricalShift(_) => "categorical_shift",
            Self::Noise(_) => "noise",
            Self::Scaling(_) => "scaling",
            Self::BrokenCharacters(_) => "broken_characters",
            Self::Outliers(_) => "outliers",
            Self::SchemaDrift(_) => "schema_drift",
            Self::SmallFiles(_) => "small_files",
            Self::PartitionSkew(_) => "partition_skew",
            Self::MalformedRows(_) => "malformed_rows",
            Self::Cdc(_) => "cdc",
        }
    }

    /// The id given in the file, if any.
    pub fn given_id(&self) -> Option<&str> {
        match self {
            Self::HotKeys(op) => op.id.as_deref(),
            Self::Duplicates(op) => op.id.as_deref(),
            Self::LateArrival(op) => op.id.as_deref(),
            Self::Missingness(op) => op.id.as_deref(),
            Self::OutOfRange(op) => op.id.as_deref(),
            Self::SwappedValues(op) => op.id.as_deref(),
            Self::CategoricalShift(op) => op.id.as_deref(),
            Self::Noise(op) => op.id.as_deref(),
            Self::Scaling(op) => op.id.as_deref(),
            Self::BrokenCharacters(op) => op.id.as_deref(),
            Self::Outliers(op) => op.id.as_deref(),
            Self::SchemaDrift(op) => op.id.as_deref(),
            Self::SmallFiles(op) => op.id.as_deref(),
            Self::PartitionSkew(op) => op.id.as_deref(),
            Self::MalformedRows(op) => op.id.as_deref(),
            Self::Cdc(op) => op.id.as_deref(),
        }
    }

    /// The column part of the default id: the column, both columns joined by a comma, or `table`.
    fn id_subject(&self) -> String {
        match self {
            Self::HotKeys(HotKeys { column, .. })
            | Self::LateArrival(LateArrival { column, .. })
            | Self::Missingness(Missingness { column, .. })
            | Self::OutOfRange(OutOfRange { column, .. })
            | Self::CategoricalShift(CategoricalShift { column, .. })
            | Self::Noise(Noise { column, .. })
            | Self::Scaling(Scaling { column, .. })
            | Self::BrokenCharacters(BrokenCharacters { column, .. })
            | Self::Outliers(Outliers { column, .. })
            | Self::PartitionSkew(PartitionSkew { column, .. }) => column.clone(),
            Self::SwappedValues(SwappedValues { columns, .. }) => columns.join(","),
            Self::Duplicates(_) | Self::SchemaDrift(_) | Self::SmallFiles(_) | Self::MalformedRows(_) | Self::Cdc(_) => "table".into(),
        }
    }

    /// The columns the op writes, adds, drops or renames, by the names the profile and the
    /// scenario give them.
    pub fn written_columns(&self) -> Vec<&str> {
        match self {
            Self::HotKeys(HotKeys { column, .. })
            | Self::Missingness(Missingness { column, .. })
            | Self::OutOfRange(OutOfRange { column, .. })
            | Self::CategoricalShift(CategoricalShift { column, .. })
            | Self::Noise(Noise { column, .. })
            | Self::Scaling(Scaling { column, .. })
            | Self::BrokenCharacters(BrokenCharacters { column, .. })
            | Self::Outliers(Outliers { column, .. })
            | Self::PartitionSkew(PartitionSkew { column, .. }) => vec![column],
            Self::LateArrival(LateArrival { column, ingest, .. }) => {
                [Some(column), ingest.as_ref()].into_iter().flatten().map(String::as_str).collect()
            }
            Self::SwappedValues(SwappedValues { columns, .. }) => columns.iter().map(String::as_str).collect(),
            Self::SchemaDrift(drift) => (drift.drop.iter())
                .chain(drift.rename.keys())
                .chain(drift.rename.values())
                .chain(drift.widen.keys())
                .chain(drift.add.iter().map(|added| &added.name))
                .map(String::as_str)
                .collect(),
            Self::Cdc(cdc) => vec![OPERATION_COLUMN, cdc.timestamp_column()],
            Self::Duplicates(_) | Self::SmallFiles(_) | Self::MalformedRows(_) => Vec::new(),
        }
    }

    /// Whether the op acts across several files, so a write to one file cannot apply it.
    pub fn spans_files(&self) -> bool {
        matches!(self, Self::SchemaDrift(_) | Self::SmallFiles(_) | Self::Cdc(_))
    }

    /// Whether the op orders the whole table, so one process writes the dataset.
    pub fn needs_one_process(&self) -> bool {
        match self {
            Self::Cdc(_) => true,
            Self::SchemaDrift(_)
            | Self::SmallFiles(_)
            | Self::MalformedRows(_)
            | Self::HotKeys(_)
            | Self::Duplicates(_)
            | Self::LateArrival(_)
            | Self::Missingness(_)
            | Self::OutOfRange(_)
            | Self::SwappedValues(_)
            | Self::CategoricalShift(_)
            | Self::Noise(_)
            | Self::Scaling(_)
            | Self::BrokenCharacters(_)
            | Self::Outliers(_)
            | Self::PartitionSkew(_) => false,
        }
    }

    /// Whether the op shapes the files a write makes rather than the rows, so a run writing no
    /// files cannot apply it.
    pub fn shapes_files(&self) -> bool {
        match self {
            Self::SchemaDrift(_) | Self::SmallFiles(_) | Self::MalformedRows(_) | Self::Cdc(_) => true,
            Self::HotKeys(_)
            | Self::Duplicates(_)
            | Self::LateArrival(_)
            | Self::Missingness(_)
            | Self::OutOfRange(_)
            | Self::SwappedValues(_)
            | Self::CategoricalShift(_)
            | Self::Noise(_)
            | Self::Scaling(_)
            | Self::BrokenCharacters(_)
            | Self::Outliers(_)
            | Self::PartitionSkew(_) => false,
        }
    }
}

/// `profile` with `patch` applied as RFC 7396 does, its `columns` matched by name, and each column
/// the patch names that the profile does not hold.
pub fn patched_profile(profile: &Value, patch: &ProfilePatch) -> (Value, Vec<String>) {
    let mut patched = profile.clone();
    let mut rest = patch.0.clone();
    let columns = rest.remove("columns");
    merge_patch(&mut patched, &Value::Object(rest));
    let Some(Value::Object(columns)) = columns else {
        return (patched, Vec::new());
    };
    let name_of = |column: &Value| column.get("name").and_then(Value::as_str).unwrap_or_default().to_string();
    let held: Vec<String> = profile
        .get("columns")
        .and_then(Value::as_array)
        .map(|listed| listed.iter().map(name_of).collect())
        .unwrap_or_default();
    let unknown = columns.keys().filter(|name| !held.contains(name)).cloned().collect();
    if let Some(Value::Array(listed)) = patched.get_mut("columns") {
        listed.retain(|column| !columns.get(&name_of(column)).is_some_and(Value::is_null));
        for column in listed.iter_mut() {
            if let Some(change) = columns.get(&name_of(column)) {
                merge_patch(column, change);
            }
        }
    }
    (patched, unknown)
}

/// `patch` applied to `target`: a mapping merges key by key, null removes a key, and anything else
/// replaces.
fn merge_patch(target: &mut Value, patch: &Value) {
    let Value::Object(changes) = patch else {
        *target = patch.clone();
        return;
    };
    if !target.is_object() {
        *target = Value::Object(Map::new());
    }
    let Value::Object(fields) = target else {
        unreachable!("the target was made a mapping above");
    };
    for (key, change) in changes {
        if change.is_null() {
            fields.remove(key);
        } else {
            merge_patch(fields.entry(key.clone()).or_insert(Value::Null), change);
        }
    }
}

/// Two perturbations would share an id, and so a stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicateId {
    pub index: usize,
    pub id: String,
}

impl Scenario {
    /// The profile the engine generates from: `profile` as given, with the scenario's patch
    /// applied. A patched column the profile does not hold is refused.
    pub fn patched_profile(&self, profile: &Value) -> Result<Value, PlanError> {
        let Some(patch) = &self.patch else {
            return Ok(profile.clone());
        };
        let (patched, unknown) = patched_profile(profile, patch);
        match unknown.first() {
            None => Ok(patched),
            Some(name) => Err(PlanError::Scenario {
                code: ScenarioFault::UnknownColumn,
                field: format!("/patch/columns/{}", pointer_token(name)),
                message: format!("{name} is not a column of the profile, so the patch cannot change it"),
            }),
        }
    }

    /// Each perturbation's id, in list order: the one given, or `<op>:<subject>:<n>`, where n
    /// counts earlier entries of the same op and subject.
    pub fn resolved_ids(&self) -> Result<Vec<String>, DuplicateId> {
        let mut earlier: BTreeMap<(&str, String), usize> = BTreeMap::new();
        let mut ids: Vec<String> = Vec::with_capacity(self.perturbations.len());
        for (index, perturbation) in self.perturbations.iter().enumerate() {
            let counted = earlier.entry((perturbation.op_name(), perturbation.id_subject())).or_default();
            let id = match perturbation.given_id() {
                Some(id) => id.to_string(),
                None => format!("{}:{}:{counted}", perturbation.op_name(), perturbation.id_subject()),
            };
            *counted += 1;
            if ids.contains(&id) {
                return Err(DuplicateId { index, id });
            }
            ids.push(id);
        }
        Ok(ids)
    }
}

/// The scenario's JSON Schema (Draft 2020-12), as the library ships it.
pub fn json_schema() -> String {
    let mut schema = schemars::schema_for!(Scenario);
    schema.insert("title".into(), "doppeldata scenario".into());
    let mut text = serde_json::to_string_pretty(&schema).expect("a derived schema always serializes");
    text.push('\n');
    text
}
