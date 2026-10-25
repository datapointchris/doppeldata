//! Which rows and files `malformed_rows` breaks. The engine places each fault per block and per
//! file, so a fault's place depends on neither the thread count nor the file layout. The writers
//! render its bytes.

use chrono::DateTime;
use rand::Rng;
use rand::RngExt;
use rand_chacha::ChaCha8Rng;

use super::Planned;
use super::Planning;
use super::ScenarioFault;
use crate::format_settings::LineTerminator;
use crate::plan::PlanError;
use crate::profile::MalformedRow;
use crate::profile::RawValueType;
use crate::rng::BLOCK_ROWS;
use crate::rng::below;
use crate::rng::rng_for;
use crate::rng::stream_key;
use crate::rng::unit;
use crate::scenario::FaultKind;
use crate::scenario::MalformedRows;

/// The line break a record of a `mixed_line_endings` file ends in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineEnd {
    Lf,
    Crlf,
    Cr,
}

/// One row the writers break.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowFault {
    /// The row within its block, or within the piece of it a file takes.
    pub row: u32,
    /// The dataset row it was generated as, or None for a copy `duplicates` added.
    pub dataset_row: Option<u64>,
    pub kind: FaultKind,
    /// The column a value of another type is written in, where the profile's `type_mix` placed
    /// it. None leaves the writer to choose one by `pick`.
    pub column: Option<String>,
    /// The text a `type_invalid` fault from the profile's `type_mix` writes.
    pub text: Option<String>,
    /// The draw a writer chooses the column, the cut or the token by.
    pub pick: u64,
}

/// The faults placed in a block's rows, and each row's line break where a file may mix them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RowFaults {
    /// In row order, at most one a row.
    pub faults: Vec<RowFault>,
    /// One a row, where the op writes `mixed_line_endings`.
    pub line_ends: Option<Vec<LineEnd>>,
}

impl RowFaults {
    /// The faults of the rows `place` keeps, split among `groups`. `place` maps a row to its group
    /// and its place there, and must keep each group's rows in their order.
    pub fn placed(&self, groups: usize, place: impl Fn(u32) -> Option<(usize, u32)>) -> Vec<RowFaults> {
        let mut placed = vec![
            RowFaults {
                faults: Vec::new(),
                line_ends: self.line_ends.as_ref().map(|_| Vec::new()),
            };
            groups
        ];
        for (row, &end) in self.line_ends.iter().flatten().enumerate() {
            if let Some((group, _)) = place(row as u32) {
                placed[group].line_ends.get_or_insert_default().push(end);
            }
        }
        for fault in &self.faults {
            if let Some((group, row)) = place(fault.row) {
                placed[group].faults.push(RowFault { row, ..fault.clone() });
            }
        }
        placed
    }
}

/// The fault one file takes as a whole.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileFault {
    pub kind: FaultKind,
    /// The draw a writer chooses the columns or the renamed header by.
    pub pick: u64,
}

/// What a row of the profile's malformed rows becomes.
#[derive(Debug, Clone, PartialEq)]
enum RowSource {
    Kind(FaultKind),
    /// A value of type `raw` in `column`, as the profile's `type_mix` records.
    Foreign {
        column: String,
        raw: RawValueType,
    },
}

/// `malformed_rows` planned: the kinds it writes, and the rates it places them at.
#[derive(Debug)]
pub struct MalformedPlan {
    index: usize,
    seed: u64,
    /// Each kind the op writes, with the pointer a refusal of it names.
    kinds: Vec<(FaultKind, String)>,
    /// Each row source with the running total of rates up to and including it.
    row_sources: Vec<(f64, RowSource)>,
    file_kinds: Vec<FaultKind>,
    files_share: f64,
    /// A kind every file takes, where the op reproduces the profile's line breaks.
    every_file: Option<FaultKind>,
    /// Each line break with the running total of shares, empty where no file mixes them.
    line_ends: Vec<(f64, LineEnd)>,
    rows_key: u64,
    picks_key: u64,
    texts_key: u64,
    ends_key: u64,
    files_key: u64,
}

/// What a profile's reject type is written as, where a writer reproduces it. A `cast` is a value
/// of another type, and no written bytes give DuckDB an `invalid_state` reject.
fn reproduced_kind(reject: MalformedRow) -> Option<FaultKind> {
    match reject {
        MalformedRow::MissingColumns => Some(FaultKind::RaggedShort),
        MalformedRow::TooManyColumns => Some(FaultKind::RaggedLong),
        MalformedRow::UnquotedValue => Some(FaultKind::UnescapedQuote),
        MalformedRow::LineSizeOverMaximum => Some(FaultKind::LineTooLong),
        MalformedRow::InvalidEncoding => Some(FaultKind::InvalidUtf8),
        MalformedRow::Cast | MalformedRow::InvalidState => None,
    }
}

pub(super) fn plan(op: &MalformedRows, planning: &Planning<'_>) -> Result<Planned, PlanError> {
    let mut plan = MalformedPlan {
        index: planning.index,
        seed: planning.seed,
        kinds: Vec::new(),
        row_sources: Vec::new(),
        file_kinds: Vec::new(),
        files_share: 0.0,
        every_file: None,
        line_ends: Vec::new(),
        rows_key: planning.key("rows"),
        picks_key: planning.key("picks"),
        texts_key: planning.key("texts"),
        ends_key: planning.key("line_ends"),
        files_key: planning.key("files"),
    };
    match &op.kinds {
        None if op.rate.is_none() && op.files_share.is_none() => reproduce(&mut plan, planning)?,
        None => {
            return Err(planning.refuse(
                ScenarioFault::InvalidParameter,
                "/kinds",
                "rate and files_share place the kinds named, and kinds names none; leave all three out to reproduce the profile's malformed rows",
            ));
        }
        Some(kinds) => name(&mut plan, planning, op, kinds)?,
    }
    let total = plan.row_sources.last().map_or(0.0, |(total, _)| *total);
    if total > 1.0 {
        return Err(planning.refuse(
            ScenarioFault::InvalidParameter,
            "",
            format!("the profile's malformed rows and values of other types cover {total} of rows, more than every row"),
        ));
    }
    Ok(Planned::Malformed(plan))
}

/// The kinds `kinds` names, placed at `rate` of rows and `files_share` of files.
fn name(plan: &mut MalformedPlan, planning: &Planning<'_>, op: &MalformedRows, kinds: &[FaultKind]) -> Result<(), PlanError> {
    if kinds.is_empty() {
        return Err(planning.refuse(ScenarioFault::InvalidParameter, "/kinds", "kinds names no fault"));
    }
    plan.kinds = kinds
        .iter()
        .enumerate()
        .map(|(at, &kind)| (kind, format!("/perturbations/{}/kinds/{at}", planning.index)))
        .collect();
    let (file_kinds, row_kinds): (Vec<FaultKind>, Vec<FaultKind>) = kinds.iter().partition(|kind| kind.is_file_kind());
    let names = |kinds: &[FaultKind]| kinds.iter().map(|kind| kind.name()).collect::<Vec<_>>().join(", ");
    match (op.rate, row_kinds.is_empty()) {
        (None, false) => {
            let message = format!("kinds names row faults ({}), and rate places them", names(&row_kinds));
            return Err(planning.refuse(ScenarioFault::InvalidParameter, "/rate", message));
        }
        (Some(_), true) => {
            let message = "rate places row faults, and kinds names only faults of whole files";
            return Err(planning.refuse(ScenarioFault::InvalidParameter, "/rate", message));
        }
        (Some(rate), false) => {
            let each = rate.0 / row_kinds.len() as f64;
            plan.row_sources = row_kinds
                .iter()
                .enumerate()
                .map(|(at, &kind)| (each * (at + 1) as f64, RowSource::Kind(kind)))
                .collect();
        }
        (None, true) => {}
    }
    match (op.files_share, file_kinds.is_empty()) {
        (None, false) => {
            let message = format!(
                "kinds names faults of whole files ({}), and files_share places them",
                names(&file_kinds)
            );
            return Err(planning.refuse(ScenarioFault::InvalidParameter, "/files_share", message));
        }
        (Some(_), true) => {
            let message = "files_share places faults of whole files, and kinds names only row faults";
            return Err(planning.refuse(ScenarioFault::InvalidParameter, "/files_share", message));
        }
        (Some(share), false) => {
            plan.files_share = share.0;
            plan.file_kinds = file_kinds.clone();
        }
        (None, true) => {}
    }
    if file_kinds.contains(&FaultKind::MixedLineEndings) {
        plan.line_ends = vec![(1.0 / 3.0, LineEnd::Lf), (2.0 / 3.0, LineEnd::Crlf), (1.0, LineEnd::Cr)];
    }
    Ok(())
}

/// The profile's malformed rows, line-break mix and values of other types, at the rates it
/// records them.
fn reproduce(plan: &mut MalformedPlan, planning: &Planning<'_>) -> Result<(), PlanError> {
    let field = format!("/perturbations/{}", planning.index);
    let csv = planning.table.source.as_ref().and_then(|source| source.csv.as_ref());
    let by_type = csv
        .and_then(|csv| csv.malformed.as_ref())
        .and_then(|malformed| malformed.by_type.as_ref());
    let mut total = 0.0;
    let mut add = |sources: &mut Vec<(f64, RowSource)>, share: f64, source: RowSource| {
        total += share;
        sources.push((total, source));
    };
    for (&reject, share) in by_type.into_iter().flatten() {
        if let Some(kind) = reproduced_kind(reject).filter(|_| share.0 > 0.0) {
            add(&mut plan.row_sources, share.0, RowSource::Kind(kind));
        }
    }
    let mut foreign_values = false;
    for column in planning.table.columns.iter().flatten() {
        let Some(mix) = column.type_mix.as_ref() else {
            continue;
        };
        let Ok(index) = planning.schema.index_of(&column.name) else {
            continue;
        };
        if matches!(
            planning.schema.field(index).data_type(),
            arrow_schema::DataType::Utf8 | arrow_schema::DataType::LargeUtf8
        ) {
            continue;
        }
        let dominant = mix.iter().max_by(|a, b| a.1.0.total_cmp(&b.1.0)).map(|(&raw, _)| raw);
        for (&raw, share) in mix.iter().filter(|&(&raw, share)| Some(raw) != dominant && share.0 > 0.0) {
            foreign_values = true;
            let source = RowSource::Foreign {
                column: column.name.clone(),
                raw,
            };
            add(&mut plan.row_sources, share.0, source);
        }
    }
    let cast = by_type
        .and_then(|by_type| by_type.get(&MalformedRow::Cast))
        .map_or(0.0, |share| share.0);
    if !foreign_values && cast > 0.0 {
        add(&mut plan.row_sources, cast, RowSource::Kind(FaultKind::TypeInvalid));
    }
    let mix: Vec<(LineTerminator, f64)> = csv
        .and_then(|csv| csv.line_terminator_mix.as_ref())
        .into_iter()
        .flatten()
        .filter(|(_, share)| share.0 > 0.0)
        .map(|(&terminator, share)| (terminator, share.0))
        .collect();
    if mix.len() > 1 {
        let whole: f64 = mix.iter().map(|(_, share)| share).sum();
        let mut running = 0.0;
        plan.line_ends = mix
            .iter()
            .map(|&(terminator, share)| {
                running += share / whole;
                let end = match terminator {
                    LineTerminator::Lf => LineEnd::Lf,
                    LineTerminator::Crlf => LineEnd::Crlf,
                };
                (running, end)
            })
            .collect();
        plan.every_file = Some(FaultKind::MixedLineEndings);
    }
    let mut kinds: Vec<FaultKind> = plan
        .row_sources
        .iter()
        .map(|(_, source)| match source {
            RowSource::Kind(kind) => *kind,
            RowSource::Foreign { .. } => FaultKind::TypeInvalid,
        })
        .chain(plan.every_file)
        .collect();
    kinds.sort();
    kinds.dedup();
    if kinds.is_empty() {
        return Err(planning.refuse(
            ScenarioFault::InvalidParameter,
            "",
            "the profile records no malformed rows, mixed line breaks or values of other types to reproduce; name kinds, with a rate or a files_share",
        ));
    }
    plan.kinds = kinds.into_iter().map(|kind| (kind, field.clone())).collect();
    Ok(())
}

/// Text that parses as `raw`, foreign to its column's type.
fn foreign_text(raw: RawValueType, rng: &mut ChaCha8Rng) -> String {
    const DAYS_FROM_EPOCH_TO_2000: i64 = 10_957;
    let instant = |rng: &mut ChaCha8Rng| {
        let seconds = (DAYS_FROM_EPOCH_TO_2000 + rng.random_range(0..10_957i64)) * 86_400;
        DateTime::from_timestamp(seconds + rng.random_range(0..86_400i64), 0).unwrap_or_default()
    };
    match raw {
        RawValueType::Integer => rng.random_range(0..100_000u32).to_string(),
        RawValueType::Decimal => format!("{}.{:02}", rng.random_range(0..10_000u32), rng.random_range(0..100u32)),
        RawValueType::Boolean => if rng.random_bool(0.5) { "true" } else { "false" }.to_string(),
        RawValueType::Date => instant(rng).format("%Y-%m-%d").to_string(),
        RawValueType::Timestamp => instant(rng).format("%Y-%m-%dT%H:%M:%S").to_string(),
        RawValueType::String => {
            let length = rng.random_range(3..=8);
            (0..length).map(|_| char::from(b'a' + rng.random_range(0..26u8))).collect()
        }
    }
}

impl MalformedPlan {
    /// The pointer to the op in its scenario.
    pub fn field(&self) -> String {
        format!("/perturbations/{}", self.index)
    }

    /// Each kind the op writes, with the pointer a refusal of it names.
    pub fn kinds(&self) -> &[(FaultKind, String)] {
        &self.kinds
    }

    /// Whether the op reproduces what the profile records, rather than placing the kinds it names.
    pub fn reproduces_profile(&self) -> bool {
        self.kinds.iter().all(|(_, field)| *field == self.field())
    }

    /// The fault the file of range `range` under `directory` takes as a whole, drawn from the
    /// range and the directory, so the file's place in a write never moves it.
    pub fn file_fault(&self, range: u32, directory: &str) -> Option<FileFault> {
        let key = self.files_key ^ stream_key("", directory, "");
        let mut rng = rng_for(self.seed, key, u64::from(range));
        if let Some(kind) = self.every_file {
            return Some(FileFault {
                kind,
                pick: rng.next_u64(),
            });
        }
        if self.file_kinds.is_empty() || unit(&mut rng) >= self.files_share {
            return None;
        }
        let kind = self.file_kinds[below(&mut rng, self.file_kinds.len() as u32) as usize];
        Some(FileFault {
            kind,
            pick: rng.next_u64(),
        })
    }

    /// The faults of `block`, which holds `rows` rows, the first `generated` of them generated
    /// there and the rest copies.
    pub(crate) fn row_faults(&self, block: u64, generated: usize, rows: usize) -> RowFaults {
        let line_ends = (!self.line_ends.is_empty()).then(|| {
            let mut rng = rng_for(self.seed, self.ends_key, block);
            (0..rows)
                .map(|_| {
                    let draw = unit(&mut rng);
                    self.line_ends
                        .iter()
                        .find(|(upper, _)| draw < *upper)
                        .map_or(LineEnd::Lf, |&(_, end)| end)
                })
                .collect()
        });
        let mut faults = Vec::new();
        if !self.row_sources.is_empty() {
            let mut rows_rng = rng_for(self.seed, self.rows_key, block);
            let mut picks = rng_for(self.seed, self.picks_key, block);
            let mut texts = rng_for(self.seed, self.texts_key, block);
            for row in 0..rows {
                let draw = unit(&mut rows_rng);
                let Some((_, source)) = self.row_sources.iter().find(|(upper, _)| draw < *upper) else {
                    continue;
                };
                let (kind, column, text) = match source {
                    RowSource::Kind(kind) => (*kind, None, None),
                    RowSource::Foreign { column, raw } => {
                        (FaultKind::TypeInvalid, Some(column.clone()), Some(foreign_text(*raw, &mut texts)))
                    }
                };
                faults.push(RowFault {
                    row: row as u32,
                    dataset_row: (row < generated).then(|| block * BLOCK_ROWS + row as u64),
                    kind,
                    column,
                    text,
                    pick: picks.next_u64(),
                });
            }
        }
        RowFaults { faults, line_ends }
    }
}
