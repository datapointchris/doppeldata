//! What a regeneration finds when it compares each object it wrote with the manifest's record of
//! it: the fields whose values differ, or the object only one of the two lists.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;

use doppeldata_core::manifest::ManifestCdc;
use doppeldata_core::manifest::ManifestFile;
use doppeldata_core::manifest::StreamFile;
use serde::Serialize;
use serde_json::Map;
use serde_json::Value;

/// An object whose record in the manifest and record as regenerated disagree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileDifference {
    /// The object's path under the destination, `/`-separated.
    pub path: String,
    /// The RFC 6901 pointer to the manifest's record of the object, such as `/files/3`, or to the
    /// list that would hold it where the manifest records none.
    pub pointer: String,
    pub kind: DifferenceKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DifferenceKind {
    /// Both records list the object, and these fields hold different values.
    Fields(Vec<FieldDifference>),
    /// The manifest records the object, and the regeneration wrote none at its path.
    NotWritten,
    /// The regeneration wrote the object, and the manifest records none at its path.
    NotRecorded,
}

/// One field of an object's record, as the manifest records it and as the regeneration wrote it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDifference {
    /// The field's key in the record, such as `sha256`.
    pub field: String,
    /// The value as text: a string as it is, anything else as JSON, and `none` where the record
    /// lacks the field.
    pub recorded: String,
    pub written: String,
}

impl FileDifference {
    /// What differs, without the path: each field's two values, or which record lacks the object.
    pub fn what(&self) -> String {
        match &self.kind {
            DifferenceKind::Fields(fields) => fields
                .iter()
                .map(|field| format!("{} {} recorded, {} written", field.field, field.recorded, field.written))
                .collect::<Vec<_>>()
                .join("; "),
            DifferenceKind::NotWritten => "the manifest records it, and the regeneration wrote no such object".into(),
            DifferenceKind::NotRecorded => "the regeneration wrote it, and the manifest records no such object".into(),
        }
    }
}

impl fmt::Display for FileDifference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.path, self.what())
    }
}

/// An object's record, compared over every field it serializes but its path.
trait ObjectRecord: Serialize {
    fn path(&self) -> &str;
}

impl ObjectRecord for ManifestFile {
    fn path(&self) -> &str {
        &self.path
    }
}

impl ObjectRecord for StreamFile {
    fn path(&self) -> &str {
        &self.path
    }
}

/// Every field of `record`, keyed as the manifest keys it.
fn fields(record: &impl Serialize) -> Map<String, Value> {
    match serde_json::to_value(record).expect("a manifest record always serializes") {
        Value::Object(fields) => fields,
        _ => unreachable!("a manifest record serializes as an object"),
    }
}

fn as_text(value: Option<&Value>) -> String {
    match value {
        None => "none".into(),
        Some(Value::String(text)) => text.clone(),
        Some(value) => value.to_string(),
    }
}

/// Each field but the path whose value differs between two records of one object, in the order
/// the recorded one holds them.
fn field_differences(recorded: &impl Serialize, written: &impl Serialize) -> Vec<FieldDifference> {
    let (recorded, written) = (fields(recorded), fields(written));
    let mut keys: Vec<&String> = recorded.keys().collect();
    keys.extend(written.keys().filter(|key| !recorded.contains_key(*key)));
    keys.into_iter()
        .filter(|key| key.as_str() != "path" && recorded.get(*key) != written.get(*key))
        .map(|key| FieldDifference {
            field: key.clone(),
            recorded: as_text(recorded.get(key)),
            written: as_text(written.get(key)),
        })
        .collect()
}

/// Every object whose record differs between `recorded` and `written`, or that only one lists, by
/// path. `list` is the pointer to the records' list in the manifest, such as `/files`.
fn differences<R: ObjectRecord>(list: &str, recorded: &[R], written: &[R]) -> Vec<FileDifference> {
    let recorded: BTreeMap<&str, (usize, &R)> = recorded
        .iter()
        .enumerate()
        .map(|(index, file)| (file.path(), (index, file)))
        .collect();
    let written: BTreeMap<&str, &R> = written.iter().map(|file| (file.path(), file)).collect();
    let paths: BTreeSet<&str> = recorded.keys().chain(written.keys()).copied().collect();
    paths
        .into_iter()
        .filter_map(|path| {
            let (pointer, kind) = match (recorded.get(path), written.get(path)) {
                (Some((index, recorded)), Some(written)) => {
                    let fields = field_differences(*recorded, *written);
                    if fields.is_empty() {
                        return None;
                    }
                    (format!("{list}/{index}"), DifferenceKind::Fields(fields))
                }
                (Some((index, _)), None) => (format!("{list}/{index}"), DifferenceKind::NotWritten),
                (None, _) => (list.to_string(), DifferenceKind::NotRecorded),
            };
            Some(FileDifference {
                path: path.into(),
                pointer,
                kind,
            })
        })
        .collect()
}

/// Every object of `files` whose record differs between the two manifests, or that only one lists.
pub(crate) fn differing_files(recorded: &[ManifestFile], written: &[ManifestFile]) -> Vec<FileDifference> {
    differences("/files", recorded, written)
}

/// Every change file and expected part whose record differs between two manifests' `cdc`
/// sections, or that only one lists.
pub(crate) fn differing_stream_files(recorded: Option<&ManifestCdc>, written: Option<&ManifestCdc>) -> Vec<FileDifference> {
    let mut differ = differences("/cdc/files", change_files(recorded), change_files(written));
    differ.extend(differences("/cdc/expected", expected_parts(recorded), expected_parts(written)));
    differ
}

fn change_files(section: Option<&ManifestCdc>) -> &[StreamFile] {
    section.map_or(&[], |section| &section.files)
}

fn expected_parts(section: Option<&ManifestCdc>) -> &[StreamFile] {
    section.map_or(&[], |section| &section.expected)
}
