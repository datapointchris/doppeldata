//! What a regeneration finds when it compares each object it wrote with the manifest's record of
//! it: the fields whose values differ, or the object only one of the two lists.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;

use doppeldata_core::manifest::ManifestCdc;
use doppeldata_core::manifest::ManifestFile;
use doppeldata_core::manifest::StreamFile;

/// An object whose record in the manifest and record as regenerated disagree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileDifference {
    /// The object's path under the destination, `/`-separated.
    pub path: String,
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
    /// The field's key in the manifest, such as `sha256`.
    pub field: &'static str,
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

/// An object's record, as the fields a regeneration compares.
trait ObjectRecord {
    fn path(&self) -> &str;
    /// Every field but the path, keyed as the manifest keys it, with its value as text.
    fn fields(&self) -> Vec<(&'static str, String)>;
}

impl ObjectRecord for ManifestFile {
    fn path(&self) -> &str {
        &self.path
    }

    fn fields(&self) -> Vec<(&'static str, String)> {
        let partition = serde_json::to_string(&self.partition).expect("a partition always serializes");
        vec![
            ("range", self.range.to_string()),
            ("first_row", self.first_row.to_string()),
            ("range_rows", self.range_rows.to_string()),
            ("rows", self.rows.to_string()),
            ("bytes", self.bytes.to_string()),
            ("sha256", self.sha256.clone()),
            ("partition", partition),
        ]
    }
}

impl ObjectRecord for StreamFile {
    fn path(&self) -> &str {
        &self.path
    }

    fn fields(&self) -> Vec<(&'static str, String)> {
        vec![
            ("rows", self.rows.to_string()),
            ("bytes", self.bytes.to_string()),
            ("sha256", self.sha256.clone()),
        ]
    }
}

/// Every object whose record differs between `recorded` and `written`, or that only one lists, by path.
fn differences<'a, R: ObjectRecord + 'a>(
    recorded: impl IntoIterator<Item = &'a R>,
    written: impl IntoIterator<Item = &'a R>,
) -> Vec<FileDifference> {
    let recorded: BTreeMap<&str, &R> = recorded.into_iter().map(|file| (file.path(), file)).collect();
    let written: BTreeMap<&str, &R> = written.into_iter().map(|file| (file.path(), file)).collect();
    let paths: BTreeSet<&str> = recorded.keys().chain(written.keys()).copied().collect();
    paths
        .into_iter()
        .filter_map(|path| {
            let kind = match (recorded.get(path), written.get(path)) {
                (Some(recorded), Some(written)) => {
                    let fields: Vec<FieldDifference> = recorded
                        .fields()
                        .into_iter()
                        .zip(written.fields())
                        .filter(|((_, recorded), (_, written))| recorded != written)
                        .map(|((field, recorded), (_, written))| FieldDifference { field, recorded, written })
                        .collect();
                    if fields.is_empty() {
                        return None;
                    }
                    DifferenceKind::Fields(fields)
                }
                (Some(_), None) => DifferenceKind::NotWritten,
                (None, _) => DifferenceKind::NotRecorded,
            };
            Some(FileDifference { path: path.into(), kind })
        })
        .collect()
}

/// Every object of `files` whose record differs between the two manifests, or that only one lists.
pub(crate) fn differing_files(recorded: &[ManifestFile], written: &[ManifestFile]) -> Vec<FileDifference> {
    differences(recorded, written)
}

/// Every change file and expected part whose record differs between two manifests' `cdc`
/// sections, or that only one lists.
pub(crate) fn differing_stream_files(recorded: Option<&ManifestCdc>, written: Option<&ManifestCdc>) -> Vec<FileDifference> {
    let listed = |section: Option<&ManifestCdc>| -> Vec<StreamFile> {
        section
            .into_iter()
            .flat_map(|section| section.files.iter().chain(&section.expected))
            .cloned()
            .collect()
    };
    let (recorded, written) = (listed(recorded), listed(written));
    differences(&recorded, &written)
}
