//! Where a dataset's objects go: a store the caller built, and a prefix within it.

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use doppeldata_core::manifest::FAULTS_FILE_NAME;
use doppeldata_core::manifest::MANIFEST_FILE_NAME;
use doppeldata_core::manifest::Manifest;
use doppeldata_core::manifest::PARTIAL_MANIFEST_FILE_NAME;
use doppeldata_core::names_stream_file;
use futures::TryStreamExt;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::PutMode;
use object_store::PutOptions;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::WriteError;
use crate::layout::PART_PREFIX;
use crate::layout::identity8;

/// What a destination URL names, read before any store is built for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DestinationUrl {
    /// A local directory.
    Local(std::path::PathBuf),
    /// A bucket and the prefix under it, from `s3://` or Hadoop's `s3a://`.
    S3 { bucket: String, prefix: Path },
    /// `memory://`, a store that keeps nothing once the write returns.
    Memory,
}

impl DestinationUrl {
    /// The destination `url` names, or a refusal of a scheme doppeldata does not write to.
    pub fn parse(url: &str) -> Result<Self, WriteError> {
        if url == "memory://" {
            return Ok(Self::Memory);
        }
        if let Some(rest) = url.strip_prefix("s3://").or_else(|| url.strip_prefix("s3a://")) {
            let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
            if bucket.is_empty() {
                return Err(WriteError::Settings(format!("{url} names no bucket")));
            }
            let prefix = Path::parse(prefix.trim_end_matches('/')).map_err(|error| WriteError::ObjectName {
                name: prefix.into(),
                reason: error.to_string(),
            })?;
            return Ok(Self::S3 {
                bucket: bucket.into(),
                prefix,
            });
        }
        if url.contains("://") {
            return Err(WriteError::Settings(format!(
                "{url} is not a local path, an s3:// or s3a:// URL, or memory://, the destinations doppeldata writes to"
            )));
        }
        Ok(Self::Local(url.into()))
    }
}

/// A store and the prefix the dataset is written under. The writers build no store, so credentials
/// stay the caller's.
#[derive(Debug, Clone)]
pub struct Destination {
    pub store: Arc<dyn ObjectStore>,
    pub prefix: Path,
    /// The destination as the manifest names it, such as `s3://bucket/orders/`.
    pub url: String,
}

impl Destination {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: Path, url: impl Into<String>) -> Self {
        Self {
            store,
            prefix,
            url: url.into(),
        }
    }

    /// A local directory, created if missing. Each object is written to a temporary name and
    /// renamed into place once complete.
    pub fn local(dir: impl AsRef<std::path::Path>) -> Result<Self, WriteError> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let store = LocalFileSystem::new_with_prefix(dir)?;
        Ok(Self::new(Arc::new(store), Path::default(), dir.display().to_string()))
    }

    pub(crate) fn object(&self, relative: &str) -> Result<Path, WriteError> {
        let mut path = self.prefix.clone();
        for segment in relative.split('/') {
            let part = object_store::path::PathPart::parse(segment).map_err(|error| WriteError::ObjectName {
                name: relative.into(),
                reason: error.to_string(),
            })?;
            path = path.join(part);
        }
        Ok(path)
    }

    /// Makes the destination this run's, then claims it by creating `claim` as
    /// `_manifest.partial.json`, which fails when another run claimed it first.
    ///
    /// A destination holding anything is refused unless `replace` is set. With it, every object
    /// must be a doppeldata manifest or `_faults.jsonl` at the prefix, a part named for the
    /// dataset one of those manifests records, or a file named as a `cdc` stream one of them
    /// records names its own, and all of them are deleted. Anything else is refused untouched.
    pub(crate) async fn prepare(&self, replace: bool, claim: &Manifest) -> Result<(), WriteError> {
        let objects = self.listed(replace).await?;
        if !objects.is_empty() {
            self.refuse_foreign(&objects).await?;
            self.delete(&objects).await?;
        }
        self.claim(claim).await
    }

    /// The destination `segment` names under this one, such as a bundle's table.
    pub fn within(&self, segment: &str) -> Result<Self, WriteError> {
        Ok(Self {
            store: Arc::clone(&self.store),
            prefix: self.object(segment)?,
            url: format!("{}/{segment}", self.url.trim_end_matches('/')),
        })
    }

    /// Every object under the prefix, refused unless `replace` is set where there is any.
    pub(crate) async fn listed(&self, replace: bool) -> Result<Vec<ObjectMeta>, WriteError> {
        let objects: Vec<ObjectMeta> = self.store.list(Some(&self.prefix)).try_collect().await?;
        match objects.first() {
            Some(first) if !replace => Err(WriteError::DestinationNotEmpty {
                url: self.url.clone(),
                objects: objects.len(),
                example: first.location.to_string(),
            }),
            _ => Ok(objects),
        }
    }

    /// Refuses `objects` unless each is a record or a part a manifest at the prefix accounts for.
    pub(crate) async fn refuse_foreign(&self, objects: &[ObjectMeta]) -> Result<(), WriteError> {
        let (parts, streams) = self.recorded_parts(objects).await?;
        let records = [
            self.object(MANIFEST_FILE_NAME)?,
            self.object(PARTIAL_MANIFEST_FILE_NAME)?,
            self.object(FAULTS_FILE_NAME)?,
        ];
        let ours = |path: &Path| {
            records.contains(path)
                || path.filename().and_then(part_suffix).is_some_and(|suffix| parts.contains(suffix))
                || streams.iter().any(|stream| stream.owns(path))
        };
        match objects.iter().find(|meta| !ours(&meta.location)) {
            Some(foreign) => Err(WriteError::ForeignObject {
                url: self.url.clone(),
                object: foreign.location.to_string(),
            }),
            None => Ok(()),
        }
    }

    pub(crate) async fn delete(&self, objects: &[ObjectMeta]) -> Result<(), WriteError> {
        for meta in objects {
            self.store.delete(&meta.location).await?;
        }
        Ok(())
    }

    /// Creates `claim` as `_manifest.partial.json`, which fails when another run claimed it first.
    pub(crate) async fn claim(&self, claim: &impl Serialize) -> Result<(), WriteError> {
        self.put_manifest(PARTIAL_MANIFEST_FILE_NAME, claim, PutMode::Create)
            .await
            .map_err(|error| match error {
                WriteError::Store(object_store::Error::AlreadyExists { .. }) => WriteError::DestinationClaimed { url: self.url.clone() },
                WriteError::Store(object_store::Error::NotImplemented { .. }) => WriteError::Unclaimable { url: self.url.clone() },
                other => other,
            })
    }

    /// The JSON object stored at `name` under the prefix, or None where there is none. One that is
    /// not a `T` is refused as foreign.
    pub(crate) async fn record<T: DeserializeOwned>(&self, objects: &[ObjectMeta], name: &str) -> Result<Option<T>, WriteError> {
        let path = self.object(name)?;
        if !objects.iter().any(|meta| meta.location == path) {
            return Ok(None);
        }
        let bytes = self.store.get(&path).await?.bytes().await?;
        serde_json::from_slice(&bytes).map(Some).map_err(|_| WriteError::ForeignObject {
            url: self.url.clone(),
            object: path.to_string(),
        })
    }

    /// The `{identity8}.{extension}` every part of a dataset recorded by a manifest at the prefix
    /// ends with, and each `cdc` stream a manifest there records. A manifest there that does not
    /// parse is refused as foreign.
    async fn recorded_parts(&self, objects: &[ObjectMeta]) -> Result<(BTreeSet<String>, Vec<RecordedStream>), WriteError> {
        let mut parts = BTreeSet::new();
        let mut streams = Vec::new();
        for name in [MANIFEST_FILE_NAME, PARTIAL_MANIFEST_FILE_NAME] {
            let Some(manifest) = self.record::<Manifest>(objects, name).await? else {
                continue;
            };
            let identity8 = identity8(&manifest.dataset_sha256);
            parts.insert(format!("{identity8}.{}", manifest.output.extension()));
            if let Some(cdc) = &manifest.cdc {
                streams.push(RecordedStream {
                    prefix: self.object(cdc.prefix.trim_end_matches('/'))?,
                    expected_prefix: self.object(cdc.expected_prefix.trim_end_matches('/'))?,
                    extension: manifest.output.extension(),
                    expected_suffix: format!("{identity8}.parquet"),
                });
            }
        }
        Ok((parts, streams))
    }

    pub(crate) async fn put_manifest(&self, name: &str, manifest: &impl Serialize, mode: PutMode) -> Result<(), WriteError> {
        let mut text = serde_json::to_vec_pretty(manifest).expect("a manifest always serializes");
        text.push(b'\n');
        let options = PutOptions {
            mode,
            ..PutOptions::default()
        };
        self.store.put_opts(&self.object(name)?, Bytes::from(text).into(), options).await?;
        Ok(())
    }

    pub(crate) async fn remove(&self, name: &str) -> Result<(), WriteError> {
        self.store.delete(&self.object(name)?).await?;
        Ok(())
    }
}

/// Where a `cdc` stream a manifest records lies, and the names its files take.
#[derive(Debug)]
struct RecordedStream {
    prefix: Path,
    expected_prefix: Path,
    extension: String,
    /// What an expected part's name holds after `part-` and its range digits.
    expected_suffix: String,
}

impl RecordedStream {
    /// Whether the stream writes `path`: a full-load or change file under its prefix, or a part
    /// of its expected table directly under the expected prefix.
    fn owns(&self, path: &Path) -> bool {
        if let Some(parts) = path.prefix_match(&self.expected_prefix) {
            let parts: Vec<_> = parts.collect();
            return matches!(parts.as_slice(), [name] if part_suffix(name.as_ref()) == Some(self.expected_suffix.as_str()));
        }
        path.prefix_match(&self.prefix).is_some_and(|parts| {
            let name: Vec<String> = parts.map(|part| part.as_ref().to_string()).collect();
            names_stream_file(&name.join("/"), &self.extension)
        })
    }
}

/// What a part's file name holds after `part-` and its range digits: `1f2e3d4c.parquet` from
/// `part-00007-1f2e3d4c.parquet`. None for any other name.
fn part_suffix(name: &str) -> Option<&str> {
    let (range, suffix) = name.strip_prefix(PART_PREFIX)?.split_once('-')?;
    (range.len() >= 5 && range.bytes().all(|byte| byte.is_ascii_digit())).then_some(suffix)
}
