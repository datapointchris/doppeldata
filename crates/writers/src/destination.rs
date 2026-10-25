//! Where a dataset's objects go: a store the caller built, and a prefix within it.

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use doppeldata_core::manifest::MANIFEST_FILE_NAME;
use doppeldata_core::manifest::Manifest;
use doppeldata_core::manifest::PARTIAL_MANIFEST_FILE_NAME;
use futures::TryStreamExt;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::PutMode;
use object_store::PutOptions;
use object_store::local::LocalFileSystem;
use object_store::path::Path;

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
    /// must be a doppeldata manifest at the prefix, or a part named for the dataset one of those
    /// manifests records, and all of them are deleted. Anything else is refused untouched.
    pub(crate) async fn prepare(&self, replace: bool, claim: &Manifest) -> Result<(), WriteError> {
        let objects: Vec<ObjectMeta> = self.store.list(Some(&self.prefix)).try_collect().await?;
        if let Some(first) = objects.first() {
            if !replace {
                return Err(WriteError::DestinationNotEmpty {
                    url: self.url.clone(),
                    objects: objects.len(),
                    example: first.location.to_string(),
                });
            }
            let parts = self.recorded_parts(&objects).await?;
            let manifests = [self.object(MANIFEST_FILE_NAME)?, self.object(PARTIAL_MANIFEST_FILE_NAME)?];
            let ours = |path: &Path| {
                manifests.contains(path) || path.filename().and_then(part_suffix).is_some_and(|suffix| parts.contains(suffix))
            };
            if let Some(foreign) = objects.iter().find(|meta| !ours(&meta.location)) {
                return Err(WriteError::ForeignObject {
                    url: self.url.clone(),
                    object: foreign.location.to_string(),
                });
            }
            for meta in &objects {
                self.store.delete(&meta.location).await?;
            }
        }
        self.put_manifest(PARTIAL_MANIFEST_FILE_NAME, claim, PutMode::Create)
            .await
            .map_err(|error| match error {
                WriteError::Store(object_store::Error::AlreadyExists { .. }) => WriteError::DestinationClaimed { url: self.url.clone() },
                WriteError::Store(object_store::Error::NotImplemented { .. }) => WriteError::Unclaimable { url: self.url.clone() },
                other => other,
            })
    }

    /// The `{identity8}.{extension}` every part of a dataset recorded by a manifest at the prefix
    /// ends with. A manifest there that does not parse is refused as foreign.
    async fn recorded_parts(&self, objects: &[ObjectMeta]) -> Result<BTreeSet<String>, WriteError> {
        let mut parts = BTreeSet::new();
        for name in [MANIFEST_FILE_NAME, PARTIAL_MANIFEST_FILE_NAME] {
            let path = self.object(name)?;
            if !objects.iter().any(|meta| meta.location == path) {
                continue;
            }
            let bytes = self.store.get(&path).await?.bytes().await?;
            let manifest: Manifest = serde_json::from_slice(&bytes).map_err(|_| WriteError::ForeignObject {
                url: self.url.clone(),
                object: path.to_string(),
            })?;
            parts.insert(format!("{}.{}", identity8(&manifest.dataset_digest), manifest.output.extension()));
        }
        Ok(parts)
    }

    pub(crate) async fn put_manifest(&self, name: &str, manifest: &Manifest, mode: PutMode) -> Result<(), WriteError> {
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

/// What a part's file name holds after `part-` and its range digits: `1f2e3d4c.parquet` from
/// `part-00007-1f2e3d4c.parquet`. None for any other name.
fn part_suffix(name: &str) -> Option<&str> {
    let (range, suffix) = name.strip_prefix(PART_PREFIX)?.split_once('-')?;
    (range.len() >= 5 && range.bytes().all(|byte| byte.is_ascii_digit())).then_some(suffix)
}
