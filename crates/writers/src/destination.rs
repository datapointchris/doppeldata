//! Where a dataset's objects go: a store the caller built, and a prefix within it.

use std::sync::Arc;

use doppeldata_core::manifest::MANIFEST_FILE_NAME;
use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path;

use crate::WriteError;
use crate::layout::PART_PREFIX;

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

    /// Refuses a destination holding any object unless `replace` is set. With it, deletes the
    /// objects an earlier run of this tool wrote, and refuses when any other object is there, so a
    /// reader never finds this run's files mixed with foreign ones.
    pub(crate) async fn prepare(&self, replace: bool) -> Result<(), WriteError> {
        let objects: Vec<_> = self.store.list(Some(&self.prefix)).try_collect().await?;
        let Some(first) = objects.first() else {
            return Ok(());
        };
        if !replace {
            return Err(WriteError::DestinationNotEmpty {
                url: self.url.clone(),
                objects: objects.len(),
                example: first.location.to_string(),
            });
        }
        let ours = |path: &Path| {
            path.filename()
                .is_some_and(|name| name.starts_with(PART_PREFIX) || name == MANIFEST_FILE_NAME)
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
        Ok(())
    }
}
