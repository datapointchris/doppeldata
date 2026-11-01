//! A bundle's write. Each table is written under `<table>/` as a dataset of its own, by the calls
//! that write one table. The bundle's destination holds the manifest naming each: claimed as
//! `_manifest.partial.json` before the first table, and stored as `_manifest.json` once all are whole.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use doppeldata_core::BundlePlan;
use doppeldata_core::manifest::BundleManifest;
use doppeldata_core::manifest::BundleManifestKind;
use doppeldata_core::manifest::BundleTable;
use doppeldata_core::manifest::MANIFEST_FILE_NAME;
use doppeldata_core::manifest::Manifest;
use doppeldata_core::manifest::ManifestIdentity;
use doppeldata_core::manifest::ManifestProfile;
use doppeldata_core::manifest::ManifestScenario;
use doppeldata_core::manifest::PARTIAL_MANIFEST_FILE_NAME;
use object_store::ObjectMeta;
use object_store::PutMode;

use crate::WriteError;
use crate::destination::Destination;
use crate::write::RunOptions;
use crate::write::WriteFailure;
use crate::write::check_identity;
use crate::write::check_regenerable;
use crate::write::now_rfc3339;
use crate::write::on_upload_runtime;

/// The bundle's manifest before any table is written: every table listed, and all of it partial.
pub fn bundle_manifest(
    plan: &BundlePlan,
    profile: ManifestProfile,
    scenario: Option<ManifestScenario>,
    destination: &Destination,
) -> BundleManifest {
    let tables = plan
        .tables()
        .map(|(name, table)| BundleTable {
            name: name.to_string(),
            manifest: format!("{name}/{MANIFEST_FILE_NAME}"),
            dataset_sha256: table.identity().sha256(),
            dataset_rows: table.identity().rows(),
            partial: true,
        })
        .collect();
    BundleManifest {
        kind: BundleManifestKind::Bundle,
        tool: format!("doppeldata {}", doppeldata_core::VERSION),
        dataset_sha256: plan.identity().sha256(),
        identity: ManifestIdentity::from(plan.identity()),
        profile,
        scenario,
        destination: destination.url.clone(),
        tables,
        set_aside: plan.set_aside(),
        partial: true,
        threads: 0,
        tasks: None,
        elapsed_ms: 0,
        written_at: now_rfc3339(),
    }
}

/// Makes `destination` the bundle's and claims it with `claim`, as [`bundle_manifest`] makes it.
///
/// A destination holding anything is refused unless `replace` is set. With it, every object must be
/// a bundle manifest at the prefix, or lie under a table directory one of them or `claim` names and
/// be what that table's own write would delete, and all of them are deleted. Anything else is
/// refused untouched. Each table is then written to its empty directory.
pub fn claim_bundle(claim: &BundleManifest, run: &RunOptions, destination: &Destination) -> Result<(), WriteFailure> {
    on_upload_runtime(run, |runtime| Ok(runtime.block_on(prepare(destination, run.replace, claim))?))
}

async fn prepare(destination: &Destination, replace: bool, claim: &BundleManifest) -> Result<(), WriteError> {
    let objects = destination.listed(replace).await?;
    if !objects.is_empty() {
        let mut tables: BTreeSet<String> = claim.tables.iter().map(|table| table.name.clone()).collect();
        for name in [MANIFEST_FILE_NAME, PARTIAL_MANIFEST_FILE_NAME] {
            if let Some(recorded) = destination.record::<BundleManifest>(&objects, name).await? {
                tables.extend(recorded.tables.into_iter().map(|table| table.name));
            }
        }
        let records = [
            destination.object(MANIFEST_FILE_NAME)?,
            destination.object(PARTIAL_MANIFEST_FILE_NAME)?,
        ];
        let mut by_table: BTreeMap<String, Vec<ObjectMeta>> = BTreeMap::new();
        for meta in &objects {
            if records.contains(&meta.location) {
                continue;
            }
            let parts: Vec<String> = meta
                .location
                .prefix_match(&destination.prefix)
                .map(|parts| parts.map(|part| part.as_ref().to_string()).collect())
                .unwrap_or_default();
            match parts.as_slice() {
                [table, _, ..] if tables.contains(table) => by_table.entry(table.clone()).or_default().push(meta.clone()),
                _ => {
                    return Err(WriteError::ForeignObject {
                        url: destination.url.clone(),
                        object: meta.location.to_string(),
                    });
                }
            }
        }
        for (table, held) in &by_table {
            destination.within(table)?.refuse_foreign(held).await?;
        }
        destination.delete(&objects).await?;
    }
    destination.claim(claim).await
}

/// Stores the bundle's manifest, each table `written` names marked whole once its manifest is the
/// one the plan makes. With every table whole it is stored as `_manifest.json`, replacing the claim.
/// Otherwise it is stored partial under the claim's name. Either way it is returned. `claim` holds
/// the time the tables were written in, and takes the most threads and tasks any table used.
pub fn commit_bundle(
    mut claim: BundleManifest,
    written: &[Manifest],
    run: &RunOptions,
    destination: &Destination,
) -> Result<BundleManifest, WriteFailure> {
    for manifest in written {
        let Some(table) = claim
            .tables
            .iter_mut()
            .find(|table| Some(table.name.as_str()) == manifest.identity.table.as_deref())
        else {
            return Err(WriteError::Settings(format!("{} is not a table of the bundle", manifest.destination)).into());
        };
        if manifest.dataset_sha256 != table.dataset_sha256 {
            return Err(WriteError::IdentityMismatch {
                plan: table.dataset_sha256.clone(),
                manifest: manifest.dataset_sha256.clone(),
                differing: Vec::new(),
            }
            .into());
        }
        table.partial = manifest.partial;
    }
    claim.partial = claim.tables.iter().any(|table| table.partial);
    claim.threads = written.iter().map(|manifest| manifest.threads).max().unwrap_or(0);
    claim.tasks = written.iter().filter_map(|manifest| manifest.tasks).max();
    claim.written_at = now_rfc3339();
    on_upload_runtime(run, |runtime| {
        if claim.partial {
            runtime.block_on(destination.put_manifest(PARTIAL_MANIFEST_FILE_NAME, &claim, PutMode::Overwrite))?;
            return Ok(claim);
        }
        runtime.block_on(destination.put_manifest(MANIFEST_FILE_NAME, &claim, PutMode::Overwrite))?;
        if let Err(error) = runtime.block_on(destination.remove(PARTIAL_MANIFEST_FILE_NAME)) {
            return Err(WriteError::ClaimNotRemoved {
                url: destination.url.clone(),
                error: Box::new(error),
            }
            .into());
        }
        Ok(claim)
    })
}

/// Refuses a bundle's `manifest` that cannot be written again table by table from `plan`, before
/// any table is: a partial one, one whose identity is not the plan's, one listing a table's
/// manifest anywhere but `<table>/_manifest.json`, and one whose table manifest in `tables`
/// [`check_regenerable`] refuses. `tables` holds each table manifest the caller found, by table.
/// A table it lacks is the caller's to refuse, because only the caller knows where it looked.
pub fn check_bundle_regenerable(
    plan: &BundlePlan,
    manifest: &BundleManifest,
    tables: &BTreeMap<String, Manifest>,
) -> Result<(), WriteError> {
    if manifest.partial {
        return Err(WriteError::PartialManifest);
    }
    check_identity(plan.identity(), &manifest.dataset_sha256, &manifest.identity)?;
    for listed in &manifest.tables {
        if listed.manifest != format!("{}/{MANIFEST_FILE_NAME}", listed.name) {
            return Err(WriteError::TableManifestElsewhere {
                table: listed.name.clone(),
                manifest: listed.manifest.clone(),
            });
        }
    }
    for (name, table) in plan.tables() {
        if let Some(recorded) = tables.get(name) {
            check_regenerable(table, recorded)?;
        }
    }
    Ok(())
}
