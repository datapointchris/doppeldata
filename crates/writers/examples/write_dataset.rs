//! Writes a bundled example profile's dataset to a local directory, an `s3://` URL or `memory://`,
//! and prints its rate and its manifest's files. `--regenerate MANIFEST` writes that manifest's
//! files again instead. An `s3://` destination takes its credentials and endpoint from the `AWS_*`
//! environment variables. `memory://` times generation and encoding with no disk or network.
//!
//! cargo run --release -p doppeldata-writers --example write_dataset -- eight-columns --rows 10000000 --file-rows 1000000 --out memory://
//! cargo run --release -p doppeldata-writers --example write_dataset -- eight-columns --format csv --writer '{"delimiter":"|"}' --out /tmp/csv

use std::env;
use std::fs;
use std::path::Path as FilePath;
use std::process::ExitCode;
use std::sync::Arc;

use doppeldata_core::manifest::Manifest;
use doppeldata_core::profile::Profile;
use doppeldata_core::profile::SourceFormat;
use doppeldata_writers::Destination;
use doppeldata_writers::DestinationUrl;
use doppeldata_writers::RunOptions;
use doppeldata_writers::WriteChoices;
use doppeldata_writers::WriteOptions;
use object_store::aws::AmazonS3Builder;
use object_store::memory::InMemory;
use object_store::path::Path;
use sha2::Digest;

const USAGE: &str = "usage: write_dataset EXAMPLE --out DIR|s3://BUCKET/PREFIX [--rows N] [--seed N] [--threads N] [--file-rows N] \
                     [--format NAME] [--writer JSON] [--partition-by COLUMN] [--partition-style hive|directory] [--replace] \
                     [--regenerate MANIFEST]";

#[derive(Default)]
struct Options {
    example: String,
    out: String,
    rows: u64,
    seed: u64,
    threads: usize,
    file_rows: Option<u64>,
    format: Option<String>,
    writer: Option<String>,
    partition_by: Vec<String>,
    partition_style: Option<String>,
    replace: bool,
    regenerate: Option<String>,
}

fn options() -> Result<Options, String> {
    let mut args = env::args().skip(1);
    let mut options = Options {
        example: args.next().ok_or(USAGE)?,
        rows: 10_000_000,
        seed: 42,
        ..Options::default()
    };
    while let Some(flag) = args.next() {
        if flag == "--replace" {
            options.replace = true;
            continue;
        }
        let value = args.next().ok_or_else(|| format!("{flag} needs a value\n{USAGE}"))?;
        let number = || {
            value
                .parse::<u64>()
                .map_err(|_| format!("{flag} takes a whole number, not {value:?}"))
        };
        match flag.as_str() {
            "--out" => options.out = value.clone(),
            "--rows" => options.rows = number()?,
            "--seed" => options.seed = number()?,
            "--threads" => options.threads = number()? as usize,
            "--file-rows" => options.file_rows = Some(number()?),
            "--format" => options.format = Some(value.clone()),
            "--writer" => options.writer = Some(value.clone()),
            "--partition-by" => options.partition_by.push(value.clone()),
            "--partition-style" => options.partition_style = Some(value.clone()),
            "--regenerate" => options.regenerate = Some(value.clone()),
            _ => return Err(format!("{flag} is not a flag\n{USAGE}")),
        }
    }
    if options.out.is_empty() {
        return Err(format!("--out is required\n{USAGE}"));
    }
    Ok(options)
}

/// The example profile, and a stand-in SHA-256 over its file. The library computes the real one
/// from the profile's canonical JSON.
fn example_profile(name: &str) -> Result<(Profile, String), String> {
    let path = FilePath::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../library/src/doppeldata/examples")
        .join(format!("{name}.profile.yaml"));
    let text = fs::read_to_string(&path).map_err(|error| format!("{}: {error}", path.display()))?;
    let document: serde_json::Value = serde_saphyr::from_str(&text).map_err(|error| error.to_string())?;
    let profile = serde_json::from_value(document).map_err(|error| error.to_string())?;
    let hex = sha2::Sha256::digest(text.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok((profile, format!("sha256:{hex}")))
}

fn destination(out: &str) -> Result<Destination, String> {
    match DestinationUrl::parse(out).map_err(|error| error.to_string())? {
        DestinationUrl::Memory => Ok(Destination::new(Arc::new(InMemory::new()), Path::default(), out)),
        DestinationUrl::S3 { bucket, prefix } => {
            let store = AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .build()
                .map_err(|error| error.to_string())?;
            Ok(Destination::new(Arc::new(store), prefix, out))
        }
        DestinationUrl::Local(dir) => Destination::local(dir).map_err(|error| error.to_string()),
    }
}

fn source_format(name: &str) -> Result<SourceFormat, String> {
    serde_json::from_value(serde_json::Value::from(name)).map_err(|_| format!("{name:?} is not a format"))
}

fn run() -> Result<(), String> {
    let options = options()?;
    let (profile, profile_sha256) = example_profile(&options.example)?;
    let run = RunOptions {
        threads: options.threads,
        replace: options.replace,
        ..RunOptions::default()
    };
    let destination = destination(&options.out)?;
    let result = match &options.regenerate {
        Some(path) => {
            let text = fs::read_to_string(path).map_err(|error| format!("{path}: {error}"))?;
            let recorded: Manifest = serde_json::from_str(&text).map_err(|error| format!("{path}: {error}"))?;
            let plan = doppeldata_core::plan(&profile, &profile_sha256, recorded.identity.seed, recorded.identity.dataset_rows)
                .map_err(|error| error.to_string())?;
            doppeldata_writers::regenerate(&plan, &recorded, &run, &destination)
        }
        None => {
            let plan = doppeldata_core::plan(&profile, &profile_sha256, options.seed, options.rows).map_err(|error| error.to_string())?;
            let choices = WriteChoices {
                format: options.format.as_deref().map(source_format).transpose()?,
                writer: options
                    .writer
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(|error| format!("--writer: {error}"))?,
                file_rows: options.file_rows,
                partition_by: Some(options.partition_by.clone()).filter(|keys| !keys.is_empty()),
                partition_style: options
                    .partition_style
                    .as_deref()
                    .map(|style| {
                        serde_json::from_value(serde_json::Value::from(style)).map_err(|_| format!("{style:?} is not hive or directory"))
                    })
                    .transpose()?,
                profile_path: Some(format!("{}.profile.yaml", options.example)),
                scenario: None,
                embedded_profile: None,
                embedded_scenario: None,
            };
            let write = WriteOptions::chosen(&profile, None, choices).map_err(|error| error.to_string())?;
            doppeldata_writers::write(&plan, &write, &run, &destination)
        }
    };
    let manifest = result.map_err(|failure| match failure.manifest {
        Some(manifest) => format!(
            "{}\nthe manifest lists {} files, partial: {}, stored: {}{}",
            failure.error,
            manifest.files.len(),
            manifest.partial,
            failure.manifest_stored,
            failure.manifest_error.map(|error| format!(" ({error})")).unwrap_or_default()
        ),
        None => failure.error.to_string(),
    })?;
    let bytes: u64 = manifest.files.iter().map(|file| file.bytes).sum();
    let rows = manifest.identity.dataset_rows;
    println!(
        "{} rows as {} on {} threads in {} ms: {:.2}M rows/s, {} files, {:.1} MB",
        rows,
        manifest.output.format_name(),
        manifest.threads,
        manifest.elapsed_ms,
        rows as f64 / manifest.elapsed_ms.max(1) as f64 / 1e3,
        manifest.files.len(),
        bytes as f64 / 1e6
    );
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
