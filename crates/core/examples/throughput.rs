//! Measures how fast the engine generates a bundled example profile, generation only, on a rayon
//! pool of a chosen size. `--ipc PATH` also writes the batches as an Arrow IPC file, so their null
//! rates and quantiles can be read back in another tool.
//!
//! cargo run --release -p doppeldata-core --example throughput -- eight-columns --rows 10000000 --threads 16

use std::env;
use std::fs;
use std::fs::File;
use std::path::Path;
use std::process::ExitCode;
use std::time::Instant;

use arrow_ipc::writer::FileWriter;
use doppeldata_core::profile::Profile;
use rayon::prelude::*;

const USAGE: &str = "usage: throughput EXAMPLE [--rows N] [--threads N] [--seed N] [--repeat N] [--ipc PATH]";

struct Options {
    example: String,
    rows: u64,
    threads: usize,
    seed: u64,
    repeat: usize,
    ipc: Option<String>,
}

fn options() -> Result<Options, String> {
    let mut args = env::args().skip(1);
    let mut options = Options {
        example: args.next().ok_or(USAGE)?,
        rows: 10_000_000,
        threads: rayon::current_num_threads(),
        seed: 42,
        repeat: 3,
        ipc: None,
    };
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| format!("{flag} needs a value\n{USAGE}"))?;
        let number = || {
            value
                .parse::<u64>()
                .map_err(|_| format!("{flag} takes a whole number, not {value:?}"))
        };
        match flag.as_str() {
            "--rows" => options.rows = number()?,
            "--threads" => options.threads = number()? as usize,
            "--seed" => options.seed = number()?,
            "--repeat" => options.repeat = number()? as usize,
            "--ipc" => options.ipc = Some(value.clone()),
            _ => return Err(format!("{flag} is not a flag\n{USAGE}")),
        }
    }
    Ok(options)
}

fn example_profile(name: &str) -> Result<Profile, String> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../library/src/doppeldata/examples")
        .join(format!("{name}.profile.yaml"));
    let text = fs::read_to_string(&path).map_err(|error| format!("{}: {error}", path.display()))?;
    let document: serde_json::Value = serde_saphyr::from_str(&text).map_err(|error| error.to_string())?;
    serde_json::from_value(document).map_err(|error| error.to_string())
}

fn run() -> Result<(), String> {
    let options = options()?;
    let profile = example_profile(&options.example)?;
    let digest = format!("sha256:{}", "0".repeat(64));
    let plan = doppeldata_core::plan(&profile, &digest, options.seed, options.rows).map_err(|e| e.to_string())?;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(options.threads)
        .build()
        .map_err(|error| error.to_string())?;
    for _ in 0..options.repeat {
        let started = Instant::now();
        let rows = pool
            .install(|| {
                (0..plan.block_count())
                    .into_par_iter()
                    .map(|block| plan.generate_block(block).map(|batch| batch.num_rows()))
                    .sum::<Result<usize, _>>()
            })
            .map_err(|error| error.to_string())?;
        let seconds = started.elapsed().as_secs_f64();
        println!(
            "{} rows on {} threads in {seconds:.3} s: {:.2}M rows/s",
            rows,
            options.threads,
            rows as f64 / seconds / 1e6
        );
    }
    if let Some(path) = options.ipc {
        let file = File::create(&path).map_err(|error| format!("{path}: {error}"))?;
        let mut writer = FileWriter::try_new_buffered(file, &plan.schema()).map_err(|e| e.to_string())?;
        for block in 0..plan.block_count() {
            writer
                .write(&plan.generate_block(block).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
        }
        writer.finish().map_err(|e| e.to_string())?;
        println!("wrote {path}");
    }
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
