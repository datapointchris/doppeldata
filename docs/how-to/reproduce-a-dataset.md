# Reproduce a dataset

A dataset is a pure function of its profile, scenario, seed and row count, and the engine's stream
version. Its `_manifest.json` records each of them. `--manifest` writes the dataset again from the
manifest and the profile, then compares every file with its record. `--embed-profile` puts the
profile in every Parquet or Arrow file, so one file alone regenerates the dataset it came from.

```bash
doppeldata data generate orders.profile.yaml --scenario hot-key --rows 50000 --seed 11 --out first/
doppeldata data generate orders.profile.yaml --manifest first/_manifest.json --out again/

doppeldata data generate orders --rows 100000 --seed 7 --scenario hot-key --embed-profile --out embedded/
doppeldata data generate embedded/part-00000-a3f28bf0.parquet --embed-profile --out again-embedded/
```

```python
import doppeldata as sd

sd.write('orders.profile.yaml', 'first/', rows=50_000, seed=11, scenario=sd.Scenario.example('hot-key'))
manifest = sd.regenerate('orders.profile.yaml', 'first/_manifest.json', 'again/')   # the fresh manifest

sd.write('orders', 'embedded/', rows=100_000, seed=7, embed_profile=True)
profile = sd.Profile.from_data_file('embedded/part-00000-a3f28bf0.parquet')
table = sd.generate('embedded/part-00000-a3f28bf0.parquet')   # its seed and rows, as written
```

## `--manifest` writes the dataset again and compares every file with its record

```text
$ doppeldata data generate orders.profile.yaml --manifest first/_manifest.json --out again/
wrote 50,000 rows as parquet in 1 file, 1.2 MB, in 0.04 s on 16 threads: 1.11M rows/s
again/_manifest.json names dataset sha256:798bab73…
every file matches its record in first/_manifest.json
```

`sd.regenerate` returns the fresh manifest, and raises `WriteFailed` where the command exits
non-zero.

- **The manifest settles the seed, rows, scale, scenario, format, writer settings and layout.**
  PROFILE is the one input given. A flag giving any of the others is refused, exit 2:

  ```text
  $ doppeldata data generate orders.profile.yaml --manifest first/_manifest.json --seed 12 --out other/
  doppeldata: E-USAGE-026: first/_manifest.json settles --seed, and a regeneration takes each value it settles from it; drop it
  ```

- **A profile that plans another dataset is refused before anything is written**, exit 1. The
  refusal names both SHA-256s and each identity key that differs. `stream_version` differs where a
  release moved the values.

  ```text
  $ doppeldata data generate edited.profile.yaml --manifest first/_manifest.json --out other/
  doppeldata: E-WRITE-210: the plan's identity sha256:e54e2852… is not the manifest's sha256:798bab73…, because the two differ in profile_sha256
  ```

- **A file whose bytes moved fails the command, exit 1, naming the file and what differed.** The
  fresh `_manifest.json` is stored first, so the two can be diffed. A manifest committed beside a
  test catches an encoder upgrade or a changed writer default this way. Here one file's record
  was edited:

  ```text
  $ doppeldata data generate orders.profile.yaml --manifest moved.json --out moved/
  doppeldata: E-WRITE-212: 1 regenerated file differs from the manifest's record:
    part-00000-798bab73.parquet: bytes 1198500 recorded, 1198499 written; sha256 sha256:0000…0000 recorded, sha256:a0bbec53… written [E-WRITE-212]
  ```

- **`--json` prints one line per differing file on stderr**, each with the file's path as its
  `field`, and the fresh manifest on stdout once every file matches.
- **A partial manifest is refused as `E-WRITE-211`, exit 1.** So is one whose files embed the
  profile, as `E-WRITE-223`, exit 2, and one recording a write to one file, as `E-WRITE-224`,
  exit 2. A dataset written with `--embed-profile` regenerates from one of its files, as below.
- **`--out` is a directory, an `s3://` URL or `memory://`.** A one-file path is refused as
  `E-WRITE-014`. `--replace` and `--threads` work as on any write.
- **S3 takes `--aws-profile`, or `aws_profile=`, for the manifest and the destination alike**, as
  in `--manifest s3://bucket/orders/_manifest.json --out s3://bucket/again/`.
- **A bundle's manifest regenerates every table, with the bundle as PROFILE.** Each table is
  written under `--out/<table>/` against its own manifest, parents first. A table whose files
  differ is still written, and the error names every differing file as `<table>/<path>`. One
  table's own manifest regenerates that table alone from the bundle.

## The manifest records every input the values depend on

```text
$ jq '{profile, identity}' first/_manifest.json
{
  "profile": {
    "name": "orders",
    "path": "orders.profile.yaml"
  },
  "identity": {
    "dataset_rows": "50000",
    "profile_sha256": "sha256:…",
    "scale": null,
    "scenario_sha256": "sha256:…",
    "seed": "11",
    "stream_version": <n>,
    "table": null
  }
}
```

- **`identity` holds each input, and `dataset_sha256` their SHA-256.** Two writes naming the same
  SHA-256 hold the same values.
- **`profile.path` names the file the write read**, and `identity.profile_sha256` says whether a
  file at that path is still the same profile. Formatting, comments and key order leave its SHA-256
  alone. A regeneration's manifest keeps the `profile` and `scenario` its manifest recorded.
- **`scenario.document` is the scenario as `identity.scenario_sha256` covers it**, so the run
  regenerates with no scenario file. Written to a file, it is a scenario `--scenario` takes.
- **`format`, `writer` and `layout` record every write setting**, defaults included. `--manifest`
  writes with them, so it writes the same bytes under the same doppeldata release.
- **Each file's `rows`, `bytes` and `sha256` are what `--manifest` compares.**

A release that raises the stream version moves the values, and the manifest's `stream_version`
names the one a dataset was made at.

## `--embed-profile` makes each file regenerate its own dataset

```text
$ doppeldata data generate embedded/part-00000-a3f28bf0.parquet --embed-profile --out again-embedded/
doppeldata: seed 7, 100,000 rows, scenario hot-key, as embedded/part-00000-a3f28bf0.parquet was written
wrote 100,000 rows as parquet in 1 file, 2.2 MB, in 0.10 s on 16 threads: 0.99M rows/s
again-embedded/_manifest.json names dataset sha256:…
```

- **Every PROFILE argument takes such a file.** `data generate`, `data plan`, `data check`,
  `profile export` and `profile expand` read the profile from it, and `sd.generate`, `sd.write` and
  the rest take its path.
- **`data generate FILE` takes the seed, rows and scenario the file was written with**, and prints
  each one it took. Giving any of them generates the dataset they make instead.
- **The rewrite holds the same bytes only with `--embed-profile` again.** Without it the
  `dataset_sha256` matches, and each file lacks the profile it held.
- **Only Parquet and Arrow hold it.** Any other format refuses the flag before writing:

  ```text
  doppeldata: E-WRITE-202: a csv file holds no metadata to embed the profile in; embed it in a parquet or arrow write
  ```

- **A profile made from data must pass `profile check` to be embedded**, because each file then
  carries every literal it holds.
- **`profile expand FILE` writes the embedded profile as YAML, as `profile create` writes it.** A
  map's own keys, such as a fit's `params`, come out sorted, because the file holds the profile as
  canonical JSON:

  ```text
  $ doppeldata profile expand embedded/part-00000-a3f28bf0.parquet --out expanded.profile.yaml
  $ head -5 expanded.profile.yaml
  kind: table
  name: orders
  privacy:
    policy: default
    k_min: 11
  ```

  DuckDB reads it as JSON:

  ```sql
  select decode(value) from parquet_kv_metadata('embedded/*.parquet') where decode(key) = 'doppeldata.profile'
  ```

## Why

[Determinism contract](../determinism-contract.md) says what identifies a dataset and why its values
never move with threads, file sizes or format.
[File writers and the manifest](../file-writers-and-manifest.md) says what the manifest records,
and [Profile exports and the embedded profile](../profile-exports-and-embedded-profiles.md) how the
embedded profile is held and checked.
