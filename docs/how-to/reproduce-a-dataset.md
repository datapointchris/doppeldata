# Reproduce a dataset

A dataset is a pure function of its profile, scenario, seed and row count, and the engine's stream
version. Its `_manifest.json` records each of them, so the same inputs write the same values
anywhere, and the same files under one release. `--embed-profile` puts the profile in every Parquet
or Arrow file, so one file alone regenerates the dataset it came from.

```bash
doppeldata data generate orders.profile.yaml --scenario hot-key --rows 50000 --seed 11 --out first/
jq '.scenario.document' first/_manifest.json > hot-key.scenario.json
doppeldata data generate orders.profile.yaml --scenario hot-key.scenario.json --rows 50000 --seed 11 --out again/

doppeldata data generate orders --rows 100000 --seed 7 --scenario hot-key --embed-profile --out embedded/
doppeldata data generate embedded/part-00000-867ecb7a.parquet --embed-profile --out again-embedded/
```

```python
import doppeldata as sd

sd.write('orders', 'embedded/', rows=100_000, seed=7, embed_profile=True)
profile = sd.Profile.from_data_file('embedded/part-00000-867ecb7a.parquet')
table = sd.generate('embedded/part-00000-867ecb7a.parquet')   # its seed and rows, as written
```

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
    "profile_digest": "sha256:…",
    "scale": null,
    "scenario_digest": "sha256:…",
    "seed": "11",
    "stream_version": <n>,
    "table": null
  }
}
```

- **`identity` holds each input, and `dataset_digest` their SHA-256.** Two writes naming the same
  SHA-256 hold the same values.
- **`profile.path` names the file the write read**, and `identity.profile_digest` says whether a
  file at that path is still the same profile. Formatting, comments and key order leave its SHA-256
  alone.
- **`scenario.document` is the scenario as `identity.scenario_digest` covers it**, so the run
  regenerates with no scenario file. Written to a file, it is a scenario `--scenario` takes.
- **`format`, `writer` and `layout` record every write setting**, defaults included. Given back to
  `data generate`, they write the same bytes under the same doppeldata release.
- **Each file's `sha256` is how a rerun is compared.** The rerun above wrote every file again
  identical:

```text
$ diff <(jq -r '.files[] | .path + " " + .sha256' first/_manifest.json) <(jq -r '.files[] | .path + " " + .sha256' again/_manifest.json) && echo "every file identical"
every file identical
```

A release that raises the stream version moves the values, and the manifest's `stream_version`
names the one a dataset was made at.

## `--embed-profile` makes each file regenerate its own dataset

```text
$ doppeldata data generate embedded/part-00000-867ecb7a.parquet --embed-profile --out again-embedded/
doppeldata: seed 7, 100,000 rows, scenario hot-key, as embedded/part-00000-867ecb7a.parquet was written
wrote 100,000 rows as parquet in 1 file, 2.2 MB, in 0.10 s on 16 threads: 0.99M rows/s
again-embedded/_manifest.json names dataset sha256:…
```

- **Every PROFILE argument takes such a file.** `data generate`, `data plan`, `data check`,
  `profile export` and `profile expand` read the profile from it, and `sd.generate`, `sd.write` and
  the rest take its path.
- **`data generate FILE` takes the seed, rows and scenario the file was written with**, and prints
  each one it took. Giving any of them generates the dataset they make instead.
- **The rewrite holds the same bytes only with `--embed-profile` again.** Without it the
  `dataset_digest` matches, and each file lacks the profile it held.
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
  $ doppeldata profile expand embedded/part-00000-867ecb7a.parquet --out expanded.profile.yaml
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
