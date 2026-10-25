# How generated blocks become files, and the manifest that regenerates them

## The writers are their own crate, so the engine carries no I/O

`crates/writers` (`doppeldata-writers`) turns a `Plan`'s blocks into files. Parquet, object_store,
tokio, csv-core and encoding_rs are its dependencies, never the engine's. The manifest's model and
the format settings are the engine's, in `doppeldata_core::manifest` and
`doppeldata_core::format_settings`, because the engine owns the list of published JSON Schemas. Both
are serde and schemars types with no I/O.

- **A profile's `source.csv`, `source.parquet` and `source.json` take the keys a manifest's `writer`
  takes, beside what the profiler saw of the files.** Each flattens the writer's settings type into a
  profile-only struct, such as `CsvSource`, whose other keys record a detected encoding, malformed
  rows or a footer's writer. The writers read the settings alone. The profile's copy reads past a key
  neither takes, as every profile section does, through `recorded_settings`. A caller's settings
  are read as the writer's own type, which refuses one. `profile.v1.json` publishes every key either
  takes.
- **`WriteOptions::from_profile` turns a profile into write settings.** The format is the caller's,
  then the profile's recorded one, then Parquet. A recorded codec applies only when the format is the
  recorded one. `override_writer` lays a caller's keys over the result, and `partition_by` sets the
  keys.
- **`WriteOptions::chosen` lays a caller's `WriteChoices` over a profile in that order.** Every
  caller goes through it, so a flag means the same thing in each. `WriteChoices` deserializes from
  JSON and refuses an unknown key.

## A layout fixes every file's rows before anything is generated

`layout()` maps contiguous row ranges to files from the plan and the settings alone. So a caller can
print the files a run will write before a byte is written.

- **A range's size is `file_rows`, or drawn from the profile's `source.layout.rows_per_file`
  quantiles.** The engine's `file_rows_draws` makes the draw, through the quantile interpolation the
  samplers use. Its stream is keyed by the seed alone, so the split is the same on every run.
- **A scenario's `small_files` replaces the profile's quantiles with its own lognormal's.** A
  `file_rows` given to the write still outranks both.
- **A drawn split's last range folds in a remainder under half the range before it.** Otherwise a
  draw just short of the rows leaves a sliver range, and a partitioned one writes a tiny second file
  into every partition.
- **The split never moves a value.** Blocks are a fixed `BLOCK_ROWS`, so a range cut mid-block takes
  a slice of the block. Two neighboring ranges each generate the block they share.
- **`planned_ranges` gives each range with the path its files take.** A partitioned range becomes
  one object per partition value it holds. So its path has `*` for each partition directory, and
  `objects_at_most` bounds its objects. The size estimate counts objects from the same list.
- **A name is `part-{range:05}-{identity8}.{ext}`.** `identity8` is the first eight hex digits of the
  dataset identity, so a run with a changed input never mixes its files with an earlier run's.
- **A scenario's `schema_drift` reshapes each file whose first row lies at or past its cut.** The
  writers drift the whole block before a range takes its rows. A layout where no file reaches the
  cut is refused before anything is written, because the drift would be silently lost.
- **A row a scenario's `duplicates` copies is filed with a home row.** A range takes the rows homed
  in it, so a copy never leaves the file its home is in.
  [scenario-overlays.md](scenario-overlays.md) says where each scope homes its copies.

## Every format writes through one byte sink

Each format's encoder writes into a `ByteSink`. At the bottom, `ObjectSink` hashes and counts the
bytes. A small object goes up as one put, and a large one as a multipart upload with a few parts in
flight. Text compression and Latin-1 are sinks over it, so a layer that rewrites bytes wraps the
same type.

- **Parquet goes through parquet's sync `ArrowWriter`, not `AsyncArrowWriter`.** `AsyncArrowWriter`
  is a sync `ArrowWriter` into a buffer that it flushes to an async writer. The shared sink does the
  same, so one sink serves every format.
- **parquet's own `object_store` feature stays off.** The writers upload through their own sink, so
  the feature would buy nothing. It would also pin the object_store parquet was built against, which
  need not be the one this crate takes.
- **No upload is left open.** Dropping an unfinished sink aborts its upload, and so does a failed
  completion. An error or a cancel leaves no half-written object and no open multipart upload.
- **The writers build no store.** The caller passes an `Arc<dyn ObjectStore>`, so credentials
  follow the caller's AWS setup. `Destination::local` is a `LocalFileSystem`, which writes each
  object to a temporary name and renames it into place.
- **`DestinationUrl::parse` is the one reading of a destination URL.** It gives a local path, an
  `s3://` or `s3a://` bucket and prefix, or `memory://`, and refuses any other scheme. Every caller
  builds its store from it.

## The writers block only on threads they spawn

The sink waits on a tokio runtime with `block_on`. That panics on a runtime's own thread, and a
caller may already be async. So the whole write runs on threads `write()` spawns, and the caller's
thread only waits for them.

- **The writers build a small upload runtime per write, or take the caller's `Handle`.** A supplied
  runtime must be multi-thread, so its workers drive the I/O while the writers' threads wait. A
  current-thread one is refused with `CurrentThreadRuntime`. The calling thread must not be one of
  its workers.
- **Worker threads take ranges in index order.** Each one generates its range's blocks and encodes
  them. So memory stays near one block per worker, plus one open object for each partition value
  the worker's range holds. A run never keeps more threads busy than it has ranges.
  [performance.md](performance.md) records how fast a write runs.
- **A cancel flag stops the workers between blocks.** Unless every range was already written, the
  write then fails with `Cancelled`.

## A run claims its destination, and `_manifest.json` appears only when every object is whole

- **`_manifest.partial.json` is the claim.** The write creates it with a conditional put before
  the first part. A second run on the same prefix fails to create it, and is refused with
  `DestinationClaimed`. A store that cannot put conditionally is refused with `Unclaimable`. S3
  sends `If-None-Match: *`, and `LocalFileSystem` and `InMemory` both create conditionally.
- **A failed or cancelled run leaves its record under the claim's name.** It carries
  `"partial": true` and lists every object that completed. A reader waiting for `_manifest.json`, as
  a Hadoop job waits for `_SUCCESS`, never takes a partial run for a whole one.
- **A write split across processes is three calls: `claim`, `write_ranges` and `commit`.** `claim`
  stores the claim. Each process's `write_ranges` writes whole ranges and returns their records.
  `commit` stores the manifest from the records it is handed, partial when a range has none. So a
  split write's partial manifest lists only the objects whose records reached `commit`.
  [generating-inside-spark.md](generating-inside-spark.md) says how `doppeldata.spark` splits one.
- **A failed write says whether its record was stored.** `WriteFailure` carries the manifest,
  `manifest_stored`, and the error from storing it.
- **A run that stored `_manifest.json` and then could not delete its claim fails with
  `ClaimNotRemoved`.** Its manifest is whole, so a caller tells it from a partial run without
  reading the store. An S3 policy that grants puts but not deletes reaches it.
- **Each `WriteError` names its code and kind through `code()`.** The kind is `Refused` when the
  call must change and nothing was written, and `Failed` otherwise. A caller maps the kind to its
  own exit status.
- **A destination holding anything is refused unless `replace` is set.** With `replace`, every
  object must be a manifest at the prefix, or a part whose name ends with the `identity8` and
  extension one of those manifests records. Anything else, such as Spark's `_SUCCESS` or pyarrow's
  `part-0.parquet`, refuses the write with nothing deleted.
- **`replace` deletes before it writes.** An interrupted replace leaves the old dataset gone and at
  most its own claim behind. The claim records the profile, seed and row count that regenerate it.
- *Rejected:* writing the new objects first and deleting the old ones after. A reader globbing the
  prefix meanwhile reads both datasets, and a rerun of the same identity writes the same names.
  Spark's overwrite mode also deletes first.

## Text formats go through csv-core, and print the cells Arrow holds as null

`arrow-csv` cannot set the line terminator or the quoting style per file. csv-core, the csv crate's
own writer, takes both, and the writers drive it field by field. A thin adapter formats each Arrow
value with `arrow-cast`. Where `Plan::text_cells` holds a cell, CSV prints its text in place of the
null. JSON prints a cell that parses as a number, `true`, `false` or `null` as itself, and any other
cell as a string.

- **An empty string is quoted where a null is bare.** The default null token is empty, and DuckDB
  and Spark read a bare empty field as null. So an empty string prints as `""`. DuckDB reads the two
  apart with `allow_quoted_nulls=false`. Spark reads both as null unless `nullValue` names another
  token, such as `\N`.
- **A Latin-1 file is windows-1252**, the encoding every decoder applies to a `latin1` label. A
  character it cannot hold is written as `?`.
- **The dialect reads back.** A `|`-delimited CSV with CRLF line ends, a byte order mark and `NULL`
  for null reads in DuckDB and Spark as rows equal to the Parquet of the same dataset, with nothing
  left over either way. The write, then each read:

```bash
cargo run --release -p doppeldata-writers --example write_dataset -- eight-columns --rows 1000000 --file-rows 250000 --format csv --writer '{"delimiter":"|","line_terminator":"crlf","bom":true,"null":"NULL"}' --out csv
```

```sql
select * from read_csv('csv/*.csv', delim='|', header=true, nullstr='NULL', new_line='\r\n')
```

```python
spark.read.csv("csv", sep="|", header=True, nullValue="NULL", schema=parquet.schema)
```

## A Parquet file carries its provenance twice

parquet's `ArrowWriter` keeps the Arrow schema's metadata only inside `ARROW:schema`. A reader that
never decodes that key, such as DuckDB's `parquet_kv_metadata` or Polars, would find no
`doppeldata.*` key. So the writers also write each metadata key as a plain key-value pair, in key
order, as pyarrow does. `created_by` is `doppeldata <version>`.

## parquet's writer cannot write INT96, and no sort order is declared

- **`timestamp_unit` casts timestamp columns to s, ms, us or ns.** parquet's Arrow writer has no
  INT96 path, so a legacy Hive or Impala INT96 column cannot be reproduced.
- *Rejected:* `sorting_columns`. Generated rows are not sorted, and a reader that trusts a declared
  sort, such as DataFusion, would prune rows it needs. Sorting the rows within a file would break the
  identical values under any split. The profile's `layout.sort_order` is likewise not honored.

## A partition key draws its shares from its own column

`partition_by` names top-level scalar columns. Each range splits its rows by those columns' values,
and writes one object per value under `key=value/` for the `hive` style, or `value/` for
`directory`. The value is in the path, so the column leaves the file.

- **A partitioned range holds `file_rows` times `partition_values` rows.** `partition_by` sets
  `partition_values` to the product of each key's recorded value count, or else its column's
  distinct count. So each object lands near the size asked for. A skewed key still gives uneven
  objects, and fewer, larger ranges feed fewer workers.
- **A recorded partition key that names no column is dropped.** A Hive key lives in the source's
  paths, and the engine generates only columns.
- **A key and a value are escaped as Hive and Spark escape a path name.** A null or empty value is
  `__HIVE_DEFAULT_PARTITION__`.
- **The object list is known only once the run writes.** The layout still fixes every range, and
  each object records its range and its partition values.

## The manifest is the regeneration record

`_manifest.json` sits beside the data. Spark, Hive and Glue skip a file whose name starts with `_`,
so it never reads as data. Its schema is `doppeldata schemas show manifest`.

- **`identity` holds the dataset identity's five values, keyed as its canonical JSON.**
  `dataset_digest` is their SHA-256. The seed and the row count are decimal strings, because JSON
  numbers are exact only to 2^53. A file's row and byte counts are JSON numbers, which no single
  object nears that bound.
- **`format` and `writer` record every setting**, defaults included, so nothing depends on a later
  default.
- **`scenario` records the scenario's name and the path it was loaded from.** A composition is
  named with its members' names joined by `+`, lists them in `composed_of`, and has no path. The
  identity's `scenario_digest` is what names the scenario exactly.
- **Each file records its path, its range, the range's first row and row count, its own rows, its
  bytes and its SHA-256.** `regenerate()` rebuilds the ranges from them. It refuses a partial
  manifest, a plan whose identity differs, and ranges that leave a gap.
- **`regenerate()` compares every file it writes with the record.** A file whose rows, bytes or
  SHA-256 differ fails the call with `RegeneratedFilesDiffer`, which names it. One tool version and
  one set of writer settings write the same bytes.
