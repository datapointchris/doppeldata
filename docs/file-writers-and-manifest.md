# How generated blocks become files, and the manifest that regenerates them

## The writers are their own crate, so the engine carries no I/O

`crates/writers` (`doppeldata-writers`) turns a `Plan`'s blocks into files. Parquet, object_store,
tokio, csv and encoding_rs are its dependencies, never the engine's. The manifest's model is the
engine's, in `doppeldata_core::manifest`, because the engine owns the list of published JSON
Schemas. It is serde and schemars types with no I/O.

## A layout fixes every file's rows before anything is generated

`layout()` maps contiguous row ranges to files from the plan and the settings alone. So a caller can
print the files a run will write before a byte is written.

- **A range's size is `file_rows`, or drawn from the profile's `source.layout.rows_per_file`
  quantiles.** The draw uses a stream keyed by the seed alone, so the split is the same on every
  run.
- **The split never moves a value.** Blocks are fixed at 65,536 rows, so a range cut mid-block
  takes a slice of the block. Two neighboring ranges each generate the block they share.
- **A name is `part-{index:05}-{identity8}.{ext}`.** `identity8` is the first eight hex digits of the
  dataset identity, so a run with a changed input never mixes its files with an earlier run's.

## Every format writes through one byte sink

Each format's encoder writes into a `ByteSink`. At the bottom, `ObjectSink` hashes and counts the
bytes, then hands them in chunks to object_store's `BufWriter`. That writer makes one put for a small
object and a multipart upload for a large one. Text compression and Latin-1 are sinks over it. A
deliberate malformation wraps the same type.

- **Parquet goes through parquet's sync `ArrowWriter`, not `AsyncArrowWriter`.** `AsyncArrowWriter`
  is a sync `ArrowWriter` into a buffer that it flushes to an async writer. The shared sink does the
  same, so one sink serves every format.
- **parquet's `object_store` feature stays off.** parquet 59 takes object_store 0.13, pyo3-object_store
  needs 0.14, and arrow stays on 59 for pyo3-arrow.
- **Dropping an unfinished sink aborts its upload.** An error or a cancel leaves no half-written
  object and no open multipart upload.
- **The writers build no store.** The caller passes an `Arc<dyn ObjectStore>`, so credentials
  follow the caller's AWS setup. `Destination::local` is a `LocalFileSystem`, which writes each
  object to a temporary name and renames it into place.

## The writers block only on threads they spawn

The sink waits on a tokio runtime with `block_on`. That panics on a runtime's own thread, and a
caller may already be async. So the whole write runs on threads `write()` spawns, and the caller's
thread only waits for them.

- **The writers build a two-thread runtime per write, or take the caller's `Handle`.** A supplied
  runtime must be multi-thread, so its workers drive the I/O while the writers' threads wait.
- **Worker threads take ranges in index order.** Each one generates its range's blocks and encodes
  them. So memory stays near one block and one open file per worker.
- **A cancel flag stops the workers between blocks.** The write then fails with a manifest marked
  `"partial": true`, listing every object that completed, and stores it as `_manifest.json`.

## Text formats go through the csv crate, and print the cells Arrow holds as null

`arrow-csv` cannot set the line terminator or the quoting style per file, and the `csv` crate can. A
thin adapter formats each Arrow value with `arrow-cast`. Where `Plan::text_cells` holds a cell, CSV
prints its text in place of the null. JSON prints a cell that parses as a number, `true`, `false`
or `null` as itself, and any other cell as a string.

- **A Latin-1 file is windows-1252**, the encoding every decoder applies to a `latin1` label. A
  character it cannot hold is written as `?`.
- **The dialect reads back.** Each of these reads `|`-delimited CSV with CRLF line ends, a byte order
  mark and `NULL` for null as rows equal to the Parquet of the same dataset, with nothing left over
  either way:
  - DuckDB's `read_csv`;
  - Spark 3.5.9 and 4.2.0;
  - AWS Glue 5's Spark 3.5.4.

  Each read named the delimiter, the header and the null token. The test set was 1M rows of
  `eight-columns`.

## A Parquet file carries its provenance twice

parquet's `ArrowWriter` keeps the Arrow schema's metadata only inside `ARROW:schema`. A reader that
never decodes that key, such as DuckDB's `parquet_kv_metadata` or Polars, would find no
`doppeldata.*` key. So the writers also write each metadata key as a plain key-value pair, in key
order, as pyarrow does. `created_by` is `doppeldata <version>`.

## parquet 59 cannot write INT96, and no sort order is declared

- **`timestamp_unit` casts timestamp columns to s, ms, us or ns.** parquet 59's writer has no INT96
  path, so a legacy Hive or Impala INT96 column cannot be reproduced.
- *Rejected:* `sorting_columns`. Generated rows are not sorted, and a reader that trusts a declared
  sort, such as DataFusion, would prune rows it needs. Sorting the rows within a file would break the
  identical values under any split. The profile's `layout.sort_order` is likewise not honored.

## A partition key draws its shares from its own column

`partition_by` names top-level scalar columns. Each range splits its rows by those columns' values,
and writes one object per value under `key=value/` for the `hive` style, or `value/` for
`directory`. The value is in the path, so the column leaves the file.

- **A key and a value are escaped as Hive and Spark escape a path name.** A null or empty value is
  `__HIVE_DEFAULT_PARTITION__`.
- **The object list is known only once the run writes.** The layout still fixes every range, and
  each object records its range's first row and its partition values.

## The manifest is the regeneration record

`_manifest.json` sits beside the data. Spark, Hive and Glue skip a file whose name starts with `_`,
so it never reads as data. Its schema is `doppeldata schemas show manifest`.

- **The identity, stream version, seed and row count come from `DatasetIdentity`.** The seed and the
  row count are decimal strings, because JSON numbers are exact only to 2^53.
- **`format` and `writer` record every setting**, defaults included, so nothing depends on a later
  default.
- **Each file records its path, its range's first row, its rows, its bytes and its SHA-256.**
  `regenerate()` rebuilds the ranges from the distinct first rows. It refuses a plan whose identity
  differs, and writes every file again to the same SHA-256 under the same tool version.
- **A destination holding anything is refused unless `replace` is set.** `replace` deletes only
  `part-` objects and `_manifest.json`. It refuses outright when any other object is there, so a
  reader never finds this run's files mixed with foreign ones.

## ZSTD Parquet writes at about 19M rows/s on 16 threads and 3.4M on one

The population is 10M rows of the `eight-columns` example in 1M-row files, seed 42. The instrument
is the manifest's `elapsed_ms`, which covers preparing the destination, every file and the manifest.
The machine is a 16-thread Ryzen 9 6900HX with other work running at a load average of 2.5 to 5:

```bash
cargo run --release -p doppeldata-writers --example write_dataset -- eight-columns --rows 10000000 --file-rows 1000000 --threads 16 --out memory://
```

| Codec | Threads | Rows/s, five runs into memory |
| --- | --- | --- |
| ZSTD level 1 | 16 | 18.7M to 19.5M |
| ZSTD level 1 | 1 | 3.37M to 3.41M |
| Snappy | 16 | 20.8M to 21.2M |

Ten files feed ten workers, so six of the sixteen threads sit idle. Written to a local disk instead,
ZSTD on 16 threads ran from 9.8M to 19.7M rows/s over ten runs. The disk's writeback and the
machine's other load decide that spread, and a `sync` before each run kept it above 14.6M.
