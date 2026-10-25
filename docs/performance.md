# How fast doppeldata generates and writes

`task bench` prints the engine-against-numpy table on this page. No workflow runs it, because timing
on a shared runner is noise.

## The engine meets every speed target it can be measured against

The targets are for a 16-thread machine. They follow from the comparison in
[python-profiler-rust-generator.md](python-profiler-rust-generator.md). A missed target is an engine
defect.

| Target | Measured | Result |
| --- | --- | --- |
| At least 2M rows/s per core, end to end to ZSTD Parquet, on a mixed-type table | 3.21M to 3.30M on one thread | met |
| At least 10M rows/s end to end on 16 threads | 17.3M to 18.8M | met |
| At least 25M rows/s generating alone on 16 threads | 58.8M to 72.6M | met |
| At least 5M values/s per core for a semantic string column | 6.25M to 7.06M names on one thread | met |
| A Gaussian-copula group or a conditional table at most twice the time of sampling its columns independently | no sampler draws either yet | — |

## Every local table comes from one profile at ZSTD level 1, on one shared 16-thread machine

- **The machine is an AMD Ryzen 9 6900HX, 8 cores and 16 threads, shared with other builds and
  tests.** The one-minute load average stayed between 4.2 and 6.5. The bench table records it
  before and after each arm.
- **The population is the bundled `eight-columns` profile, 10M rows, seed 42.** Its columns are a
  sequential id, a lognormal amount, a Poisson quantity, a Zipf-weighted category of 50 values, a
  uniform 2025 timestamp, a Bernoulli flag, a normal score with 5% nulls and a random 10-character
  code.
- **Every write is 1M-row Parquet files, one row group each, ZSTD level 1, unless a row says
  otherwise.** Level 1 is also the engine's default.
- **Each row is one untimed run, then the lowest and highest rate of five timed runs.**
- **The versions are doppeldata 0.10.0, Rust 1.99.0, the parquet crate 59.3.1, the fake crate
  5.1.0, CPython 3.13 with the GIL, numpy 2.5.4, pyarrow 26.0.0, Faker 40.43.0 and Mimesis 22.2.0.**
  `uv sync` built the extension with cargo's release profile. The bench scripts pin the Python
  packages.

## On 16 threads the engine writes Parquet over twice as fast as numpy with pyarrow

`task bench` printed this:

| Arm | Threads | Work | Per second, range of 5 runs | Output | Load average, before and after |
| --- | --- | --- | --- | --- | --- |
| engine 0.10.0, `sd.generate` | 1 | generate 10M rows | 7.81M to 8.68M | — | 5.3 to 5.3 |
| engine 0.10.0, `sd.generate` | 16 | generate 10M rows | 58.8M to 72.6M | — | 5.3 to 5.3 |
| engine 0.10.0, `data generate` | 1 | write 10M rows as ZSTD-1 Parquet | 3.21M to 3.3M | 337.8 MB | 5.3 to 4.9 |
| engine 0.10.0, `data generate` | 16 | write 10M rows as ZSTD-1 Parquet | 17.3M to 18.8M | 337.8 MB | 4.9 to 5.9 |
| engine 0.10.0, `data generate` | 1 and 16 | SHA-256 of every run | identical | 8b52936ac81e2247 | — |
| numpy 2.5.4 + pyarrow 26.0.0 | 1 | generate 10M rows | 4.33M to 4.4M | — | 5.9 to 5.5 |
| numpy 2.5.4 + pyarrow 26.0.0 | 1 | write 10M rows as ZSTD-1 Parquet | 2.52M to 2.6M | 337.8 MB | 5.5 to 4.4 |
| numpy 2.5.4 + pyarrow 26.0.0 | 16 | generate 10M rows | 9.11M to 10.9M | — | 4.4 to 4.4 |
| numpy 2.5.4 + pyarrow 26.0.0 | 16 | write 10M rows as ZSTD-1 Parquet | 6.72M to 7.89M | 337.8 MB | 4.4 to 4.9 |
| Faker 40.43.0, `name()` | 1 | 200,000 names | 16k to 18k | — | 4.9 to 6.5 |
| Mimesis 22.2.0, `full_name()` | 1 | 200,000 names | 555k to 601k | — | 6.5 to 6.5 |
| engine, `name` provider | 1 | 200,000 names | 6.25M to 7.06M | — | 6.5 to 6.5 |

- **On one thread the engine writes 1.2 to 1.3 times as fast.** Generating alone, it is 1.8 to 2.0
  times as fast. The encoder's share of each write narrows the gap.
- **On 16 threads it writes 2.2 to 2.8 times as fast, and generates 5.4 to 8.0 times as fast.**
  numpy's pool scales 2.6 to 3.1 times on the write, and the engine 5.2 to 5.9 times.
- **Ten files are ten tasks, so six of the sixteen threads sit idle in both write arms.**
  Generating alone splits the rows into 65,536-row blocks, which keep every thread busy.
- **The twelve engine writes, six at each thread count, hash the same.** `engine_throughput.py`
  fails when any two differ.

What each arm holds:

- **The engine arm runs doppeldata and nothing else.** `sd.generate` is timed around the call, and
  builds one Arrow table of every row. `doppeldata data generate` is timed by its manifest's
  `elapsed_ms`, from preparing the destination to writing the manifest. So the interpreter's start
  and the CLI's imports fall outside it.
- **The control runs no doppeldata code.** `numpy_throughput.py` draws each file's columns with
  numpy, builds a pyarrow table, and writes it with pyarrow on a pool of `--workers` threads. It is
  timed around the pool. Generating alone builds each table and drops it.
- **The two arms encode with different Parquet writers.** The engine's is the Rust parquet crate.
  pyarrow's is Arrow C++. Both dictionary-encode and fall back to plain, and their files differ by
  19 KB in 338 MB. Past codec, level, file rows and row groups, each keeps its own defaults, such as
  which statistics it writes. So the write column measures a writer as well as a generator.
- **The values differ.** The control draws each column from numpy's distributions. The engine
  samples the profile's fitted families, quantiles and top values, from a stream per column and
  block. So the arms make the same table shape, never the same rows.
- **Both write arms write to a tmpfs**, the system temp directory, and delete each run's files.

## The `name` provider draws ten to thirteen times as many names as Mimesis

Mimesis draws 31 to 38 times as many as Faker. Each generator makes 200,000 person names on one
thread. Faker and Mimesis return Python strings in a list. The engine fills an Arrow string column
through `sd.generate`, from a one-column profile whose `semantic.generator_hint.provider` is `name`.
`name_generators.py` builds that profile when it runs.

## Snappy writes no faster than ZSTD level 1, and a partition key of 50 values holds a write to one thread

Each row is the writers' own example writing into memory, which keeps nothing:

```bash
cargo run --release -p doppeldata-writers --example write_dataset -- eight-columns --rows 10000000 --file-rows 1000000 --out memory://
```

The rate is the manifest's `elapsed_ms`, and peak memory is GNU time's maximum resident set. The load
average ran from 4.2 to 5.7.

| Flags added | Rows/s | Written | Peak memory |
| --- | --- | --- | --- |
| `--threads 16` | 17.7M to 19.7M | 337.8 MB | 619 to 864 MB |
| `--threads 1` | 3.15M to 3.30M | 337.8 MB | 436 to 443 MB |
| `--threads 16 --writer '{"compression":"snappy"}'` | 17.3M to 19.4M | 424.3 MB | 887 MB to 1.11 GB |
| `--threads 16 --partition-by category` | 1.39M to 1.67M | 362.4 MB in 50 files | 1.17 to 1.18 GB |

- **Snappy writes 26% more bytes at the same rate.** So ZSTD level 1 is the better default.
- **A partitioned range holds `file_rows` times the key's 50 values**, so the whole run is one range
  on one worker. It then writes at about half one unpartitioned thread's rate, into 50 open objects.
- **The CLI writes at the writers' own rate.** `data generate` to a tmpfs and this example into
  memory overlap at one thread and at 16, so the CLI and the SDK under it add nothing measurable.

## On Glue 5.1, nine executors write 1B rows in 64 s through `sd.spark.write`, and in 97 s through Spark's writer

One run of each mode, each its own run of the Glue job below:

| Mode | Timed work | Seconds | Rows/s | Per executor | Written | Run billed | DPU-hours |
| --- | --- | --- | --- | --- | --- | --- | --- |
| direct | `sd.spark.write` | 63.7 | 15.70M | 1.74M | 33.76 GB in 1,000 files | 256 s | 0.711 |
| spark | `sd.spark.generate`, then `frame.write.parquet` | 96.5 | 10.36M | 1.15M | 41.20 GB in 954 files | 173 s | 0.482 |

- **Writing directly is 1.5 times as fast.** Spark's files also hold 22% more bytes. Its Parquet
  writer defaults to Snappy, and doppeldata's to ZSTD level 1.
- **Spark mode ran 954 tasks, one per chunk of `CHUNK_BLOCKS` engine blocks.** Spark wrote one
  file per task.
- **Each executor core wrote about 436k rows/s**, against 3.2M on one local thread. The run did not
  split a task's time between generating, encoding, uploading and Spark's own overhead, so the gap
  is not attributed.
- **A billed run is longer than its timed work.** It also holds start-up, installing the wheel, and
  the job's diagnostic tasks before and after the timed call. How those split was not measured.
- **A DPU-hour's price depends on the region**, and is on <https://aws.amazon.com/glue/pricing/>.

The job and how it was timed:

- **Glue 5.1, a `glueetl` job in Python 3**: Spark 3.5.6-amzn-1, Python 3.11.15, pyarrow 22.0.0, and
  the pandas Glue installs.
- **Ten G.1X workers**: one driver, and nine executors of 4 cores and 10g each. Dynamic allocation
  is off. The execution class is STANDARD, with a 60-minute timeout and no retries.
- **`--additional-python-modules` names the library's cp311-abi3 manylinux2014 x86_64 wheel on
  S3**, and nothing else. It was an unstamped build, which reports version 0.0.0.
- **The population is the bundled `eight-columns` profile, 1B rows, seed 42, 1M rows a file**,
  written to a bucket in the job's region.
- **The job times each mode's call on the driver with `time.monotonic`.** Rows/s divides 1B by that
  time, and per executor divides it by the nine executors registered after the call. The direct
  manifest's `elapsed_ms` is 63,606, which agrees.
- **Before and after the timed call, the job ran one diagnostic task per executor core.** Each
  resolved boto3's credentials and imported one chunk, which
  [generating-inside-spark.md](generating-inside-spark.md) reports.

The timed part of the job, with `mode` and `out` its job arguments:

```python
spark = SparkSession.builder.getOrCreate()
profile = sd.Profile.example('eight-columns')
started = time.monotonic()
if mode == 'direct':
    manifest = sd.spark.write(spark, profile, out, rows=10**9, seed=42, file_rows=1_000_000)
else:
    sd.spark.generate(spark, profile, rows=10**9, seed=42).write.mode('errorifexists').parquet(out)
seconds = time.monotonic() - started
```

- **Billed time and DPU-hours are the job run's `ExecutionTime` and `DPUSeconds`**, as Glue reports
  them.
