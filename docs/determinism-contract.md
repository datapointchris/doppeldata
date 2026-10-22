# The determinism contract

## A dataset is identified by five values

A generated dataset is a pure function of:

1. the engine's stream version;
2. the canonical SHA-256 of the profile;
3. the canonical SHA-256 of the scenario;
4. the seed;
5. the row count.

The manifest written beside the output must record all five, plus a SHA-256 over them. Three committed
files — profile, scenario and seed — reproduce a dataset anywhere.

## Each column draws from a stream named for it, never numbered

Every (seed, table, column path, perturbation) derives its own ChaCha8 stream from a hash of its
name. So adding, removing or reordering a column leaves every other column's values unchanged.

A copula group, the set of numeric columns correlated through one matrix, draws from one stream keyed
by its sorted column names. Adding an unrelated column changes nothing. Adding a column to the group
changes that group's values.

## A fixed block of 65,536 rows picks the stream position

The block size is internal and fixed. It never follows batch size, file size or thread count. So
changing the number of rows per file, the number of threads or the output format changes the files
and never the values.

## The block is the unit of parallelism, because draws are not fixed-width

Rejection-sampling distributions (Gamma, Poisson, ziggurat normals) consume a variable count of random
words per value. That rules out skipping ahead to an arbitrary row. A block starts its stream fresh,
so any thread can produce any block.

## Values match across every writer, and Parquet bytes only within one engine version

The guarantees, strongest first:

1. Arrow values are identical.
2. CSV and JSON bytes are identical.
3. Parquet bytes are identical for one engine version and one set of writer settings.

An upgrade to the Arrow or Parquet writer that changes encoder output, or any change to a sampler's
draw order, raises the stream version. The stream version is part of a dataset's identity, so a
dataset generated before the change is still named exactly.
