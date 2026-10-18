# doppeldata

Profile a dataset once, commit the profile, and generate synthetic data from it whenever a test or a
benchmark needs some.

A profile is a YAML file holding a dataset's types, distributions, skew, null and error rates, file
layout and table relationships. It holds no rows. doppeldata reads CSV, TSV, JSON, Parquet, Arrow,
ORC, Avro and Spark DataFrames. It writes any of them back at any size: in memory, to disk, to S3, or
inside Spark executors.

A scenario file layered on a profile bends the data on purpose: a hot join key, a burst of malformed
rows, a flood of small files. The same profile, scenario and seed always produce the same data.

## License

MIT
