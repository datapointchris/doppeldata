# Prior art, and why none of it is a dependency

doppeldata is MIT-licensed, so a dependency must allow use anywhere, by anyone, on any platform.

## SDV and its libraries are source-available, so the copula is written here

SDV, and the copulas, ctgan and rdt libraries beside it, are under the Business Source License 1.1.
The Gaussian copula and the conditional tables doppeldata needs are implemented in its own engine.

## SDMetrics loads copulas on import, so its scores are computed here

SDMetrics is MIT-licensed, and it requires copulas. Importing `sdmetrics.single_column` or
`sdmetrics.column_pairs` loads copulas, through the package's own `__init__`. So `data check
--against` takes six of its scores by name and definition, and imports none of its code.

## sweetviz renders the side-by-side HTML

sweetviz (MIT) renders `data check --html`. It is imported only there, so the library declares it
nowhere, and `--html` without it exits 2 naming it.

## dbldatagen may only be used with Databricks

dbldatagen's license allows use only in connection with Databricks Services. doppeldata runs on a
laptop, in AWS Glue and in any Spark. Its `baseColumn` idea, deriving one column from another, is
borrowed as an idea and no code.

## jenga is GPL-3.0, so its vocabulary is borrowed and its code is not

jenga corrupts values in pandas frames, and it is under the GPL-3.0. badgers (BSD-3-Clause) does similar work. doppeldata's value perturbations take their names from both,
so a perturbation reads the same to someone who knows either.

## Soda Core is under the Elastic License 2.0

Soda Core checks data quality, which overlaps with checking generated data against its profile. The
Elastic License 2.0 is not an open-source license. A profile instead exports to formats that existing
checkers read: Glue Data Quality rulesets, Table Schema, JSON Schema and pandera.

## A machine-learning synthesizer was rejected on reviewability

A synthesizer that learns from real rows, such as mostlyai, needs PyTorch and fits the rows
themselves. Its learned weights cannot be reviewed the way a profile can, which defeats committing
the profile as the reviewed artifact. doppeldata works without a model.

## Differential privacy is an optional policy, not the default

Differential-privacy noise distorts the tails and the skew that a performance scenario exists to
reproduce. It is never the default. Data that needs it gets a policy of its own beside `default`,
`strict` and `none`.

## A fit calls scipy itself, because fitter is GPL-3.0 and distfit wraps the same calls

fitter is under the GPL-3.0. distfit (MIT) fits scipy.stats families through the same calls, and
would add a dependency for no measured gain. scipy (BSD-3-Clause) is imported only by profiling,
so the library declares it nowhere, and the CLI declares it.

## Frozen bin edges are borrowed from Glue Data Quality, and the zero mixture from NeMo

Glue Data Quality's Distribution analyzer bins a numeric column at equal width and keeps a
categorical column's most frequent values. It freezes a column's bins and values after its first
run, and rebins only after sustained drift outside them
(<https://docs.aws.amazon.com/glue/latest/dg/dqdl-rule-types-Distribution.html>). doppeldata copies
the frozen edges and the overflow count. Rebinning stays explicit: profiling again without
`edges_from`.

A zero-inflated fit is the `bernoulli_mixture` of NeMo Data Designer (Apache-2.0): a share of zeros
beside a family fitted to the rest.
