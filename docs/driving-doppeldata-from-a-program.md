# Driving doppeldata from a program

A program or a coding agent learns what doppeldata takes from `capabilities show --json`. It reads
what each refusal was from its issue code, and how each column will be drawn from `data plan
--json`. A person reads the same checks as GitHub Markdown.

## `capabilities show` lists what the installed doppeldata refuses outside of

```bash
doppeldata capabilities show          # each command, then each list on a line
doppeldata capabilities show --json   # the same lists whole, as one document
```

| Key | Holds | Read from |
| --- | --- | --- |
| `commands` | each command's path, summary, arguments and flags, with each flag's type, choices and default | the CLI's own command tree |
| `formats`, `partition_styles` | what a write takes | the published manifest schema |
| `export_formats` | what `profile export --format` takes | the exporters the library registers |
| `engines` | each profiling engine, the formats it reads and the packages it needs | the engine table `profile()` reads |
| `families` | each distribution family a numeric tail is fitted to, and whether it is discrete | the engine |
| `fakes` | every provider `sd.fakes` names | `sd.fakes` |
| `semantic_types` | each type an installed detector pack declares, with its pack, sensitivity and generator hint | the packs |
| `ops` | each op, its fields, and the formats a write takes it in | the published scenario schema, and the engine |
| `fault_kinds` | each kind `malformed_rows` writes, the formats that hold it and those `data check` reads back | the engine |
| `metrics`, `expected_metrics` | what `data check` measures, with each threshold, and what an expectation names | the metric table, and the scenario schema |
| `policies`, `default_k_min` | each privacy policy, and the `k_min` none given takes | the policy module |
| `schemas` | each name `schemas show` takes | the published schemas |
| `issue_codes` | every registered code, with its area, exit status, what raises it and its hint | the registry |
| `exit_codes` | what each exit status means | the library |

- **Every list comes from the module that refuses a value outside it.** So an op, a format, a fake
  or a code added appears with no edit here. An op added to the engine's model appears once `task
  generate` writes it into the scenario schema.
- **The formats each op takes are decided once, in the engine.** `op_takes_format` in
  `crates/core/src/scenario.rs` is what the writers' refusals of `cdc` and `schema_drift` read, and
  what the manifest lists. It matches every `Op` by name, so an op added fails to compile until it
  says which formats take it.
- **The library builds the manifest, and the command line hands it the commands.** Only a command
  can walk its own tree, and the library never imports Typer, so `sd.capabilities(commands)` takes
  the list the CLI walked from its root. Called with none, it lists everything else.
- **Listing the semantic types loads every installed detector pack**, and the packs import pyarrow.
- **`schema_version` rises when a key is removed or changes meaning.** A key added leaves it.

## `data plan --json` traces each column to the profile field it reads

`doppeldata data plan orders --scenario hot-key --out out/ --json` gives `customer_id` as:

```json
{
  "name": "customer_id",
  "physical_type": "int64",
  "sampler_kind": "numeric",
  "summary": "uniform between the extremes, at the profiled distinct ratio",
  "parameters": [
    {"name": "null_fraction", "field": "/columns/1/null_fraction", "value": 0.0},
    {"name": "distinct_ratio", "field": "/columns/1/distinct/ratio", "value": 0.2},
    {"name": "approx_values", "field": "/columns/1/distinct/approx_values", "value": 20000},
    {"name": "extremes", "field": "/columns/1/numeric/extremes", "value": {"high": 99999, "low": 10, "method": "digit_bounds"}},
    {"name": "integral", "field": "/columns/1/numeric/integral", "value": true}
  ],
  "null_fraction": 0.0,
  "origin": "overlaid",
  "reads": [],
  "ops": ["hot_keys:customer_id:0"],
  "patched_fields": []
}
```

- **The engine records each field a sampler reads when it builds the sampler.** So the trace
  follows the branch the engine took: a text column drawn from a hinted pattern names
  `semantic/generator_hint/pattern`, and one drawn from its stored pattern names `string/pattern`.
- **A value is read from the profile with the scenario's patch applied**, the document the engine
  planned from. A field the profile leaves out is left out of `parameters`, and reads as its
  default.
- **A column drawn by a dependence entry names it too.** A derived column names its
  `/columns/<i>/derived`, a lookup its `/dataset/dependencies/<i>`, and a conditional draw each
  `/dataset/conditionals`, `conditional_quantiles` or `conditional_fractions` entry it takes. An
  entry the plan sets aside is named by none.
- **`ops` comes from the columns each op writes as planned**, so an op that writes rows rather than
  a column, such as `duplicates`, names no column.

## Every report a person reads is GitHub Markdown

`profile check`, `scenario check`, `data check` and `profile diff` print a Markdown report without
`--json`. Each opens with a `#` heading holding the verdict. Each issue is a table row of its field,
its code, what was found and its hint, so a report pastes into a pull request or an issue as it is.

- **A check prints its report on stdout and its verdict on stderr.** The verdict is the one issue
  the root group prints for any refusal, so it reads the same under `--json`.

## Rejected

- *Rejected:* a `--markdown` flag. The text a person reads is already the report, and a terminal
  shows Markdown legibly. A flag would make two text forms to keep in step.
- *Rejected:* a published JSON Schema for the manifest. Every published schema is derived from the
  engine's Rust model, and the manifest is assembled in Python from the library's own registries.
  A hand-written schema beside it would be the one schema nothing checks against its source.
- *Rejected:* a hand-written list of ops, formats or codes for the manifest. It would drift from
  what the engine refuses, and an agent reading it would be told a value is taken that is refused.
