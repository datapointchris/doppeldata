# How a column is given a semantic type

## A column takes the type whose validator passes the most of its sampled values

Detection runs in `doppeldata.profiling.semantic_types`, on the seeded sample of `VALUE_SAMPLE_ROWS`
rows that grex also reads. Every engine's backend draws that sample, so detection is the same on
each. A column's nulls are dropped first, so a column with nulls is validated on fewer values.

- **A validator passing `ASSIGN_RATE` of the values assigns its type.** One passing
  `HINTED_ASSIGN_RATE` assigns it where the column's name hints the type too.
- **Where a type's hint names a check digit, the profiler tests it.** A value passes only where its
  validator accepts it and its last digit is the check digit of the digits before it. The engine's
  `check_digits_hold` decides that, so the profiler and the generator read one rule.
- **Of the types that qualify, the most values passed wins.** A tie goes to the hinted type, then to
  the pack and the detector listed first.
- **`quality.invalid_fraction` is the share that failed.** A card-number column with 0.3% bad check
  digits writes `invalid_fraction: 0.003`, and the generator leaves that share of its Luhn digits wrong.
- **`confidence` is the pass rate.** A name hint halves the doubt left, so 0.9 with a hint is 0.95. A
  type its name hint assigns is given `HINT_ASSIGNED_CONFIDENCE`.
- **`detectors` lists the methods that agreed.** The first is the detector's `method`: `pattern`
  for a regular expression alone, or `validator` for any other check, such as a lookup or a parse.
  `check_digit` follows where the profiler tested one, and `name_hint` where the name hints the type.
- **A column with fewer than `k_min` sampled values is left untyped.**
- **Typing changes no statistic.** A typed column's values stay in the profile as profiled.

## A name hint is a run of the column name's words

A column name splits into lower-case words at underscores, hyphens, digits and case changes. So
`customerEMail`, `Customer-E-Mail` and `customer_e_mail` are each `customer e mail`. A hint splits
the same way, so `ipv4` is `ipv 4` and `memberName` is `member name`. A hint matches where its words
run in order inside the name's, so `e_mail` and `mail` both hint `email` there.

## A type its name hint assigns needs the hint to end the column's name

Two kinds of detector type a column only where its name hints the type:

- **One with no validator.** Only the hint assigns it.
- **One with `hint_required`.** Its validator confirms the hint, and is too loose to type a column
  alone. The general pack's person names take letters, spaces, apostrophes, periods and hyphens.
  Its undashed SSNs and 3- to 5-digit ZIP codes take numbers that most counts and account numbers
  would pass.

For both, the hint must end the name. A word after it, such as `id`, `hash`, `count` or `length`,
says the column holds something about the type. So `customer_first_name` takes a person's name, and
`last_name_id`, `customer_name_hash`, `zip_count` and `ssn_last4` take nothing.

A person's name is three detectors of the one type, split by the part of the name a column holds, so
a first-name column draws first names. A bare `name` hints none of them, because a product, a
company or a file has one too.

## An integer column is typed only where its name hints the type and a validator agrees

A text source parses a column of 16-digit card numbers as `int64`, because they fit. It parses SSNs
and ZIP codes as integers too, dropping their leading zeros. So detection reads an integer column
as its values' decimal text. Every 5-digit count would pass as a ZIP code, so a validator alone
never types an integer column.

A column's kind decides which detectors it may take. `hint_required` and a missing validator decide
where the hint must sit:

```text
                     validator      hint_required     no validator
  string column      any name       hint ends name    hint ends name
  integer column     hint in name   hint ends name    never
```

An integer card column whose name hints nothing stays untyped. No `generator_hint` is written on an
integer column, because the numeric sampler draws no hint.

## A hint is written only where it draws the column's own format

The engine draws a hint's `provider` first, then its `pattern`, and only then the pattern grex
stored. So a hint would replace the column's format with its own. Where grex stored a pattern:

- **A `provider` is dropped**, because its format cannot be compared with the stored pattern.
- **A `pattern` is kept only where the column's values match it**, at the rate that typed the
  column.
- **A `check_digit` is always kept**, because it rewrites the last digit of whatever draws the value.

So a phone column of `(415) 555-0123` and a spaced card column keep their formats. A dashed phone
column keeps the hint's NANP-valid pattern. A column of upper-case UUIDs or hyphenated MAC addresses
takes the provider's lower-case, colon-separated form, because grex stores no pattern for either.

## A hint names a provider where one draws valid values, and a pattern where none does

A hint replaces a provider that fails its type's validator, or that draws the wrong part of a
value:

| Type | Provider that fails | Hint instead |
| --- | --- | --- |
| `phone_number` | fake's `phone_number` adds an extension to half its numbers. `cell_number` starts an area code or an exchange with 0 or 1 in many of its numbers | `[2-9]\d\d-[2-9]\d\d-\d{4}` |
| `person_name` | `name` would put full names in a first-name column | `first_name`, `last_name` or `name` |

These hold for the fake version `crates/core/Cargo.toml` pins.
`cli/tests/test_detector_pack_hints.py` generates from every shipped pack's hints and fails
naming each whose values stop passing its own validator, as a bump of fake or `rand_regex` could
make them. `us_ssn`'s pattern spells out each
excluded area, group and serial, because `rand_regex` has no lookaround. `url` draws from the
reserved `example` domains, so no generated link reaches anyone.

## The general pack types identifiers by format and check digit

`doppeldata/profiling/detector_packs/general.py` lists the general pack's types, each with its
validator, sensitivity and Glue managed type.

- **A NANP area code and exchange never start with 0 or 1.** Keeping that rule stops a column of
  10-digit NPIs, most of which start with 1, typing as phone numbers.
- **An SSN without hyphens is typed only where the name hints it.** Most random 9-digit codes pass
  the area, group and serial rules, so an unhinted account number would type as an SSN.
- **A UUID is an identifier whether or not its values repeat.** A member's UUID repeats across that
  member's claims. Typed `none`, it would reach `top_values` once `k_min` rows held it.

A detector records Glue's managed type as `glue_type`, and the profile does not write it. The
profile's own `semantic.type` is a lowercase snake identifier, which stays stable if Glue renames a
type.

## The healthcare pack ships with the library, and policy healthcare enables it

`doppeldata/profiling/detector_packs/healthcare.py` lists its types. `--pack healthcare` enables it
under any policy, and `--policy healthcare` enables it without being named.

- **An NPI and a DEA number are typed by their check digits.** A 10-digit number starting 1 or 2
  passes an NPI's shape, and one in ten passes its check digit, so a column of other numbers stays
  untyped.
- **An ICD-10-CM or HCPCS Level II code is typed by its shape where the name hints it, and by
  membership where the caller supplies an `icd10cm` or `hcpcs_level2` list.** Most short words pass
  ICD-10-CM's shape, and a product code HCPCS's. The hint must end the name, so `diagnosis_code`
  and `px` hint a code, and `diagnosis_date` hints none. Its generator hint is the `icd10cm` or
  `hcpcs_level2` provider, which draws a code in the same shape. `healthcare-policy.md` says how
  each list is written from its publisher's file.
- **A CPT code is typed by its shape alone**, and only where the name hints it. Five digits is a ZIP
  code's shape too.
- **An NDC is typed by its shape, and by membership where the caller supplies an `ndc` list.** An
  NDC without hyphens is typed only where the name hints it.
- **A member, medical record or claim number has no published format.** It is typed by its name
  hint and a stable shape: the values' most common shape holds a digit, and enough of them share it.

## A detector may read a code list the caller supplies, recorded by its sha256

A `Detector` whose `code_list` names a list, such as `ndc` or `icd10cm`, reads the file a run supplies under
that name: `profile create --code-list ndc=FILE`, or `profile(..., code_lists={'ndc': path})`.
FILE holds one code a line. The detector's `validate` takes the list's lines as a second argument,
or None where the run supplied none, so the type falls back to what it decides without one.

- **`generated.code_lists` records each list's name and the sha256 of its content.** It records no
  path, because a committed profile must not name a machine's directories. An earlier profile made
  with another list, or none, is not reused by `--edges-from`.
- **A name no enabled detector reads is a usage error**, exit 2, naming the lists the enabled
  detectors read, or the shipped packs that read one.
- **Detection reads the list, and generation does not.** A typed column is drawn from its hint, so
  its generated codes are not the list's. `data check` profiles generated data without the list,
  and passes a type change on a type a list decides, because only a run reading it could confirm
  the type.

*Rejected:* naming the list in an environment variable. It is state no argument shows, and the
profile would not say which list typed it.

## A pack is a plugin, registered under an entry point by its name

A third party adds types without forking by building a `DetectorPack` of `Detector`s, both from
`doppeldata.profiling.detector_packs`. It registers the pack under the `doppeldata.detector_packs`
entry-point group, with the pack's name as the entry point's:

```toml
[project.entry-points."doppeldata.detector_packs"]
acme = "acme_detectors:PACK"
```

- **A pack is code the profiler runs on the data.** Its validators are handed the sampled values of
  every column they may type, so enable only a pack trusted with the data. The profile holds none
  of those values. No pack runs unless a run names it.
- **The general pack runs on every profile.** `profile(..., packs=['acme'])` and
  `profile create --pack acme` enable others. The general pack runs first, then the rest by name,
  so the order a run names them in changes nothing.
- **A pack the library ships is found without an entry point**, so an install with no package
  metadata still has it.
- **`Detector` and `DetectorPack` are frozen dataclasses the library owns.**
- **A pack is checked when it loads.** Its name must match the entry point's and be a lowercase
  snake identifier. Each type, sensitivity, method and hint must be one the profile format
  accepts, read from its published schema. Each hint pattern must be one the engine can draw from.
  Detectors of one type must agree on its sensitivity and Glue type. A mistake is named at the pack,
  rather than as the written profile failing its schema, or as generation failing wherever it runs.
- **Two enabled packs may not declare one type.** The privacy policy reads a type's sensitivity
  from one pack.
- **A pack named but not installed is `refused`, and exits 2.** Any other pack error is `failed`,
  exits 1 and names the pack. A validator's error also names the type and the column. The kinds
  are a stopped write's, and `errors.py` maps them to exit statuses for both.
- **`semantic.pack` names the pack that typed each column**, so a reviewer sees whose rule assigned
  the type. `generated.detector_packs` names each pack a run enabled, with the distribution and
  version that provided it. An earlier profile stands unchanged only where those match, and they
  are left out of the digest, as `tool` and `fitter` are.

## Detection costs little beside the sample it shares with grex

Detection runs each candidate type's validator over the `VALUE_SAMPLE_ROWS` sample. grex draws that
sample for any table whose string columns hold a dominant shape. Detection adds the sample's draw
only to a table where grex would not: one whose string columns hold no dominant shape, or one with
a hinted integer column and no string column.

## No model is needed for the general pack's types

Every general type but a person's name is detectable by its format and check digit.

*Rejected:* DataProfiler's character-CNN labeler. It needs TensorFlow 2.16 or later and Keras 3, too
heavy for a profiler that must install on Glue.

*Rejected for now:* Presidio's named-entity recognizer, which would type names and free-text
entities better than a column name does. Its spaCy models are downloaded from GitHub release assets,
which a network that blocks GitHub's asset hosts cannot fetch.

Sources: [Glue sensitive data managed types](https://docs.aws.amazon.com/glue/latest/dg/sensitive-data-managed-data-types.html),
[DataProfiler](https://github.com/capitalone/DataProfiler) 0.14.0, [fake](https://github.com/cksac/fake-rs),
[rand_regex](https://crates.io/crates/rand_regex).
