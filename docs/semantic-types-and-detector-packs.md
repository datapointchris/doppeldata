# How a column is given a semantic type

## A column takes the type whose validator passes the most of its sampled values

Detection runs in `doppeldata.profiling.semantic_types`, on the seeded sample of `VALUE_SAMPLE_ROWS`
rows that grex also reads. Every engine's backend draws that sample, so detection is the same on
each. A column's nulls are dropped first, so a column with nulls is validated on fewer values.

- **A validator passing `ASSIGN_RATE` of the values assigns its type.** One passing
  `HINTED_ASSIGN_RATE` assigns it where the column's name hints the type too.
- **Of the types that qualify, the highest pass rate wins.** A tie goes to the hinted type, then to
  the pack and the detector listed first.
- **`quality.invalid_rate` is the share the winning validator failed.** A card-number column with
  0.3% bad check digits writes `invalid_rate: 0.003`, and the generator leaves that share of its
  Luhn digits wrong.
- **`confidence` is the pass rate.** A name hint halves the doubt left, so 0.9 with a hint is 0.95.
- **`detectors` lists the methods that agreed:** `pattern` for a validator, `check_digit` where the
  hint carries one, and `name_hint`.
- **A column with fewer than `k_min` sampled values is left untyped.**
- **Typing changes no statistic.** What a typed column keeps is the privacy policy's decision.

## A name hint is a run of the column name's words

A column name splits into lower-case words at underscores, hyphens, digits and case changes. So
`customerEMail`, `Customer-E-Mail` and `customer_e_mail` are each `customer e mail`. A hint is
written in snake case, and matches where its words run in order inside the name's. `e_mail` and
`mail` both hint `email` there.

A hint never assigns a type alone, except a type whose detector has no validator. A person's name is
the general pack's one such type, and its confidence is `HINT_ONLY_CONFIDENCE`. It is three
detectors of the one type, split by the part of the name a column holds, so a first-name column
draws first names. A bare `name` hints none of them, because a product, a company or a file has one
too.

## An integer column is typed only where its name hints the type

A text source parses a column of 16-digit card numbers as `int64`, because they fit. So detection
also reads each integer column whose name hints a type, as its values' decimal text, and assigns
only a hinted type. A validator alone never types an integer column, because every 5-digit count
would pass as a ZIP code.

An integer card column whose name hints nothing stays untyped. A `generator_hint` on an integer
column is written, and the numeric sampler ignores it.

## Each generator hint draws values its own validator passes

A hint names a provider where one draws valid values, and a pattern `rand_regex` draws where none
does. Three providers fail their type's validator:

| Type | Provider that fails | Hint instead |
| --- | --- | --- |
| `us_zip` | fake's `zip_code` draws 3-, 4- and 5-digit codes, and `post_code` draws half ZIP+4 | `\d{5}` |
| `phone_number` | fake's `phone_number` adds an extension to half its numbers. `cell_number` starts an area code with 1 in 11%, and an exchange with 0 or 1 in 20% | `[2-9]\d\d-[2-9]\d\d-\d{4}` |
| `person_name` | none fail, but `name` would put full names in a first-name column | `first_name`, `last_name` or `name` |

`us_ssn`'s pattern spells out each excluded area, group and serial, because `rand_regex` has no
lookaround. `url` draws from the reserved `example` domains, so no generated link reaches anyone.

The engine generated 100,000 rows from a profile holding each hint and nothing else. Each column
passed its own validator at 1.0, and `credit_card`, given an `invalid_rate` of 0.003, at 0.9969.

## The general pack types identifiers by format and check digit

| Type | Validator | Sensitivity | Glue managed type |
| --- | --- | --- | --- |
| `email` | an RFC 5322 subset | identifier | `EMAIL` |
| `phone_number` | E.164, or NANP with area code and exchange from 2 to 9 | identifier | `PHONE_NUMBER` |
| `credit_card` | 13 to 19 digits, an IIN prefix, Luhn | identifier | `CREDIT_CARD` |
| `us_ssn` | `AAA-GG-SSSS`, area not 000, 666 or 9xx, group not 00, serial not 0000 | identifier | `USA_SSN` |
| `uuid` | RFC 9562 shape, version 1 to 8, variant `10` | identifier | none |
| `ipv4_address`, `ipv6_address` | Python's `ipaddress` | identifier | `IP_ADDRESS` |
| `mac_address` | six hex octets, or three dotted groups of four | identifier | `MAC_ADDRESS` |
| `url` | a scheme and a host | quasi_identifier | none |
| `us_zip` | ZIP or ZIP+4 | quasi_identifier | none |
| `us_state` | a USPS code: the states, DC, the territories and the armed forces codes | none | none |
| `person_name` | none, assigned by name hint | identifier | `PERSON_NAME` |
| `free_text` | 4 or more words, in a column averaging over 40 characters | sensitive | none |

- **A NANP area code and exchange never start with 0 or 1.** Keeping that rule stops a column of
  10-digit NPIs, most of which start with 1, typing as phone numbers.
- **An SSN is typed only with its hyphens.** About 89% of random 9-digit codes pass the area, group
  and serial rules. So accepting undashed SSNs would type an account number drawn from the valid
  range as an SSN.
- **A UUID is an identifier whether or not its values repeat.** A member's UUID repeats across that
  member's claims. Typed `none`, it would reach `top_values` once `k_min` rows held it.

Glue's names are the import and export vocabulary. The profile's own `semantic.type` is a lowercase
snake identifier, which stays stable if Glue renames a type.

## A pack is a plugin, registered under an entry point by its name

A third party adds types without forking by implementing `DetectorPack` from
`doppeldata.profiling.detector_packs`. It registers the pack under the `doppeldata.detector_packs`
entry-point group, with the pack's name as the entry point's:

```toml
[project.entry-points."doppeldata.detector_packs"]
acme = "acme_detectors:PACK"
```

- **The general pack runs on every profile.** `profile(..., packs=['acme'])` and
  `profile create --pack acme` enable others. The general pack runs first, then the rest by name,
  so the order a run names them in changes nothing.
- **A pack the library ships is found without an entry point**, so an install with no package
  metadata still has it.
- **Each member of the protocol is a read-only property.** A frozen dataclass and a plain class both
  satisfy it. A class attribute must be annotated as `Sensitivity`, because a bare string is
  inferred as `str`.
- **`validate` is None where a type has no validator.** The detector then says so before anything
  calls it, and only its name hints assign it.
- **A pack is checked when it loads.** Its name must match the entry point's, each type must be a
  lowercase snake identifier, and each sensitivity and hint must be one the profile format accepts.
  A mistake is named at the pack, rather than as the written profile failing its schema.
- **A pack named but not installed exits 2.** One that fails to load, breaks the protocol, or whose
  validator raises exits 1, naming the pack, the type and the column.
- **`semantic.pack` names the pack that typed each column**, so a reviewer sees why a column was
  suppressed. `generated.detector_packs` names each pack a run enabled, with the distribution and
  version that provided it. An earlier profile stands unchanged only where those match, and they
  are left out of the digest, as `tool` and `fitter` are.

## Detection adds a few hundredths of a second to a 10M-row profile

On a 10M-row Parquet file of six columns, two of them strings, on 16 threads:

| Part | Time |
| --- | --- |
| loading the general pack, once pyarrow is imported | 0.008 s |
| typing the two string columns over the sample | 0.015 to 0.042 s |
| drawing the 10,000-row sample | 0.29 s |

The whole profile took 4.8 to 7.0 s with detection or without, as other load on the machine
varied. That file's email column holds a dominant shape, so grex draws the sample either way.
Detection adds the sample's cost only to a table where grex would not draw it: one whose string
columns hold no dominant shape, or one with a hinted integer column and no string column.

## No model is needed for the general pack's types

Every type above but a person's name is detectable by its format and check digit.

*Rejected:* DataProfiler's character-CNN labeler. It needs TensorFlow 2.16 or later and Keras 3, too
heavy for a profiler that must install on Glue.

*Rejected for now:* Presidio's named-entity recognizer, which would type names and free-text
entities better than a column name does. Its spaCy models are downloaded from GitHub release assets,
which a network that blocks GitHub's asset hosts cannot fetch.

Sources: [Glue sensitive data managed types](https://docs.aws.amazon.com/glue/latest/dg/sensitive-data-managed-data-types.html),
[DataProfiler](https://github.com/capitalone/DataProfiler) 0.14.0, [fake](https://github.com/cksac/fake-rs) 5.1.0,
[rand_regex](https://crates.io/crates/rand_regex) 0.19.0.
