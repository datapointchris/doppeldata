# Review and commit a profile

A profile is the file a reviewer reads before it is shared or committed. Its `privacy` section
lists every data value the file keeps and the rule that admitted it, and `profile check` recomputes
each rule from the file alone, so CI can hold every committed profile to its policy.

```bash
doppeldata profile create data/orders/ --out orders.profile.yaml
yq '.privacy' orders.profile.yaml
doppeldata profile check orders.profile.yaml
git add orders.profile.yaml
```

```python
import doppeldata as sd

profile = sd.profile('data/orders/', policy='strict', k_min=50)
profile.save('orders.profile.yaml')
```

## `privacy` names every value the file keeps, and the rule that let it in

```yaml
privacy:
  policy: default
  k_min: 11
  suppressed:
    - {column: customer_email, reason: identifier, semantic: email}
  audit:
    - {column: order_id, field: /numeric/extremes, values: 2, rule: trimmed_k, min_rows: 11}
    - {column: quantity, field: /numeric/top_values, values: 10, rule: count >= k_min, min_rows: 24}
    - {column: status, field: /string/top_values, values: 5, rule: count >= k_min, min_rows: 1586}
    - {column: ship_zip, field: /string/pattern, values: 1, rule: generalized}
```

- **`suppressed` lists each column the policy withholds values from**, and why. Here
  `customer_email` is typed `email`, an identifier, so it keeps its lengths and shapes and no
  address.
- **`audit` lists each field holding a value from the data.** `field` points into its column,
  `values` counts what it holds, and `min_rows` is the fewest rows any of them rests on.
- **`dataset_audit` does the same for a conditional table, quantiles, rates or a lookup table**, and
  `withheld` names each dataset field left out for a floor it failed.

Read the audit list first. A value a reviewer would not publish, such as a customer's name in a
`top_values`, shows there with the column it sits in.

## `profile check` exits 0 when clean and 3 on any issue

```text
$ doppeldata profile check orders.profile.yaml
# doppeldata profile check passed

`orders.profile.yaml`, 1 table.

## orders

25 literal fields under policy `default`, `k_min` 11, and 0 issues.
```

It reads no data. It validates the schema first, then holds every literal to its audit entry and
rule. A hand edit that misspells a key fails at the key's pointer:

```text
$ doppeldata profile check typo.profile.yaml
# doppeldata profile check failed

`typo.profile.yaml`, 1 table.

## orders

7 issues against the schema.

| Field | Code | Issue | Hint |
| --- | --- | --- | --- |
| `/columns/0/null_fracton` | E-PROFILE-006 | 'null_fracton' is not a field here; the closest is null_fraction | doppeldata schemas show profile prints every field |
```

- **The report is GitHub Markdown**, so it pastes into a pull request as it is. The verdict prints
  on stderr, such as `doppeldata: E-PROFILE-029: typo.profile.yaml breaks its schema or its privacy
  policy in 7 places`.
- **`--json` prints `passed`, the profile and each table's `issues`**, each issue with its `code`,
  `field`, `rule` and `treatment`.
- **A file it cannot read is a usage error, exit 2.**
- **A seed imported from a schema fails it**, because no policy wrote it.

A CI step runs it on every committed profile:

```bash
for file in profiles/*.profile.yaml; do doppeldata profile check "$file" || exit 1; done
```

## The policy decides what a column may keep

`--policy` sets it when the profile is written, and `doppeldata capabilities show` lists each one:

| Policy | Keeps |
| --- | --- |
| `default` | no value fewer than `k_min` rows hold; an identifier keeps its format and no value, free text its lengths |
| `strict` | the same, and each quasi-identifier, such as a ZIP code or a birth date, is held as an identifier; no conditional table, lookup or lag is given one |
| `healthcare` | `default`'s rules and HIPAA Safe Harbor's, with no conditional table, lookup or lag given a quasi-identifier, as [Profile healthcare data](profile-healthcare-data.md) shows |
| `none` | every value, for data already synthetic or public |

`--k-min N` raises the floor:

```text
$ doppeldata profile create data/orders/ --policy strict --k-min 50 --out strict.profile.yaml
$ yq '.privacy.suppressed' strict.profile.yaml
- {column: customer_email, reason: identifier, semantic: email}
- {column: ship_zip, reason: quasi_identifier, semantic: us_zip}
```

`profile check` always reports a `none` profile, so one is never committed by mistake:

```text
$ doppeldata profile check none.profile.yaml
...
| Field | Code | Issue | Hint |
| --- | --- | --- | --- |
| `/privacy/policy` | E-POLICY-027 | policy is none; expected one of default, strict, healthcare |  |
doppeldata: E-PROFILE-029: none.profile.yaml breaks its schema or its privacy policy in 1 place
```

## Edit a profile by hand, then check it again

Every field is one a reviewer may edit, such as a null rate, a top value's share or a correlation.
`doppeldata schemas show profile` prints the JSON Schema each file is held to, and an editor that
reads JSON Schema completes the keys from it. A rewrite through doppeldata keeps the comments and
key order. Run `profile check` after each edit, then `data plan` to see how each column will draw.

## Why

[The profile format and its privacy floor](../profile-format-and-privacy-floor.md) says why a
profile is YAML, why no value rests on fewer than 11 rows, what each audit rule admits, and what
`profile check` can and cannot recompute.
