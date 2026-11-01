# Profile healthcare data

`--policy healthcare` holds a profile to HIPAA Safe Harbor on top of the default floor. It also
types NPIs and ICD-10-CM, HCPCS, CPT and NDC codes with the healthcare detector pack, and a code
list you supply types a code column by membership.

```bash
doppeldata profile create claims.csv --policy healthcare --out claims.profile.yaml
awk '{ print $2 }' icd10cm-order-2027.txt > icd10cm.txt
doppeldata profile create claims.csv --policy healthcare --code-list icd10cm=icd10cm.txt --out claims.profile.yaml
doppeldata profile check claims.profile.yaml
```

```python
import doppeldata as sd

profile = sd.profile('claims.csv', policy='healthcare', code_lists={'icd10cm': 'icd10cm.txt'})
profile.save('claims.profile.yaml')
```

`doppeldata data generate claims --rows 50000 --seed 1 --format csv --out claims.csv` writes 50,000
rows of the claims example, to follow along without real claims.

## `privacy.suppressed` names each column Safe Harbor reaches, and why

```text
$ yq '.privacy.suppressed' claims.profile.yaml
- {column: claim_id, reason: identifier, semantic: claim_number}
- {column: member_id, reason: identifier, semantic: health_plan_beneficiary_number}
- {column: member_dob, reason: date_to_year}
- {column: member_age, reason: age_over_89}
- {column: member_zip, reason: zip_prefix, semantic: us_zip}
- {column: rendering_npi, reason: identifier, semantic: npi}
- {column: service_date, reason: date_to_year}
```

| Reason | The column keeps |
| --- | --- |
| `identifier` | its format and lengths, and no value. A Safe Harbor type, an NPI, and a place below a state are identifiers |
| `date_to_year` | its extremes widened to whole years, and its month and weekday shares held to `k_min`. Every date and timestamp is read as a person's |
| `age_over_89` | ages capped at 90, every age over 89 counted in one bin from 89.5 to 90.5 |
| `zip_prefix` | three-digit prefixes held to `k_min`, a restricted prefix counted as `000` |

A date of birth's low year is raised to 90 years before `generated.at`, so no birth year names an
age over 89:

```text
$ yq '.generated.at, (.columns[] | select(.name == "member_dob" or .name == "member_age") | [.name, .temporal.extremes // .numeric.extremes])' claims.profile.yaml
2026-11-01T07:14:33Z
- member_dob
- {low: '1936-01-01', high: '2008-12-31', granularity: day}
- member_age
- {low: 18, high: 90, method: trimmed_k}
```

`profile check` recomputes each rule from the file:

```text
$ doppeldata profile check claims.profile.yaml
# doppeldata profile check passed

`claims.profile.yaml`, 1 table.

## claims

24 literal fields under policy `healthcare`, `k_min` 11, and 0 issues.
```

`doppeldata profile show claims` prints a profile written under this policy, and
`doppeldata profile check claims` passes it.

## A code list types a code column only where it holds the column's codes

Without a list, a code column is typed by its shape where its name hints the code. A list adds
membership, and a list missing the codes leaves the column untyped:

```text
yq -o=json -I0 '.columns[] | select(.name == "diagnosis_code") | .semantic | [.type, .detectors]' FILE
```

| Profiled with | Prints |
| --- | --- |
| no `--code-list` | `["icd10cm",["pattern","name_hint"]]` |
| `--code-list icd10cm=icd10cm.txt`, holding the column's codes | `["icd10cm",["pattern","validator","name_hint"]]` |
| `--code-list icd10cm=other-icd.txt`, holding none of them | `[null,null]` |

FILE holds one code a line. Write each list from the file its publisher issues:

| List | FILE holds | Written by |
| --- | --- | --- |
| `icd10cm` | the code, with or without its dot | `awk '{ print $2 }' icd10cm-order-2027.txt`, from NCHS's code descriptions zip |
| `hcpcs_level2` | the code | `awk 'substr($0, 11, 1) == 3 { print substr($0, 1, 5) }' HCPC2026_OCT_ANWEB_09232026.txt`, from CMS's quarterly alphanumeric zip |
| `ndc` | a package code as the FDA's directory writes it, or as HIPAA's 11 digits | `cut -f3 package.txt \| tail -n +2`, from the directory's package file |

- **NCHS's order file holds every header and billable code**, without dots. Its third field is 1
  for a billable code, so `awk '$3 == 1 { print $2 }'` writes the billable codes alone.
- **CMS's file holds each procedure code on a record whose 11th character is 3.** A record
  starting with three blanks is a modifier's. Terminated codes stay in the file, because historical
  claims carry them.
- **The NDC directory lists marketed products**, so a claim naming a withdrawn NDC needs a list
  holding it.
- **The profile records each list's name and sha256, never its path**:

  ```text
  $ yq '.generated.code_lists' claims.profile.yaml
  {icd10cm: 'sha256:6b36b937c45262b9e5d3315c2f9550ea4092394393106d465c02754feb9e0498'}
  ```

- **A list no enabled detector reads is refused**, exit 2, naming those it reads:

  ```text
  doppeldata: E-DATA-020: no enabled detector reads a code list named 'icd10'; the enabled detectors read hcpcs_level2, by healthcare hcpcs_level2; icd10cm, by healthcare icd10cm; ndc, by healthcare ndc
    closest: icd10cm
    valid:
      hcpcs_level2
      icd10cm
      ndc
  ```

  Under another policy, `--pack healthcare` enables the pack that reads them.

A generated code is drawn by its format, so it may name no code in the list. `data check` profiles
generated data without the list.

## Why

[The healthcare policy](../healthcare-policy.md) says what each Safe Harbor category becomes in a
profile, why every date is a person's, and why no code set ships.
[How a column is given a semantic type](../semantic-types-and-detector-packs.md) says how the
healthcare pack types each code, and how a detector reads a list.
