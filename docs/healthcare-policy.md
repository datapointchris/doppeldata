# The healthcare policy

## healthcare adds HIPAA Safe Harbor to the default floor

`profile create --policy healthcare` applies every `default` rule, then the Safe Harbor method of 45
CFR 164.514(b)(2). Safe Harbor removes 18 categories of identifier from a dataset: names; geographic
subdivisions smaller than a state; every element of a date tied to a person except its year;
telephone and fax numbers; email addresses; Social Security numbers; medical record numbers; health
plan beneficiary numbers; account numbers; certificate and license numbers; vehicle identifiers;
device identifiers; URLs; IP addresses; biometrics; full-face photos; and any other unique
identifying number. A profile holds aggregates rather than records, so the policy applies each
category to what an aggregate can give away. The policy also enables the healthcare detector pack.

`doppeldata/privacy_policy/column_treatment.py` decides each column's rule, from what the profile
states about the column, in this order:

1. A column typed `free_text` keeps its lengths.
2. A ZIP column keeps three-digit prefixes where it holds strings. It is a ZIP column where it is
   typed `us_zip`, or where its name ends in one of `ZIP_HINTS`, from `doppeldata/column_name_words.py`.
   A ZIP column held as a number is an identifier. A ZIP below 10000 stored as a number has lost
   its leading zeros, so its first three digits are not its prefix.
3. A Safe Harbor type is an identifier, whatever sensitivity the profile gives it.
   `SAFE_HARBOR_TYPES` lists them, and the library fails to import where it names a type no shipped
   pack declares. A column whose name holds one of `GEOGRAPHY_HINTS` is an identifier too.
4. Every date and timestamp column keeps the years of its extremes.
5. A numeric column whose name says birth, such as `year_of_birth`, is an identifier.
6. A column whose name holds one of `AGE_HINTS`, anywhere in the name, caps its ages at 90.
7. Otherwise the `default` rules hold.

An NPI names a provider rather than the patient, and a provider's NPIs are published. It is an
identifier under `healthcare` anyway, because a small practice's NPI beside a ZIP prefix and a year
narrows its patients to a few people.

## A ZIP code keeps its three-digit prefix, and a restricted prefix becomes 000

The statistics pass counts each value's first three characters, and keeps a prefix only where it is
three digits. A prefix on the restricted list is counted as `000`, and the counts are then held to
`k_min` as any top value is. The column writes them as `string.prefixes`: `values`, the
`other_rate` of rows whose prefix holds fewer than `k_min` rows, and the `suffix` drawn after each
prefix. The suffix is `\d{2}`, or `\d{2}-\d{4}` where the dominant shape is a ZIP+4. The column
keeps no `top_values` and no `pattern`, because a whole ZIP code or a pattern's literals could name
a place a prefix does not.

The column's distinct count is of its prefixes. The generator draws a listed prefix and a value of
the suffix, so the generated column holds whole ZIP codes. It draws the rest from the column's
shapes, at as many distinct prefixes as the count leaves.

The restricted prefixes are the three-digit ZIP areas that held 20,000 or fewer people in the 2000
Census. HHS's de-identification guidance lists them, and `privacy_policy/restricted_zip_prefixes.txt`
copies that list. `rg -v '^#' library/src/doppeldata/privacy_policy/restricted_zip_prefixes.txt`
prints it.

## An age over 89 is counted in one bin and drawn as 90

Typing runs before binning, so an age's histogram is binned to the cap: every edge above 89.5 gives
way to one last bin from 89.5 to 90.5, and the rows above that bin are counted in it. Edges frozen
from an earlier profile by `--edges-from` keep those below 89.5 and end in the same bin. Its
extremes and quantiles are capped at 90. It holds no fitted family, whose support could reach past
90. An age held as text keeps no top values, which would list each age over 89 alone. The numeric
sampler draws a histogram's bins only between the outermost quantiles, and its tail ends at the
extremes, so no generated age exceeds 90.

## Every date keeps the years of its extremes, and no birth year is more than 90 years back

Every date and timestamp column is read as tied to a person, whatever its name. A list of
person-date names misses schemas the policy is run on, such as MIMIC-IV's `admittime` and OMOP's
`condition_start_date`, so the rule fails closed and has no opt-out. The trimmed extremes widen to
the first day of the low's year and the last day of the high's. The gap rate is counted over those
whole years, because the gap rate and the distinct count together give the span between the
extremes in days. The month, weekday and hour shares stay, each cell held to `k_min`.

A date of birth is a column whose name holds `birth`, or a word ending in `dob`. Its low year is
raised to the year of `generated.at` less 90, so no birth year names an age over 89. Safe Harbor
treats a birth year as an element of an age. `profile check` reports a date of birth in a file that
states no `generated.at` as `birth-date-unanchored`, because nothing then bounds its years.

## A place below a state is an identifier

Safe Harbor removes every geographic subdivision smaller than a state. No shipped pack types a
city, county or street, so a column whose name holds one of `GEOGRAPHY_HINTS` is an identifier. A
state column keeps its values, because the general pack declares `us_state` with the sensitivity
`none`.

## The engine ships the ICD-10-CM and HCPCS lists, and generates codes from them

The healthcare pack types an ICD-10-CM or HCPCS Level II code by its membership in a list the
engine embeds from `crates/core/code_lists/`. A list holds one code a line without its dot, so an
update diffs line by line. Its first line names its release and the file it was taken from, and
`head -1 crates/core/code_lists/*.txt` prints both. The library reads each list from the extension
once a process.

The `icd10cm` and `hcpcs_level2` providers draw from the same lists, so a generated code passes the
validator that typed its column. A draw is uniform over the list, after the column's top values. An
ICD-10-CM code longer than its category is written with a dot after the category, where the
column's dominant shape has one. A list's codes are part of the generated stream, so a list update
raises the stream version.

- **ICD-10-CM** is every header and billable code in NCHS's order file for its fiscal year. A
  category's second character may be a letter, so the validator's shape is a letter and two letters
  or digits, and membership decides the rest.
- **HCPCS Level II** is every procedure code in CMS's quarterly alphanumeric file. Terminated codes
  are kept, because historical claims carry them.
- **CPT** is copyrighted by the AMA, so the package ships no CPT list. A CPT code is typed by its
  shape and a name hint, and generated by its shape.
- **The NDC directory** is public but large, so the package ships none. An NDC is typed by its
  segments, or by its digits and a name hint, and generated by its shape. `--code-list ndc=FILE`
  types it only where FILE holds it, as the FDA's directory writes a package code or as HIPAA's 11
  digits. `cut -f3 package.txt | tail -n +2` writes that list from the directory's package file.
  The directory lists marketed products, so a claim naming a withdrawn NDC needs a list holding it.

`rg -c -v '^#' crates/core/code_lists/icd10cm.txt` counts a list's codes. Each file stays under the
size limit the pre-commit hooks set on an added file.

## HCPCS Level II is published for public use, and ICD-10-CM's redistribution terms are not established

- **HCPCS Level II** is maintained by CMS, a US federal agency, and published on its quarterly
  update page. HL7's terminology review of US Core records it as available for public use.
- **ICD-10-CM** is NCHS's clinical modification of ICD-10. WHO owns ICD-10, and authorized NCHS to
  develop ICD-10-CM. NCHS publishes its files at no cost. Neither NCHS's nor CMS's ICD-10 page
  states a license for redistributing them. In 2014 WHO wrote to an open-source project that
  ICD-10's codes should not be licensed under its open-source license, and that redistributing
  ICD-10 requires WHO's permission. The engine ships the codes alone, without their descriptions.
  Whether that needs WHO's permission is not established.

## The claims example is the policy's reference

`doppeldata profile show claims` prints a hand-written claims table under `policy: healthcare`, and
`profile check claims` passes it. Its `member_id` is an integer identifier that holds no literal,
only the bounds of its digit count. Its NPI column is typed by the healthcare pack. Its ICD-10-CM
and HCPCS columns are typed by it and generated by its providers. Its `member_dob` and
`service_date` keep year-level extremes, and `member_dob`'s low is 90 years before its
`generated.at`. Its `member_age` ends in the 90.5 bin, and its `member_zip` lists `000` among its
prefixes.

## Sources

- [45 CFR 164.514](https://www.law.cornell.edu/cfr/text/45/164.514): the Safe Harbor method, its
  18 categories, the 89-year age limit and the 20,000-person ZIP rule.
- [HHS guidance on de-identification](https://www.hhs.gov/hipaa/for-professionals/special-topics/de-identification/index.html):
  the restricted three-digit ZIP prefixes from the 2000 Census.
- [Duke Health IRB, de-identification of health information](https://irb.duhs.duke.edu/sites/default/files/2022-04/deidentification_of_health_information_9-28-2021.pdf):
  the same prefixes, quoted from the HHS guidance.
- [CMS cell size suppression policy, via ResDAC](https://resdac.org/node/1506): the `k_min` of 11
  the `default` policy starts from.
- [NCHS ICD-10-CM files](https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Publications/ICD10CM/):
  every fiscal year's code files, in a directory of its own.
- [NCHS ICD-10-CM page](https://www.cdc.gov/nchs/icd/icd-10-cm/index.html): WHO's ownership of
  ICD-10, and its authorization of ICD-10-CM.
- [CMS HCPCS quarterly update](https://www.cms.gov/medicare/coding-billing/healthcare-common-procedure-system/quarterly-update):
  every quarter's alphanumeric HCPCS file.
- [HL7 US Core terminology license review](https://www.hl7.org/fhir/us/core/STU4/qa-hta.html): HCPCS
  Level II as maintained by CMS and available for public use.
- [CamCOPS, ICD-9-CM and ICD-10 tasks](https://camcops.readthedocs.io/en/latest/tasks/icd.html):
  WHO's 2014 letters on licensing and redistributing ICD-10's codes.
