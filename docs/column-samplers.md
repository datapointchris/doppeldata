# How each column kind is drawn from its profile

## A numeric column draws a uniform level through a piecewise-linear inverse CDF

The inverse CDF is a list of (level, value) knots. The stored quantiles are the frame, with the
extremes at levels 0 and 1. A histogram's edges, at their cumulative shares, fill the body between
the outermost quantiles. A knot that would put a value out of order is dropped, so the quantiles
win where sources disagree.

`zero_fraction` adds a flat run at zero, from `negative_fraction` to `negative_fraction + zero_fraction`. The run
is clamped between the last knot below zero and the first above it, so it stays in order and is
always kept. The rates are stored rounded, so their sum can land just past the share at the next
edge. Clamping there keeps the zeros a column holds, short of the rounding.

An edge's share counts every row, the histogram's `overflow` included. Edge `i` sits at level
`(rows_below + bin_rows[0] + … + bin_rows[i - 1]) / (rows_below + sum(bin_rows) + rows_above)`, so
edge 0 sits at `rows_below`'s share. A profile without `overflow` has `rows_below` and `rows_above`
at 0, and its edges run from
level 0 to 1. A profile whose edges were frozen from an earlier one still places them at the shares
its own rows give.

Every value is one uniform level, whatever the knots came from. So a profiler that adds a histogram
changes the values but not the draw order, and the samplers need no change to read it.

`integral` rounds the value to the nearest integer. So the profiler writes an integer column's
histogram edges halfway between integers, and each bin draws the integers it counted. `scale`
rounds the value to that many decimal places, and a decimal type to its own scale. So a float
column every value of which has two places generates cents. A `sequence` emits
`start + step · row` and draws nothing.

## Beyond the outermost quantile, a fitted tail replaces the straight line

A trimmed extreme sits far past p99 in any right-skewed column. A straight line from p99 to it puts
too much weight in the tail. On the `eight-columns` example's `amount`, a lognormal with a stored
mean of 33.12:

| Construction | Mean |
| --- | --- |
| linear across every knot | 44.34 |
| the p99-to-extreme segment's share of that | 11.03 |
| the true lognormal's share above p99 | 3.06 |
| generated, with the fitted tail, from 1M rows | 36.35 |

The generated standard deviation is 46.51, against a stored 43.41. The linear rows integrate the
piecewise-linear inverse CDF over `amount`'s stored quantiles and extremes, and the lognormal row
integrates its fit. The generated rows were measured at stream version 1, as the mean and standard
deviation of `amount` in the file this writes:

```bash
cargo run --release -p doppeldata-core --example throughput -- eight-columns --rows 1000000 --repeat 0 --ipc amount.arrow
```

- **Where `fit` names a family with a closed form, the tail is that family's conditional tail.** It
  is conditioned on the range from the outermost quantile to the extreme, and truncated there. The
  body stays on the quantiles.
- **The tail is tabulated at plan time into dense knots**, each leaving a geometrically smaller share
  of the tail beyond it. A draw then costs one uniform and one lookup, whatever the family.
- **A discrete family's tail includes the stored quantile's own mass**, so an integral column's
  generated quantile still lands on the stored value.
- **A family whose support stops short of the extreme leaves that tail straight.** A lognormal
  whose `loc` sits just below zero draws nothing below `loc`, so toward a low extreme far under it
  the tail would put every value between them on one segment. The line from the quantile to the
  extreme draws there.
- **Where the body starts at or above zero and the low extreme sits below it, `negative_fraction`
  bounds the lower tail's share below zero.** The levels from it to the body's first knot draw
  between zero and that knot, so a tail fitted to the body cannot carry p001's whole share below
  zero.
- **The remaining lift is the body.** Below p99, linear interpolation across the stored quantiles gives 33.31
  against 30.06 for the true lognormal. A histogram's many knots shrink it.

## A numeric column with few values draws each listed value by its rate

The profiler lists a numeric column's values as `numeric.top_values`, beside an `other_fraction`, where
it holds at most `NUMERIC_TOP_VALUES_THRESHOLD` distinct values. An alias table draws a listed value
by its `fraction`, or the `other_fraction`. A value drawn from the `other_fraction` comes from the
column's inverse CDF with every level rounding to a listed value cut out, and the levels left keep
their widths. Where no level is left, the `other_fraction` is drawn as the listed values are.

*Rejected:* scaling the levels left by the rows each histogram bin holds outside the list. It moved
no `data check` metric past its threshold on the taxi round trip below. The floor merges each bin
holding 1 to `k_min − 1` rows outside the list, which left one bin in the two columns it moved.

- **A lattice column's distinct count can show values beyond its trimmed extremes.** On an integer
  column, a decimal, or a float with a `scale`, the values the extremes leave room for are the points
  between them that no listed value takes. Where `distinct.approx_values` holds more than the listed
  values and that room, the rest lie beyond the extremes.
- **At most one value hides for each row the extremes' method leaves beyond them.** `trimmed_k`
  leaves `k_min − 1` rows beyond each extreme, and `exact` none. The values on one side share half
  that side's rows. A profile stating no `privacy.k_min` is read at `DEFAULT_K_MIN`, the floor the
  profiler applies by default.
- **They are drawn on the side the histogram's overflow names**: above where only `rows_above`
  counts rows, below where only `rows_below` does, and otherwise above and below in turn. Their rows
  come out of the `other_fraction`, never on top of it.
- **A continuous column never draws past its extremes.** Its room between them is unbounded.

`data plan` describes the curve from what `numeric_cdf` built, so each tail reads as fitted or
linear as it is drawn. It counts the profile's values apart from those beyond the extremes. The
determinism fixture's `top_values_with_one_beyond_the_extremes` reads "4 top values and 1 beyond
the extremes, the rest from quantiles with linear tails".

A list stating no `other_fraction` draws no value beyond the extremes, because those rows come out
of it. On the NYC yellow taxi trips for February to April 2024, `payment_type` holds six values.
Value 5 sits on 2 rows, so the privacy floor recodes them as value 1, the modal value. Value 3 stays
listed on its 64,979 rows, and the generator draws the five listed values alone. The taxi figures on
this page come from this round trip, with each column's values counted by Polars from
`taxi-generated/`:

```bash
doppeldata profile create yellow_tripdata_2024-02.parquet yellow_tripdata_2024-03.parquet \
  yellow_tripdata_2024-04.parquet --out taxi.profile.yaml
doppeldata data generate taxi.profile.yaml --rows 10000000 --out taxi-generated/
doppeldata data check taxi.profile.yaml taxi-generated/
```

## A string column takes the most specific source its profile holds

1. **`top_values`**, or else **`prefixes`**: an alias table over the rates. A draw of `other_fraction`
   generates a value from the next source down. A drawn prefix is followed by a value of the
   `suffix` pattern. A ZIP column under `healthcare` keeps three-digit prefixes, so `\d{2}` makes
   each a ZIP code again.
2. **`semantic.generator_hint.provider`**: a named generator, such as `email` or `city`. The names
   are doppeldata's own, an enum in the profile model, so the published schema lists them and
   replacing the faker library breaks no profile. A profile naming another fails validation.
   `icd10cm` and `hcpcs_level2` draw a code in the shape the healthcare pack types the column by.
   The code may name nothing in the code set, and `healthcare-policy.md` says how each is drawn.
   `zip_code` is five digits, which fake 5.1's `ZipCode` is not.
3. **A pattern**, from `generator_hint.pattern` or `string.pattern`. A line anchor is dropped,
   because a value is the whole line, and `\d` and the other classes are ASCII.
4. **`shapes`**: a shape is drawn by rate, then each `A` becomes an upper-case letter, each `a` a
   lower-case one, and each `9` a digit.
5. **Characters** of `charset.classes`, at a length drawn from `length`.

A column typed by the general pack that holds no hint, `string.pattern` or `string.shapes` takes
its type's hint before the engine reads the profile. So a hand-written `type: email` column draws
addresses, not characters. The profiler writes no hint where grex stored a pattern, so a profiled
column keeps its own format.

`generator_hint.check_digit: luhn` writes the Luhn digit over a value's last digit, wrong on a
`quality.invalid_fraction` share of values. `npi` is the Luhn digit over `80840` and the first nine
digits, as the NPI standard prefixes them. `dea` is the last digit of the sum of the first, third
and fifth digits plus twice the sum of the second, fourth and sixth. *Rejected:* the `luhn` crate. Its `checksum` allocates on
every call, and it panics on lower-case input and on a `u8` overflow past about 28 digits.

`email` is `first.last` at `example.com`, `example.net` or `example.org`, domains reserved so no
generated address reaches anyone. That holds 3,007 × 474 × 3 = 4,275,954 addresses. *Rejected:*
fake's `SafeEmail`. It is a first name at one of the three domains, so it holds 9,021 addresses.

## A key stated unique draws each string of its pattern or shapes at most once

A pattern drawn fresh on each row repeats a value about n²/2N times over n rows of N strings. A
column that alone is a primary or candidate key, of `uniqueness` 1 or none stated, and draws from a
pattern or from shapes, holds no value twice instead. It ranks, enciphers and unranks, as
format-preserving encryption builds a cipher over a regular language.

- **A keyed permutation takes each row to a rank among the strings.** It is the Feistel network
  with cycle walking that places a bundle's foreign keys, drawn from the `key_rank` stream.
- **A DFA of the pattern turns the rank into its string.** A compiled pattern bounds each unbounded
  repetition at `PATTERN_MAX_REPEAT` repeats past its least, as the fresh draw runs it. The DFA
  counts the strings ending within each number of bytes of each state. A rank takes the transition
  whose strings hold it, a byte at a time. A DFA holds one path per string, so two ranks never
  reach one string.
- **A column's shapes are ranked as one alternation of their strings.** `profile create` writes
  shapes and no pattern for a key whose letters vary. Each string is ranked once, so each shape
  takes its share of the strings, and the stated fractions give way.
- **Each string is as likely as any other.** So a ranged repetition such as `\d{6,8}` draws mostly
  its longest strings, where the fresh draw takes each repeat count as often.
- **Past 2^64 strings, each place the permutation reaches takes its own band of ranks.** A hash of
  the place picks the rank inside it. Past 2^128 the counts saturate. A transition with more
  strings past it than any rank reaches is then picked by a hash of the rank.
- **One position takes fewer characters past 2^128 strings.** It is the first position whose
  strings past each character count under 2^128. A rank below 2^128 reaches only the first
  2^128 ÷ those strings of its characters, rounded down, plus one. In `[a-z]{30}` that is the third
  position, with 26^27 strings past each letter, so it takes 3 of the 26.
- **A check digit is written over its site, so the key counts what surrounds it.** The site is the
  last byte the check digit's kind can be written over. The key ranks the part before the site, then
  closes it on the smallest site byte and the shortest tail holding no other site. Each row's part
  is its own, so its check digit, right or wrong, leaves the value its own too.
- **A child reads the key at its parent's row**, so a foreign key holds the strings the parent
  writes.
- **A key holding for every row is refused where it cannot be drawn.** More rows than its strings
  is refused at plan time as `E-PROFILE-125`. A pattern or shapes whose DFA or count table passes
  `AUTOMATON_BYTES` is refused as `E-PROFILE-126`. A key holds for every row where it states no
  `uniqueness`, or states 1 of rows no sample left unread and no privacy floor wrote, as every
  export reads it.
- **A profiled 1 takes the draw where it can, and draws fresh where it cannot.** A profiler writes
  a key that fewer than `k_min` rows break as 1, so its 1 states nothing of those rows. No export
  marks it unique, so neither refusal is raised. A key whose stated `uniqueness` is under 1 draws
  fresh.
- **`cdc` accepts such a key while its full load and inserts fit in the strings.** An insert draws
  its key at its own row past the full load, which the permutation still spans. More is refused as
  `E-PROFILE-125`.
- **`data plan` traces the draw to the key.** The trace lists the key's `columns` and `uniqueness`,
  and beside a stated 1 the `generated.engine` and `generated.sample.strategy` it is read against.
  The distinct ratio, which the draw ignores, is left out.

*Rejected:* an affine map `a·i + b mod N`. Neighboring rows' keys step by one constant, which shows
in their digits.

*Rejected:* a mixed-radix count over the parsed pattern. A pattern writing one string two ways, as
`\d{1,2}\d{1,2}` writes `123`, would give that string two ranks.

*Rejected:* the fresh draw for a key holding for every row past the bound. It would repeat a key
the profile states unique.

*Rejected:* one string count for both draws. The fresh draw samples rand_regex's derivations, which
the parsed pattern's product counts, and sizes its distinct index against that. Counting distinct
strings means building the DFA for every pattern column, which would put the bound and its refusal
on columns the fresh draw serves.

## A column below a distinct ratio of 1 draws each value at an index

A generated string or number is drawn fresh by default, so its distinct count is whatever the
source yields. With `distinct.ratio`, a row instead draws an index from `0..K` on a stream of its
own. The value at an index comes from a stream positioned at that index. So a value is a pure
function of its index, no pool of values is held, and any block still generates alone.

- **At or above the source's rows, the target is `distinct.ratio` times the generated rows.** So a
  10M-row run of a 100K-row profile holds 100 times the profiled distinct count, and its ratio
  matches the profile's. A coded column such as a ZIP code might saturate at its profiled count
  instead. One profile cannot show which, so the ratio holds.
- **Below the source's rows, the target is what a random sample of them holds.** With `N` source
  rows holding `D` distinct values, `n` rows hold `D · (1 − (1 − n/N)^(N/D))`, the sampled end of
  the range `data check` accepts. `N` is `dataset.rows`, or `D` over the ratio where the
  profile states none. So a short run of a ten-value column still draws all ten, where the ratio
  would ask for one.
- **K is chosen so the expected count of distinct indices meets the target.** `n` uniform draws
  from `0..K` hold `K · (1 − e^(−n/K))` distinct indices. `n` counts the rows not null.
- **A source whose strings can be counted has its collisions counted too.** `m` distinct indices
  drawn into `S` equally likely strings give `S · (1 − e^(−m/S))` distinct strings, so the index
  target is raised to meet the value target. A pattern is counted from its syntax, as if every
  string were equally likely. A shape is counted per character at its rate. `email`, `city`, and
  the providers drawing one entry of one list are counted from fake's lists, and `zip_code` from
  its digits. `icd10cm` and
  `hcpcs_level2` are counted from their shapes, ICD-10-CM's at the share of each length it draws. Characters and every other provider are uncounted, and each
  distinct index is taken as a distinct string.
- **Where fresh draws reach no more than the target, the column draws fresh**, with no index. A
  ratio of 1 is always that case. So is a source whose variety sits below the target, which caps
  the column's distinct count.
- **With `top_values` or `prefixes`, the index covers only the `other_fraction`.** Its target is the
  column's less the listed values' count, over the rows the share draws, for strings and numbers
  alike. A conditional table's share of values the column lists none of draws through the same
  index, so it holds the same values.
- **With `prefixes`, the distinct ratio is of prefixes**, so the `other_fraction`'s strings are
  counted by their first characters, as many as a prefix holds. A shape counts them per
  character; any other source counts its whole strings.
- **A check digit draws from the value's stream, after the value**, so whether a value's digit is
  wrong is a function of its index too.
- **A list element's rows are the elements its lists are expected to hold**: the list's rows,
  times the non-empty share, times the mean length.

On 1M rows of the bundled `orders` example, at seed 42 and stream version 2:

| Column | Source | Profiled ratio | Indexed, uncounted | Indexed, counted |
| --- | --- | --- | --- | --- |
| `ship_zip` | pattern `^\d{5}$`, 100,000 strings | 0.09 | 0.059167 | 0.089921 |
| `customer_email` | `email`, 4,275,954 strings | 0.195 | 0.190699 | 0.195046 |

The ratios are each column's distinct values, nulls left out, over all rows, read by Polars from
the file this writes:

```bash
cargo run --release -p doppeldata-core --example throughput -- orders --rows 1000000 --repeat 0 --ipc orders.arrow
```

## A numeric column's index picks a band of levels, and K counts where bands share a value

The levels split into K equal bands. The value at index `i` is drawn at a level inside band `i`,
offset by a draw from the index's own stream. A uniform index and a uniform offset make a uniform
level, so the values still follow the inverse CDF. And a few indices reach each piece of it in
proportion, where independent levels could leave a short piece with none.

A rounded value's levels form a cell, and neighboring bands give one value where an edge splits a
cell between them. The collision count the strings use assumes independent draws, so it overstates
how often bands collide. K is solved from each cell's chance of being reached instead, averaged over
where the cell starts inside a band.

- **A cell `w` bands wide, with `w` at most 1, is reached with chance `q·w − q²·w³ / 6`**, where
  `q` is the chance a band is drawn. It lies inside one band, or an edge splits it between two.
- **A wider cell misses only where every band it spans misses.** A band the cell holds a share `f`
  of misses it with chance `1 − q·f`, so a whole band misses with chance `1 − q`.
- **Each piece of the inverse CDF is one stratum of cells**, each flat run or end value one cell,
  and an unrounded piece cells without limit.

With each column's distinct values counted by Polars:

| Column | Target | Generated |
| --- | --- | --- |
| taxi `tolls_amount`, cents, 10M rows | 1,938 | 1,907 |
| `orders` `customer_id`, 100,000 rows | 20,000 | 19,997 |

`customer_id` is the bundled `orders` profile's, with its extremes set to 10,000 and 99,999 and its
ratio to 0.2, generated by `data generate --rows 100000`. *Rejected:* solving K as if each band
gave a value of its own. Neighboring bands share a value wherever an edge splits a cell, so that K
falls short of the target.

A column whose fresh draws hold fewer values than its target draws fresh. Its quantiles bound it,
at the rows or the lattice points each piece holds, whichever is fewer. The taxi trips'
`tip_amount` holds 5,180 of the 5,541 cents its target asks. Its profile states nothing above p999
but the fit, so the cents the source holds past p999 are drawn only as the fitted tail reaches
them.

## A temporal column is uniform over its range unless the profile weights it

With no `month_fractions`, `weekday_fractions` or `hour_fractions` and no `gap_fraction`, a value
is uniform over the extremes at the column's granularity, from one draw. Otherwise a day is drawn
with weight `month_fractions` times `weekday_fractions`, each spread over the days holding it, with
`gap_fraction`'s share of days left empty. Then an hour is drawn by `hour_fractions`, and an offset
within it.

The extremes are the observed minimum and maximum, so they rarely fall on midnight. The first and
last day hold only their granules inside the extremes, and count in a month's or weekday's share
by that fraction of the day. So a stored weekday share holds exactly, and no value is clamped onto
an extreme.

## A missing-value token is a separate cell, and null in Arrow

A `missing_token_fractions` token such as `N/A` in an integer column cannot be an Arrow value. The batch
holds a null there, and `Plan::text_cells` returns the token a CSV or JSON writer prints in its
place.

- **Each token's fraction in `missing_token_fractions` is a share of all rows.** A null row prints a
  token by those fractions, and prints nothing in the share of rows the tokens leave uncovered.
  Tokens whose fractions sum above the `null_fraction` are refused.
- **A nested field's `missing_token_fractions` are refused.** A text cell names a row of a
  top-level column, so it has nowhere to put a value inside a list or struct.

## A text source's column holding values of other types generates as text

A column generates as a string column where its `physical_type` is an integer, float, decimal,
boolean, date or timestamp, and its `type_mix_fractions` share of that type is under 1. Only a source read as
text records such a share. A reader of the profile's type turns the other values into nulls, so the
generated column holds text, as its source did, and the exports type it as a string. The rule reads
the column alone, so a scenario changing `source.format` changes no column's type. Every other
column keeps its type, and a nested field follows the same rule.

- **Each row draws one `type_mix_fractions` share.** The draw is one word a row from the column's
  `type_mix` stream, taken on null rows too, so the shares never move with the null mask.
- **The column's own share is its sampler's draw, written in the source's layout.** A date or
  timestamp takes its `temporal.format`. A whole float keeps its `.0`, so a reader counts it as the
  column's type rather than as an integer.
- **Every other share is text parsing as its type, and as nothing else.** The profile states
  nothing of these values, so they are drawn plainly. A string share holds no vowel, so it never
  spells `null`, `true` or `false`, which a JSON or CSV reader takes for another type.
- **Each share profiles back as itself.** `cli/tests/test_type_mix_round_trip.py` writes a
  column of each own type as CSV and NDJSON, and profiles the files back to their shares.
- **The own draw holds the column's distinct values in its own rows.** A text source's distinct
  count is taken over the values parsing as the column's type, so the other shares add none to it.
- **A stated key repeats no value.** Where `dataset.keys` names the column, each value of another
  type is derived from its row rather than drawn.
- **A dependence reads the own draw typed.** A copula, conditional table, lookup or expression reads
  its parent's own values. The text is written after every dependence has drawn.
- **A `cdc` update recomputes a derived column from the text, read back as its type.** A row holding
  another type reads as null there, as it does to any reader of the type.
- **A JSON writer prints each value as the JSON its text parses as.** The field carries
  `doppeldata.type_mix` metadata naming the own type. So an NDJSON file holds `48213` as a number
  and `abc` as a string, which the profiler counts as its source's shares. A CSV writer quotes by
  bytes, so it needs no such field.
- **A relationship's column takes its parent's form.**
  [generating-related-tables.md](generating-related-tables.md) says which.
- **`malformed_rows` adds no value of another type to a text column.**
  [scenario-overlays.md](scenario-overlays.md) says what it reproduces instead.
