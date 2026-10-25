# How each column kind is drawn from its profile

## A numeric column draws a uniform level through a piecewise-linear inverse CDF

The inverse CDF is a list of (level, value) knots. The stored quantiles are the frame, with the
extremes at levels 0 and 1. A histogram's edges, at their cumulative shares, fill the body between
the outermost quantiles. A knot that would put a value out of order is dropped, so the quantiles
win where sources disagree.

`zero_rate` adds a flat run at zero, from `negative_rate` to `negative_rate + zero_rate`. The run
is clamped between the last knot below zero and the first above it, so it stays in order and is
always kept. The rates are stored rounded, so their sum can land just past the share at the next
edge. Clamping there keeps the zeros a column holds, short of the rounding.

An edge's share counts every row, the histogram's `overflow` included. Edge `i` sits at level
`(below + counts[0] + … + counts[i - 1]) / (below + sum(counts) + above)`, so edge 0 sits at
`below`'s share. A profile without `overflow` has `below` and `above` at 0, and its edges run from
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
  with `loc` −0.22 draws no value below −0.22, so toward a trimmed low of −62 its tail would put
  every value between them on one segment. The line from the quantile to the extreme draws there.
- **Where the body starts at or above zero and the low extreme sits below it, `negative_rate` is
  the lower tail's share below zero.** The levels from it to the body's first knot draw between
  zero and that knot. Spread over p001's whole share, the fitted tail of the NYC yellow taxi
  trips' `tip_amount` put 8,531 of 10M generated values below zero where 381 source values sat.
- **The remaining lift is the body.** Below p99, linear interpolation across the stored quantiles gives 33.31
  against 30.06 for the true lognormal. A histogram's many knots shrink it.

## A numeric column with few values draws each listed value by its rate

The profiler lists a numeric column's values as `numeric.top_values`, beside an `other_rate`, where
it holds at most `NUMERIC_TOP_VALUES_THRESHOLD` distinct values. An alias table draws a listed value
by its rate, or the `other` share. A value drawn from the `other` share comes from the column's
inverse CDF with every level rounding to a listed value cut out.

- **The levels left are scaled by what each histogram bin holds outside the list.** A bin's weight
  is its count less its listed values' rows. So a ramp the quantiles draw between two listed values
  keeps no levels where the counts give it no rows. Where no bin holds anything outside the list,
  the levels keep their own widths.
- **A lattice column's distinct count can show values beyond its trimmed extremes.** On an integer
  column, a decimal, or a float with a `scale`, the values the extremes leave room for are the points
  between them that no listed value takes. Where `distinct.approx` holds more than the listed
  values and that room, the rest lie beyond the extremes. At most `2 · (k_min − 1)` hide there, and
  each side's share `(k_min − 1) / 2` source rows.
- **They are drawn on the side the histogram's overflow names**: above where only `above` counts
  rows, below where only `below` does, and otherwise above and below in turn. Their rows come out of
  the `other` share, never on top of it.
- **A continuous column never draws past its extremes.** Its room between them is unbounded.

On the NYC yellow taxi trips for February to April 2024, `payment_type` holds six values. Value 5
sits on 2 rows, and value 3's 64,979 rows fold into `other` so it holds `k_min` rows. The extremes
are 0 and 4, so the sixth value is drawn as 5, on 2 of 10M generated rows. `boundary_adherence`
stays at 1.0.

## A string column takes the most specific source its profile holds

1. **`top_values`**, or else **`prefixes`**: an alias table over the rates. A draw of `other_rate`
   generates a value from the next source down. A drawn prefix is followed by a value of the
   `suffix` pattern. A ZIP column under `healthcare` keeps three-digit prefixes, so `\d{2}` makes
   each a ZIP code again.
2. **`semantic.generator_hint.provider`**: a named generator, such as `email` or `city`. The names
   are doppeldata's own, an enum in the profile model, so the published schema lists them and
   replacing the faker library breaks no profile. A profile naming another fails validation.
   `icd10cm` and `hcpcs_level2` draw a code uniformly from the list the engine ships, which the
   healthcare pack's validator types the column by.
3. **A pattern**, from `generator_hint.pattern` or `string.pattern`. A line anchor is dropped,
   because a value is the whole line, and `\d` and the other classes are ASCII.
4. **`shapes`**: a shape is drawn by rate, then each `A` becomes an upper-case letter, each `a` a
   lower-case one, and each `9` a digit.
5. **Characters** of `charset.classes`, at a length drawn from `length`.

`generator_hint.check_digit: luhn` writes the Luhn digit over a value's last digit, wrong on a
`quality.invalid_rate` share of values. `npi` is the Luhn digit over `80840` and the first nine
digits, as the NPI standard prefixes them. `dea` is the last digit of the sum of the first, third
and fifth digits plus twice the sum of the second, fourth and sixth. *Rejected:* the `luhn` crate. Its `checksum` allocates on
every call, and it panics on lower-case input and on a `u8` overflow past about 28 digits.

`email` is `first.last` at `example.com`, `example.net` or `example.org`, domains reserved so no
generated address reaches anyone. That holds 3,007 × 474 × 3 = 4,275,954 addresses. *Rejected:*
fake's `SafeEmail`. It is a first name at one of the three domains, so it holds 9,021 addresses.

## A column below a distinct ratio of 1 draws each value at an index

A generated string or number is drawn fresh by default, so its distinct count is whatever the
source yields. With `distinct.ratio`, a row instead draws an index from `0..K` on a stream of its
own. The value at an index comes from a stream positioned at that index. So a value is a pure
function of its index, no pool of values is held, and any block still generates alone.

- **The target is `distinct.ratio` times the generated rows**, never `distinct.approx`. So a
  10M-row run of a 100K-row profile holds 100 times the profiled distinct count, and its ratio
  matches the profile's. A coded column such as a ZIP code might saturate at its profiled count
  instead. One profile cannot show which, so the ratio holds.
- **K is chosen so the expected count of distinct indices meets the target.** `n` uniform draws
  from `0..K` hold `K · (1 − e^(−n/K))` distinct indices. `n` counts the rows not null.
- **A source whose strings can be counted has its collisions counted too.** `m` distinct indices
  drawn into `S` equally likely strings give `S · (1 − e^(−m/S))` distinct strings, so the index
  target is raised to meet the value target. A pattern is counted from its syntax, as if every
  string were equally likely. A shape is counted per character at its rate. `email`, `city`, and
  the providers drawing one entry of one list are counted from fake's lists, and the code-list
  providers from their lists. Characters and every other provider are uncounted, and each
  distinct index is taken as a distinct string.
- **Where fresh draws reach no more than the target, the column draws fresh**, with no index. A
  ratio of 1 is always that case. So is a source whose variety sits below the target, which caps
  the column's distinct count.
- **With `top_values`, the index covers only the `other` share**, and the target subtracts the
  top values' count.
- **With `prefixes`, the distinct ratio is of prefixes**, so the `other` share's strings are
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

## A numeric column's index picks a band of levels, and K counts where bands share a value

The levels split into K equal bands. The value at index `i` is drawn at a level inside band `i`,
offset by a draw from the index's own stream. A uniform index and a uniform offset make a uniform
level, so the values still follow the inverse CDF. And a few indices reach each piece of it in
proportion. Independent levels could leave a short piece with none: the taxi trips'
`congestion_surcharge` held 3 to 9 distinct values across eight seeds that way, and holds 7, the
source's count, at each of them in bands.

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

At stream version 5, with each column's distinct values counted by Polars:

| Column | Target | One value per band | Averaged over cell starts |
| --- | --- | --- | --- |
| taxi `tolls_amount`, cents, 10M rows | 1,918 | 1,669 | 1,912 |
| `orders` `customer_id` bounded at 10,000 and 99,999, 100,000 rows | 20,000 | 19,834 | 19,997 |

The strings' collision count gives `customer_id` 22,353. A column whose fresh draws hold fewer
values than its target draws fresh. Its quantiles bound it, at the rows or the lattice points each
piece holds, whichever is fewer. The taxi trips' `tip_amount` holds 5,180 of the 5,484 cents its
ratio asks. Its profile states nothing above p999 but the fit, and the fitted upper tail draws a
third of the source's rows above 40.

The ratios are each column's distinct values, nulls left out, over all rows, read by Polars from
the file this writes:

```bash
cargo run --release -p doppeldata-core --example throughput -- orders --rows 1000000 --repeat 0 --ipc orders.arrow
```

## A temporal column is uniform over its range unless the profile weights it

With no `by_month`, `by_dow` or `by_hour` and no `gap_rate`, a value is uniform over the extremes at
the column's granularity, from one draw. Otherwise a day is drawn with weight `by_month` times
`by_dow`, each spread over the days holding it, with `gap_rate`'s share of days left empty. Then an
hour is drawn by `by_hour`, and an offset within it.

The extremes are the observed minimum and maximum, so they rarely fall on midnight. The first and
last day hold only their granules inside the extremes, and count in a month's or weekday's share
by that fraction of the day. So a stored weekday share holds exactly, and no value is clamped onto
an extreme.

## A value only text can carry is a separate cell, and null in Arrow

A `type_mix` share such as 0.1% `string` in an integer column cannot be an Arrow value. The batch
holds a null there, and `Plan::text_cells` returns the text a CSV or JSON writer prints in its place.

- **A string column's `type_mix` adds nothing.** A string holds any raw value as itself, and the
  column's string statistics were taken over all of them, the ones parsing as numbers included.
- **Each `missing_tokens` rate is a share of all rows.** A null row prints a token by those rates,
  and prints nothing in the share of rows the tokens leave uncovered. Tokens whose rates sum above
  the null rate are refused.
- **A nested field's `type_mix` and `missing_tokens` are refused.** A text cell names a row of a
  top-level column, so it has nowhere to put a value inside a list or struct.
