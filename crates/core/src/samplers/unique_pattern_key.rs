//! A key stated unique, drawn from its pattern with no string twice: rank, encipher, unrank, as
//! format-preserving encryption builds a cipher over a regular language. A keyed permutation takes
//! each row to a rank among the pattern's strings. The rank is read off a DFA of the pattern, which
//! holds one path per string, by counting the strings past each transition. So two rows never hold
//! one string, however ambiguous the pattern is.

use std::collections::HashMap;

use regex_automata::Anchored;
use regex_automata::MatchKind;
use regex_automata::dfa::Automaton;
use regex_automata::dfa::StartKind;
use regex_automata::dfa::dense;
use regex_automata::nfa::thompson;
use regex_automata::util::start;
use regex_syntax::hir::Hir;
use regex_syntax::hir::HirKind;
use regex_syntax::hir::Look;
use regex_syntax::hir::Repetition;

use crate::keyed_permutation::KeyedPermutation;
use crate::rng::splitmix;

/// The bytes the automaton may take, and the bytes the table counting its strings may take.
pub(crate) const AUTOMATON_BYTES: usize = 8 << 20;

/// A count at or past this is held as this, so a sum or product of counts never wraps.
const SATURATED: u128 = u128::MAX;

/// Which strings of the pattern a key holds one of on each row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeyEnd {
    /// Each whole string.
    Whole,
    /// Each string ending in an ASCII digit, counted by what precedes the digit, because a check
    /// digit is written over it. So two rows differ before their check digit.
    BeforeCheckDigit,
}

/// A run of consecutive bytes a state reads into one live state.
#[derive(Debug, Clone, Copy)]
struct ByteRun {
    first: u8,
    bytes: u16,
    next: u32,
}

#[derive(Debug)]
pub(crate) struct UniquePatternKey {
    end: KeyEnd,
    /// Each state's runs, state 0 the start.
    runs: Vec<Vec<ByteRun>>,
    /// Whether a string the key holds ends at each state.
    ends: Vec<bool>,
    /// The digit closing a string at each state that ends one, where the key ends before a check digit.
    closing_digit: Vec<u8>,
    /// The bytes the longest string takes.
    budget: usize,
    /// `within[left · states + state]`: the strings ending within `left` bytes of `state`.
    within: Vec<u128>,
    strings: u128,
}

impl UniquePatternKey {
    /// The key drawing `hir`'s strings, each unbounded repetition run to `max_repeat` extra repeats,
    /// as rand_regex runs it. An error where the automaton passes [`AUTOMATON_BYTES`].
    pub(crate) fn new(hir: &Hir, max_repeat: u32, end: KeyEnd) -> Result<Self, String> {
        let too_large = |error: String| format!("its automaton cannot be built within {AUTOMATON_BYTES} bytes: {error}");
        let bounded = bounded(hir, max_repeat);
        let budget = bounded
            .properties()
            .maximum_len()
            .ok_or_else(|| too_large("its longest string is longer than a usize counts".to_string()))?;
        let whole = Hir::concat(vec![bounded, Hir::look(Look::End)]);
        let nfa = thompson::Compiler::new()
            .configure(
                thompson::Config::new()
                    .which_captures(thompson::WhichCaptures::None)
                    .nfa_size_limit(Some(AUTOMATON_BYTES)),
            )
            .build_from_hir(&whole)
            .map_err(|error| too_large(error.to_string()))?;
        let dfa = dense::Builder::new()
            .configure(
                dense::Config::new()
                    .match_kind(MatchKind::All)
                    .start_kind(StartKind::Anchored)
                    .accelerate(false)
                    .determinize_size_limit(Some(AUTOMATON_BYTES))
                    .dfa_size_limit(Some(AUTOMATON_BYTES)),
            )
            .build_from_nfa(&nfa)
            .map_err(|error| too_large(error.to_string()))?;
        let start = dfa
            .start_state(&start::Config::new().anchored(Anchored::Yes))
            .map_err(|error| too_large(error.to_string()))?;
        let mut numbered = HashMap::from([(start, 0usize)]);
        let mut order = vec![start];
        let mut runs: Vec<Vec<ByteRun>> = Vec::new();
        while runs.len() < order.len() {
            let id = order[runs.len()];
            let mut state_runs: Vec<ByteRun> = Vec::new();
            for byte in 0..=u8::MAX {
                let next = dfa.next_state(id, byte);
                if dfa.is_dead_state(next) {
                    continue;
                }
                if dfa.is_quit_state(next) {
                    return Err(too_large(format!("byte {byte} quits it")));
                }
                let next = *numbered.entry(next).or_insert_with(|| {
                    order.push(next);
                    order.len() - 1
                }) as u32;
                match state_runs.last_mut() {
                    Some(run) if run.next == next && u16::from(run.first) + run.bytes == u16::from(byte) => run.bytes += 1,
                    _ => state_runs.push(ByteRun {
                        first: byte,
                        bytes: 1,
                        next,
                    }),
                }
            }
            runs.push(state_runs);
            let table_bytes = order
                .len()
                .saturating_mul(budget.saturating_add(1))
                .saturating_mul(size_of::<u128>());
            if table_bytes > AUTOMATON_BYTES {
                return Err(too_large(format!("counting {} states over {budget} bytes", order.len())));
            }
        }
        let accepts: Vec<bool> = order.iter().map(|&id| dfa.is_match_state(dfa.next_eoi_state(id))).collect();
        let states = order.len();
        let (ends, closing_digit) = match end {
            KeyEnd::Whole => (accepts, vec![0; states]),
            KeyEnd::BeforeCheckDigit => {
                let closing: Vec<Option<u8>> = runs
                    .iter()
                    .map(|state_runs| {
                        state_runs.iter().find_map(|run| {
                            (run.first..=run.first.saturating_add((run.bytes - 1) as u8))
                                .find(|byte| byte.is_ascii_digit())
                                .filter(|_| accepts[run.next as usize])
                        })
                    })
                    .collect();
                (
                    closing.iter().map(Option::is_some).collect(),
                    closing.iter().map(|digit| digit.unwrap_or(0)).collect(),
                )
            }
        };
        let mut within = vec![0u128; (budget + 1) * states];
        for (state, &ends_here) in ends.iter().enumerate() {
            within[state] = u128::from(ends_here);
        }
        for left in 1..=budget {
            let (counted, rest) = within.split_at_mut(left * states);
            let below = &counted[(left - 1) * states..];
            for state in 0..states {
                rest[state] = runs[state].iter().fold(u128::from(ends[state]), |total, run| {
                    total.saturating_add(below[run.next as usize].saturating_mul(u128::from(run.bytes)))
                });
            }
        }
        let strings = within[budget * states];
        Ok(Self {
            end,
            runs,
            ends,
            closing_digit,
            budget,
            within,
            strings,
        })
    }

    /// How many strings the key holds, [`SATURATED`] where 2^128 − 1 or more.
    pub(crate) fn strings(&self) -> u128 {
        self.strings
    }

    /// What the key counts, as `data plan` names it.
    pub(crate) fn counted(&self) -> String {
        let count = match self.strings {
            SATURATED => "2^128 or more".to_string(),
            strings => strings.to_string(),
        };
        match self.end {
            KeyEnd::Whole => format!("{count} strings"),
            KeyEnd::BeforeCheckDigit => format!("{count} strings before their check digit"),
        }
    }

    /// The places the row permutation runs over: one per string, up to 2^64 − 1.
    fn domain(&self) -> u64 {
        self.strings.min(u128::from(u64::MAX)) as u64
    }

    /// The key's draw under `seed`, from the stream `key`.
    pub(crate) fn seeded(&self, seed: u64, key: u64) -> SeededPatternKey<'_> {
        SeededPatternKey {
            pattern: self,
            rows: KeyedPermutation::new(self.domain(), seed, key),
            mix: splitmix(seed ^ splitmix(key ^ 0x6B65_795F_7261_6E6B)),
        }
    }

    /// The string `rank` names, written to `out`. Where more strings lie past some transition than
    /// any rank reaches, the transition is picked by a hash of the rank, and the rank stays whole for
    /// what follows. Whether that happens depends on the state alone, so two ranks reaching one
    /// string would have walked one path, losing the same counts, and so are one rank.
    fn push_rank(&self, mut rank: u128, mix: u64, out: &mut Vec<u8>) {
        let states = self.runs.len();
        let (mut state, mut left) = (0usize, self.budget);
        while left > 0 {
            let below = &self.within[(left - 1) * states..left * states];
            let runs = &self.runs[state];
            let past_every_rank = |run: &&ByteRun| below[run.next as usize] == SATURATED;
            let saturated: u64 = runs.iter().filter(past_every_rank).map(|run| u64::from(run.bytes)).sum();
            let taken = if saturated > 0 {
                let hash = splitmix(mix ^ splitmix(rank as u64 ^ splitmix((rank >> 64) as u64 ^ out.len() as u64)));
                let mut pick = hash % saturated;
                runs.iter()
                    .filter(past_every_rank)
                    .find_map(|run| match pick < u64::from(run.bytes) {
                        true => Some((run.first + pick as u8, run.next)),
                        false => {
                            pick -= u64::from(run.bytes);
                            None
                        }
                    })
            } else {
                if self.ends[state] {
                    if rank == 0 {
                        break;
                    }
                    rank -= 1;
                }
                runs.iter().filter(|run| below[run.next as usize] > 0).find_map(|run| {
                    let each = below[run.next as usize];
                    match each.checked_mul(u128::from(run.bytes)) {
                        Some(span) if rank >= span => {
                            rank -= span;
                            None
                        }
                        _ => {
                            let byte = run.first + (rank / each) as u8;
                            rank %= each;
                            Some((byte, run.next))
                        }
                    }
                })
            };
            let Some((byte, next)) = taken else { break };
            out.push(byte);
            state = next as usize;
            left -= 1;
        }
        if self.end == KeyEnd::BeforeCheckDigit {
            out.push(self.closing_digit[state]);
        }
    }
}

/// A key's draw under one seed: the permutation of its rows and the hash spreading them.
pub(crate) struct SeededPatternKey<'a> {
    pattern: &'a UniquePatternKey,
    rows: KeyedPermutation,
    mix: u64,
}

impl SeededPatternKey<'_> {
    /// The string row `row` holds, which no other row holds. Past 2^64 strings, the row's place among
    /// 2^64 is spread across them: each place takes its own band of ranks, and a hash its rank in it.
    pub(crate) fn push(&self, row: u64, out: &mut String) {
        let place = u128::from(self.rows.forward(row));
        let rank = match self.pattern.strings / u128::from(u64::MAX) {
            0 | 1 => place,
            band => {
                let low = place as u64;
                let hash = (u128::from(splitmix(low ^ self.mix)) << 64) | u128::from(splitmix(low ^ splitmix(self.mix)));
                place * band + hash % band
            }
        };
        let mut bytes = Vec::with_capacity(self.pattern.budget.min(64) + 1);
        self.pattern.push_rank(rank, self.mix, &mut bytes);
        out.push_str(&String::from_utf8_lossy(&bytes));
    }
}

/// `hir` with each unbounded repetition bounded at `max_repeat` repeats past its least, and its
/// capture groups dropped, so its strings are finitely many.
fn bounded(hir: &Hir, max_repeat: u32) -> Hir {
    match hir.kind() {
        HirKind::Repetition(repetition) => Hir::repetition(Repetition {
            min: repetition.min,
            max: Some(repetition.max.unwrap_or(repetition.min.saturating_add(max_repeat))),
            greedy: repetition.greedy,
            sub: Box::new(bounded(&repetition.sub, max_repeat)),
        }),
        HirKind::Capture(capture) => bounded(&capture.sub, max_repeat),
        HirKind::Concat(parts) => Hir::concat(parts.iter().map(|part| bounded(part, max_repeat)).collect()),
        HirKind::Alternation(branches) => Hir::alternation(branches.iter().map(|branch| bounded(branch, max_repeat)).collect()),
        _ => hir.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use regex_automata::dfa::regex::Regex;

    use super::*;

    #[test]
    fn every_row_holds_a_string_of_the_pattern_no_other_row_holds() {
        // `\d{1,2}\d{1,2}` writes "123" two ways, so only a count of distinct strings holds it once.
        let cases = [
            (r"\d{1,2}\d{1,2}", KeyEnd::Whole, 11_100),
            (r"[AB]\d{1,2}\d", KeyEnd::BeforeCheckDigit, 220),
        ];
        for (pattern, end, strings) in cases {
            let hir = regex_syntax::ParserBuilder::new().unicode(false).build().parse(pattern).unwrap();
            let key = UniquePatternKey::new(&hir, 16, end).unwrap();
            assert_eq!(key.strings(), strings, "{pattern}");
            let matches = Regex::new(&format!("^(?:{pattern})$")).unwrap();
            let seeded = key.seeded(7, 11);
            let mut held = HashSet::new();
            for row in 0..strings as u64 {
                let mut value = String::new();
                seeded.push(row, &mut value);
                assert!(matches.is_match(&value), "{pattern} drew {value:?}");
                let before_check_digit = match end {
                    KeyEnd::Whole => value.as_str(),
                    KeyEnd::BeforeCheckDigit => &value[..value.len() - 1],
                };
                assert!(held.insert(before_check_digit.to_string()), "{pattern} drew {value:?} twice");
            }
        }
    }
}
