//! A key stated unique, drawn from its pattern or shapes with no string twice: rank, encipher, unrank, as
//! format-preserving encryption builds a cipher over a regular language. A keyed permutation takes
//! each row to a rank among the pattern's strings. The rank is read off a DFA of the pattern, which
//! holds one path per string, by counting the strings past each transition. So two rows never hold
//! one string, however ambiguous the pattern is.

use std::collections::HashMap;
use std::collections::VecDeque;

use regex_automata::Anchored;
use regex_automata::MatchKind;
use regex_automata::dfa::Automaton;
use regex_automata::dfa::StartKind;
use regex_automata::dfa::dense;
use regex_automata::nfa::thompson;
use regex_automata::util::start;
use regex_syntax::hir::Hir;
use regex_syntax::hir::Look;

use super::text::holds_check_digit;
use crate::keyed_permutation::KeyedPermutation;
use crate::profile::CheckDigit;
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
    /// Each string counted by what precedes its check digit's site, the last byte the kind can be
    /// written over, because the digit is written there. A string closes on the site and a tail
    /// holding no other site, so two rows differ before their check digit.
    BeforeCheckDigit(CheckDigit),
}

/// A run of consecutive bytes a state reads into one live state.
#[derive(Debug, Clone, Copy)]
struct ByteRun {
    first: u8,
    bytes: u16,
    next: u32,
}

impl ByteRun {
    fn each(&self) -> impl Iterator<Item = u8> + use<> {
        let first = self.first;
        (0..self.bytes).map(move |at| first + at as u8)
    }
}

#[derive(Debug)]
pub(crate) struct UniquePatternKey {
    end: KeyEnd,
    /// Each state's runs, state 0 the start.
    runs: Vec<Vec<ByteRun>>,
    /// Whether a string the key holds ends at each state.
    ends: Vec<bool>,
    /// Where the key ends before a check digit, the site byte closing a string at each state that
    /// ends one, and the state it reads into.
    closing: Vec<Option<(u8, u32)>>,
    /// The next step of each state's shortest tail holding no site to the end of a string.
    tail_steps: Vec<Option<(u8, u32)>>,
    /// The bytes the longest string takes.
    budget: usize,
    /// `within[left · states + state]`: the strings ending within `left` bytes of `state`.
    within: Vec<u128>,
    strings: u128,
}

impl UniquePatternKey {
    /// The key drawing `hir`'s strings. `hir` bounds every repetition, as a compiled pattern's
    /// does. An error where the automaton passes [`AUTOMATON_BYTES`].
    pub(crate) fn new(hir: &Hir, end: KeyEnd) -> Result<Self, String> {
        let too_large = |error: String| format!("its automaton cannot be built within {AUTOMATON_BYTES} bytes: {error}");
        let budget = hir
            .properties()
            .maximum_len()
            .ok_or_else(|| too_large("its longest string is longer than a usize counts".to_string()))?;
        let whole = Hir::concat(vec![hir.clone(), Hir::look(Look::End)]);
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
        let (ends, closing, tail_steps) = match end {
            KeyEnd::Whole => (accepts, Vec::new(), Vec::new()),
            KeyEnd::BeforeCheckDigit(kind) => {
                let tail_steps = tails_holding_no_site(&runs, &accepts, kind);
                let closes = |state: usize| accepts[state] || tail_steps[state].is_some();
                let closing: Vec<Option<(u8, u32)>> = runs
                    .iter()
                    .map(|state_runs| {
                        state_runs.iter().filter(|run| closes(run.next as usize)).find_map(|run| {
                            let site = run.each().find(|&byte| holds_check_digit(kind, byte))?;
                            Some((site, run.next))
                        })
                    })
                    .collect();
                (closing.iter().map(Option::is_some).collect(), closing, tail_steps)
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
            closing,
            tail_steps,
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
            KeyEnd::BeforeCheckDigit(_) => format!("{count} strings before their check digit"),
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
    /// what follows. Whether that happens depends only on the state and the bytes left, so two ranks
    /// reaching one string walked one path, lost the same counts, and are one rank.
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
        if let Some(&Some((site, mut at))) = self.closing.get(state) {
            out.push(site);
            while let Some((byte, next)) = self.tail_steps[at as usize] {
                out.push(byte);
                at = next;
            }
        }
    }
}

/// Each state's first step on its shortest tail of bytes `kind` is never written over that ends a
/// string. None where the state ends one, or no such tail does. Found breadth first, back from the
/// states a string ends at.
fn tails_holding_no_site(runs: &[Vec<ByteRun>], accepts: &[bool], kind: CheckDigit) -> Vec<Option<(u8, u32)>> {
    let mut into: Vec<Vec<(u32, u8)>> = vec![Vec::new(); runs.len()];
    for (from, state_runs) in runs.iter().enumerate() {
        for run in state_runs {
            if let Some(byte) = run.each().find(|&byte| !holds_check_digit(kind, byte)) {
                into[run.next as usize].push((from as u32, byte));
            }
        }
    }
    let mut found = accepts.to_vec();
    let mut steps = vec![None; runs.len()];
    let mut queue: VecDeque<u32> = (0..runs.len() as u32).filter(|&state| accepts[state as usize]).collect();
    while let Some(state) = queue.pop_front() {
        for &(from, byte) in &into[state as usize] {
            if !found[from as usize] {
                found[from as usize] = true;
                steps[from as usize] = Some((byte, state));
                queue.push_back(from);
            }
        }
    }
    steps
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use regex_automata::dfa::regex::Regex;

    use super::*;

    #[test]
    fn every_row_holds_a_string_of_the_pattern_no_other_row_holds() {
        // `\d{1,2}\d{1,2}` writes "123" two ways, so only a count of distinct strings holds it once.
        // `[AB]\d{1,2}\d[XY]` takes its check digit before a letter, so each key is what surrounds it.
        let cases = [
            (r"\d{1,2}\d{1,2}", KeyEnd::Whole, 11_100),
            (r"[AB]\d{1,2}\d[XY]", KeyEnd::BeforeCheckDigit(CheckDigit::Luhn), 220),
        ];
        for (pattern, end, strings) in cases {
            let hir = regex_syntax::ParserBuilder::new().unicode(false).build().parse(pattern).unwrap();
            let key = UniquePatternKey::new(&hir, end).unwrap();
            assert_eq!(key.strings(), strings, "{pattern}");
            let matches = Regex::new(&format!("^(?:{pattern})$")).unwrap();
            let seeded = key.seeded(7, 11);
            let mut held = HashSet::new();
            for row in 0..strings as u64 {
                let mut value = String::new();
                seeded.push(row, &mut value);
                assert!(matches.is_match(&value), "{pattern} drew {value:?}");
                let around_check_digit = match end {
                    KeyEnd::Whole => value.clone(),
                    KeyEnd::BeforeCheckDigit(_) => {
                        let site = value.bytes().rposition(|byte| byte.is_ascii_digit()).unwrap();
                        format!("{}_{}", &value[..site], &value[site + 1..])
                    }
                };
                assert!(held.insert(around_check_digit), "{pattern} drew {value:?} twice");
            }
        }
    }
}
