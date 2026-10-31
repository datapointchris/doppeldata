//! How many characters a text source's values hold: each provider's fewest and most, read from the
//! lists and templates it draws from, and a pattern whose repetitions are bounded so every value
//! drawn from it holds between two lengths.

use std::collections::BTreeMap;

use fake::locales::Data;
use fake::locales::EN;
use regex_syntax::ast::Ast;
use regex_syntax::ast::RepetitionKind;
use regex_syntax::ast::RepetitionRange;
use regex_syntax::ast::parse::Parser;

use super::PATTERN_MAX_REPEAT;
use super::text::EMAIL_DOMAINS;
use super::text::HCPCS_LEVEL2_DIGITS;
use super::text::ICD10CM_CATEGORY_DIGITS;
use super::text::ICD10CM_EXTENSION_PER_MILLE;
use super::text::ZIP_CODE_DIGITS;
use crate::profile::Provider;

/// The fewest and most characters a value holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Lengths {
    shortest: usize,
    longest: usize,
}

impl Lengths {
    const fn exactly(characters: usize) -> Self {
        Self::between(characters, characters)
    }

    const fn between(shortest: usize, longest: usize) -> Self {
        Self { shortest, longest }
    }

    fn of(lengths: impl IntoIterator<Item = usize>) -> Self {
        lengths.into_iter().map(Self::exactly).reduce(Self::or).unwrap_or(Self::exactly(0))
    }

    fn of_each(lengths: impl IntoIterator<Item = Self>) -> Self {
        lengths.into_iter().reduce(Self::or).unwrap_or(Self::exactly(0))
    }

    fn listed(values: &[&str]) -> Self {
        Self::of(values.iter().map(|value| value.chars().count()))
    }

    /// A value of `self` followed by one of `next`.
    fn then(self, next: Self) -> Self {
        Self::between(self.shortest + next.shortest, self.longest.saturating_add(next.longest))
    }

    /// A value of either.
    fn or(self, other: Self) -> Self {
        Self::between(self.shortest.min(other.shortest), self.longest.max(other.longest))
    }

    fn times(self, count: usize) -> Self {
        Self::between(self.shortest * count, self.longest.saturating_mul(count))
    }
}

/// A fake template's value, each placeholder replaced by a value of its lengths.
fn template(template: &str, placeholders: &[(&str, Lengths)]) -> Lengths {
    let mut literal = template.chars().count();
    let mut filled = Lengths::exactly(0);
    for &(placeholder, lengths) in placeholders {
        let times = template.matches(placeholder).count();
        literal -= times * placeholder.chars().count();
        filled = filled.then(lengths.times(times));
    }
    filled.then(Lengths::exactly(literal))
}

/// fake 5.1 draws an American Express number of 15 digits, a Visa of 13 or 16, and a Mastercard of
/// 16. The table is private to fake.
const CREDIT_CARD_DIGITS: Lengths = Lengths::between(13, 16);

fn provider_value_lengths(provider: Provider) -> Lengths {
    let one = Lengths::exactly(1);
    let first = Lengths::listed(EN::NAME_FIRST_NAME);
    let last = Lengths::listed(EN::NAME_LAST_NAME);
    let lowercase_first = Lengths::of(EN::NAME_FIRST_NAME.iter().map(|name| name.to_lowercase().chars().count()));
    let letters_of = |names: &[&str]| Lengths::of(names.iter().map(|name| name.chars().filter(char::is_ascii_alphabetic).count()));
    let word = Lengths::listed(EN::LOREM_WORD);
    let name = template(EN::NAME_TPL, &[("{FirstName}", first), ("{LastName}", last)]);
    let street_name = template(
        EN::ADDRESS_STREET_TPL,
        &[
            ("{StreetName}", first.or(last)),
            ("{StreetSuffix}", Lengths::listed(EN::ADDRESS_STREET_SUFFIX)),
        ],
    );
    // fake's username is a first name alone, a word and a first name, a first name and a number to
    // 255, or a first name and a word.
    let username = lowercase_first
        .or(word.then(one).then(lowercase_first))
        .or(lowercase_first.then(Lengths::between(1, 3)))
        .or(lowercase_first.then(one).then(word));
    match provider {
        Provider::Email => letters_of(EN::NAME_FIRST_NAME)
            .then(one)
            .then(letters_of(EN::NAME_LAST_NAME))
            .then(one)
            .then(Lengths::listed(&EMAIL_DOMAINS)),
        Provider::FreeEmail => username.then(one).then(Lengths::listed(EN::INTERNET_FREE_EMAIL_PROVIDER)),
        Provider::Username => username,
        Provider::Ipv4 => Lengths::between(1, 3).times(4).then(Lengths::exactly(3)),
        Provider::Ipv6 => Lengths::between(1, 4).times(8).then(Lengths::exactly(7)),
        Provider::MacAddress => Lengths::exactly(2).times(6).then(Lengths::exactly(5)),
        Provider::UserAgent => Lengths::listed(EN::INTERNET_USER_AGENT),
        Provider::Name => name,
        Provider::FirstName => first,
        Provider::LastName => last,
        Provider::PhoneNumber => Lengths::listed(EN::PHONE_NUMBER_FORMATS),
        Provider::CellNumber => Lengths::listed(EN::PHONE_CELL_NUMBER_FORMATS),
        Provider::Company => {
            let suffix = Lengths::listed(EN::COMPANY_SUFFIX);
            Lengths::of_each(
                EN::COMPANY_NAME_TPLS
                    .iter()
                    .map(|tpl| template(tpl, &[("{Name_1}", last), ("{Name_2}", last), ("{Suffix}", suffix)])),
            )
        }
        Provider::JobTitle => template(
            EN::JOB_TITLE_TPL,
            &[
                ("{Seniority}", Lengths::listed(EN::JOB_SENIORITY)),
                ("{Field}", Lengths::listed(EN::JOB_FIELD)),
                ("{Position}", Lengths::listed(EN::JOB_POSITION)),
            ],
        ),
        Provider::StreetAddress => Lengths::listed(EN::ADDRESS_BUILDING_NUMBER_FORMATS).then(one).then(street_name),
        Provider::StreetName => street_name,
        Provider::BuildingNumber => Lengths::listed(EN::ADDRESS_BUILDING_NUMBER_FORMATS),
        Provider::City => {
            let suffix = Lengths::listed(EN::ADDRESS_CITY_SUFFIX);
            let prefixed = template(
                EN::ADDRESS_CITY_WITH_PREFIX_TPL,
                &[
                    ("{CityPrefix}", Lengths::listed(EN::ADDRESS_CITY_PREFIX)),
                    ("{CityName}", name),
                    ("{CitySuffix}", suffix),
                ],
            );
            prefixed.or(template(
                EN::ADDRESS_CITY_TPL,
                &[("{CityName}", first.or(last)), ("{CitySuffix}", suffix)],
            ))
        }
        Provider::State => Lengths::listed(EN::ADDRESS_STATE),
        Provider::StateAbbr => Lengths::listed(EN::ADDRESS_STATE_ABBR),
        Provider::ZipCode => Lengths::exactly(ZIP_CODE_DIGITS),
        Provider::PostCode => Lengths::listed(EN::ADDRESS_POSTCODE_FORMATS),
        Provider::Country => Lengths::listed(EN::ADDRESS_COUNTRY),
        Provider::CountryCode => Lengths::listed(EN::ADDRESS_COUNTRY_CODE),
        Provider::CreditCard => CREDIT_CARD_DIGITS,
        Provider::Uuid => Lengths::exactly(2).times(16).then(Lengths::exactly(4)),
        Provider::Icd10cm => {
            // A dot follows the category where a profile's shapes hold one.
            let dot = Lengths::between(0, 1);
            let extension = Lengths::between(0, ICD10CM_EXTENSION_PER_MILLE.len() - 1);
            Lengths::exactly(1 + ICD10CM_CATEGORY_DIGITS).then(dot).then(extension)
        }
        Provider::HcpcsLevel2 => Lengths::exactly(1 + HCPCS_LEVEL2_DIGITS),
    }
}

/// The fewest and most characters each provider's values hold, by the provider's name. Each is
/// read from the lists and templates the provider draws from, so every value it draws holds
/// between them. `icd10cm` counts the dot a profile's shapes may give it.
pub fn provider_lengths() -> BTreeMap<String, (usize, usize)> {
    Provider::ALL
        .into_iter()
        .map(|provider| {
            let lengths = provider_value_lengths(provider);
            (provider.name(), (lengths.shortest, lengths.longest))
        })
        .collect()
}

/// A repetition's least and most counts as the pattern states them, the most None where unbounded.
fn stated_counts(kind: &RepetitionKind) -> (u32, Option<u32>) {
    match kind {
        RepetitionKind::ZeroOrOne => (0, Some(1)),
        RepetitionKind::ZeroOrMore => (0, None),
        RepetitionKind::OneOrMore => (1, None),
        RepetitionKind::Range(RepetitionRange::Exactly(count)) => (*count, Some(*count)),
        RepetitionKind::Range(RepetitionRange::AtLeast(least)) => (*least, None), // codespell:ignore atleast
        RepetitionKind::Range(RepetitionRange::Bounded(least, most)) => (*least, Some(*most)),
    }
}

/// Every repetition in `ast`, each before those inside it.
fn repetitions<'a>(ast: &'a Ast, found: &mut Vec<&'a regex_syntax::ast::Repetition>) {
    match ast {
        Ast::Repetition(repetition) => {
            found.push(repetition);
            repetitions(&repetition.ast, found);
        }
        Ast::Group(group) => repetitions(&group.ast, found),
        Ast::Concat(concat) => concat.asts.iter().for_each(|ast| repetitions(ast, found)),
        Ast::Alternation(alternation) => alternation.asts.iter().for_each(|ast| repetitions(ast, found)),
        Ast::Empty(_)
        | Ast::Flags(_)
        | Ast::Literal(_)
        | Ast::Dot(_)
        | Ast::Assertion(_)
        | Ast::ClassUnicode(_)
        | Ast::ClassPerl(_)
        | Ast::ClassBracketed(_) => {}
    }
}

/// The lengths of the values `ast` draws with each repetition, in [`repetitions`]' order, between
/// the counts `counts` gives it.
fn drawn_lengths(ast: &Ast, counts: &[(u32, u32)], next: &mut usize) -> Lengths {
    match ast {
        Ast::Empty(_) | Ast::Flags(_) | Ast::Assertion(_) => Lengths::exactly(0),
        Ast::Literal(_) | Ast::Dot(_) | Ast::ClassUnicode(_) | Ast::ClassPerl(_) | Ast::ClassBracketed(_) => Lengths::exactly(1),
        Ast::Repetition(repetition) => {
            let (least, most) = counts[*next];
            *next += 1;
            let each = drawn_lengths(&repetition.ast, counts, next);
            Lengths::between(each.shortest * least as usize, each.longest.saturating_mul(most as usize))
        }
        Ast::Group(group) => drawn_lengths(&group.ast, counts, next),
        Ast::Concat(concat) => concat
            .asts
            .iter()
            .fold(Lengths::exactly(0), |total, ast| total.then(drawn_lengths(ast, counts, next))),
        Ast::Alternation(alternation) => Lengths::of_each(alternation.asts.iter().map(|ast| drawn_lengths(ast, counts, next))),
    }
}

/// `pattern` with its repetitions bounded so that every value the engine draws from it holds at
/// least `least` and at most `most` characters, or None where no bounds do, or where it does not
/// parse. A pattern whose values already fit is returned as given.
///
/// Only the repetition operators are rewritten, so the rest of the pattern keeps its text. Each
/// repetition whose counts span a range has its least count raised by one shared amount, until the
/// shortest value reaches `least`. Then its most is held to one shared amount above its least,
/// until the longest value fits `most`. An unbounded repetition draws at most the engine's extra
/// repeats.
pub fn pattern_within_lengths(pattern: &str, least: Option<usize>, most: Option<usize>) -> Option<String> {
    let ast = Parser::new().parse(pattern).ok()?;
    let (least, most) = (least.unwrap_or(0), most.unwrap_or(usize::MAX));
    let mut found = Vec::new();
    repetitions(&ast, &mut found);
    let stated: Vec<_> = found.iter().map(|repetition| stated_counts(&repetition.op.kind)).collect();
    let lengths = |counts: &[(u32, u32)]| drawn_lengths(&ast, counts, &mut 0);
    let raised = |by: u32| -> Vec<(u32, u32)> {
        stated
            .iter()
            .map(|&(low, high)| match high {
                Some(high) => (low.saturating_add(by).min(high), high),
                None => (low.saturating_add(by), low.saturating_add(by).saturating_add(PATTERN_MAX_REPEAT)),
            })
            .collect()
    };
    let now = lengths(&raised(0));
    if now.shortest >= least && now.longest <= most {
        return Some(pattern.to_owned());
    }
    // A raise that lengthens the shortest value lengthens it by a character at least, so `least`
    // raises reach `least` where any number does.
    let raise = (0..=u32::try_from(least).unwrap_or(u32::MAX)).find(|&by| lengths(&raised(by)).shortest >= least)?;
    let floor = raised(raise);
    let held = |above: u32| -> Vec<(u32, u32)> {
        floor
            .iter()
            .map(|&(low, high)| (low, high.min(low.saturating_add(above))))
            .collect()
    };
    let widest = floor.iter().map(|&(low, high)| high - low).max().unwrap_or(0);
    let above = (0..=widest).rev().find(|&above| lengths(&held(above)).longest <= most)?;
    let counts = held(above);
    let mut fitted = pattern.to_owned();
    // From the end, so each operator's span still points at its text.
    for (index, repetition) in found.iter().enumerate().rev() {
        let (low, high) = counts[index];
        let unchanged = match stated[index] {
            (stated_low, Some(stated_high)) => (low, high) == (stated_low, stated_high),
            (stated_low, None) => low == stated_low && high == low.saturating_add(PATTERN_MAX_REPEAT),
        };
        if unchanged {
            continue;
        }
        let bounds = if low == high {
            format!("{{{low}}}")
        } else {
            format!("{{{low},{high}}}")
        };
        let lazy = if repetition.greedy { "" } else { "?" };
        let span = repetition.op.span;
        fitted.replace_range(span.start.offset..span.end.offset, &format!("{bounds}{lazy}"));
    }
    Some(fitted)
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;

    use super::*;
    use crate::samplers::text::push_icd10cm;
    use crate::samplers::text::push_provided;

    #[test]
    fn every_value_a_provider_draws_holds_its_stated_lengths() {
        let mut rng = ChaCha8Rng::seed_from_u64(7);
        let stated = provider_lengths();
        for provider in Provider::ALL {
            let (shortest, longest) = stated[&provider.name()];
            for draw in 0..20_000 {
                let mut value = String::new();
                if provider == Provider::Icd10cm {
                    push_icd10cm(draw % 2 == 0, &mut rng, &mut value);
                } else {
                    push_provided(provider, &mut rng, &mut value);
                }
                let length = value.chars().count();
                assert!(
                    (shortest..=longest).contains(&length),
                    "{provider:?} drew {value:?}, outside {shortest} to {longest}"
                );
            }
        }
    }
}
