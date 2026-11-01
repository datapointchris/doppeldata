//! String columns other than categoricals: a named provider, a stored pattern, the shape
//! histogram, or characters of the profiled classes at a profiled length. A check digit is applied
//! after generation, with a share of `quality.invalid_fraction` left wrong.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::OffsetSizeTrait;
use arrow_array::builder::GenericStringBuilder;
use arrow_buffer::NullBuffer;
use arrow_schema::DataType;
use fake::Fake;
use fake::faker::address::en as address;
use fake::faker::company::en as company;
use fake::faker::creditcard::en as creditcard;
use fake::faker::internet::en as internet;
use fake::faker::job::en as job;
use fake::faker::name::en as name;
use fake::faker::phone_number::en as phone;
use fake::locales::Data;
use fake::locales::EN;
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use rand_distr::Distribution;
use rand_distr::weighted::WeightedAliasIndex;
use regex_syntax::hir::Class;
use regex_syntax::hir::Hir;
use regex_syntax::hir::HirKind;

use super::BlockPosition;
use super::StreamKeys;
use super::distinct_values::Stratum;
use super::distinct_values::ValueIndex;
use super::distinct_values::Variety;
use super::inverse_cdf::InverseCdf;
use crate::profile::CharClass;
use crate::profile::CheckDigit;
use crate::profile::Provider;
use crate::rng::Bernoulli;
use crate::rng::below;
use crate::rng::rng_for;
use crate::rng::unit;

/// The Arrow type a string column is stored as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StringOutput {
    Utf8,
    LargeUtf8,
}

impl StringOutput {
    pub(crate) fn from_type(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::Utf8 => Some(Self::Utf8),
            DataType::LargeUtf8 => Some(Self::LargeUtf8),
            _ => None,
        }
    }

    pub(crate) fn data_type(self) -> DataType {
        match self {
            Self::Utf8 => DataType::Utf8,
            Self::LargeUtf8 => DataType::LargeUtf8,
        }
    }

    /// An array of `rows` strings. `value` writes every row's string, a null row's included, so the
    /// stream advances the same whatever the null mask.
    pub(crate) fn build(self, rows: usize, nulls: Option<&NullBuffer>, value: impl FnMut(&mut String)) -> ArrayRef {
        match self {
            Self::Utf8 => build_strings::<i32>(rows, nulls, value),
            Self::LargeUtf8 => build_strings::<i64>(rows, nulls, value),
        }
    }
}

fn build_strings<O: OffsetSizeTrait>(rows: usize, nulls: Option<&NullBuffer>, mut value: impl FnMut(&mut String)) -> ArrayRef {
    let mut builder = GenericStringBuilder::<O>::with_capacity(rows, rows * 12);
    let mut text = String::new();
    for row in 0..rows {
        text.clear();
        value(&mut text);
        if nulls.is_some_and(|nulls| nulls.is_null(row)) {
            builder.append_null();
        } else {
            builder.append_value(&text);
        }
    }
    Arc::new(builder.finish())
}

/// The faker behind each of doppeldata's provider names.
pub(super) fn push_provided(provider: Provider, rng: &mut ChaCha8Rng, out: &mut String) {
    let value: String = match provider {
        Provider::Email => {
            push_email(rng, out);
            return;
        }
        Provider::FreeEmail => internet::FreeEmail().fake_with_rng(rng),
        Provider::Username => internet::Username().fake_with_rng(rng),
        Provider::Ipv4 => internet::IPv4().fake_with_rng(rng),
        Provider::Ipv6 => internet::IPv6().fake_with_rng(rng),
        Provider::MacAddress => internet::MACAddress().fake_with_rng(rng),
        Provider::UserAgent => internet::UserAgent().fake_with_rng(rng),
        Provider::Name => name::Name().fake_with_rng(rng),
        Provider::FirstName => name::FirstName().fake_with_rng(rng),
        Provider::LastName => name::LastName().fake_with_rng(rng),
        Provider::PhoneNumber => phone::PhoneNumber().fake_with_rng(rng),
        Provider::CellNumber => phone::CellNumber().fake_with_rng(rng),
        Provider::Company => company::CompanyName().fake_with_rng(rng),
        Provider::JobTitle => job::Title().fake_with_rng(rng),
        Provider::StreetAddress => {
            let number: String = address::BuildingNumber().fake_with_rng(rng);
            let street: String = address::StreetName().fake_with_rng(rng);
            format!("{number} {street}")
        }
        Provider::StreetName => address::StreetName().fake_with_rng(rng),
        Provider::BuildingNumber => address::BuildingNumber().fake_with_rng(rng),
        Provider::City => address::CityName().fake_with_rng(rng),
        Provider::State => address::StateName().fake_with_rng(rng),
        Provider::StateAbbr => address::StateAbbr().fake_with_rng(rng),
        Provider::ZipCode => {
            push_digits(rng, ZIP_CODE_DIGITS, out);
            return;
        }
        Provider::PostCode => address::PostCode().fake_with_rng(rng),
        Provider::Country => address::CountryName().fake_with_rng(rng),
        Provider::CountryCode => address::CountryCode().fake_with_rng(rng),
        Provider::CreditCard => creditcard::CreditCardNumber().fake_with_rng(rng),
        Provider::Uuid => {
            push_uuid(rng, out);
            return;
        }
        Provider::Icd10cm => {
            push_icd10cm(false, rng, out);
            return;
        }
        Provider::HcpcsLevel2 => {
            push_hcpcs_level2(rng, out);
            return;
        }
    };
    out.push_str(&value);
}

/// An ICD-10-CM code's category is a letter and this many digits.
pub(super) const ICD10CM_CATEGORY_DIGITS: usize = 2;
/// The share, per mille, of ICD-10-CM's codes holding 0 to 4 characters after their three-character
/// category.
pub(super) const ICD10CM_EXTENSION_PER_MILLE: [u32; 5] = [19, 103, 149, 205, 524];
/// The extension's characters are digits, but for its fourth, a code's seventh character, which is
/// a letter.
const ICD10CM_EXTENSION_LETTER_AT: usize = 3;
/// An HCPCS Level II code's letter is one of A to V, and its digits follow.
const HCPCS_LEVEL2_LETTERS: u32 = 22;
pub(super) const HCPCS_LEVEL2_DIGITS: usize = 4;
/// A US ZIP code's digits, and the codes they spell. fake 5.1's `ZipCode` picks three, four or
/// five digits uniformly.
pub(super) const ZIP_CODE_DIGITS: usize = 5;
const ZIP_CODES: f64 = 100_000.0;

fn push_upper(rng: &mut ChaCha8Rng, letters: u32, out: &mut String) {
    out.push(char::from(UPPER[below(rng, letters) as usize]));
}

fn push_digits(rng: &mut ChaCha8Rng, digits: usize, out: &mut String) {
    for _ in 0..digits {
        out.push(char::from(DIGITS[below(rng, 10) as usize]));
    }
}

/// An ICD-10-CM code by its format, not from the code set: a letter and two digits, then an
/// extension at the code set's share of each length, after a dot where `dotted`.
pub(super) fn push_icd10cm(dotted: bool, rng: &mut ChaCha8Rng, out: &mut String) {
    push_upper(rng, 26, out);
    push_digits(rng, ICD10CM_CATEGORY_DIGITS, out);
    let (mut drawn, mut extension) = (below(rng, 1000), 0);
    while drawn >= ICD10CM_EXTENSION_PER_MILLE[extension] {
        drawn -= ICD10CM_EXTENSION_PER_MILLE[extension];
        extension += 1;
    }
    if dotted && extension > 0 {
        out.push('.');
    }
    for at in 0..extension {
        if at == ICD10CM_EXTENSION_LETTER_AT {
            push_upper(rng, 26, out);
        } else {
            push_digits(rng, 1, out);
        }
    }
}

/// The ICD-10-CM codes of each extension length, and the share drawn at that length. A category is
/// one of 26 letters and 100 digit pairs, and each extension character one of 10 digits or 26
/// letters.
fn icd10cm_variety() -> Variety {
    let strata = ICD10CM_EXTENSION_PER_MILLE
        .iter()
        .enumerate()
        .map(|(extension, &per_mille)| Stratum {
            values: (0..extension)
                .map(|at| if at == ICD10CM_EXTENSION_LETTER_AT { 26.0 } else { 10.0 })
                .product::<f64>()
                * 26.0
                * 100.0,
            share: f64::from(per_mille) / 1000.0,
        });
    Variety::Strata(strata.collect())
}

/// An HCPCS Level II code by its format, not from the code set: a letter A to V and four digits.
fn push_hcpcs_level2(rng: &mut ChaCha8Rng, out: &mut String) {
    push_upper(rng, HCPCS_LEVEL2_LETTERS, out);
    push_digits(rng, HCPCS_LEVEL2_DIGITS, out);
}

/// The reserved domains an `email` address is at, so no generated address reaches anyone.
pub(super) const EMAIL_DOMAINS: [&str; 3] = ["example.com", "example.net", "example.org"];

/// `first.last` at a reserved domain, in lower-case letters. A last name's apostrophe is dropped.
fn push_email(rng: &mut ChaCha8Rng, out: &mut String) {
    fn push_letters(part: &str, out: &mut String) {
        out.extend(part.chars().filter(char::is_ascii_alphabetic).map(|c| c.to_ascii_lowercase()));
    }
    let first: &str = name::FirstName().fake_with_rng(rng);
    let last: &str = name::LastName().fake_with_rng(rng);
    push_letters(first, out);
    out.push('.');
    push_letters(last, out);
    out.push('@');
    out.push_str(EMAIL_DOMAINS[below(rng, EMAIL_DOMAINS.len() as u32) as usize]);
}

/// How many strings a provider draws. A provider choosing one entry of one list draws each
/// equally often, and `email` is a first name, a last name and a domain. fake's `city` is a
/// suffixed last name in three draws of five, a suffixed first name in one, and a prefixed,
/// suffixed full name in the last. Every other provider is uncounted.
fn provider_variety(provider: Provider) -> Variety {
    let entries = |list: &[&str]| list.iter().collect::<BTreeSet<_>>().len() as f64;
    let (first, last) = (entries(EN::NAME_FIRST_NAME), entries(EN::NAME_LAST_NAME));
    let strings = match provider {
        Provider::Email => first * last * EMAIL_DOMAINS.len() as f64,
        Provider::FirstName => first,
        Provider::LastName => last,
        Provider::State => entries(EN::ADDRESS_STATE),
        Provider::StateAbbr => entries(EN::ADDRESS_STATE_ABBR),
        Provider::Country => entries(EN::ADDRESS_COUNTRY),
        Provider::CountryCode => entries(EN::ADDRESS_COUNTRY_CODE),
        Provider::Icd10cm => return icd10cm_variety(),
        Provider::HcpcsLevel2 => f64::from(HCPCS_LEVEL2_LETTERS) * 10_000.0,
        Provider::ZipCode => ZIP_CODES,
        Provider::City => {
            let suffixed = |names: f64| names * entries(EN::ADDRESS_CITY_SUFFIX);
            let stratum = |values, share| Stratum { values, share };
            return Variety::Strata(vec![
                stratum(suffixed(last), 0.6),
                stratum(suffixed(first), 0.2),
                stratum(suffixed(entries(EN::ADDRESS_CITY_PREFIX) * first * last), 0.2),
            ]);
        }
        Provider::FreeEmail
        | Provider::Username
        | Provider::Ipv4
        | Provider::Ipv6
        | Provider::MacAddress
        | Provider::UserAgent
        | Provider::Name
        | Provider::PhoneNumber
        | Provider::CellNumber
        | Provider::Company
        | Provider::JobTitle
        | Provider::StreetAddress
        | Provider::StreetName
        | Provider::BuildingNumber
        | Provider::PostCode
        | Provider::CreditCard
        | Provider::Uuid => return Variety::Uncounted,
    };
    Variety::uniform(strings)
}

/// A version 4 UUID in its hyphenated lowercase form.
pub(crate) fn push_uuid(rng: &mut ChaCha8Rng, out: &mut String) {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&rng.next_u64().to_le_bytes());
    bytes[8..].copy_from_slice(&rng.next_u64().to_le_bytes());
    bytes[6] = (bytes[6] & 0x0F) | 0x40;
    bytes[8] = (bytes[8] & 0x3F) | 0x80;
    for (index, byte) in bytes.iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        out.push_str(&format!("{byte:02x}"));
    }
}

/// A stored pattern compiled for generation, and the count of strings it generates.
#[derive(Debug, Clone)]
pub(crate) struct Pattern {
    regex: rand_regex::Regex,
    pub(crate) strings: f64,
}

impl Pattern {
    pub(crate) fn push(&self, rng: &mut ChaCha8Rng, out: &mut String) {
        let value: String = self.regex.sample(rng);
        out.push_str(&value);
    }
}

/// A stored pattern compiled for generation. A line anchor is dropped, because a generated value is
/// the whole line. `\d` and the other classes are ASCII, as the profiler's patterns mean them.
pub(crate) fn compile_pattern(pattern: &str, max_repeat: u32) -> Result<Pattern, String> {
    let ascii = regex_syntax::ParserBuilder::new().unicode(false).build().parse(pattern);
    let hir = match ascii {
        Ok(hir) => hir,
        Err(_) => regex_syntax::Parser::new()
            .parse(pattern)
            .map_err(|error| format!("pattern {pattern:?} does not parse: {error}"))?,
    };
    let hir = without_anchors(&hir);
    let strings = pattern_strings(&hir, max_repeat);
    let regex = rand_regex::Regex::with_hir(hir, max_repeat).map_err(|error| format!("pattern {pattern:?} cannot generate: {error}"))?;
    Ok(Pattern { regex, strings })
}

/// The strings `hir` generates, counting each alternative apart even where two overlap. An
/// unbounded repetition runs to `max_repeat` extra repeats, as rand_regex generates it.
fn pattern_strings(hir: &Hir, max_repeat: u32) -> f64 {
    match hir.kind() {
        HirKind::Empty | HirKind::Look(_) | HirKind::Literal(_) => 1.0,
        HirKind::Class(Class::Unicode(class)) => class
            .ranges()
            .iter()
            .map(|range| f64::from(u32::from(range.end()) - u32::from(range.start())) + 1.0)
            .sum(),
        HirKind::Class(Class::Bytes(class)) => class
            .ranges()
            .iter()
            .map(|range| f64::from(range.end() - range.start()) + 1.0)
            .sum(),
        HirKind::Capture(capture) => pattern_strings(&capture.sub, max_repeat),
        HirKind::Repetition(repetition) => {
            let each = pattern_strings(&repetition.sub, max_repeat);
            let most = repetition.max.unwrap_or(repetition.min.saturating_add(max_repeat));
            let (mut power, mut total) = (1.0_f64, 0.0_f64);
            for repeats in 0..=most {
                if repeats >= repetition.min {
                    total += power;
                }
                if total.is_infinite() {
                    break;
                }
                power *= each;
            }
            total
        }
        HirKind::Concat(parts) => parts.iter().map(|part| pattern_strings(part, max_repeat)).product(),
        HirKind::Alternation(branches) => branches.iter().map(|branch| pattern_strings(branch, max_repeat)).sum(),
    }
}

fn without_anchors(hir: &Hir) -> Hir {
    match hir.kind() {
        HirKind::Look(_) => Hir::empty(),
        HirKind::Concat(parts) => Hir::concat(parts.iter().map(without_anchors).collect()),
        HirKind::Alternation(branches) => Hir::alternation(branches.iter().map(without_anchors).collect()),
        _ => hir.clone(),
    }
}

/// A value's shape, the form a profile's `string.shapes` stores and [`Shape::new`] reads back: each
/// ASCII upper-case letter as `A`, each ASCII lower-case letter as `a`, each ASCII digit as `9`,
/// and every other character, a non-ASCII letter included, as itself.
pub fn shape_of(value: &str) -> String {
    value
        .chars()
        .map(|c| match c {
            'A'..='Z' => 'A',
            'a'..='z' => 'a',
            '0'..='9' => '9',
            other => other,
        })
        .collect()
}

/// One character of a shape: `A` an upper-case letter, `a` a lower-case one, `9` a digit, and any
/// other character itself.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ShapeChar {
    Upper,
    Lower,
    Digit,
    Literal(char),
}

/// A stored shape's characters, and the share of values drawn in it.
#[derive(Debug, Clone)]
pub(crate) struct Shape {
    chars: Vec<ShapeChar>,
    share: f64,
}

impl Shape {
    pub(crate) fn new(shape: &str, share: f64) -> Self {
        let chars = shape
            .chars()
            .map(|c| match c {
                'A' => ShapeChar::Upper,
                'a' => ShapeChar::Lower,
                '9' => ShapeChar::Digit,
                other => ShapeChar::Literal(other),
            })
            .collect();
        Self { chars, share }
    }

    fn strings(&self) -> f64 {
        self.leading_strings(self.chars.len())
    }

    /// The distinct strings the shape's first `chars` characters can hold.
    fn leading_strings(&self, chars: usize) -> f64 {
        self.chars
            .iter()
            .take(chars)
            .map(|c| match c {
                ShapeChar::Upper | ShapeChar::Lower => 26.0,
                ShapeChar::Digit => 10.0,
                ShapeChar::Literal(_) => 1.0,
            })
            .product()
    }
}

const UPPER: &[u8; 26] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const LOWER: &[u8; 26] = b"abcdefghijklmnopqrstuvwxyz";
const DIGITS: &[u8; 10] = b"0123456789";
const PUNCTUATION: &[u8] = b"!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~";

/// The characters a string of `classes` draws from, upper and lower letters and digits when none
/// are named.
pub(crate) fn alphabet(classes: Option<&[CharClass]>) -> Vec<u8> {
    let classes = classes
        .filter(|classes| !classes.is_empty())
        .unwrap_or(&[CharClass::Upper, CharClass::Lower, CharClass::Digit]);
    let mut alphabet = Vec::new();
    for class in classes {
        match class {
            CharClass::Upper => alphabet.extend_from_slice(UPPER),
            CharClass::Lower => alphabet.extend_from_slice(LOWER),
            CharClass::Digit => alphabet.extend_from_slice(DIGITS),
            CharClass::Space => alphabet.push(b' '),
            CharClass::Punct => alphabet.extend_from_slice(PUNCTUATION),
            CharClass::Other => {}
        }
    }
    if alphabet.is_empty() {
        alphabet.extend_from_slice(LOWER);
    }
    alphabet
}

/// Where a string column's values come from.
#[derive(Debug, Clone)]
pub(crate) enum TextSource {
    Provider(Provider),
    /// The `icd10cm` provider, with a dot after the category where `dotted`.
    Icd10cm {
        dotted: bool,
    },
    Pattern(Box<Pattern>),
    Shapes {
        choice: WeightedAliasIndex<f64>,
        shapes: Vec<Shape>,
    },
    Characters {
        length: InverseCdf,
        alphabet: Vec<u8>,
    },
}

impl TextSource {
    /// How many strings the source draws. A pattern's strings are counted as drawn equally often,
    /// though an alternation or a ranged repetition draws some more often than others.
    pub(crate) fn variety(&self) -> Variety {
        match self {
            Self::Provider(provider) => provider_variety(*provider),
            Self::Icd10cm { .. } => icd10cm_variety(),
            Self::Pattern(pattern) => Variety::uniform(pattern.strings),
            Self::Shapes { shapes, .. } => Variety::Strata(
                shapes
                    .iter()
                    .map(|shape| Stratum {
                        values: shape.strings(),
                        share: shape.share,
                    })
                    .collect(),
            ),
            Self::Characters { .. } => Variety::Uncounted,
        }
    }

    /// How many distinct strings the source's first `chars` characters take. Only shapes are
    /// counted by character, so any other source counts its whole strings.
    pub(crate) fn leading_variety(&self, chars: usize) -> Variety {
        match self {
            Self::Shapes { shapes, .. } => Variety::Strata(
                shapes
                    .iter()
                    .map(|shape| Stratum {
                        values: shape.leading_strings(chars),
                        share: shape.share,
                    })
                    .collect(),
            ),
            _ => self.variety(),
        }
    }

    pub(crate) fn push(&self, rng: &mut ChaCha8Rng, out: &mut String) {
        match self {
            Self::Provider(provider) => push_provided(*provider, rng, out),
            Self::Icd10cm { dotted } => push_icd10cm(*dotted, rng, out),
            Self::Pattern(pattern) => pattern.push(rng, out),
            Self::Shapes { choice, shapes } => {
                for &c in &shapes[choice.sample(rng)].chars {
                    match c {
                        ShapeChar::Upper => out.push(char::from(UPPER[below(rng, 26) as usize])),
                        ShapeChar::Lower => out.push(char::from(LOWER[below(rng, 26) as usize])),
                        ShapeChar::Digit => out.push(char::from(DIGITS[below(rng, 10) as usize])),
                        ShapeChar::Literal(literal) => out.push(literal),
                    }
                }
            }
            Self::Characters { length, alphabet } => {
                let length = length.value_at(unit(rng)).round().max(0.0) as usize;
                for _ in 0..length {
                    out.push(char::from(alphabet[below(rng, alphabet.len() as u32) as usize]));
                }
            }
        }
    }
}

/// The Luhn check digit for `payload`, a sequence of digits 0 to 9 read left to right.
fn luhn_check_digit(payload: impl DoubleEndedIterator<Item = u8>) -> u8 {
    let sum: u32 = payload
        .rev()
        .enumerate()
        .map(|(index, digit)| {
            let digit = u32::from(digit);
            if index % 2 == 0 {
                let doubled = digit * 2;
                if doubled > 9 { doubled - 9 } else { doubled }
            } else {
                digit
            }
        })
        .sum();
    ((10 - sum % 10) % 10) as u8
}

/// The DEA check digit for `payload`: the last digit of the digits at odd positions from the left
/// plus twice those at even positions.
fn dea_check_digit(payload: impl Iterator<Item = u8>) -> u8 {
    let sum: u32 = payload
        .enumerate()
        .map(|(index, digit)| u32::from(digit) * if index % 2 == 0 { 1 } else { 2 })
        .sum();
    (sum % 10) as u8
}

/// Every NPI is read as a card number issued under the health industry prefix 80840.
const NPI_PREFIX: [u8; 5] = [8, 0, 8, 4, 0];

/// A check digit written over a value's last digit, wrong on a share of values.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CheckDigitDraw {
    pub(crate) kind: CheckDigit,
    pub(crate) invalid: Bernoulli,
}

impl CheckDigitDraw {
    fn apply(&self, invalid_rng: &mut ChaCha8Rng, value: &mut String) {
        let wrong = self.invalid.draw(invalid_rng);
        let offset = if wrong { 1 + below(invalid_rng, 9) as u8 } else { 0 };
        let Some((last, right)) = check_digit_site(self.kind, value) else {
            return;
        };
        let digit = (right + offset) % 10;
        value.replace_range(last..=last, char::from(b'0' + digit).encode_utf8(&mut [0; 4]));
    }
}

/// Where `value`'s check digit sits, which is its last ASCII digit, and the digit `kind` computes
/// there from the ASCII digits before it. None for a value holding no digit.
fn check_digit_site(kind: CheckDigit, value: &str) -> Option<(usize, u8)> {
    let last = value.bytes().rposition(|b| b.is_ascii_digit())?;
    let payload = value.as_bytes()[..last].iter().filter(|b| b.is_ascii_digit()).map(|b| b - b'0');
    let right = match kind {
        CheckDigit::Luhn => luhn_check_digit(payload),
        CheckDigit::Npi => luhn_check_digit(NPI_PREFIX.into_iter().chain(payload)),
        CheckDigit::Dea => dea_check_digit(payload),
    };
    Some((last, right))
}

/// Whether `value` holds the `kind` check digit where generation writes one.
pub fn check_digit_holds(kind: CheckDigit, value: &str) -> bool {
    check_digit_site(kind, value).is_some_and(|(last, right)| value.as_bytes()[last] - b'0' == right)
}

#[derive(Debug, Clone)]
pub(crate) struct TextSampler {
    pub(crate) source: TextSource,
    pub(crate) check_digit: Option<CheckDigitDraw>,
    /// Where set, each row's value is the one at an index the row draws.
    pub(crate) index: Option<ValueIndex>,
    /// Each row's value is the one at its own row's index, so a child can read it at any row.
    pub(crate) by_row: bool,
    pub(crate) output: StringOutput,
}

impl TextSampler {
    pub(crate) fn generate(&self, at: &BlockPosition, keys: &StreamKeys, rows: usize, nulls: Option<NullBuffer>) -> ArrayRef {
        let block_stream = |key| rng_for(at.seed, key, at.block);
        let nulls = nulls.as_ref();
        if self.by_row {
            let mut row = at.first_row;
            return self.output.build(rows, nulls, |out| {
                self.push_at_row(at.seed, keys, row, out);
                row += 1;
            });
        }
        match self.index {
            None => {
                let (mut values, mut invalid) = (block_stream(keys.values), block_stream(keys.invalid));
                self.output.build(rows, nulls, |out| {
                    self.source.push(&mut values, out);
                    self.apply_check_digit(&mut invalid, out);
                })
            }
            // The check digit draws from the value's own stream after it, so whether a value's
            // digit is wrong is a function of its index too.
            Some(index) => {
                let mut indices = block_stream(keys.index);
                self.output.build(rows, nulls, |out| {
                    let mut value = index.value_stream(at.seed, keys, &mut indices);
                    self.source.push(&mut value, out);
                    self.apply_check_digit(&mut value, out);
                })
            }
        }
    }

    /// The value at row `row`'s own index, as [`Self::by_row`] draws it.
    pub(crate) fn push_at_row(&self, seed: u64, keys: &StreamKeys, row: u64, out: &mut String) {
        let mut value = rng_for(seed, keys.indexed_values, row);
        self.source.push(&mut value, out);
        self.apply_check_digit(&mut value, out);
    }

    fn apply_check_digit(&self, rng: &mut ChaCha8Rng, out: &mut String) {
        if let Some(check_digit) = &self.check_digit {
            check_digit.apply(rng, out);
        }
    }
}
