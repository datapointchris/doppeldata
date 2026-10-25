//! String columns other than categoricals: a named provider, a stored pattern, the shape
//! histogram, or characters of the profiled classes at a profiled length. A check digit is applied
//! after generation, with a share of `quality.invalid_rate` left wrong.

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
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use rand_distr::Distribution;
use rand_distr::weighted::WeightedAliasIndex;
use regex_syntax::hir::Hir;
use regex_syntax::hir::HirKind;

use super::inverse_cdf::InverseCdf;
use crate::profile::CharClass;
use crate::rng::Bernoulli;
use crate::rng::below;
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

/// The providers `semantic.generator_hint.provider` names. The names are doppeldata's, so a change
/// of faker library breaks no profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Provider {
    Email,
    FreeEmail,
    Username,
    Ipv4,
    Ipv6,
    MacAddress,
    UserAgent,
    Name,
    FirstName,
    LastName,
    PhoneNumber,
    CellNumber,
    Company,
    JobTitle,
    StreetAddress,
    StreetName,
    BuildingNumber,
    City,
    State,
    StateAbbr,
    ZipCode,
    PostCode,
    Country,
    CountryCode,
    CreditCard,
    Uuid,
}

const PROVIDERS: [(&str, Provider); 26] = [
    ("email", Provider::Email),
    ("free_email", Provider::FreeEmail),
    ("username", Provider::Username),
    ("ipv4", Provider::Ipv4),
    ("ipv6", Provider::Ipv6),
    ("mac_address", Provider::MacAddress),
    ("user_agent", Provider::UserAgent),
    ("name", Provider::Name),
    ("first_name", Provider::FirstName),
    ("last_name", Provider::LastName),
    ("phone_number", Provider::PhoneNumber),
    ("cell_number", Provider::CellNumber),
    ("company", Provider::Company),
    ("job_title", Provider::JobTitle),
    ("street_address", Provider::StreetAddress),
    ("street_name", Provider::StreetName),
    ("building_number", Provider::BuildingNumber),
    ("city", Provider::City),
    ("state", Provider::State),
    ("state_abbr", Provider::StateAbbr),
    ("zip_code", Provider::ZipCode),
    ("post_code", Provider::PostCode),
    ("country", Provider::Country),
    ("country_code", Provider::CountryCode),
    ("credit_card", Provider::CreditCard),
    ("uuid", Provider::Uuid),
];

impl Provider {
    pub(crate) fn from_name(name: &str) -> Result<Self, String> {
        PROVIDERS
            .iter()
            .find(|(known, _)| *known == name)
            .map(|&(_, provider)| provider)
            .ok_or_else(|| {
                let known: Vec<&str> = PROVIDERS.iter().map(|(known, _)| *known).collect();
                format!("provider `{name}` is not one of {}", known.join(", "))
            })
    }

    fn push(self, rng: &mut ChaCha8Rng, out: &mut String) {
        let value: String = match self {
            Self::Email => internet::SafeEmail().fake_with_rng(rng),
            Self::FreeEmail => internet::FreeEmail().fake_with_rng(rng),
            Self::Username => internet::Username().fake_with_rng(rng),
            Self::Ipv4 => internet::IPv4().fake_with_rng(rng),
            Self::Ipv6 => internet::IPv6().fake_with_rng(rng),
            Self::MacAddress => internet::MACAddress().fake_with_rng(rng),
            Self::UserAgent => internet::UserAgent().fake_with_rng(rng),
            Self::Name => name::Name().fake_with_rng(rng),
            Self::FirstName => name::FirstName().fake_with_rng(rng),
            Self::LastName => name::LastName().fake_with_rng(rng),
            Self::PhoneNumber => phone::PhoneNumber().fake_with_rng(rng),
            Self::CellNumber => phone::CellNumber().fake_with_rng(rng),
            Self::Company => company::CompanyName().fake_with_rng(rng),
            Self::JobTitle => job::Title().fake_with_rng(rng),
            Self::StreetAddress => {
                let number: String = address::BuildingNumber().fake_with_rng(rng);
                let street: String = address::StreetName().fake_with_rng(rng);
                format!("{number} {street}")
            }
            Self::StreetName => address::StreetName().fake_with_rng(rng),
            Self::BuildingNumber => address::BuildingNumber().fake_with_rng(rng),
            Self::City => address::CityName().fake_with_rng(rng),
            Self::State => address::StateName().fake_with_rng(rng),
            Self::StateAbbr => address::StateAbbr().fake_with_rng(rng),
            Self::ZipCode => address::ZipCode().fake_with_rng(rng),
            Self::PostCode => address::PostCode().fake_with_rng(rng),
            Self::Country => address::CountryName().fake_with_rng(rng),
            Self::CountryCode => address::CountryCode().fake_with_rng(rng),
            Self::CreditCard => creditcard::CreditCardNumber().fake_with_rng(rng),
            Self::Uuid => {
                push_uuid(rng, out);
                return;
            }
        };
        out.push_str(&value);
    }
}

/// A version 4 UUID in its hyphenated lowercase form.
fn push_uuid(rng: &mut ChaCha8Rng, out: &mut String) {
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

/// A stored pattern compiled for generation. A line anchor is dropped, because a generated value is
/// the whole line. `\d` and the other classes are ASCII, as the profiler's patterns mean them.
pub(crate) fn compile_pattern(pattern: &str, max_repeat: u32) -> Result<rand_regex::Regex, String> {
    let ascii = regex_syntax::ParserBuilder::new().unicode(false).build().parse(pattern);
    let hir = match ascii {
        Ok(hir) => hir,
        Err(_) => regex_syntax::Parser::new()
            .parse(pattern)
            .map_err(|error| format!("pattern {pattern:?} does not parse: {error}"))?,
    };
    rand_regex::Regex::with_hir(without_anchors(&hir), max_repeat).map_err(|error| format!("pattern {pattern:?} cannot generate: {error}"))
}

fn without_anchors(hir: &Hir) -> Hir {
    match hir.kind() {
        HirKind::Look(_) => Hir::empty(),
        HirKind::Concat(parts) => Hir::concat(parts.iter().map(without_anchors).collect()),
        HirKind::Alternation(branches) => Hir::alternation(branches.iter().map(without_anchors).collect()),
        _ => hir.clone(),
    }
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

pub(crate) fn shape_chars(shape: &str) -> Vec<ShapeChar> {
    shape
        .chars()
        .map(|c| match c {
            'A' => ShapeChar::Upper,
            'a' => ShapeChar::Lower,
            '9' => ShapeChar::Digit,
            other => ShapeChar::Literal(other),
        })
        .collect()
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
    Pattern(Box<rand_regex::Regex>),
    Shapes {
        choice: WeightedAliasIndex<f64>,
        shapes: Vec<Vec<ShapeChar>>,
    },
    Characters {
        length: InverseCdf,
        alphabet: Vec<u8>,
    },
}

impl TextSource {
    pub(crate) fn push(&self, rng: &mut ChaCha8Rng, out: &mut String) {
        match self {
            Self::Provider(provider) => provider.push(rng, out),
            Self::Pattern(pattern) => {
                let value: String = pattern.sample(rng);
                out.push_str(&value);
            }
            Self::Shapes { choice, shapes } => {
                for &c in &shapes[choice.sample(rng)] {
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

/// A check digit written over a value's last digit, wrong on a share of values.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CheckDigit {
    pub(crate) invalid: Bernoulli,
}

impl CheckDigit {
    pub(crate) fn from_name(name: &str) -> Result<(), String> {
        match name {
            "luhn" => Ok(()),
            other => Err(format!("check digit `{other}` is not one of luhn")),
        }
    }

    fn apply(&self, invalid_rng: &mut ChaCha8Rng, value: &mut String) {
        let wrong = self.invalid.draw(invalid_rng);
        let offset = if wrong { 1 + below(invalid_rng, 9) as u8 } else { 0 };
        let Some(last) = value.bytes().rposition(|b| b.is_ascii_digit()) else {
            return;
        };
        let payload = value.as_bytes()[..last].iter().filter(|b| b.is_ascii_digit()).map(|b| b - b'0');
        let digit = (luhn_check_digit(payload) + offset) % 10;
        value.replace_range(last..=last, char::from(b'0' + digit).encode_utf8(&mut [0; 4]));
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TextSampler {
    pub(crate) source: TextSource,
    pub(crate) check_digit: Option<CheckDigit>,
    pub(crate) output: StringOutput,
}

impl TextSampler {
    pub(crate) fn generate(&self, rng: &mut ChaCha8Rng, invalid_rng: &mut ChaCha8Rng, rows: usize, nulls: Option<NullBuffer>) -> ArrayRef {
        self.output.build(rows, nulls.as_ref(), |out| {
            self.source.push(rng, out);
            if let Some(check_digit) = &self.check_digit {
                check_digit.apply(invalid_rng, out);
            }
        })
    }
}
