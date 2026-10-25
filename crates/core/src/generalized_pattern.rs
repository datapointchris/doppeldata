//! A string column's values generalized by grex into one regular expression, for `string.pattern`.

use grex::RegExpBuilder;
use regex_syntax::Parser;
use regex_syntax::hir::Class;
use regex_syntax::hir::Hir;
use regex_syntax::hir::HirKind;

/// The anchored expression grex builds from `values`, with digits as `\d` and repeats counted. It is
/// `None` when a literal sits in an alternation or an optional part, or a class other than `\d`
/// appears, because each of those holds characters only some values have.
pub fn generalize(values: &[String]) -> Option<String> {
    if values.is_empty() {
        return None;
    }
    let pattern = RegExpBuilder::from(values)
        .with_conversion_of_digits()
        .with_conversion_of_repetitions()
        .build();
    let hir = Parser::new().parse(&pattern).ok()?;
    let digit = Parser::new().parse(r"\d").ok()?;
    every_value_shares(&hir, &|class| class == &digit, false).then_some(pattern)
}

/// Whether `pattern` holds no literal only some values have: none sits in an alternation or an
/// optional part, and each class is `\d` or a union of the whole ASCII digit, upper-case and
/// lower-case ranges. A class narrower than those names characters the data held. `None` when the
/// pattern does not parse.
pub fn is_generalized(pattern: &str) -> Option<bool> {
    let hir = Parser::new().parse(pattern).ok()?;
    let digit = Parser::new().parse(r"\d").ok()?;
    Some(every_value_shares(
        &hir,
        &|class| class == &digit || whole_ascii_ranges(class),
        false,
    ))
}

fn whole_ascii_ranges(class: &Hir) -> bool {
    const WHOLE: [(char, char); 3] = [('0', '9'), ('A', 'Z'), ('a', 'z')];
    match class.kind() {
        HirKind::Class(Class::Unicode(ranges)) => ranges.iter().all(|range| WHOLE.contains(&(range.start(), range.end()))),
        HirKind::Class(Class::Bytes(ranges)) => ranges
            .iter()
            .all(|range| WHOLE.contains(&(char::from(range.start()), char::from(range.end())))),
        _ => false,
    }
}

fn every_value_shares(hir: &Hir, class_allowed: &dyn Fn(&Hir) -> bool, in_some_values_only: bool) -> bool {
    match hir.kind() {
        HirKind::Empty | HirKind::Look(_) => true,
        HirKind::Literal(_) => !in_some_values_only,
        HirKind::Class(_) => class_allowed(hir),
        HirKind::Repetition(repetition) => every_value_shares(&repetition.sub, class_allowed, in_some_values_only || repetition.min == 0),
        HirKind::Capture(capture) => every_value_shares(&capture.sub, class_allowed, in_some_values_only),
        HirKind::Concat(parts) => parts
            .iter()
            .all(|part| every_value_shares(part, class_allowed, in_some_values_only)),
        HirKind::Alternation(branches) => branches.iter().all(|branch| every_value_shares(branch, class_allowed, true)),
    }
}
