//! A string column's values generalized by grex into one regular expression, for `string.pattern`.

use grex::RegExpBuilder;
use regex_syntax::Parser;
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
    every_value_shares(&hir, &digit, false).then_some(pattern)
}

fn every_value_shares(hir: &Hir, digit: &Hir, in_some_values_only: bool) -> bool {
    match hir.kind() {
        HirKind::Empty | HirKind::Look(_) => true,
        HirKind::Literal(_) => !in_some_values_only,
        HirKind::Class(_) => hir == digit,
        HirKind::Repetition(repetition) => every_value_shares(&repetition.sub, digit, in_some_values_only || repetition.min == 0),
        HirKind::Capture(capture) => every_value_shares(&capture.sub, digit, in_some_values_only),
        HirKind::Concat(parts) => parts.iter().all(|part| every_value_shares(part, digit, in_some_values_only)),
        HirKind::Alternation(branches) => branches.iter().all(|branch| every_value_shares(branch, digit, true)),
    }
}
