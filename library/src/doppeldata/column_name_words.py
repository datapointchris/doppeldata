"""A column name's words, whether a name hint runs in them, and the hints the detector packs and the privacy policy both read."""

import re

# An upper-case run before a capitalized word, a capitalized or lower-case word, or a run of digits.
NAME_WORD = re.compile(r'[A-Z]+(?![a-z])|[A-Z]?[a-z]+|\d+')
# What a ZIP column's name ends in. The general pack types a column by them, and `healthcare` treats one as a ZIP code.
ZIP_HINTS = ('zip', 'zipcode', 'zip_code', 'postal', 'postal_code', 'postcode')


def name_words(name: str) -> tuple[str, ...]:
    """A column name's or a name hint's words, lower-cased: `customerEMail` and `customer_e_mail` are each customer, e, mail."""
    return tuple(word.lower() for word in NAME_WORD.findall(name))


def hint_runs_in(words: tuple[str, ...], hint: str, *, at_end: bool = False) -> bool:
    """Whether the words of `hint` run in order in a column name's `words`, ending them where `at_end`."""
    parts = name_words(hint)
    starts = [len(words) - len(parts)] if at_end else range(len(words) - len(parts) + 1)
    return any(start >= 0 and words[start : start + len(parts)] == parts for start in starts)
