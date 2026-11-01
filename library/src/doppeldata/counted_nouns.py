"""A count beside its noun, as a refusal, a report or a log line prints it."""


def counted(count: int, noun: str) -> str:
    """`count` with thousands separators beside `noun`, made plural by an `s` unless the count is one: 1 file, 2,000 rows."""
    return f'{count:,} {noun}' + ('' if count == 1 else 's')
