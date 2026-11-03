"""What a check's profile of DATA reads to hold PROFILE's semantic typing, and what it records where the hold decided.

It imports nothing, so the frozen layout and the check's report read it where pyarrow is not installed.
"""

import dataclasses as dc


@dc.dataclass(frozen=True)
class FrozenTyping:
    """The semantic type an earlier profile gave a column, None where it left it untyped, and about how many sampled
    values decided it, None where the profile states no count of them, as one expanded from a dict of fakes does."""

    type: str | None
    sampled: int | None


@dc.dataclass(frozen=True)
class HeldTyping:
    """A column the margin typed otherwise than its sample alone would: the type the earlier profile gave it, the type
    its sample alone gives, the type it takes, each None for untyped, and the candidate whose share decided it, with the
    values that candidate passed, the values sampled, and the cut and margin it was judged at."""

    earlier: str | None
    alone: str | None
    held: str | None
    decided_by: str
    passed: int
    sampled: int
    cut: float
    margin: float

    def message(self, column: str) -> str:
        return (
            f'{column}: the earlier profile {typed(self.earlier)}, its sample alone {typed(self.alone)}, and the margin '
            f'{typed(self.held)}. {self.passed / self.sampled:.2%} of its {self.sampled:,} sampled values pass '
            f'{self.decided_by}, within {self.margin:.2%} of the {self.cut:.0%} cut'
        )


def typed(semantic_type: str | None) -> str:
    return f'types it {semantic_type}' if semantic_type else 'leaves it untyped'
