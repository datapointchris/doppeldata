"""The expressions a profiler tries between numeric columns, and the one rule every engine counts their rows by.

A candidate is `left operator right` giving `target`, the operator one of `+ - * /`. Each engine counts three sets of
rows: those holding all three columns, the informative ones among them, and those of the latter where the target lies
within the candidate's tolerance of the result. A row is not informative where the target is zero and the result lies
within the tolerance of zero, since an expression of two mostly zero columns holds there whatever its operator. A
division by zero holds for no row. Each engine spells the few operations below its own way, and the rule is written
once over them.
"""

import dataclasses as dc
import functools
import re
from collections.abc import Callable
from typing import Any
from typing import Protocol

OPERATORS = ('+', '-', '*', '/')
UNQUOTED_NAME = re.compile(r'[A-Za-z_][A-Za-z0-9_]*')


@dc.dataclass(frozen=True)
class ExpressionCandidate:
    """`left operator right` as it may give `target`, whose values are rounded to `scale` decimal places."""

    left: str
    operator: str
    right: str
    target: str
    scale: int

    @property
    def tolerance(self) -> float:
        return 0.5 * 10.0**-self.scale

    @property
    def expr(self) -> str:
        return f'{quoted(self.left)} {self.operator} {quoted(self.right)}'


class ExpressionOps(Protocol):
    """The operations an engine spells its own way, over its column values and its conditions."""

    def arithmetic(self, operator: str, left: Any, right: Any) -> Any:
        """`left operator right`, the operator one of `+ - *`."""
        ...

    def quotient(self, left: Any, right: Any) -> Any:
        """`left / right`, null where `right` is zero."""
        ...

    def held(self, value: Any) -> Any:
        """Whether the value is not null."""
        ...

    def is_zero(self, value: Any) -> Any: ...

    def within(self, value: Any, tolerance: float) -> Any:
        """Whether the value lies within `tolerance` of zero, a null value false."""
        ...

    def both(self, first: Any, second: Any) -> Any: ...

    def negated(self, condition: Any) -> Any: ...


def counted_rows(candidate: ExpressionCandidate, value: Callable[[str], Any], ops: ExpressionOps) -> tuple[Any, Any, Any]:
    """The conditions selecting the rows holding the candidate's three columns, the informative ones among them, and
    those of the latter where the target is the result, each column's values as `value` gives them."""
    left, right, target = value(candidate.left), value(candidate.right), value(candidate.target)
    if candidate.operator == '/':
        computed = ops.quotient(left, right)
    else:
        computed = ops.arithmetic(candidate.operator, left, right)
    held = functools.reduce(ops.both, (ops.held(left), ops.held(right), ops.held(target)))
    trivial = ops.both(ops.is_zero(target), ops.within(computed, candidate.tolerance))
    informative = ops.both(held, ops.negated(trivial))
    close = ops.within(ops.arithmetic('-', target, computed), candidate.tolerance)
    return held, informative, ops.both(informative, close)


class ArrowOps:
    """The operations over pyarrow arrays, which the screen on the sample counts with."""

    def arithmetic(self, operator: str, left: Any, right: Any) -> Any:
        import pyarrow.compute as pc

        return {'+': pc.add, '-': pc.subtract, '*': pc.multiply}[operator](left, right)

    def quotient(self, left: Any, right: Any) -> Any:
        import pyarrow as pa
        import pyarrow.compute as pc

        return pc.if_else(pc.not_equal(right, 0), pc.divide(left, right), pa.scalar(None, pa.float64()))

    def held(self, value: Any) -> Any:
        import pyarrow.compute as pc

        return pc.is_valid(value)

    def is_zero(self, value: Any) -> Any:
        import pyarrow.compute as pc

        return pc.equal(value, 0)

    def within(self, value: Any, tolerance: float) -> Any:
        import pyarrow.compute as pc

        return pc.fill_null(pc.less_equal(pc.abs(value), tolerance), False)

    def both(self, first: Any, second: Any) -> Any:
        import pyarrow.compute as pc

        return pc.and_kleene(first, second)

    def negated(self, condition: Any) -> Any:
        import pyarrow.compute as pc

        return pc.invert(condition)


def quoted(name: str) -> str:
    return name if UNQUOTED_NAME.fullmatch(name) else f'`{name}`'
