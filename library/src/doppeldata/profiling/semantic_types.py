"""Each column's semantic type, from the enabled packs' detectors run over a seeded sample of its values.

Imported where profiling runs, because a detector validates a pyarrow array.
"""

import dataclasses as dc
import functools
import logging
import math
from collections.abc import Iterable
from collections.abc import Mapping
from typing import cast

import pyarrow as pa
import pyarrow.compute as pc

from doppeldata import _core
from doppeldata.column_name_words import hint_runs_in
from doppeldata.column_name_words import name_words
from doppeldata.errors import DetectorPackError
from doppeldata.issue_codes import DETECTOR_PACK_BROKEN
from doppeldata.profiling.backend import ColumnSpec
from doppeldata.profiling.detector_packs import Detector
from doppeldata.profiling.detector_packs import EnabledPack
from doppeldata.profiling.detector_packs import ListValidator
from doppeldata.profiling.detector_packs import SuppliedCodeList
from doppeldata.profiling.detector_packs import Validator
from doppeldata.profiling.semantic_type_hold import FrozenTyping
from doppeldata.profiling.semantic_type_hold import HeldTyping
from doppeldata.profiling.statistic_catalog import ColumnKind

log = logging.getLogger(__name__)

# A column takes a type whose validator passes this share of its sampled values.
ASSIGN_RATE = 0.95
# Or this share, where a column-name hint agrees.
HINTED_ASSIGN_RATE = 0.80
# A check's profile of DATA changes PROFILE's typing only where a share clears its cut by this many standard errors of
# the gap between two samples' shares.
HELD_TYPING_STANDARD_ERRORS = 3.0
# A type its name hint assigns, with no validator or one only confirming the hint, is given this confidence.
HINT_ASSIGNED_CONFIDENCE = 0.6
VALIDATOR = 'validator'


@dc.dataclass(frozen=True)
class Candidate:
    """A type a column may take: the pack's detector, and whether the column's name hints it."""

    pack: str
    detector: Detector
    hinted: bool


@dc.dataclass(frozen=True)
class DetectedColumn:
    """A column detection reads: its kind, and the types it may take, in the packs' order."""

    kind: ColumnKind
    candidates: list[Candidate]


@dc.dataclass(frozen=True)
class Typing:
    """The type one column was given, by which pack's detector, on what evidence, and what the generator is told to draw."""

    candidate: Candidate
    sampled: int
    # The sampled values the validator and any check digit passed, or None where the type has no validator.
    passed: int | None
    generator_hint: dict[str, str]
    # Whether the validator read a code list the caller supplied, which is a lookup beside its own method.
    listed: bool = False

    @property
    def confidence(self) -> float:
        """The pass rate, with a name hint halving the doubt left, or `HINT_ASSIGNED_CONFIDENCE` for a type its name hint assigns."""
        if self.passed is None or self.candidate.detector.hint_required:
            return HINT_ASSIGNED_CONFIDENCE
        rate = self.passed / self.sampled
        return 1 - (1 - rate) / 2 if self.candidate.hinted else rate

    @property
    def methods(self) -> list[str]:
        """The detection methods, as a profile's `semantic.detectors` names them."""
        detector = self.candidate.detector
        looked_up = [VALIDATOR] if self.listed and detector.method != VALIDATOR else []
        check_digit = ['check_digit'] if 'check_digit' in detector.generator_hint else []
        tested = [] if self.passed is None else [detector.method, *looked_up, *check_digit]
        return [*tested, *(['name_hint'] if self.candidate.hinted else [])]


def detected_columns(specs: Iterable[ColumnSpec], packs: list[EnabledPack]) -> dict[str, DetectedColumn]:
    """Each column detection reads, with the types `fits` lets it take. A column no type fits is left out."""
    detectors = [(enabled.pack.name, detector) for enabled in packs for detector in enabled.pack.detectors]
    detected = {}
    for spec in specs:
        words = name_words(spec.name)
        candidates = [
            Candidate(pack, detector, hinted(words, detector)) for pack, detector in detectors if fits(spec.kind, detector, words)
        ]
        if candidates:
            detected[spec.name] = DetectedColumn(spec.kind, candidates)
    return detected


def fits(kind: ColumnKind, detector: Detector, words: tuple[str, ...]) -> bool:
    """Whether a column of `kind` whose name has `words` may take the detector's type.

    An integer column is read as its values' decimal text, which any 5-digit count would pass as a ZIP code, so it
    takes only a type its name hints and a validator confirms. A type its name hint assigns, having no validator or
    one that requires the hint, needs the hint to end the name. A word after it, such as id, hash or length, says the
    column holds something about the type rather than the type.
    """
    assigned_by_hint = detector.validate is None or detector.hint_required
    if kind is ColumnKind.STRING:
        return not assigned_by_hint or hinted(words, detector, at_end=True)
    if kind is ColumnKind.INTEGER and detector.validate is not None:
        return hinted(words, detector, at_end=assigned_by_hint)
    return False


def column_typings(
    sample: pa.Table,
    columns: Mapping[str, DetectedColumn],
    patterns: Mapping[str, str],
    k_min: int,
    code_lists: Mapping[str, SuppliedCodeList],
    earlier: Mapping[str, FrozenTyping],
) -> tuple[dict[str, Typing], dict[str, HeldTyping]]:
    """Each typed column's typing, of `columns` as `detected_columns` gives them, beside the patterns grex stores for
    them, and each column `earlier` held to another typing than its sample alone gives.

    A column with fewer than k_min sampled values is left untyped, as is one no candidate takes. The type is the one
    whose validator passes the most values, of those passing `plain_cut`. A tie goes to the hinted type, then to the
    pack and detector listed first. Where no validator takes the column, the first type with no validator is assigned.
    A validator reading a code list is handed the one in `code_lists`. A column `earlier` names keeps the earlier
    profile's typing unless a share clears its cut by `held_margin`.
    """
    typings, held = {}, {}
    for column, detected in columns.items():
        values = sample.column(column).drop_null().combine_chunks().cast(pa.string())
        if len(values) < k_min:
            continue
        assigned, holding = assigned_candidate(column, values, detected.candidates, code_lists, earlier.get(column))
        if holding:
            held[column] = holding
            log.info(holding.message(column))
        if assigned:
            candidate, passed = assigned
            hint = written_hint(candidate, detected.kind, values, patterns.get(column))
            listed = passed is not None and candidate.detector.code_list in code_lists
            typings[column] = Typing(candidate, len(values), passed, hint, listed)
    return typings, held


def assigned_candidate(
    column: str,
    values: pa.Array,
    candidates: list[Candidate],
    code_lists: Mapping[str, SuppliedCodeList],
    earlier: FrozenTyping | None,
) -> tuple[tuple[Candidate, int | None] | None, HeldTyping | None]:
    """The candidate the column takes, and how many values it passed, or None where it has no validator. Beside it, where
    `earlier`, an earlier profile's typing of the column, moved a cut far enough to change the outcome, how it did.
    """
    tested: list[tuple[Candidate, int]] = []
    hint_assigned = None
    for candidate in candidates:
        detector = candidate.detector
        if detector.validate is None:
            hint_assigned = hint_assigned or candidate
            continue
        where = f'the {detector.type} validator of the detector pack {candidate.pack!r}, on the column {column!r},'
        tested.append((candidate, passes(bound(detector, code_lists), detector.generator_hint.get('check_digit'), values, where)))
    by_hint: tuple[Candidate, int | None] | None = (hint_assigned, None) if hint_assigned else None
    validated = most_passed(tested, len(values), earlier)
    best = validated or by_hint
    if earlier is None:
        return best, None
    validated_alone = most_passed(tested, len(values), None)
    alone = validated_alone or by_hint
    if typed_as(alone) == typed_as(best):
        return best, None
    # The margin turned one candidate away or let one stand: the type its sample alone gives, or else the one it kept.
    denied = validated_alone is not None and typed_as(validated_alone) != earlier.type
    decided, passed = cast(tuple[Candidate, int], validated_alone if denied else validated)
    cut = plain_cut(decided)
    holding = HeldTyping(
        earlier.type,
        typed_as(alone),
        typed_as(best),
        decided.detector.type,
        passed,
        len(values),
        cut,
        held_margin(cut, len(values), earlier),
    )
    return best, holding


def most_passed(tested: list[tuple[Candidate, int]], sampled: int, earlier: FrozenTyping | None) -> tuple[Candidate, int] | None:
    """Of the `tested` candidates passing the share `assign_rate` asks of `sampled` values, the one passing the most, and
    how many it passed. A tie goes to the hinted type, then to the one listed first."""
    ranked = [
        ((passed, candidate.hinted, -order), candidate, passed)
        for order, (candidate, passed) in enumerate(tested)
        if passed / sampled >= assign_rate(candidate, sampled, earlier)
    ]
    if not ranked:
        return None
    _, candidate, passed = max(ranked, key=lambda entry: entry[0])
    return candidate, passed


def plain_cut(candidate: Candidate) -> float:
    """The share of the sampled values a candidate's validator must pass to type a column: `ASSIGN_RATE`, or
    `HINTED_ASSIGN_RATE` where the column's name hints the type."""
    return HINTED_ASSIGN_RATE if candidate.hinted else ASSIGN_RATE


def assign_rate(candidate: Candidate, sampled: int, earlier: FrozenTyping | None) -> float:
    """The share of `sampled` values the candidate's validator must pass to type the column.

    That is `plain_cut`. Against an earlier profile's typing, the cut falls by `held_margin` for the type it gave and
    rises by as much for every other type. So a share sampling alone could carry across the cut leaves the earlier
    typing standing.
    """
    cut = plain_cut(candidate)
    if earlier is None:
        return cut
    margin = held_margin(cut, sampled, earlier)
    return cut - margin if candidate.detector.type == earlier.type else cut + margin


def held_margin(cut: float, sampled: int, earlier: FrozenTyping) -> float:
    """`HELD_TYPING_STANDARD_ERRORS` standard errors of the gap between two samples' shares at `cut`, over `sampled`
    values and the earlier profile's.

    The earlier profile states only which side of the cut its share fell, and a share at the cut is the one sampling
    carries across it most easily, so the gap is taken there. An earlier profile stating no rows adds no term, because
    its type is a parameter rather than a measurement.
    """
    inverse = sum(1 / count for count in (earlier.sampled, sampled) if count)
    return HELD_TYPING_STANDARD_ERRORS * math.sqrt(cut * (1 - cut) * inverse)


def typed_as(assigned: tuple[Candidate, int | None] | None) -> str | None:
    return assigned[0].detector.type if assigned else None


def bound(detector: Detector, code_lists: Mapping[str, SuppliedCodeList]) -> Validator:
    """The detector's validator, handed the codes of the list it reads where it names one, or None where none was supplied."""
    if detector.code_list is None:
        return cast(Validator, detector.validate)
    supplied = code_lists.get(detector.code_list)
    codes = code_array(supplied) if supplied else None
    listed = cast(ListValidator, detector.validate)
    return lambda values: listed(values, codes)


@functools.cache
def code_array(supplied: SuppliedCodeList) -> pa.Array:
    return pa.array(supplied.codes, pa.string())


def written_hint(candidate: Candidate, kind: ColumnKind, values: pa.Array, stored_pattern: str | None) -> dict[str, str]:
    """The type's generator hint, less what the engine would not draw in the column's own format.

    The engine ignores a hint on a column that is not a string. A hint's provider or pattern takes the place of the
    pattern grex stores, so where one is stored the provider is dropped, and the pattern is kept only where the
    column's values match it. A check digit applies to whatever draws the value.
    """
    if kind is not ColumnKind.STRING:
        return {}
    hint = dict(candidate.detector.generator_hint)
    if stored_pattern is not None:
        hint.pop('provider', None)
        if 'pattern' in hint and matching_share(values, hint['pattern']) < plain_cut(candidate):
            del hint['pattern']
    return hint


def matching_share(values: pa.Array, pattern: str) -> float:
    """The share of `values` the whole of `pattern` matches, or 0 where RE2 does not take the pattern."""
    try:
        matched = pc.match_substring_regex(values, f'^(?:{pattern})$')
    except pa.ArrowInvalid:
        return 0.0
    return (pc.sum(matched).as_py() or 0) / len(values)


def passes(validate: Validator, check_digit: str | None, values: pa.Array, where: str) -> int:
    """How many values `validate` passes whose `check_digit`, where one is named, holds.

    A validator that raises, or answers in another shape, fails the profile, naming `where` it ran.
    """
    try:
        passed = validate(values)
    except Exception as error:
        raise DetectorPackError(DETECTOR_PACK_BROKEN, f'{where} raised {type(error).__name__}: {error}') from error
    if not isinstance(passed, pa.Array | pa.ChunkedArray) or passed.type != pa.bool_() or len(passed) != len(values):
        message = f'{where} returned {describe(passed)}, not one boolean for each of the {len(values)} values it was given'
        raise DetectorPackError(DETECTOR_PACK_BROKEN, message)
    accepted = values.filter(passed.fill_null(False))
    if check_digit:
        return sum(_core.check_digits_hold(check_digit, accepted.to_pylist()))
    return len(accepted)


def describe(value: object) -> str:
    if isinstance(value, pa.Array | pa.ChunkedArray):
        return f'{len(value)} values of type {value.type}'
    return f'a {type(value).__name__}'


def hinted(words: tuple[str, ...], detector: Detector, *, at_end: bool = False) -> bool:
    """Whether the words of one of the detector's name hints run in order in the column name's words, ending them where `at_end`."""
    return any(hint_runs_in(words, hint, at_end=at_end) for hint in detector.name_hints)
