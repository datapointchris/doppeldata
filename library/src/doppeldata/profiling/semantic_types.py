"""Each column's semantic type, from the enabled packs' detectors run over a seeded sample of its values.

Imported where profiling runs, because a detector validates a pyarrow array.
"""

import dataclasses as dc
import re
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping

import pyarrow as pa
import pyarrow.compute as pc

from doppeldata.errors import DetectorPackError
from doppeldata.profiling.backend import ColumnSpec
from doppeldata.profiling.detector_packs import Detector
from doppeldata.profiling.detector_packs import EnabledPack
from doppeldata.profiling.statistic_catalog import ColumnKind

# A column takes a type whose validator passes this share of its sampled values.
ASSIGN_RATE = 0.95
# Or this share, where a column-name hint agrees.
HINTED_ASSIGN_RATE = 0.80
# A type with no validator is assigned by a name hint alone, and no more surely than this.
HINT_ONLY_CONFIDENCE = 0.6
# An upper-case run before a capitalized word, a capitalized or lower-case word, or a run of digits.
NAME_WORD = re.compile(r'[A-Z]+(?![a-z])|[A-Z]?[a-z]+|\d+')


@dc.dataclass(frozen=True)
class Typing:
    """The type one column was given, by which pack's detector, and on what evidence."""

    pack: str
    detector: Detector
    hinted: bool
    sampled: int
    # The sampled values the validator passed, or None where the type has no validator.
    passed: int | None

    @property
    def confidence(self) -> float:
        """The pass rate, with a name hint halving the doubt left, or `HINT_ONLY_CONFIDENCE` for a type with no validator."""
        if self.passed is None:
            return HINT_ONLY_CONFIDENCE
        rate = self.passed / self.sampled
        return 1 - (1 - rate) / 2 if self.hinted else rate

    @property
    def methods(self) -> list[str]:
        """The detection methods, as a profile's `semantic.detectors` names them."""
        validated = [] if self.passed is None else ['pattern', *(['check_digit'] if 'check_digit' in self.detector.generator_hint else [])]
        return [*validated, *(['name_hint'] if self.hinted else [])]


def detected_columns(specs: Iterable[ColumnSpec], packs: list[EnabledPack]) -> dict[str, bool]:
    """Each column detection reads, and whether only a type its name hints may be assigned to it.

    That is every string column, and each integer column whose name hints a type. An integer column is read as its
    values' decimal text, which any 5-digit count would pass as a ZIP code, so it takes only a type its name hints:
    a card number stored as an integer, or a member id.
    """
    detectors = [detector for enabled in packs for detector in enabled.pack.detectors]
    detected = {}
    for spec in specs:
        if spec.kind is ColumnKind.STRING:
            detected[spec.name] = False
        elif spec.kind is ColumnKind.INTEGER and any(hinted(name_words(spec.name), detector) for detector in detectors):
            detected[spec.name] = True
    return detected


def column_typings(sample: pa.Table, columns: Mapping[str, bool], packs: list[EnabledPack], k_min: int) -> dict[str, Typing]:
    """Each typed column's typing, of `columns` as `detected_columns` gives them.

    A column with fewer than k_min sampled values is left untyped, as is one no detector takes. The type is the one
    whose validator passes the most values, of those passing `ASSIGN_RATE`, or `HINTED_ASSIGN_RATE` where the name hints
    it. A tie goes to the hinted type, then to the pack and detector listed first. Where no validator takes the column,
    the first type with no validator that its name hints is assigned.
    """
    detectors = [(enabled.pack.name, detector) for enabled in packs for detector in enabled.pack.detectors]
    typings = {}
    for column, hint_required in columns.items():
        values = sample.column(column).drop_null().combine_chunks().cast(pa.string())
        if len(values) >= k_min:
            typing = column_typing(column, values, detectors, hint_required)
            if typing:
                typings[column] = typing
    return typings


def column_typing(column: str, values: pa.Array, detectors: list[tuple[str, Detector]], hint_required: bool) -> Typing | None:
    words = name_words(column)
    best: tuple[tuple[int, bool, int], Typing] | None = None
    hint_only = None
    for order, (pack, detector) in enumerate(detectors):
        is_hinted = hinted(words, detector)
        if hint_required and not is_hinted:
            continue
        validate = detector.validate
        if validate is None:
            if is_hinted and hint_only is None:
                hint_only = Typing(pack, detector, is_hinted, len(values), None)
            continue
        passed = passes(validate, values, f'the {detector.type} validator of the detector pack {pack!r}, on the column {column!r},')
        if passed / len(values) < (HINTED_ASSIGN_RATE if is_hinted else ASSIGN_RATE):
            continue
        rank = (passed, is_hinted, -order)
        if best is None or rank > best[0]:
            best = rank, Typing(pack, detector, is_hinted, len(values), passed)
    return best[1] if best else hint_only


def passes(validate: Callable[[pa.Array], pa.BooleanArray], values: pa.Array, where: str) -> int:
    """How many values `validate` passes. One that raises, or answers in another shape, fails the profile, naming `where` it ran."""
    try:
        passed = validate(values)
    except Exception as error:
        raise DetectorPackError(f'{where} raised {type(error).__name__}: {error}') from error
    if not isinstance(passed, pa.Array | pa.ChunkedArray) or passed.type != pa.bool_() or len(passed) != len(values):
        raise DetectorPackError(f'{where} returned {describe(passed)}, not one boolean for each of the {len(values)} values it was given')
    return pc.sum(passed.fill_null(False)).as_py() or 0


def describe(value: object) -> str:
    if isinstance(value, pa.Array | pa.ChunkedArray):
        return f'{len(value)} values of type {value.type}'
    return f'a {type(value).__name__}'


def name_words(name: str) -> tuple[str, ...]:
    """The column name's words, lower-cased. `customerEMail`, `Customer-E-Mail` and `customer_e_mail` are each customer, e, mail."""
    return tuple(word.lower() for word in NAME_WORD.findall(name))


def hinted(words: tuple[str, ...], detector: Detector) -> bool:
    """Whether the words of one of the detector's snake-case name hints run in order somewhere in the column name's words."""
    for hint in detector.name_hints:
        parts = tuple(hint.split('_'))
        if any(words[start : start + len(parts)] == parts for start in range(len(words) - len(parts) + 1)):
            return True
    return False
