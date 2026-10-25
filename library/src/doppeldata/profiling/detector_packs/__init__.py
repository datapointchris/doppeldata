"""Detector packs: the detectors a pack holds, the packs the library ships, and the entry points that add others.

A third party adds semantic types without forking by registering a `DetectorPack` under the
`doppeldata.detector_packs` entry-point group. The entry point's name is the pack's name, which a run enables it by:

    [project.entry-points."doppeldata.detector_packs"]
    acme = "acme_detectors:PACK"

A pack is code the profiler imports and runs, and its validators are handed the sampled values of the columns it may
type. So enable only a pack trusted with the data. The profile holds none of those values.
"""

import dataclasses as dc
import importlib
import importlib.metadata
import re
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping
from typing import TYPE_CHECKING
from typing import Any

from doppeldata import _core
from doppeldata.errors import DetectorPackError
from doppeldata.published_schemas import published_enum
from doppeldata.published_schemas import published_fields

if TYPE_CHECKING:
    import pyarrow as pa

ENTRY_POINT_GROUP = 'doppeldata.detector_packs'
# The pack every run enables.
GENERAL = 'general'
# Each pack the library ships, as an entry point's `module:attribute`, so an install without package metadata still finds them.
BUILT_IN = {GENERAL: 'doppeldata.profiling.detector_packs.general:PACK'}
# A pack's name and a semantic type are lowercase snake identifiers, which stay stable when Glue renames a type.
IDENTIFIER = re.compile(r'[a-z][a-z0-9_]*')
# An upper-case run before a capitalized word, a capitalized or lower-case word, or a run of digits.
NAME_WORD = re.compile(r'[A-Z]+(?![a-z])|[A-Z]?[a-z]+|\d+')

Validator = Callable[['pa.Array'], 'pa.BooleanArray']


@dc.dataclass(frozen=True)
class Detector:
    """One semantic type: the values it accepts, the column names that hint it, and what the generator draws for it.

    `type` is the semantic type a column is given, a lowercase snake identifier such as `email`. `sensitivity` is a
    value of the profile format's `Sensitivity`, which the privacy policy decides from. `name_hints` are words a
    column's name holds where it hints the type, in any case convention: `e_mail` hints `customerEMail`.

    `generator_hint` is what the generator draws, such as `{'provider': 'email'}` or a `pattern`. Where it names a
    `check_digit`, the profiler tests each value's check digit, so `validate` need not.

    `validate` says whether each value of a string array holding no nulls is of the type, and `method` is how it
    decides, as `semantic.detectors` names it: `pattern` for a regular expression alone, otherwise `validator`.
    With `hint_required`, only a column whose name hints the type takes it, and the validator confirms the hint.
    A type with no validator is assigned by its name hints alone.

    `glue_type` is the AWS Glue sensitive-data managed type, such as `EMAIL`. The profile does not write it.
    """

    type: str
    sensitivity: str
    name_hints: tuple[str, ...]
    generator_hint: Mapping[str, str]
    validate: Validator | None = None
    method: str = 'validator'
    hint_required: bool = False
    glue_type: str | None = None


@dc.dataclass(frozen=True)
class DetectorPack:
    """A named set of detectors, such as `general` or a domain's own."""

    name: str
    detectors: tuple[Detector, ...]


@dc.dataclass(frozen=True)
class EnabledPack:
    """A pack a run enabled, and the distribution and version that provide it, as `generated.detector_packs` records it."""

    pack: DetectorPack
    provided_by: str


def name_words(name: str) -> tuple[str, ...]:
    """A column name's or a name hint's words, lower-cased: `customerEMail` and `customer_e_mail` are each customer, e, mail."""
    return tuple(word.lower() for word in NAME_WORD.findall(name))


def enabled_packs(names: Iterable[str]) -> list[EnabledPack]:
    """The general pack, then each other pack named, by name, so the order a run names them in changes nothing.

    A pack the library ships is found first. Any other is the entry point of that name in `ENTRY_POINT_GROUP`. Two
    packs may not declare one type, because the privacy policy reads a type's sensitivity from one pack.
    """
    requested = sorted(set(names) - {GENERAL})
    enabled = [load(name) for name in (GENERAL, *requested)]
    owners: dict[str, str] = {}
    for each in enabled:
        for kind in dict.fromkeys(detector.type for detector in each.pack.detectors):
            if kind in owners:
                raise DetectorPackError(f'the detector packs {owners[kind]!r} and {each.pack.name!r} both declare the type {kind}')
            owners[kind] = each.pack.name
    return enabled


def load(name: str) -> EnabledPack:
    if name in BUILT_IN:
        module, attribute = BUILT_IN[name].split(':')
        return EnabledPack(checked(name, getattr(importlib.import_module(module), attribute)), f'doppeldata {_core.VERSION}')
    found = list(importlib.metadata.entry_points(group=ENTRY_POINT_GROUP, name=name))
    if not found:
        installed = sorted({*BUILT_IN, *(entry.name for entry in importlib.metadata.entry_points(group=ENTRY_POINT_GROUP))})
        raise DetectorPackError(
            f'no detector pack named {name!r} is installed; installed packs: {", ".join(installed)}. '
            f'A pack registers under the {ENTRY_POINT_GROUP!r} entry-point group',
            exit_code=2,
        )
    if len(found) > 1:
        sources = ', '.join(f'{entry.value} from {provider(entry)}' for entry in found)
        raise DetectorPackError(f'more than one installed distribution registers a detector pack named {name!r}: {sources}')
    entry = found[0]
    try:
        pack = entry.load()
    except Exception as error:
        raise DetectorPackError(f'the detector pack {name!r} ({entry.value} from {provider(entry)}) failed to load: {error}') from error
    return EnabledPack(checked(name, pack), provider(entry))


def provider(entry: importlib.metadata.EntryPoint) -> str:
    return f'{entry.dist.name} {entry.dist.version}' if entry.dist else entry.module


def checked(name: str, pack: Any) -> DetectorPack:
    """The pack, once every field a profile writes from it is one the profile format accepts and the generator draws.

    A third party's mistake is then named at the pack that made it, rather than as the written profile failing its
    schema, or as generation failing wherever it runs.
    """

    def refuse(problem: str) -> DetectorPackError:
        return DetectorPackError(f'the detector pack {name!r} {problem}')

    if not isinstance(pack, DetectorPack):
        raise refuse(f'is a {type(pack).__name__}, not a doppeldata DetectorPack')
    if pack.name != name:
        raise refuse(f'is registered as {name!r} but names itself {pack.name!r}')
    if not IDENTIFIER.fullmatch(name):
        raise refuse('is not named by a lowercase snake identifier')
    if not isinstance(pack.detectors, tuple | list):
        raise refuse(f'holds its detectors in a {type(pack.detectors).__name__}, not a tuple')
    sensitivities, methods, hint_fields = published_enum('Sensitivity'), published_enum('Detector'), published_fields('GeneratorHint')
    first_of_type: dict[str, Detector] = {}
    for detector in pack.detectors:
        if not isinstance(detector, Detector):
            raise refuse(f'holds a {type(detector).__name__}, not a doppeldata Detector')
        kind = detector.type
        if not isinstance(kind, str) or not IDENTIFIER.fullmatch(kind):
            raise refuse(f'has a detector whose type {kind!r} is not a lowercase snake identifier')
        if detector.sensitivity not in sensitivities:
            raise refuse(f'gives {kind} the sensitivity {detector.sensitivity!r}; choose from {", ".join(sensitivities)}')
        if detector.method not in methods:
            raise refuse(f'gives {kind} the method {detector.method!r}; choose from {", ".join(methods)}')
        if detector.validate is not None and not callable(detector.validate):
            raise refuse(f'gives {kind} a validator that is a {type(detector.validate).__name__}, not a callable')
        if not isinstance(detector.name_hints, tuple | list) or not all(
            isinstance(hint, str) and name_words(hint) for hint in detector.name_hints
        ):
            raise refuse(f'gives {kind} the name hints {detector.name_hints!r}; each is a string holding a word')
        if (detector.validate is None or detector.hint_required) and not detector.name_hints:
            raise refuse(f'gives {kind} no name hint, and only a name hint can assign it')
        for key, value in detector.generator_hint.items():
            if key not in hint_fields:
                raise refuse(f'gives {kind} the generator hint {key!r}; a hint takes {", ".join(map(repr, hint_fields))}')
            allowed = hint_fields[key]
            if not isinstance(value, str) or (allowed is not None and value not in allowed):
                raise refuse(f'hints {kind} with the {key} {value!r}, which the generator does not draw')
            problem = _core.pattern_problem(value) if key == 'pattern' else None
            if problem:
                raise refuse(f'hints {kind} with a pattern the generator cannot draw from: {problem}')
        first = first_of_type.setdefault(kind, detector)
        if (first.sensitivity, first.glue_type) != (detector.sensitivity, detector.glue_type):
            raise refuse(f'gives {kind} two sensitivities or Glue types; the detectors of one type agree on both')
    return pack
