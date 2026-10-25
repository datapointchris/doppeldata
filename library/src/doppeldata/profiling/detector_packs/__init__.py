"""Detector packs: the protocol a pack implements, the packs the library ships, and the entry points that add others.

A third party adds semantic types without forking by registering a pack under the `doppeldata.detector_packs`
entry-point group. The entry point's name is the pack's name, which a run enables it by, and it points at an
object implementing `DetectorPack`:

    [project.entry-points."doppeldata.detector_packs"]
    acme = "acme_detectors:PACK"
"""

import dataclasses as dc
import importlib
import importlib.metadata
import re
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
from typing import TYPE_CHECKING
from typing import Any
from typing import Literal
from typing import Protocol

from doppeldata import _core
from doppeldata.errors import DetectorPackError
from doppeldata.published_schemas import published_enum

if TYPE_CHECKING:
    import pyarrow as pa

ENTRY_POINT_GROUP = 'doppeldata.detector_packs'
# The pack every run enables.
GENERAL = 'general'
# Each pack the library ships, as an entry point's `module:attribute`, so an install without package metadata still finds them.
BUILT_IN = {GENERAL: 'doppeldata.profiling.detector_packs.general:PACK'}
# A pack's name and a semantic type are lowercase snake identifiers, which stay stable when Glue renames a type.
IDENTIFIER = re.compile(r'[a-z][a-z0-9_]*')
# The generator hint's keys that name one of the profile format's enums. `pattern` takes any regular expression.
HINT_ENUMS = {'provider': 'Provider', 'check_digit': 'CheckDigit'}

Sensitivity = Literal['identifier', 'quasi_identifier', 'sensitive', 'none']


class Detector(Protocol):
    """One semantic type: the values it accepts, the column names that hint it, and what the generator draws for it."""

    @property
    def type(self) -> str:
        """The semantic type a column is given, such as `email`."""
        ...

    @property
    def sensitivity(self) -> Sensitivity:
        """How much a column of this type says about a person, which the privacy policy decides from.

        A class attribute is annotated as `Sensitivity`, because a bare string is inferred as `str`.
        """
        ...

    @property
    def glue_type(self) -> str | None:
        """The AWS Glue sensitive-data managed type, such as `EMAIL`, or None where Glue has none."""
        ...

    @property
    def name_hints(self) -> tuple[str, ...]:
        """Column-name words that hint the type, in snake case: `e_mail` hints a column named `customerEMail`."""
        ...

    @property
    def generator_hint(self) -> Mapping[str, str]:
        """What the generator draws: `{'provider': 'email'}`, or `{'pattern': '…', 'check_digit': 'luhn'}`."""
        ...

    @property
    def validate(self) -> 'Callable[[pa.Array], pa.BooleanArray] | None':
        """Whether each value is of the type, given a string array holding no nulls.

        None where the type has no validator, such as a person's name, so only a name hint assigns it.
        """
        ...


class DetectorPack(Protocol):
    """A named set of detectors, such as `general` or a domain's own."""

    @property
    def name(self) -> str: ...

    @property
    def detectors(self) -> Sequence[Detector]: ...


@dc.dataclass(frozen=True)
class EnabledPack:
    """A pack a run enabled, and the distribution and version that provide it, as `generated.detector_packs` records it."""

    pack: DetectorPack
    provided_by: str


def enabled_packs(names: Iterable[str]) -> list[EnabledPack]:
    """The general pack, then each other pack named, by name, so the order a run names them in changes nothing.

    A pack the library ships is found first. Any other is the entry point of that name in `ENTRY_POINT_GROUP`.
    """
    requested = sorted(set(names) - {GENERAL})
    return [load(name) for name in (GENERAL, *requested)]


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
    """The pack, once every field a profile writes from it is one the profile format accepts.

    A third party's mistake is then named at the pack that made it, rather than as the written profile failing its schema.
    """

    def refuse(problem: str) -> DetectorPackError:
        return DetectorPackError(f'the detector pack {name!r} {problem}')

    if getattr(pack, 'name', None) != name:
        raise refuse(f'is registered as {name!r} but names itself {getattr(pack, "name", None)!r}')
    sensitivities = set(published_enum('Sensitivity'))
    drawn = {key: set(published_enum(definition)) for key, definition in HINT_ENUMS.items()}
    for detector in pack.detectors:
        kind = getattr(detector, 'type', None)
        if not isinstance(kind, str) or not IDENTIFIER.fullmatch(kind):
            raise refuse(f'has a detector whose type {kind!r} is not a lowercase snake identifier')
        if detector.sensitivity not in sensitivities:
            raise refuse(f'gives {kind} the sensitivity {detector.sensitivity!r}; choose from {", ".join(sorted(sensitivities))}')
        for key, value in detector.generator_hint.items():
            if key not in (*HINT_ENUMS, 'pattern'):
                raise refuse(f"gives {kind} the generator hint {key!r}; a hint takes 'provider', 'pattern' and 'check_digit'")
            if key in drawn and value not in drawn[key]:
                raise refuse(f'hints {kind} with the {key} {value!r}, which the generator does not draw')
        if detector.validate is None and not detector.name_hints:
            raise refuse(f'gives {kind} neither a validator nor a name hint, so nothing can assign it')
    return pack
