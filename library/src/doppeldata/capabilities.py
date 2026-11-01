"""Everything a program driving doppeldata can name, read from the registries the library runs on.

The command surface comes from the caller, because only a command can walk its own tree. Every other list comes from the
module that refuses a value outside it, so an op, format, fake or code added without docs still appears.
"""

import importlib.metadata
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any

from doppeldata import _core
from doppeldata.issue_codes import registered
from doppeldata.privacy_policy import APPLIED_POLICIES
from doppeldata.privacy_policy import DEFAULT_K_MIN
from doppeldata.profile_comparison.metrics import METRICS
from doppeldata.profile_exports import EXPORT_FORMATS
from doppeldata.profiling import ENGINES
from doppeldata.profiling.detector_packs import BUILT_IN
from doppeldata.profiling.detector_packs import ENTRY_POINT_GROUP
from doppeldata.profiling.detector_packs import load
from doppeldata.published_schemas import output_formats
from doppeldata.published_schemas import partition_styles
from doppeldata.published_schemas import schema_summaries
from doppeldata.scenario_format import expected_metrics
from doppeldata.scenario_format import fault_kinds
from doppeldata.scenario_format import op_schemas
from doppeldata.short_profiles import providers

# A key removed or a meaning changed raises this. A key added does not.
SCHEMA_VERSION = 1
EXIT_CODES = {
    '0': 'it ran, and any check it ran passed',
    '1': 'the work was attempted and failed: a write stopped, the data changed while it was read, or two profiles drifted',
    '2': 'the call must change, and nothing was written',
    '3': 'a check found what a person must act on',
    '4': 'a store or service never answered',
}


def capabilities(commands: Sequence[Mapping[str, Any]] = ()) -> dict[str, Any]:
    """The document `doppeldata capabilities show --json` prints, around the `commands` the caller walked from its own tree.

    Listing the semantic types loads every detector pack installed, and the packs import pyarrow.
    """
    formats = output_formats()
    return {
        'schema_version': SCHEMA_VERSION,
        'tool': 'doppeldata',
        'version': _core.VERSION,
        'stream_version': _core.STREAM_VERSION,
        'commands': [dict(command) for command in commands],
        'exit_codes': EXIT_CODES,
        'formats': formats,
        'partition_styles': partition_styles(),
        'export_formats': list(EXPORT_FORMATS),
        'engines': [
            {
                'engine': name,
                'reads': sorted(source.value for source in engine.reads),
                'packages': [package.distribution for package in engine.packages],
            }
            for name, engine in ENGINES.items()
        ],
        'families': [{'family': name, 'discrete': discrete} for name, discrete in _core.fitted_families().items()],
        'fakes': providers(),
        'semantic_types': semantic_types(),
        'ops': [
            {'op': op, 'formats': _core.op_formats(op), 'fields': [field for field in schema['properties'] if field != 'op']}
            for op, schema in op_schemas().items()
        ],
        'fault_kinds': [fault_kind(kind, formats) for kind in fault_kinds()],
        'metrics': [{'metric': metric.name, 'summary': metric.summary, 'threshold': metric.threshold} for metric in METRICS],
        'expected_metrics': expected_metrics(),
        'policies': [policy.value for policy in APPLIED_POLICIES],
        'default_k_min': DEFAULT_K_MIN,
        'schemas': [summary.name for summary in schema_summaries()],
        'issue_codes': [code.to_json() for code in registered()],
    }


def semantic_types() -> list[dict[str, Any]]:
    """Each type a detector pack installed declares, once each, with the pack declaring it."""
    installed = sorted({*BUILT_IN, *(entry.name for entry in importlib.metadata.entry_points(group=ENTRY_POINT_GROUP))})
    types: dict[str, dict[str, Any]] = {}
    for name in installed:
        for detector in load(name).pack.detectors:
            types.setdefault(
                detector.type,
                {
                    'type': detector.type,
                    'pack': name,
                    'sensitivity': detector.sensitivity,
                    'generator_hint': dict(detector.generator_hint),
                    'code_list': detector.code_list,
                },
            )
    return list(types.values())


def fault_kind(kind: str, formats: list[str]) -> dict[str, Any]:
    facts = {format: _core.fault_kind_facts(kind, format) for format in formats}
    return {
        'kind': kind,
        'breaks': 'file' if next(iter(facts.values()))[0] else 'row',
        'formats': [format for format, (_, held, _) in facts.items() if held],
        'read_back': [format for format, (_, _, read_back) in facts.items() if read_back],
    }
