"""Profile a dataset into a reviewable file holding no rows, then generate synthetic data from it."""

import importlib
from types import ModuleType

from doppeldata.dataset_generation import ColumnSampler
from doppeldata.dataset_generation import PlannedRange
from doppeldata.dataset_generation import WritePlan
from doppeldata.dataset_generation import generate
from doppeldata.dataset_generation import plan
from doppeldata.dataset_generation import stream
from doppeldata.dataset_generation import write
from doppeldata.errors import ColumnsNotFrozen
from doppeldata.errors import DetectorPackError
from doppeldata.errors import DoppeldataError
from doppeldata.errors import FitsSkipped
from doppeldata.errors import PoorFits
from doppeldata.errors import ProfilingEnginesMissing
from doppeldata.errors import ProfilingWarning
from doppeldata.errors import SourceError
from doppeldata.errors import StoppedWrite
from doppeldata.errors import WriteFailed
from doppeldata.errors import WriteInterrupted
from doppeldata.manifest import Manifest
from doppeldata.profile_format import Bundle
from doppeldata.profile_format import InvalidProfile
from doppeldata.profile_format import Issue
from doppeldata.profile_format import Profile
from doppeldata.profile_format import UngenerableProfile
from doppeldata.profiling import profile

__all__ = [
    'Bundle',
    'ColumnSampler',
    'ColumnsNotFrozen',
    'DetectorPackError',
    'DoppeldataError',
    'FitsSkipped',
    'InvalidProfile',
    'Issue',
    'Manifest',
    'PlannedRange',
    'PoorFits',
    'Profile',
    'ProfilingEnginesMissing',
    'ProfilingWarning',
    'SourceError',
    'StoppedWrite',
    'UngenerableProfile',
    'WriteFailed',
    'WriteInterrupted',
    'WritePlan',
    'generate',
    'plan',
    'profile',
    'stream',
    'write',
]


def __getattr__(name: str) -> ModuleType:
    """`doppeldata.spark` on first use, so importing doppeldata never imports pyspark."""
    if name == 'spark':
        return importlib.import_module('doppeldata.spark')
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
