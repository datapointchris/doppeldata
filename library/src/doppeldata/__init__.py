"""Profile a dataset into a reviewable file holding no rows, then generate synthetic data from it."""

import importlib
from types import ModuleType
from typing import TYPE_CHECKING

from doppeldata import fakes
from doppeldata.dataset_generation import ColumnSampler
from doppeldata.dataset_generation import PlannedRange
from doppeldata.dataset_generation import WritePlan
from doppeldata.dataset_generation import generate
from doppeldata.dataset_generation import plan
from doppeldata.dataset_generation import stream
from doppeldata.dataset_generation import write
from doppeldata.errors import CheckFailed
from doppeldata.errors import ColumnsNotFrozen
from doppeldata.errors import DetectorPackError
from doppeldata.errors import DoppeldataError
from doppeldata.errors import EarlierProfileNotReused
from doppeldata.errors import FitsSkipped
from doppeldata.errors import InvalidCodeList
from doppeldata.errors import InvalidThresholds
from doppeldata.errors import PackagesMissing
from doppeldata.errors import PoorFits
from doppeldata.errors import ProfilesDrifted
from doppeldata.errors import ProfilingEnginesMissing
from doppeldata.errors import ProfilingWarning
from doppeldata.errors import SourceError
from doppeldata.errors import StoppedWrite
from doppeldata.errors import WriteFailed
from doppeldata.errors import WriteInterrupted
from doppeldata.inline_columns import choice
from doppeldata.inline_columns import dates
from doppeldata.inline_columns import decimals
from doppeldata.inline_columns import integers
from doppeldata.manifest import Manifest
from doppeldata.profile_comparison import check
from doppeldata.profile_comparison import diff
from doppeldata.profile_comparison.reports import CheckReport
from doppeldata.profile_comparison.reports import DiffReport
from doppeldata.profile_comparison.reports import ExpectationResult
from doppeldata.profile_comparison.thresholds import Thresholds
from doppeldata.profile_format import Bundle
from doppeldata.profile_format import InvalidProfile
from doppeldata.profile_format import Issue
from doppeldata.profile_format import Profile
from doppeldata.profile_format import UngenerableProfile
from doppeldata.profiling import profile
from doppeldata.profiling import profile_tables
from doppeldata.scenario_format import InvalidScenario
from doppeldata.scenario_format import Scenario

__all__ = [
    'Bundle',
    'CheckFailed',
    'CheckReport',
    'ColumnSampler',
    'ColumnsNotFrozen',
    'DetectorPackError',
    'DiffReport',
    'DoppeldataError',
    'EarlierProfileNotReused',
    'ExpectationResult',
    'FitsSkipped',
    'InvalidCodeList',
    'InvalidProfile',
    'InvalidScenario',
    'InvalidThresholds',
    'Issue',
    'Manifest',
    'PackagesMissing',
    'PlannedRange',
    'PoorFits',
    'Profile',
    'ProfilesDrifted',
    'ProfilingEnginesMissing',
    'ProfilingWarning',
    'Scenario',
    'SourceError',
    'StoppedWrite',
    'Thresholds',
    'UngenerableProfile',
    'WriteFailed',
    'WriteInterrupted',
    'WritePlan',
    'check',
    'choice',
    'dates',
    'decimals',
    'diff',
    'fakes',
    'generate',
    'integers',
    'plan',
    'profile',
    'profile_tables',
    'stream',
    'write',
]


if TYPE_CHECKING:
    from doppeldata import spark as spark
else:

    def __getattr__(name: str) -> ModuleType:
        """`doppeldata.spark` on first use, so importing doppeldata never imports pyspark."""
        if name == 'spark':
            return importlib.import_module('doppeldata.spark')
        raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
