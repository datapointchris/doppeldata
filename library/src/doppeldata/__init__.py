"""Profile a dataset into a reviewable file holding no rows, then generate synthetic data from it."""

import importlib
import logging
from types import ModuleType
from typing import TYPE_CHECKING

from doppeldata import fakes
from doppeldata import issue_codes
from doppeldata.bundle_generation import BundleWritePlan
from doppeldata.capabilities import capabilities
from doppeldata.dataset_generation import ColumnTrace
from doppeldata.dataset_generation import PlannedRange
from doppeldata.dataset_generation import SamplerParameter
from doppeldata.dataset_generation import WritePlan
from doppeldata.errors import CheckFailed
from doppeldata.errors import CheckStopped
from doppeldata.errors import ColumnsNotFrozen
from doppeldata.errors import DetectorPackError
from doppeldata.errors import DoppeldataError
from doppeldata.errors import EarlierProfileNotReused
from doppeldata.errors import FitsSkipped
from doppeldata.errors import IncompatibleInstall
from doppeldata.errors import InvalidArgument
from doppeldata.errors import InvalidCodeList
from doppeldata.errors import InvalidThresholds
from doppeldata.errors import IssueCarrier
from doppeldata.errors import PackagesMissing
from doppeldata.errors import PoorFits
from doppeldata.errors import ProfileFailsCheck
from doppeldata.errors import ProfileNotFound
from doppeldata.errors import ProfilesDrifted
from doppeldata.errors import ProfilingEnginesMissing
from doppeldata.errors import ProfilingWarning
from doppeldata.errors import SourceError
from doppeldata.errors import StoppedWrite
from doppeldata.errors import UnsupportedArgument
from doppeldata.errors import WriteFailed
from doppeldata.errors import WriteInterrupted
from doppeldata.inline_columns import choice
from doppeldata.inline_columns import dates
from doppeldata.inline_columns import decimals
from doppeldata.inline_columns import integers
from doppeldata.issue_codes import Issue
from doppeldata.issue_codes import IssueCode
from doppeldata.manifest import BundleManifest
from doppeldata.manifest import Manifest
from doppeldata.profile_comparison import check
from doppeldata.profile_comparison import diff
from doppeldata.profile_comparison.reports import BundleCheckReport
from doppeldata.profile_comparison.reports import CheckReport
from doppeldata.profile_comparison.reports import DiffReport
from doppeldata.profile_comparison.reports import ExpectationResult
from doppeldata.profile_comparison.thresholds import Thresholds
from doppeldata.profile_exports import export
from doppeldata.profile_format import Bundle
from doppeldata.profile_format import InvalidProfile
from doppeldata.profile_format import Profile
from doppeldata.profile_format import UngenerableProfile
from doppeldata.profiling import profile
from doppeldata.profiling import profile_tables
from doppeldata.scenario_format import InvalidScenario
from doppeldata.scenario_format import Scenario
from doppeldata.table_or_bundle_generation import generate
from doppeldata.table_or_bundle_generation import plan
from doppeldata.table_or_bundle_generation import sample
from doppeldata.table_or_bundle_generation import stream
from doppeldata.table_or_bundle_generation import write

__all__ = [
    'Bundle',
    'BundleCheckReport',
    'BundleManifest',
    'BundleWritePlan',
    'CheckFailed',
    'CheckReport',
    'CheckStopped',
    'ColumnTrace',
    'ColumnsNotFrozen',
    'DetectorPackError',
    'DiffReport',
    'DoppeldataError',
    'EarlierProfileNotReused',
    'ExpectationResult',
    'FitsSkipped',
    'IncompatibleInstall',
    'InvalidArgument',
    'InvalidCodeList',
    'InvalidProfile',
    'InvalidScenario',
    'InvalidThresholds',
    'Issue',
    'IssueCarrier',
    'IssueCode',
    'Manifest',
    'PackagesMissing',
    'PlannedRange',
    'PoorFits',
    'Profile',
    'ProfileFailsCheck',
    'ProfileNotFound',
    'ProfilesDrifted',
    'ProfilingEnginesMissing',
    'ProfilingWarning',
    'SamplerParameter',
    'Scenario',
    'SourceError',
    'StoppedWrite',
    'Thresholds',
    'UngenerableProfile',
    'UnsupportedArgument',
    'WriteFailed',
    'WriteInterrupted',
    'WritePlan',
    'capabilities',
    'check',
    'choice',
    'dates',
    'decimals',
    'diff',
    'export',
    'fakes',
    'generate',
    'integers',
    'issue_codes',
    'plan',
    'profile',
    'profile_tables',
    'sample',
    'stream',
    'write',
]

# The library's progress and notices go to the `doppeldata` logger, which writes nothing until the caller adds a handler.
logging.getLogger(__name__).addHandler(logging.NullHandler())

if TYPE_CHECKING:
    from doppeldata import spark as spark
else:

    def __getattr__(name: str) -> ModuleType:
        """`doppeldata.spark` on first use, so importing doppeldata never imports pyspark."""
        if name == 'spark':
            return importlib.import_module('doppeldata.spark')
        raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
