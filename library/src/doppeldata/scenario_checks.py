"""A scenario checked against a profile without generating: its document validated, its patch applied, and each
perturbation and the file layout planned as a write with the same settings would plan them."""

import dataclasses as dc
import json
import os
from typing import ClassVar

from doppeldata import issue_codes
from doppeldata.counted_nouns import counted
from doppeldata.dataset_generation import WriteChoices
from doppeldata.dataset_generation import prepared_write
from doppeldata.issue_codes import Issue
from doppeldata.issue_codes import issue_table
from doppeldata.profile_sources import ProfileSource
from doppeldata.profile_sources import profile_from
from doppeldata.profile_sources import read_as
from doppeldata.scenario_format import InvalidScenario
from doppeldata.scenario_format import Scenario


@dc.dataclass(frozen=True)
class ScenarioCheck:
    """A scenario checked against a profile: the scenario as named, the profile's name, each issue at its JSON Pointer, and
    for a scenario that holds, its SHA-256 and how many perturbations and expectations it declares."""

    failure_code: ClassVar[str] = issue_codes.SCENARIO_CHECK_FAILED

    scenario: str
    profile: str
    issues: list[Issue]
    sha256: str | None = None
    perturbations: int = 0
    expectations: int = 0

    @property
    def passed(self) -> bool:
        return not self.issues

    def to_json(self) -> str:
        """The check as `scenario check --json` prints it."""
        plain = dc.asdict(self) | {'passed': self.passed, 'issues': [issue.to_json() for issue in self.issues]}
        return json.dumps(plain, indent=2, ensure_ascii=False) + '\n'

    def to_markdown(self) -> str:
        """The check for a person, as GitHub Markdown naming each issue at its JSON Pointer."""
        if self.passed:
            held = f'{counted(self.perturbations, "perturbation")} and {counted(self.expectations, "expectation")}'
            verdict = f'{self.scenario} holds for profile `{self.profile}`: {held}, scenario {self.sha256}.'
            return f'# doppeldata scenario check passed\n\n{verdict}\n'
        lines = ['# doppeldata scenario check failed', '', f'{self.scenario} does not hold for profile `{self.profile}`.', '']
        return '\n'.join([*lines, *issue_table(self.issues)]).rstrip('\n') + '\n'

    def failure_message(self) -> str:
        """What `CheckFailed` says of a scenario that does not hold."""
        return f'{self.scenario} does not hold for profile {self.profile} in {counted(len(self.issues), "place")}'


def check_scenario(
    scenario: Scenario | str | os.PathLike[str],
    profile: ProfileSource,
    *,
    rows: int | None = None,
    seed: int | None = None,
    format: str | None = None,
    file_rows: int | None = None,
    partition_by: list[str] | None = None,
) -> ScenarioCheck:
    """Check `scenario`, a Scenario or the file or example `Scenario.named` reads, against `profile` without generating.

    It validates the scenario, applies its patch, and plans each perturbation and the file layout as `write` would with the
    same arguments. A scenario that does not validate or does not hold is a failed check, naming each issue. A refusal of
    the arguments raises as it is.
    """
    with read_as('profile'):
        found = profile_from(profile)
    name = str(found.document['name'])
    named = scenario.source if isinstance(scenario, Scenario) else os.fspath(scenario)
    choices = WriteChoices(format, file_rows, partition_by, None, None, False)
    try:
        applied = scenario if isinstance(scenario, Scenario) else Scenario.named(scenario)
        prepared = prepared_write(profile, 'memory://', rows, seed, choices, applied)
        prepared.engine.ranges_json(prepared.options)
    except InvalidScenario as error:
        return ScenarioCheck(error.source, name, list(error.issues))
    plain = applied.to_plain()
    return ScenarioCheck(named, name, [], applied.sha256, len(plain.get('perturbations') or []), len(plain.get('expect') or []))
