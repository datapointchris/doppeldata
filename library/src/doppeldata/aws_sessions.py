"""The boto3 session every AWS call doppeldata makes is built from: the AWS profile a public call names as `aws_profile`,
else `AWS_PROFILE` and boto3's default chain.

Each public call that reaches S3 or the Glue catalog sets its `aws_profile` for its own duration through
`aws_profile_chosen`, and resets it when it returns. Every session, client and Polars credential provider built inside the
call reads the choice, and nothing reads it after the call returns, so a reader a call hands back holds the session it was
built with. A call naming no profile inherits the one an outer call named. This module imports no boto3, so a call that
reaches no AWS service needs none installed.
"""

import contextlib
import contextvars
import importlib
import logging
import os
from collections.abc import Iterator
from typing import Any

from doppeldata import issue_codes
from doppeldata.errors import InvalidArgument
from doppeldata.errors import PackagesMissing
from doppeldata.errors import uninstalled

log = logging.getLogger(__name__)

CHOSEN_PROFILE: contextvars.ContextVar[str | None] = contextvars.ContextVar('doppeldata_aws_profile', default=None)


@contextlib.contextmanager
def aws_profile_chosen(aws_profile: str | None) -> Iterator[None]:
    """Build every AWS session inside the block from the profile `aws_profile` names, refusing one the AWS config lacks before
    anything runs. None keeps the profile an outer call chose, else boto3's own choice."""
    if aws_profile is None:
        yield
        return
    refuse_unknown_profile(aws_profile)
    token = CHOSEN_PROFILE.set(aws_profile)
    try:
        yield
    finally:
        CHOSEN_PROFILE.reset(token)


def refuse_unknown_profile(aws_profile: str) -> None:
    """Refuse `aws_profile` where the AWS config and credentials files hold no profile of that name. It reads those files
    alone, so it sends nothing."""
    if uninstalled({'boto3': 'boto3'}):
        raise PackagesMissing('aws_profile', ['boto3'], doing='naming an AWS profile')
    boto3 = importlib.import_module('boto3')
    exceptions = importlib.import_module('botocore.exceptions')
    try:
        boto3.Session(profile_name=aws_profile)
    except exceptions.ProfileNotFound as error:
        # botocore's own session lists the profiles without choosing one, so a bad AWS_PROFILE beside it raises nothing.
        held = sorted(importlib.import_module('botocore.session').Session().available_profiles)
        message = f'{error}; name a profile the AWS config holds'
        raise InvalidArgument(issue_codes.AWS_SETTINGS, message, field='aws_profile', value=aws_profile, valid=held) from error


def chosen_profile() -> str | None:
    """The AWS profile a public call named, or None where boto3 chooses one itself."""
    return CHOSEN_PROFILE.get()


def profile_in_force() -> str:
    """The AWS profile a session is built from, as a message names it: the one a call named, else AWS_PROFILE's, else `default`."""
    return chosen_profile() or os.environ.get('AWS_PROFILE') or 'default'


def aws_session() -> Any:
    """A boto3 session of the AWS profile the call chose, else of boto3's own choice."""
    boto3 = importlib.import_module('boto3')
    session = boto3.Session(profile_name=chosen_profile())
    log.debug(f'AWS session from profile {profile_in_force()}')
    return session


def polars_credentials() -> Any:
    """What Polars reads S3 with: an AWS credential provider of the profile the call chose, else its own `auto`."""
    chosen = chosen_profile()
    if chosen is None:
        return 'auto'
    polars = importlib.import_module('polars')
    return polars.CredentialProviderAWS(profile_name=chosen)
