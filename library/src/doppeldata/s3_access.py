"""How doppeldata reaches S3: through boto3, with the credentials, region and endpoint its chain resolves for the AWS profile
a call names, else from the AWS environment.

Profiling lists and reads S3 with boto3 itself. A write uploads through the engine's object store, which takes its credentials
from a callable over boto3's, so an SSO profile or any other source boto3 reads works for both.
"""

import datetime as dt
import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from urllib.parse import urlsplit

import botocore.credentials
import botocore.exceptions

from doppeldata import issue_codes
from doppeldata.aws_access import ATTEMPTS
from doppeldata.aws_access import AWS_ERRORS
from doppeldata.aws_access import CLIENT_CONFIG
from doppeldata.aws_access import CONNECT_TIMEOUT
from doppeldata.aws_access import CREDENTIAL_ADVICE
from doppeldata.aws_access import CREDENTIAL_ERRORS
from doppeldata.aws_access import answered
from doppeldata.aws_access import http_status
from doppeldata.aws_access import no_answer
from doppeldata.aws_access import unanswered
from doppeldata.aws_sessions import aws_session
from doppeldata.aws_sessions import profile_in_force
from doppeldata.errors import InvalidArgument
from doppeldata.errors import WriteFailed

log = logging.getLogger(__name__)

# The store asks for refreshable credentials again this often. botocore refreshes them at least ten minutes before they expire, so
# each answer outlives the interval.
REFRESH_EVERY = dt.timedelta(minutes=5)


def endpoint_variable() -> str | None:
    """The environment variable naming the S3 endpoint, when one does."""
    return next((variable for variable in ('AWS_ENDPOINT_URL_S3', 'AWS_ENDPOINT_URL') if os.environ.get(variable)), None)


def s3_session() -> tuple[Any, Any]:
    """boto3's session and S3 client for the AWS profile the call chose, else the AWS environment's.

    Raises InvalidArgument naming the setting boto3 cannot use: an AWS_PROFILE the AWS config does not hold, or an endpoint that
    is not a URL.
    """
    try:
        session = aws_session()
        return session, session.client('s3', config=CLIENT_CONFIG)
    except botocore.exceptions.ProfileNotFound as error:
        message = f'{error}; set AWS_PROFILE to a profile the AWS config holds'
        raise InvalidArgument(issue_codes.AWS_SETTINGS, message, field='environment', value=os.environ.get('AWS_PROFILE')) from error
    except ValueError as error:
        message = f'{error}, from {endpoint_variable() or "the AWS profile"}'
        raise InvalidArgument(issue_codes.AWS_SETTINGS, message, field='environment') from error


def endpoint_named(client: Any) -> str:
    """The S3 endpoint the client calls, and the setting that chose it, which a failure names."""
    endpoint = client.meta.endpoint_url
    chosen_by = endpoint_variable()
    if chosen_by:
        return f'{endpoint} (from {chosen_by})'
    if (urlsplit(endpoint).hostname or '').endswith(('.amazonaws.com', '.amazonaws.com.cn')):
        return f'{endpoint} (the AWS endpoint for region {client.meta.region_name})'
    return f'{endpoint} (from AWS profile {profile_in_force()})'


class Boto3Credentials:
    """The credentials boto3's chain resolved, as pyo3-object_store's S3 store asks a credential provider for them."""

    # The store asks again once an answer's expiry is this close. The expiry is REFRESH_EVERY away, set here rather than by the
    # credentials, so a threshold of zero asks once per interval. The store's own default is five minutes, which would ask on
    # every request.
    refresh_threshold = dt.timedelta(0)

    def __init__(self, credentials: botocore.credentials.Credentials) -> None:
        self.credentials = credentials

    def __call__(self) -> dict[str, Any]:
        frozen = self.credentials.get_frozen_credentials()
        refreshable = isinstance(self.credentials, botocore.credentials.RefreshableCredentials)
        return {
            'access_key_id': frozen.access_key,
            'secret_access_key': frozen.secret_key,
            'token': frozen.token,
            'expires_at': dt.datetime.now(dt.UTC) + REFRESH_EVERY if refreshable else None,
        }


@contextmanager
def write_errors(url: str, endpoint: str) -> Iterator[None]:
    """Each boto3 failure reaching `url` as a WriteFailed naming the endpoint, coded NO_ANSWER when no answer came."""
    try:
        yield
    except CREDENTIAL_ERRORS as error:
        raise WriteFailed(issue_codes.NO_CREDENTIALS, f'{url} cannot be written: {error}; {CREDENTIAL_ADVICE}') from error
    except AWS_ERRORS as error:
        if unanswered(error):
            raise WriteFailed(issue_codes.NO_ANSWER, no_answer(endpoint, 'S3', error)) from error
        if not isinstance(error, botocore.exceptions.ClientError):
            raise WriteFailed(issue_codes.STORE, f'{url} cannot be written through {endpoint}: {error}') from error
        status = http_status(error)
        if status == 404:
            raise WriteFailed(issue_codes.BUCKET_MISSING, f'{url} names a bucket {endpoint} does not hold') from error
        code = issue_codes.STORE_REFUSED if status == 403 else issue_codes.STORE
        raise WriteFailed(code, f'{url} cannot be written: {endpoint} answered {answered(error)}') from error


def s3_store_settings(url: str, bucket: str) -> dict[str, Any]:
    """The keyword arguments of the S3 store a write to `bucket`, at `url`, uploads through.

    It resolves boto3's credentials and asks the bucket for its region, so missing credentials or a missing bucket refuse the
    write before a byte is sent. The store calls the endpoint boto3 resolves for that region, so one AWS_ENDPOINT_URL or a
    profile names, such as a VPC endpoint, serves both. It makes as many attempts as boto3 does.
    """
    try:
        session, client = s3_session()
    except InvalidArgument as error:
        raise WriteFailed(error.code, f'{url} cannot be written: {error.message}') from error
    with write_errors(url, endpoint_named(client)):
        credentials = session.get_credentials()
        if credentials is None:
            raise botocore.exceptions.NoCredentialsError()
        head = client.head_bucket(Bucket=bucket)
        region = head['ResponseMetadata']['HTTPHeaders'].get('x-amz-bucket-region') or client.meta.region_name
        endpoint = session.client('s3', region_name=region, config=CLIENT_CONFIG).meta.endpoint_url
    client_options: dict[str, Any] = {'connect_timeout': CONNECT_TIMEOUT}
    if endpoint.startswith('http://'):
        client_options['allow_http'] = True
    log.info('%s: uploading through %s in region %s, with AWS profile %s', url, endpoint, region, profile_in_force())
    return {
        'config': {'region': region, 'endpoint': endpoint},
        'client_options': client_options,
        'retry_config': {'max_retries': ATTEMPTS - 1},
        'credential_provider': Boto3Credentials(credentials),
    }
