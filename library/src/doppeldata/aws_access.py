"""How doppeldata calls AWS: the settings every boto3 client takes, and which of boto3's failures mean no answer came.

S3 writes, S3 listings and the Glue Data Catalog each raise their own error, and ask this module which kind of failure
they hold, so one outage gives one exit code whichever of them met it.
"""

import datetime as dt
from typing import Any

import botocore.config
import botocore.exceptions
from botocore.retries.standard import RetryContext
from botocore.retries.standard import ThrottledRetryableChecker

# A request gives up rather than hang on an endpoint that never answers, after as many attempts as the AWS CLI's standard mode makes.
# boto3's clients and a write's upload store both take these.
ATTEMPTS = 3
CONNECT_TIMEOUT = dt.timedelta(seconds=5)
CLIENT_CONFIG = botocore.config.Config(
    connect_timeout=CONNECT_TIMEOUT.total_seconds(), read_timeout=20, retries={'max_attempts': ATTEMPTS, 'mode': 'standard'}
)
# Every failure of a call: an answer the service gave, or one boto3 raised before or instead of an answer.
AWS_ERRORS = (botocore.exceptions.ClientError, botocore.exceptions.BotoCoreError)
CREDENTIAL_ERRORS = (
    botocore.exceptions.NoCredentialsError,
    botocore.exceptions.PartialCredentialsError,
    botocore.exceptions.CredentialRetrievalError,
    botocore.exceptions.SSOError,
    botocore.exceptions.TokenRetrievalError,
)
UNANSWERED_ERRORS = (
    botocore.exceptions.EndpointConnectionError,
    botocore.exceptions.ConnectTimeoutError,
    botocore.exceptions.ReadTimeoutError,
    botocore.exceptions.ConnectionClosedError,
)
CREDENTIAL_ADVICE = 'run `aws sso login`, or set AWS_PROFILE to a profile with credentials'
THROTTLED = ThrottledRetryableChecker()


def unanswered(error: Exception) -> bool:
    """Whether `error` means no answer came: a connection that failed or timed out, or a server fault or throttling on every
    attempt. Throttling is each code boto3's standard retry mode retries as throttling, Glue's ThrottlingException among them.
    """
    if isinstance(error, botocore.exceptions.ClientError):
        status = http_status(error)
        return (
            status == 429 or status >= 500 or THROTTLED.is_retryable(RetryContext(attempt_number=ATTEMPTS, parsed_response=error.response))
        )
    return isinstance(error, UNANSWERED_ERRORS)


def http_status(error: Exception) -> int:
    """The HTTP status the service answered `error` with, or 0 where no answer came."""
    response: dict[str, Any] = getattr(error, 'response', None) or {}
    status: int = response.get('ResponseMetadata', {}).get('HTTPStatusCode', 0)
    return status


def error_code(error: Exception) -> str | None:
    """The error code the service answered `error` with, such as `EntityNotFoundException`, or None where no answer came."""
    response: dict[str, Any] = getattr(error, 'response', None) or {}
    code: str | None = response.get('Error', {}).get('Code')
    return code


def answered(error: botocore.exceptions.ClientError) -> str:
    """What the service answered: its error code, then its message."""
    reported = error.response.get('Error', {})
    return f'{reported.get("Code")}, {reported.get("Message")}'


def no_answer(endpoint: str, service: str, error: Exception) -> str:
    """What a failure `unanswered` holds says: the endpoint, and what `service` returned on every attempt."""
    if isinstance(error, botocore.exceptions.ClientError):
        return f'no answer from {endpoint}: {service} answered {answered(error)} on every attempt'
    return f'no answer from {endpoint}: {error}, {ATTEMPTS} attempts'
