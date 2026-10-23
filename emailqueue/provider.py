"""Stand-in for the third-party email provider (SES, SendGrid, ...).

It never sends real mail. Failures are driven by fields on the job so tests can
force a deterministic number of transient failures or a permanent failure.
"""


class TransientProviderError(Exception):
    """Worth retrying: timeouts, 5xx, 429 from the provider."""


class PermanentProviderError(Exception):
    """Never worth retrying: invalid address, 4xx validation errors."""


class FakeEmailProvider:
    def send(self, job, attempt_number: int) -> str:
        if job.simulate_permanent_failure:
            raise PermanentProviderError(f"invalid recipient {job.to_email}")
        if attempt_number <= job.simulate_transient_failures:
            raise TransientProviderError(f"provider timeout (attempt {attempt_number})")
        return f"msg-{job.id}"


provider = FakeEmailProvider()
