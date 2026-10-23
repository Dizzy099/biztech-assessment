"""Prove the rate limit held: the most provider calls in any rolling window.

Every provider call writes a SendAttempt with admitted_at_ms, the Redis server
time at which the limiter admitted it. A two-pointer sweep over the sorted
timestamps finds the busiest window of EMAIL_RATE_WINDOW_SECONDS.
"""
from django.conf import settings
from django.core.management.base import BaseCommand

from emailqueue.models import SendAttempt


class Command(BaseCommand):
    help = "Show total provider calls and the maximum in any rolling rate-limit window."

    def handle(self, *args, **opts):
        window_ms = int(settings.EMAIL_RATE_WINDOW_SECONDS * 1000)
        times = sorted(SendAttempt.objects.values_list("admitted_at_ms", flat=True))
        busiest, start = 0, 0
        for end, t in enumerate(times):
            while t - times[start] >= window_ms:  # drop calls outside the window ending at t
                start += 1
            busiest = max(busiest, end - start + 1)
        limit = settings.EMAIL_RATE_LIMIT
        self.stdout.write(f"provider calls:          {len(times)}")
        self.stdout.write(f"max in any {window_ms // 1000}s window: {busiest}  (limit {limit})")
        if busiest <= limit:
            self.stdout.write(self.style.SUCCESS("OK - limit never exceeded"))
        else:
            self.stdout.write(self.style.ERROR("LIMIT EXCEEDED"))
