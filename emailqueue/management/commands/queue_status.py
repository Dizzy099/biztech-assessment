"""Job counts by status, plus the live Redis state of the queue and the limiter.

    python manage.py queue_status            # print once
    python manage.py queue_status --watch    # refresh every 2 s (Ctrl+C to stop)
"""
import os
import time

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db.models import Count

from emailqueue.models import EmailJob
from emailqueue.ratelimiter import _client


class Command(BaseCommand):
    help = "Show email job counts by status and the Redis queue / rate-limit state."

    def add_arguments(self, parser):
        parser.add_argument("--watch", action="store_true", help="refresh until Ctrl+C")
        parser.add_argument("--interval", type=float, default=2.0, help="seconds between refreshes")

    def handle(self, *args, **opts):
        if not opts["watch"]:
            self.show()
            return
        try:
            while True:
                os.system("cls" if os.name == "nt" else "clear")
                self.show()
                time.sleep(opts["interval"])
        except KeyboardInterrupt:
            pass

    def show(self):
        self.stdout.write(time.strftime("%H:%M:%S"))
        self.stdout.write("-- database (EmailJob.status) --")
        counts = dict(EmailJob.objects.values_list("status").annotate(n=Count("id")))
        for status in EmailJob.Status.values:
            self.stdout.write(f"{status:<8} {counts.get(status, 0)}")

        broker = _client(settings.CELERY_BROKER_URL)
        limiter = _client(settings.REDIS_URL)
        # Count only admissions still inside the window, using Redis's clock (same as the limiter).
        sec, usec = limiter.time()
        now_ms = sec * 1000 + usec // 1000
        window_ms = int(settings.EMAIL_RATE_WINDOW_SECONDS * 1000)
        in_window = limiter.zcount(settings.EMAIL_RATE_LIMIT_KEY, f"({now_ms - window_ms}", "+inf")

        self.stdout.write("-- redis --")
        self.stdout.write(f"waiting in queue 'celery':    {broker.llen('celery')}")
        self.stdout.write(f"held by worker (unacked):     {broker.hlen('unacked')}")
        self.stdout.write(f"sent in last {window_ms // 1000}s (limit {settings.EMAIL_RATE_LIMIT}): {in_window}")
