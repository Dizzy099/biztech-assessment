"""Clear demo data so a recording starts from zero (development use only)."""
from django.conf import settings
from django.core.management.base import BaseCommand

from emailqueue.models import EmailJob
from emailqueue.ratelimiter import _client


class Command(BaseCommand):
    help = "Delete all EmailJob/SendAttempt rows and the rate-limit key in Redis."

    def handle(self, *args, **opts):
        deleted, _ = EmailJob.objects.all().delete()  # SendAttempt rows cascade
        _client(settings.REDIS_URL).delete(settings.EMAIL_RATE_LIMIT_KEY)
        self.stdout.write(self.style.SUCCESS(f"Deleted {deleted} rows and cleared the rate-limit window."))
