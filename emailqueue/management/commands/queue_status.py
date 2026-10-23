from django.core.management.base import BaseCommand
from django.db.models import Count

from emailqueue.models import EmailJob


class Command(BaseCommand):
    help = "Show email job counts by status."

    def handle(self, *args, **opts):
        rows = EmailJob.objects.values("status").annotate(n=Count("id")).order_by("status")
        for row in rows:
            self.stdout.write(f"{row['status']:<8} {row['n']}")
