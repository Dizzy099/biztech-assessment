from django.core.management.base import BaseCommand
from django.db import transaction

from emailqueue.models import EmailJob
from emailqueue.tasks import send_email


class Command(BaseCommand):
    help = "Re-queue dead-lettered emails (after fixing the cause)."

    def handle(self, *args, **opts):
        with transaction.atomic():
            ids = list(EmailJob.objects.filter(status=EmailJob.Status.DEAD).values_list("id", flat=True))
            EmailJob.objects.filter(id__in=ids).update(status=EmailJob.Status.QUEUED, attempts=0)
            transaction.on_commit(lambda: [send_email.delay(str(i)) for i in ids])
        self.stdout.write(self.style.SUCCESS(f"Re-queued {len(ids)} dead-lettered emails."))
