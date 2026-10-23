from django.core.management.base import BaseCommand

from emailqueue.models import EmailJob
from emailqueue.services import enqueue_emails


class Command(BaseCommand):
    help = "Enqueue a burst of emails (for the demo / recording)."

    def add_arguments(self, parser):
        parser.add_argument("--count", type=int, default=500)
        parser.add_argument("--fail-every", type=int, default=50,
                            help="every Nth job fails once, then succeeds on retry")
        parser.add_argument("--dead", type=int, default=1,
                            help="number of jobs that fail permanently (dead-letter)")

    def handle(self, *args, **opts):
        jobs = []
        for i in range(opts["count"]):
            jobs.append(EmailJob(
                kind=EmailJob.Kind.ORDER_CONFIRMATION,
                to_email=f"user{i}@example.com",
                subject=f"Order #{i} confirmed",
                simulate_transient_failures=1 if opts["fail_every"] and i % opts["fail_every"] == 0 else 0,
                simulate_permanent_failure=i < opts["dead"],
            ))
        enqueue_emails(jobs)
        self.stdout.write(self.style.SUCCESS(f"Enqueued {len(jobs)} emails."))
