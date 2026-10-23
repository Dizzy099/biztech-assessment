import time

from django.core.management.base import BaseCommand
from django.db import connection
from django.test.utils import CaptureQueriesContext
from rest_framework.test import APIRequestFactory

from orders.views import OrderSummaryView, RegressedOrderSummaryView


class Command(BaseCommand):
    help = "Print query count and time for the regressed vs fixed summary endpoint."

    def add_arguments(self, parser):
        parser.add_argument("customer_id", type=int)

    def handle(self, *args, **opts):
        factory = APIRequestFactory()
        rows = []
        for label, view in (("regressed", RegressedOrderSummaryView), ("fixed", OrderSummaryView)):
            request = factory.get("/api/orders/summary/", {"customer_id": opts["customer_id"]})
            with CaptureQueriesContext(connection) as ctx:
                start = time.perf_counter()
                response = view.as_view()(request)
                elapsed_ms = (time.perf_counter() - start) * 1000
            rows.append((label, len(ctx.captured_queries), elapsed_ms, response.data["order_count"]))

        self.stdout.write(f"{'endpoint':<10} {'queries':>8} {'time_ms':>9} {'orders':>7}")
        for label, queries, ms, count in rows:
            self.stdout.write(f"{label:<10} {queries:>8} {ms:>9.1f} {count:>7}")
