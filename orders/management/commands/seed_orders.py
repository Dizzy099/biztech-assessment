from django.core.management.base import BaseCommand
from django.db import transaction

from orders.factories import create_customer_with_orders


class Command(BaseCommand):
    help = "Seed a heavy customer (>200 orders, the incident case) and a light one."

    def add_arguments(self, parser):
        parser.add_argument("--orders", type=int, default=250)
        parser.add_argument("--items", type=int, default=3)

    @transaction.atomic
    def handle(self, *args, **opts):
        heavy = create_customer_with_orders("Heavy customer", opts["orders"], opts["items"])
        light = create_customer_with_orders("Light customer", 5, opts["items"])
        self.stdout.write(self.style.SUCCESS(
            f"Heavy customer id={heavy.id} ({opts['orders']} orders), light customer id={light.id} (5 orders)\n"
            f"Try: /api/orders/summary/?customer_id={heavy.id} and "
            f"/api/orders/summary/regressed/?customer_id={heavy.id}"
        ))
