from django.db import models
from django.db.models import Prefetch


class Customer(models.Model):
    name = models.CharField(max_length=120)

    def __str__(self):
        return self.name


class Product(models.Model):
    name = models.CharField(max_length=200)
    sku = models.CharField(max_length=40, unique=True)
    price = models.DecimalField(max_digits=10, decimal_places=2)

    def __str__(self):
        return self.name


class OrderQuerySet(models.QuerySet):
    def for_dashboard(self):
        """Queryset used by /api/orders/summary/ (the fixed version).

        - select_related("customer"): customer columns arrive via a SQL JOIN in
          the same query as the orders.
        - Prefetch("items", ...select_related("product")): ONE extra query,
          `SELECT ... FROM orderitem JOIN product ... WHERE order_id IN (...)`,
          whose rows Django stitches onto each order's `items` cache in Python.

        Total: 2 queries no matter how many orders the customer has.
        """
        return self.select_related("customer").prefetch_related(
            Prefetch("items", queryset=OrderItem.objects.select_related("product"))
        )

    def for_dashboard_regressed(self):
        """What the same method looked like after the bad deploy (kept to
        reproduce the incident). The view did not change - this queryset did.

        Without the prefetch, the serializer's `order.items.all()` runs one
        query per order and `item.product.name` one query per item:
        1 + N + (N x items) queries -> ~1,000 for 250 orders x 3 items.
        """
        return self.all()


class Order(models.Model):
    class Status(models.TextChoices):
        PLACED = "placed"
        SHIPPED = "shipped"
        DELIVERED = "delivered"

    customer = models.ForeignKey(Customer, on_delete=models.CASCADE, related_name="orders")
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.PLACED)
    created_at = models.DateTimeField(auto_now_add=True)

    objects = OrderQuerySet.as_manager()

    class Meta:
        ordering = ["-created_at"]
        # The dashboard filters by customer and sorts newest-first; this composite
        # index serves both, so the ORDER BY does not need a separate sort step.
        indexes = [models.Index(fields=["customer", "-created_at"], name="order_customer_recent")]


class OrderItem(models.Model):
    # ForeignKey creates an index on order_id automatically, which is what makes
    # the prefetch's `WHERE order_id IN (...)` an index lookup, not a table scan.
    order = models.ForeignKey(Order, on_delete=models.CASCADE, related_name="items")
    product = models.ForeignKey(Product, on_delete=models.PROTECT)
    quantity = models.PositiveIntegerField(default=1)
    unit_price = models.DecimalField(max_digits=10, decimal_places=2)
