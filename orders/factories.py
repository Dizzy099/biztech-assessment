"""Shared data builder for the seed command and the tests (one source of truth)."""
from decimal import Decimal

from .models import Customer, Order, OrderItem, Product


def ensure_products(count=20):
    existing = list(Product.objects.order_by("id")[:count])
    missing = count - len(existing)
    if missing > 0:
        start = len(existing)
        Product.objects.bulk_create(
            Product(name=f"Product {i}", sku=f"SKU-{i:04d}", price=Decimal("10.00") + i)
            for i in range(start, start + missing)
        )
        existing = list(Product.objects.order_by("id")[:count])
    return existing


def create_customer_with_orders(name, order_count, items_per_order=3):
    """bulk_create keeps seeding fast: 250 orders x 3 items is 3 INSERT batches,
    not 1,000 single-row INSERTs."""
    products = ensure_products()
    customer = Customer.objects.create(name=name)
    orders = Order.objects.bulk_create(Order(customer=customer) for _ in range(order_count))
    items = [
        OrderItem(
            order=order,
            product=products[(o_idx + i) % len(products)],
            quantity=1 + (o_idx + i) % 3,
            unit_price=products[(o_idx + i) % len(products)].price,
        )
        for o_idx, order in enumerate(orders)
        for i in range(items_per_order)
    ]
    OrderItem.objects.bulk_create(items)
    return customer
