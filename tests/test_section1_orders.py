"""Section 1: prove the regression and the fix with query counts.

Views are called through APIRequestFactory (not the test client) so middleware
- including django-silk, which writes its own rows per request - does not add
queries to the count.
"""
import pytest
from django.db import connection
from django.test.utils import CaptureQueriesContext
from rest_framework.test import APIRequestFactory

from orders.factories import create_customer_with_orders
from orders.views import OrderSummaryView, RegressedOrderSummaryView

pytestmark = pytest.mark.django_db


def call(view, customer_id):
    request = APIRequestFactory().get("/api/orders/summary/", {"customer_id": customer_id})
    with CaptureQueriesContext(connection) as ctx:
        response = view.as_view()(request)
    return response, len(ctx.captured_queries)


@pytest.mark.parametrize("order_count", [5, 250])
def test_fixed_endpoint_query_count_is_constant(order_count):
    customer = create_customer_with_orders("c", order_count)
    response, queries = call(OrderSummaryView, customer.id)
    assert response.status_code == 200
    assert response.data["order_count"] == order_count
    # 1 query: orders JOIN customer; 1 query: items JOIN product WHERE order_id IN (...)
    assert queries == 2


def test_regressed_endpoint_grows_with_order_count():
    customer = create_customer_with_orders("c", 250, items_per_order=3)
    _, queries = call(RegressedOrderSummaryView, customer.id)
    # 1 (orders)
    # + 250 x 2 (order.items.all() is evaluated twice per order - once by the
    #   nested `items` field, once by get_total - and with no prefetch cache
    #   each call is a new query)
    # + 750 (item.product, one per item)
    assert queries == 1 + 250 * 2 + 250 * 3


def test_fix_does_not_change_the_response():
    customer = create_customer_with_orders("c", 20)
    fixed, _ = call(OrderSummaryView, customer.id)
    regressed, _ = call(RegressedOrderSummaryView, customer.id)
    assert fixed.data == regressed.data


def test_missing_customer_id_is_rejected():
    request = APIRequestFactory().get("/api/orders/summary/")
    assert OrderSummaryView.as_view()(request).status_code == 400
