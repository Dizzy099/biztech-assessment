from decimal import Decimal

from rest_framework.response import Response
from rest_framework.views import APIView

from .models import Order
from .serializers import OrderSummarySerializer


class OrderSummaryView(APIView):
    """GET /api/orders/summary/?customer_id=<id>

    The view body is identical for the fixed and the regressed endpoint; only the
    queryset method differs. That mirrors the incident: "no code change was made
    to that view" - the regression came from the shared queryset it calls.
    (customer_id as a query param keeps the demo simple; in production it would
    come from request.user.)
    """

    queryset_method = "for_dashboard"

    def get(self, request):
        customer_id = request.query_params.get("customer_id")
        if not customer_id or not customer_id.isdigit():
            return Response({"detail": "customer_id (integer) is required"}, status=400)

        orders = getattr(Order.objects, self.queryset_method)().filter(customer_id=customer_id)
        data = OrderSummarySerializer(orders, many=True).data
        grand_total = sum((Decimal(o["total"]) for o in data), Decimal("0"))
        return Response(
            {
                "customer_id": int(customer_id),
                "order_count": len(data),
                "grand_total": str(grand_total),
                "orders": data,
            }
        )


class RegressedOrderSummaryView(OrderSummaryView):
    """Reproduces the post-deploy behaviour, for profiling before/after."""

    queryset_method = "for_dashboard_regressed"
