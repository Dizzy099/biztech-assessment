from decimal import Decimal

from rest_framework import serializers

from .models import Order, OrderItem


class OrderItemSerializer(serializers.ModelSerializer):
    # source="product.name" walks the FK. Without select_related("product") this
    # line is one SQL query PER ITEM - the deepest layer of the N+1.
    product_name = serializers.CharField(source="product.name")
    line_total = serializers.SerializerMethodField()

    class Meta:
        model = OrderItem
        fields = ["product_name", "quantity", "unit_price", "line_total"]

    def get_line_total(self, item):
        return str(item.unit_price * item.quantity)


class OrderSummarySerializer(serializers.ModelSerializer):
    items = OrderItemSerializer(many=True)
    total = serializers.SerializerMethodField()

    class Meta:
        model = Order
        fields = ["id", "status", "created_at", "items", "total"]

    def get_total(self, order):
        # Deliberately computed in Python over items.all(): .all() reads the
        # prefetch cache. order.items.aggregate(...) or .filter(...) would build a
        # NEW queryset, bypass the cache and silently bring the N+1 back.
        return str(sum((i.unit_price * i.quantity for i in order.items.all()), Decimal("0")))
