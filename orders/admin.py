from django.contrib import admin

from .models import Customer, Order, OrderItem, Product

admin.site.register(Customer)
admin.site.register(Product)


@admin.register(Order)
class OrderAdmin(admin.ModelAdmin):
    list_display = ["id", "customer", "status", "created_at"]
    list_select_related = ["customer"]  # avoids an N+1 on the changelist itself
    list_filter = ["status"]
    raw_id_fields = ["customer"]


admin.site.register(OrderItem)
