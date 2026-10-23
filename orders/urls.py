from django.urls import path

from .views import OrderSummaryView, RegressedOrderSummaryView

urlpatterns = [
    path("summary/", OrderSummaryView.as_view(), name="orders-summary"),
    path("summary/regressed/", RegressedOrderSummaryView.as_view(), name="orders-summary-regressed"),
]
