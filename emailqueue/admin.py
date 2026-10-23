from django.contrib import admin

from .models import EmailJob, SendAttempt


@admin.register(EmailJob)
class EmailJobAdmin(admin.ModelAdmin):
    list_display = ["id", "kind", "to_email", "status", "attempts", "deferrals", "created_at"]
    list_filter = ["status", "kind"]
    show_full_result_count = False  # avoid a COUNT(*) on large tables


admin.site.register(SendAttempt)
