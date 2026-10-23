"""Setup smoke tests. Keep them: they prove the environment works from a clean clone."""
import pytest
import redis
from django.conf import settings


@pytest.mark.django_db
def test_django_and_database_work():
    from django.contrib.auth.models import User

    User.objects.create(username="smoke")
    assert User.objects.count() == 1


def test_redis_is_reachable():
    client = redis.Redis.from_url(settings.REDIS_URL)
    try:
        assert client.ping() is True
    except redis.exceptions.ConnectionError:
        pytest.fail("Redis is not running. Start it with: docker compose up -d")
