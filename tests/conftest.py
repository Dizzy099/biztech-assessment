import pytest
import redis

TEST_REDIS_URL = "redis://127.0.0.1:6379/15"  # isolated from the dev database (db 0)


@pytest.fixture
def test_redis(settings):
    """Point everything Redis-related at db 15 and start from an empty database."""
    client = redis.Redis.from_url(TEST_REDIS_URL)
    try:
        client.ping()
    except redis.exceptions.ConnectionError:
        pytest.fail("Redis is not running. Start it with: docker compose up -d")
    client.flushdb()
    settings.REDIS_URL = TEST_REDIS_URL
    yield client
    client.flushdb()
