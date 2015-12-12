from fakeredis import FakeStrictRedis

import pytest


@pytest.fixture()
def redis(monkeypatch):
    fake_redis = FakeStrictRedis()
    fake_redis.flushall()
    monkeypatch.setattr("celeryone.celeryone.QueueOne.redis", fake_redis)
    return fake_redis
