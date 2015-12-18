from celery import Celery
from celery.result import AsyncResult
from celeryone import QueueOne, AlreadyQueued
from freezegun import freeze_time
import pytest


app = Celery()
app.conf.ONCE_REDIS_URL = 'redis://localhost:1337/0'
app.conf.ONCE_DEFAULT_TIMEOUT = 30 * 60
app.conf.CELERY_ALWAYS_EAGER = True


def helper_test_result(result, id='test'):
    assert result is not None
    assert isinstance(result, AsyncResult)
    assert result.id == id


@app.task(name="example", base=QueueOne, one_options={'keys': ['a']})
def example(redis, a=1):
    return redis.hgetall("qo_example_a-1")


@app.task(name="example_id", base=QueueOne, one_options={'keys': ['a'], 'use_id': True})
def example_id(redis, a=1):
    return redis.hgetall("qo_example_id_test_id")


@app.task(name="example_unlock_before_run", base=QueueOne, one_options={'keys': ['a'], 'unlock_before_run': True})
def example_unlock_before_run(redis, a=1):
    return redis.hgetall("qo_example_unlock_before_run_a-1")


@app.task(name="example_unlock_before_run_set_key", base=QueueOne, one_options={'keys': ['a'], 'unlock_before_run': True})
def example_unlock_before_run_set_key(redis, a=1):
    result = redis.hgetall("qo_example_unlock_before_run_set_key_a-1")
    redis.hmset("qo_example_unlock_before_run_set_key_a-1", {'task_id': 'test_id', 'expires': None})
    return result


def test_delay_1(redis):
    result = example.delay(redis)

    assert bool(result.get()) is True
    assert result.get().get(b'task_id') == b'qoid_ZXhhbXBsZXFvX2V4YW1wbGVfYS0x'
    assert result.get().get(b'expires') is not None
    assert bool(redis.hgetall("qo_example_a-1")) is False


def test_delay_1_set_task_id(redis):
    result = example.apply_async(args=(redis,), task_id='test_id')

    assert bool(result.get()) is True
    assert result.get().get(b'task_id') == b'test_id'
    assert result.get().get(b'expires') is not None
    assert bool(redis.hgetall("qo_example_a-1")) is False


def test_delay_1_use_task_id(redis):
    result = example_id.apply_async(args=(redis,), task_id='test_id')

    assert bool(result.get()) is True
    assert result.get().get(b'task_id') == b'test_id'
    assert result.get().get(b'expires') is not None
    assert bool(redis.hgetall("qo_example_id_test_id")) is False


def test_delay_2(redis):
    redis.hmset("qo_example_a-1", {'task_id': 'test_id', 'expires': None})
    try:
        example.delay(redis)
        pytest.fail("Didn't raise AlreadyQueued.")
    except AlreadyQueued:
        pass


@freeze_time("2012-01-14")  # 1326499200
def test_delay_3(redis):
    redis.hmset("qo_example_a-1", {'task_id': 'test_id', 'expires': 1326499200 - 60 * 60})
    example.delay(redis)


def test_delay_unlock_before_run_1(redis):
    result = example_unlock_before_run.delay(redis)
    assert bool(result.get()) is False
    assert bool(redis.hgetall("qo_example_unlock_before_run_a-1")) is False


def test_delay_unlock_before_run_2(redis):
    result = example_unlock_before_run_set_key.delay(redis)
    assert bool(result.get()) is False

    assert redis.hgetall("qo_example_unlock_before_run_set_key_a-1").get(b'expires') is None
    assert redis.hgetall("qo_example_unlock_before_run_set_key_a-1").get(b'task_id') == b'test_id'


def test_apply_async_1(redis):
    result = example.apply_async(args=(redis, ))
    assert bool(result.get()) is True
    assert redis.get("qo_example_a-1") is None


def test_apply_async_2(redis):
    redis.hmset("qo_example_a-1", {'task_id': 'test_id', 'expires': None})
    try:
        example.apply_async(args=(redis, ))
        pytest.fail("Didn't raise AlreadyQueued.")
    except AlreadyQueued:
        pass


def test_apply_async_3(redis):
    redis.hmset("qo_example_a-1", {'task_id': 'test_id', 'expires': None})
    result = example.apply_async(args=(redis, ), one_options={'fail': False})
    helper_test_result(result, 'test_id')


def test_apply_async_unlock_before_run_1(redis):
    result = example_unlock_before_run.apply_async(args=(redis, ))
    assert bool(result.get()) is False
    assert bool(redis.hgetall("qo_example_unlock_before_run_a-1")) is False


def test_apply_async_unlock_before_run_2(redis):
    result = example_unlock_before_run_set_key.apply_async(args=(redis, ))
    assert bool(result.get()) is False

    assert redis.hgetall("qo_example_unlock_before_run_set_key_a-1").get(b'expires') is None
    assert redis.hgetall("qo_example_unlock_before_run_set_key_a-1").get(b'task_id') == b'test_id'


@freeze_time("2012-01-14")  # 1326499200
def test_apply_async_4(redis):
    redis.hmset("qo_example_a-1", {'task_id': 'test_id', 'expires': 1326499200 - 60 * 60})
    example.apply_async(args=(redis, ))


def test_redis():
    assert example.redis.connection_pool.connection_kwargs['host'] == "localhost"
    assert example.redis.connection_pool.connection_kwargs['port'] == 6379
    assert example.redis.connection_pool.connection_kwargs['db'] == 0


def test_default_timeout():
    assert example.default_timeout == 60 * 60