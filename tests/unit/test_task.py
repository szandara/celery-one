import mock
from celery import task
from celeryone import QueueOne, AlreadyQueued
from freezegun import freeze_time
import pytest


@task(name='simple_example', base=QueueOne)
def simple_example():
    return "simple"


@task(name='bound_task', bind=True, base=QueueOne)
def bound_task(self, a, b):
    return a + b


@task(name='args_example', base=QueueOne)
def args_example(a, b):
    return a + b


@task(name='select_args_example', base=QueueOne, one_options={'keys': ['a']})
def select_args_example(a, b):
    return a + b


def test_get_key_simple():
    assert "qo_simple_example" == simple_example.get_key_from_args()


def test_get_key_args_1():
    assert "qo_args_example_a-1_b-2" == args_example.get_key_from_args(kwargs={'a':1, 'b': 2})


def test_get_key_args_2():
    assert "qo_args_example_a-1_b-2" == args_example.get_key_from_args(args=(1, 2, ))


def test_get_key_select_args_1():
    assert "qo_select_args_example_a-1" == select_args_example.get_key_from_args(kwargs={'a':1, 'b': 2})


def test_get_key_bound_task():
    assert "qo_bound_task_a-1_b-2" == bound_task.get_key_from_args(kwargs={'a': 1, 'b': 2})


@freeze_time("2012-01-14")  # 1326499200
def test_raise_or_lock(redis):
    print(redis.hgetall("test"))
    assert not bool(redis.hgetall("test"))
    QueueOne().raise_or_lock(key="test", queue_one_data={'task_id': 'test_id', 'expires': 60})
    assert bool(redis.hgetall("test"))
    assert redis.ttl("test") == 60


@freeze_time("2012-01-14")  # 1326499200
def test_raise_or_lock_locked(redis):
    # Set to expire in 30 seconds!
    redis.hmset("test", {'task_id': 'test_id', 'expires': 1326499200 + 30})
    with pytest.raises(AlreadyQueued) as e:
        QueueOne().raise_or_lock(key="test", queue_one_data={'task_id': 'test_id', 'expires': 60})
    assert e.value.countdown == 30
    assert e.value.message == "Task test_id expires in 30 seconds"

@freeze_time("2012-01-14")  # 1326499200
def test_raise_or_lock_locked_and_expired(redis):
    # Set to have expired 30 ago seconds!
    redis.hmset("test", {'task_id': 'test_id', 'expires': 1326499200 - 30})
    QueueOne().raise_or_lock(key="test", queue_one_data={'task_id': 'test_id', 'expires': 60})
    assert bool(redis.hgetall("test"))
    assert redis.ttl("test") == 60

def test_clear_lock(redis):
    redis.hmset("test", {'task_id': 'test_id', 'expires': 1326499200 + 30})
    QueueOne().clear_lock("test")
    assert not bool(redis.hgetall("test"))


@task(name='task_id_example', base=QueueOne, one_options={'keys': ['a']})
def task_id_example(a, b):
    return a + b

def test_task_id_key():
    assert "qo_task_id_example_a_key" == task_id_example.get_key_from_id(task_id='a_key')

def test_task_id_key_num():
    assert "qo_task_id_example_10" == task_id_example.get_key_from_id(task_id=10)

@mock.patch('celeryone.celeryone.get_redis')
def test_redis_cached_property(get_redis_mock):
    # Remove any side effect previous tests could have had
    del simple_example.app._cached_redis

    assert get_redis_mock.call_count == 0
    assert not hasattr(simple_example.app, '_cached_redis')
    simple_example.redis
    assert get_redis_mock.call_count == 1
    assert hasattr(simple_example.app, '_cached_redis')
    simple_example.redis
    assert hasattr(simple_example.app, '_cached_redis')
    # as simple_example.redis is a cached_property, get_redis has not been called again
    assert get_redis_mock.call_count == 1
