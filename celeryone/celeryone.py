
from celery import Task, task
from celery.result import AsyncResult
from inspect import getcallargs

import base64
from .helpers import queue_one_key, get_redis, now_unix, cached_celery_property

class AlreadyQueued(Exception):
    def __init__(self, task_id, countdown):
        self.message = "Task {} expires in {} seconds".format(task_id, countdown)
        self.countdown = countdown
        self.task_id = task_id


class QueueOne(Task):
    AlreadyQueued = AlreadyQueued
    one_options = {
        'fail': False,
        'unlock_before_run': False,
        'use_id': False
    }

    """
    'There can be only one'. - Highlander (1986)

    An abstract tasks with the ability to detect if it has already been queued.
    When running the task (through .delay/.apply_async) it checks if the tasks
    is not already queued.
    """
    abstract = True
    one_options = {}

    @property
    def config(self):
        app = self._get_app()
        return app.conf

    @cached_celery_property
    def redis(self):
        """
        Return a connection pool to redis.

        This property is cached at the celery app level, so that only the first
        executed QueueOne task needs to instanciate a redis connection. The
        other ones will only re-use it.

        """
        return get_redis(
            getattr(self.config, "ONE_REDIS_URL", "redis://localhost:6379/0"))

    @property
    def default_timeout(self):
        return getattr(
            self.config, "ONE_DEFAULT_TIMEOUT", 60 * 60)

    def apply_async(self, args=None, kwargs=None, **options):
        """
        Queues a task using its task_id, raises an exception by default if already queued.
        The task_id must be set for this task type.

        :param \*args: positional arguments passed on to the task.
        :param \*\*kwargs: keyword arguments passed on to the task.
        :keyword \*\*one_options: (optional)
            :param: fail: (optional)
                If True, wouldn't raise an exception if already queued.
                Instead will return AsyncResult(task_id).
            :param: timeout: (optional)
                An `int' number of seconds after which the lock will expire.
                If not set, defaults to 1 hour.
            :param: use_id: (optional)
                If True, it will try to use the task_id to identify a same running task

        """

        # Has the user set the id?
        has_no_id = 'task_id' not in options

        task_id = options.get('task_id', None)

        if task_id:
            task_id = str(task_id)

        # Is this task part of a group?
        is_in_chord = 'chord' in options

        one_options = options.get('one_options', {})
        use_id = one_options.get('use_id', self.one_options.get('use_id', False))
        must_fail = one_options.get('fail', self.one_options.get('fail', True))
        timeout = one_options.get('timeout', self.one_options.get('expires', self.default_timeout))
        key = self.get_key(one_options, task_id, args, kwargs)

        if not use_id and not task_id:
            task_id = self.generate_id_from_key(key)

        if has_no_id:
            options['task_id'] = task_id
        celery_uno_data = {'task_id': task_id, 'expires': timeout}

        try:
            self.raise_or_lock(key, celery_uno_data)
        except self.AlreadyQueued as e:
            if not must_fail:
                if is_in_chord:
                    return no_op.apply_async(**options)
                else:
                    return AsyncResult(e.task_id)
            raise e
        return super(QueueOne, self).apply_async(args, kwargs, **options)

    def generate_id_from_key(self, key):
        """
        Generate a b64 representation of the queue one key
        :param key:
        :return:
        """
        return 'qoid_' + base64.b64encode((self.name + key).encode('utf-8')).decode()

    def get_key(self, one_options, task_id, args, kwargs):
        use_id = one_options.get('use_id', self.one_options.get('use_id', False))
        # The user can specify whether he wants to use the task id or the parameters
        if use_id:
            if not task_id:
                raise ValueError('Asked to use the task_id but the task_id is not set')
            key = self.get_key_from_id(task_id)
        else:
            key = self.get_key_from_args(args, kwargs)

        return key

    def get_key_from_id(self, task_id=None):
        """
        Generate the key from the id of the task
        """
        if not task_id:
            raise ValueError('Task id cannot be null')

        keys = ['qo', self.name, str(task_id)]
        return '_'.join(keys)

    def get_key_from_args(self, args=None, kwargs=None):
        """
        Generate the key from the name of the task (e.g. 'tasks.example') and
        args/kwargs.
        """
        restrict_to = self.one_options.get('keys', None)
        args = args or {}
        kwargs = kwargs or {}
        call_args = getcallargs(self.run, *args, **kwargs)

        # Remove the task instance from the kwargs. This only happens when the
        # task has the 'bind' attribute set to True. We remove it, as the task
        # has a memory pointer in its repr, that will change between the task
        # caller and the celery worker
        if isinstance(call_args.get('self'), Task):
            del call_args['self']
        key = queue_one_key(self.name, call_args, restrict_to)
        return key

    def raise_or_lock(self, key, queue_one_data):
        """
        Checks if the task is locked and raises an exception, else locks
        the task.
        """
        now = now_unix()
        # Check if the task is already queued.
        running_task = self.redis.hgetall(key)

        if bool(running_task):
            running_task_id = running_task.get(b'task_id', None).decode()
            running_expire_time = running_task.get(b'expires', None)

            if running_expire_time:
                running_expire_time = running_expire_time.decode()
                remaining = int(running_expire_time) - now
                if remaining > 0:
                    raise self.AlreadyQueued(running_task_id, remaining)
            elif running_task_id:
                raise self.AlreadyQueued(running_task_id, None)

        expire_time = queue_one_data['expires']

        # If the user requires an expire time we set it manually on the hset
        if expire_time:
            timeout = queue_one_data['expires']
            queue_one_data['expires'] += now

            self.redis.hmset(key, queue_one_data)

            self.redis.expire(key, timeout)
        else:
            self.redis.hmset(key, queue_one_data)

    def get_unlock_before_run(self):
        return self.one_options.get('unlock_before_run', False)

    def clear_lock(self, key):
        self.redis.delete(key)

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        """
        After a task has run (both successfully or with a failure) clear the
        lock if "unlock_before_run" is False.
        """
        # Only clear the lock after the task's execution if the
        # "unlock_before_run" option is False
        if not self.get_unlock_before_run():
            key = self.get_key(self.one_options, task_id, args, kwargs)
            self.clear_lock(key)

    def __call__(self, *args, **kwargs):
        # Only clear the lock before the task's execution if the
        # "unlock_before_run" option is True

        if self.get_unlock_before_run():
            key = self.get_key(self.one_options, kwargs.get('task_id', None), args, kwargs)
            self.clear_lock(key)

        return super(QueueOne, self).__call__(*args, **kwargs)

@task(name='no_op')
def no_op():
    """
    This task is used to perform an empty operation. It is used to make celery uno compatible with
    groups.
    :return:
    """
    pass
