Celery One [DEPRECATED]
===========

|Build Status|

Celery One allows you to prevent multiple execution and queuing of `celery <http://www.celeryproject.org/>`_ tasks.
The project is a fork of `celery-once <https://github.com/TrackMaven/celery-once>`_ to which I give all credits
for the initial architecture and part of this README :). The project was created to add more functionality to the original version.
``celery_one`` is compatible with ``celery_once``'s previous behaviour if properly configured.

The main differences are the following:

* Option to prevent tasks with same id
* Option to get an AsyncResult if the task is already queue
* Compatibility with chords and groups
* Optional infinite timeout
* Redis connection pool

Installation
============

To install ``celery_one`` with pip just run:

::

     pip install -e git+https://github.com/szandara/celery-one.git#egg=celery_one


Requirements
============

* `Celery <http://www.celeryproject.org/>`_. Built to run with Celery 3.1. Older versions may work, but are not officially supported.
* `Redis <http://redis.io/>`_ is used as a distributed locking mechanism.

Simple Usage
=====

To use ``celery_one``, your tasks need to inherit from an `abstract <http://celery.readthedocs.org/en/latest/userguide/tasks.html#abstract-classes>`_ base task called ``QueueOne``.

You may need to tune the following Celery configuration options...

    * ``ONE_REDIS_URL`` should point towards a running Redis instance (defaults to ``redis://localhost:6379/0``)
    * ``ONE_DEFAULT_TIMEOUT`` how many seconds after a lock has been set before it should automatically timeout (defaults to 3600 seconds, or 1 hour).


.. code:: python

    from celery import Celery
    from celery_one import QueueOne
    from time import sleep

    celery = Celery('tasks', broker='amqp://guest@localhost//')
    celery.conf.ONE_REDIS_URL = 'redis://localhost:6379/0'
    celery.conf.ONE_DEFAULT_TIMEOUT = 60 * 60

    @celery.task(base=QueueOne)
    def slow_task():
        sleep(2)
        return "Done!"


Behind the scenes, this overrides ``apply_async`` and ``delay``. To do so, ``celery_one`` uses Redis. Each time a task
the user calls an asynchronous task, it locks a key into Redis. If a key with a same key is pushed to the queue, its
execution is prevented by either raising an ``AlreadyQueued`` exception or returning the AsyncResult of the already running task.
The key of the task is generated from its name and parameters.

.. code:: python

    example.delay(10)
    example.delay(10)
    Traceback (most recent call last):
        ..
    AlreadyQueued()

.. code:: python

    result1 = example.apply_async(args=(10,))
    result2 = example.apply_async(args=(10,), one_options={'fail':False}) # No failure here
    assert result1.id == result2.id

    result = example.apply_async(args=(10,))
    Traceback (most recent call last):
        ..
    AlreadyQueued()

Options
=====

``use_id``
------------

If this option is set, the key will be generated using the id of the task. This can be useful when working
with meaningful task ids or when the arguments are not necessarily indication of different tasks.

.. code:: python

    @celery.task(base=QueueOne, one_options={'use_id':True})
    def slow_task_no_fail(a):
        print("Running")
        sleep(10)
        return "Done: " + str(a)

    result1 = slow_task_no_fail.apply_async(args=(10), task_id=id1)
    result2 = slow_task_no_fail.apply_async(args=(12), task_id=id1)

    Traceback (most recent call last):
        ..
    AlreadyQueued()


``fail``
------------

Optionally, instead of raising an ``AlreadyQueued`` exception, the task can return an `AsyncResult <http://docs.celeryproject.org/en/latest/reference/celery.result.html>`_.
To do so, set the option in the celery task or directly in the ``apply_async`` call.

.. code:: python

    @celery.task(base=QueueOne, one_options={'fail':False})
    def slow_task_no_fail():
        print("Running")
        sleep(2)
        return "Done!"

    result1 = slow_task_no_fail.apply_async(args=(10))
    result2 = slow_task_no_fail.apply_async(args=(10))

    print(result1.get())
    print(result2.get())

    Output:

    Running
    Done!
    Done!


``keys``
--------

By default ``QueueOne`` creates a lock based on the task's name and its arguments and values.
Take for example, the following task below...

.. code:: python

    @celery.task(base=QueueOne)
    def slow_add(a, b):
        sleep(2)
        return a + b

Running the task with different arguments will default to checking against different locks.

.. code:: python

    slow_add(1, 1)
    slow_add(1, 2)

If you want to specify locking based on a subset, or no arguments you can adjust the keys ``celery_one`` looks at in the task's `options <http://celery.readthedocs.org/en/latest/userguide/tasks.html#list-of-options>`_ with ``one_options={'keys': [..]}``

.. code:: python

    @celery.task(base=QueueOne, one_options={'keys': ['a']})
    def slow_add(a, b):
        sleep(30)
        return a + b

    example.delay(1, 1)
    # Checks if any tasks are running with the `a=1`
    example.delay(1, 2)
    Traceback (most recent call last):
        ..
    AlreadyQueued()
    example.delay(2, 2)

.. code:: python

    @celery.task(base=QueueOne, one_options={'keys': []})
    def slow_add(a, b):
        sleep(30)
        return a + b

    # Will enforce only one task can run, no matter what arguments.
    example.delay(1, 1)
    example.delay(2, 2)
    Traceback (most recent call last):
        ..
    AlreadyQueued()


``timeout``
-----------
As a fall back, ``celery_one`` will clear a lock after 60 minutes.
This is set globally in Celery's configuration with ``ONE_DEFAULT_TIMEOUT`` but can be set for individual tasks using...

.. code:: python

    @celery.task(base=QueueOne, one_options={'timeout': 60 * 60 * 10})
    def long_running_task():
        sleep(60 * 60 * 3)

``timeout`` can also be set to None, causing ``celery_one`` to prevent adding same tasks until the already running one
is complete. *NOTE*: This might result in dangerous behaviors such as deadlocks or failing task executions. Use with care!

``unlock_before_run``
---------------------

By default, the lock is removed after the task has executed (using celery's `after_return <https://celery.readthedocs.org/en/latest/reference/celery.app.task.html#celery.app.task.Task.after_return>`_). This behaviour can be changed setting the task's option ``unlock_before_run``. When set to ``True``, the lock will be removed just before executing the task.

**Caveat**: any retry of the task won't re-enable the lock!

.. code:: python

    @celery.task(base=QueueOne, one_options={'unlock_before_run': True})
    def slow_task():
        sleep(30)
        return "Done!"


Support
=======

* Tests are run against Python 2.7 and 3.3. Other versions may work, but are not officially supported.

Contributing
============

Contributions are welcome, and they are greatly appreciated! See `contributing
guide <CONTRIBUTING.rst>`_ for more details.


.. |Build Status| image:: https://travis-ci.org/szandara/celery-one.svg
   :target: https://travis-ci.org/szandara/celery-one
