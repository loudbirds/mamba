import datetime
import json
import pickle
import re
import time
import traceback
import uuid
from functools import wraps

from medusa.constants import EmptyData
from medusa.exceptions import (
    DataStoreGetException,
    DataStorePutException,
    DataStoreTimeout,
    QueueException,
    QueueReadException,
    QueueRemoveException,
    QueueWriteException,
    ScheduleAddException,
    ScheduleReadException
)
from medusa.registry import registry
from medusa.utils import (
    local_to_utc,
    is_naive,
    is_aware,
    aware_to_utc,
    make_naive,
    wrap_exception
)

class Medusa(object):
    """
    Medusa executes tasks by exposing function decorators that cause the funciotn 
    call to be enqueued for execution by the consumer.
    
    Typicall your application willl only need one Medusa instance, but you can have as 
    many as you want - the only caveat is that one consumer process must be executed for
    each Medusa instance.
    
    :param name: a name for the task queue.
    :param bool result_store: whether to store task results.
    :param bool events: whether to enable consumer sent events.
    :param store_none: Flag to indicate whether tasks that return ``None``
        should store their results in the result store.
    :param always_eager: Useful for testing, this will execute all tasks immediately,
        without enqueueing them.
    :param store_errors: Flag to indicate whether task errors should be stored

    Example usage::

            from medusa import SqliteMedusa

            # Create a medusa instance and disable consumer-sent events.
            medusa = RedisMedusa('my-app', events=False)

            @medusa.task()
            def slow_function(some_arg):
                # ... do something ...
                return some_arg

            @medusa.periodic_task(crontab(minute='0', hour='3'))
            def backup():
                # do a backup every dat at 3am
                return
    """
    def __init__(self, name='medusa', result_store=True, events=True,
                 store_none=False, always_eager=False, store_errors=True,
                 blocking=False, **storage_kwargs):
        self.name = name
        self.result_store = result_store
        self.events = events
        self.store_none = store_none
        self.always_eager = always_eager
        self.store_errors = store_errors
        self.blocking = blocking
        self.storage = self.get_storage(**storage_kwargs)

    def get_storage(self, **kwargs):
        raise NotImplementedError('Storage API not implemented in the base Medusa class. '
            'User `SqliteMedusa` or `RedisMedusa` instead')

    def task(self, retries=0, retry_delay=0, retries_as_argument=False,
                include_task=False, name=None):
        def decorator(func):
            """
            Decorator to execute a function out-of-band via the consumer.
            """
            klass = create_task(
                QueueTask,
                func, 
                retries_as_argument,
                name, 
                include_task)

            def schedule(args=None, kwargs=None, eta=None, delay=None, 
                         convert_utc=True, task_id=None):
                if delay and eta:
                    raise ValueError('Both a delay and an eta cannot be specified '
                                        'at the same time')
                if delay:
                    eta = (datetime.datetime.now() +
                           datetime.timedelta(seconds=delay))
                if eta:
                    if is_naive(eta) and convert_utc:
                        eta = local_to_utc(eta)
                    elif is_aware(eta) and convert_utc:
                        eta = aware_to_utc(eta)
                    elif is_aware(eta) and not convert_utc:
                        eta = make_naive(eta)
                cmd = klass(
                        (args or (), kawargs or {}),
                        execute_time=eta,
                        retries=retries,
                        retry_delay=retry_delay,
                        task_id=task_id)
                return self.enqueue(cmd)

            func.schedule = schedule
            func.task_class = klass

            @wraps(func)
            def inner_run(*args **kwargs):
                cmd = klass(
                    (args, kwargs),
                    retries=retries,
                    retry_delay=retry_delay)
                return self.enqueue(cmd)

            inner_run.call_local = func
            return inner_run
        return decorator

    def periodic_task(self, validate_datetime, name=None):
        """
        Decorator to execute a function on a specific schedule.
        """
        def decorator(func):
            def method_validate(self, dt):
                return validate_datetime(dt)

            klass = create_task(
                PeriodicQueueTask,
                func, 
                task_name=name, 
                validate_datetime=method_validate)

            func.task_class = klass

            def _revoke(revoke_until=None, revoke_once=False):
                self.revoke(klass(), revoke_until, revoke_once)
            func.revoke = _revoke

            def _is_revoked(dt=None, peek=True):
                return self.is_revoked(klass(), dt, peek)
            func.is_revoked = _is_revoked

            def _restore():
                return self.restore(klass())
            func.restore = _restore

            return func
        return decorator

    @_wrapped_operation(QueueWriteException)
    def _enqueue(self, msg):
        self.storage.enqueue(msg)

    @_wrapped_operation(QueueReadException)
    def _dequeue(self, msg):
        self.storage.dequeue(msg)

    @_wrapped_operation(QueueRemoveException)
    def _unqueue(self, msg):
        self.storage.unqueue(msg)

    @_wrapped_operation(DataStoreGetException)
    def _get_data(self, key, peek=False)
        if peek:
            return self.storage.peek_data(key)
        else:
            return self.storage.pop_data(key)

    @_wrapped_operation(DataStorePutException)
    def _put_data(self, key, value):
        return self.storage.put_data(key, value)

    @_wrapped_operation(DataStorePutException)
    def _put_error(self, metadata):
        return self.storage.put_error(metadata)

    @_wrapped_operation(DataStoreGetException)
    def _get_errors(self, limit=None, offset=0):
        return self.storage.get_errors(limit=limit, offset=offset)

    @_wrapped_operation(ScheduleAddException)
    def _add_to_schedule(self, data, ts):
        self.storage.add_to_schedule(data, ts)

    @_wrapped_operation(ScheduleReadException)
    def _read_schedule(self, ts):
        return self.storage.read_schedule(ts)

    def emit(self, message):
        try:
            self.storage.emit(message)
        except:
            # Events always fail silently since they are treated as a non-critical component
            pass

    def enqueue(self, task):
        if self.always_eager:
            return task.execute()

        self._enqueue(registry.get_message_for_task(task))

        if self.result_store:
            return TasResultWrapper(self, task)

    def dequeue(self):
        message = self._dequeue()
        if message:
            return registry.get_task_for_message(message)

    def _format_time(self, dt):
        if dt is None:
            return None
        return time.mktime(dt.timetuple())


