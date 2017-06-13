import json
import re
import time
import operator
import threading
from peewee import *
import abc

# todo: test for redis, if redis is not available use alternative storage, SQLITE
# import redis
# from redis.exception Import ConnectioError

from medusa.api import Medusa
from medusa.constants import EmptyData





class BaseStorage(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, name="medusa", **storage_kwargs):
        self.name = name

    @abc.abstractmethod
    def enqueue(self, data):
        return

    @abc.abstractmethod
    def dqueue(self):
        return

    @abc.abstractmethod
    def unqueue(self, data):
        return

    @abc.abstractmethod
    def queue_size(self):
        return

    @abc.abstractmethod
    def enqueued_items(self, limit=None):
        return

    @abc.abstractmethod
    def flush_queue(self):
        return

    @abc.abstractmethod
    def add_to_schedule(self, data, ts):
        return

    @abc.abstractmethod
    def read_schedule(self, ts):
        return

    @abc.abstractmethod
    def schedule_size(self):
        return

    @abc.abstractmethod
    def scheduled_items(self, limit=None):
        return

    @abc.abstractmethod
    def flush_schedule(self):
        return

    @abc.abstractmethod
    def put_data(self, key, value):
        return

    @abc.abstractmethod
    def peek_data(self, key):
        return

    @abc.abstractmethod
    def pop_data(self, key):
        return

    @abc.abstractmethod
    def has_data_for_key(self, key):
        return

    @abc.abstractmethod
    def result_store_size(self):
        return

    @abc.abstractmethod
    def result_items(self):
        return

    @abc.abstractmethod
    def flush_results(self):
        return

    @abc.abstractmethod
    def put_error(self, metadata):
        return

    @abc.abstractmethod
    def get_errors(self, limit=None, offset=0):
        return

    @abc.abstractmethod
    def flush_errors(self):
        return

    @abc.abstractmethod
    def emit(self, message):
        return

    @abc.abstractmethod
    def __iter__(self):
        return

    @abc.abstractmethod
    def flush_all(self):
        self.flush_queue()
        self.flush_schedule()
        self.flush_results()
        self.flush_errors()


class Task(Model):
    """
    The task in the queue
    """
    queue = CharField()
    data = BlobField()


class Schedule(Model):
    """
    The schedule of the Task
    """
    queue = CharField()
    data = BlobField()
    timestamp = TimestampField()


class KeyValue(Model):
    """
    The key/value of the Task
    """
    queue = CharField()
    key = CharField()
    value = BlobField()

class SqliteStorage(BaseStorage):
    def __init__(self, name='medusa', filename='medusa.db', **storage_kwargs):
        super(SqliteStorage, self).__init__(name)
        self._filename = filename
        self._database = SqliteDatabase(filename, **storage_kwargs)
        self.initialize_task_table()

    def initialize_task_table(self):
        Task._meta.database = self._database
        Schedule._meta.database = self._database
        KeyValue._meta.database = self._database
        self._database.create_tables([Task, Schedule, KeyValue], True)

    def delete(self):
        return Task.delete().where(Task.queue == self.name)

    def tasks(self, *columns):
        return Task.select(*columns).where(Task.queue == self.name)

    def schedule(self, *columns):
        return Schedule.select(*columns)\
            .where(Schedule.queue == self.name)\
            .order_by(Schedule.timestamp)

    def kv(self, *columns):
        return KeyValue.select(*columns).where(KeyValue.queue == self.name)

    """
    Queue
    """
    def enqueue(self, data):
        Task.create(queue=self.name, data=data)

    def dequeue(self):
        try:
            task = (self.tasks().order_by(Task.id).limit(1).get())
        except Task.DoesNotExist:
            return
        res = self.delete().where(Task.id == task.id).execute()
        if res == 1:
            return task.data

    def unqueue(self, data):
        return self\
            .delete()\
            .where(Task.data == data)\
            .execute()

    def queue_size(self):
        return self.tasks().count()

    def enqueued_items(self, limit=None):
        query = self.tasks(Task.data).tuples()
        if limit is not None:
            query = query.limit(limit)
        return map(operator.itemgetter(0), query)

    def flush_queue(self):
        self.delete().execute()


    """
    Schedule
    """
    def add_to_schedule(self, data, ts):
        Schedule.create(data=data, timestamp=ts, queue=self.name)

    def read_schedule(self, ts):
        tasks = self\
            .schedule(Schedule.id, Schedule.data)\
            .where(Schedule.timestamp <= ts)\
            .tuples()
        id_list, data = [], []
        for task_id, task_data in tasks:
            id_list.append(task_id)
            data.append(task_data)
        if id_list:
            Schedule\
                .delete()\
                .where(Schedule.id << id_list)\
                .execute()
        return data

    def schedule_size(self):
        # todo: convert this to more idiomatic python __size___
        # todo: move this into its own object
        return self.schedule().count()

    def scheduled_items(self, limit=None):
        tasks = self\
            .schedule(Schedule.data)\
            .order_by(Schedule.timestamp)\
            .tuples()
        return map(operator.itemgetter(0), tasks)

    def flush_schedule(self):
        return Schedule.delete().where(Schedule.queue == self.name).execute()

    """
    Data
    """
    def put_data(self, key, value):
        KeyValue.create(queue=self.name, key=key, value=value)

    def peek_data(self, key):
        try:
            kv = self.
