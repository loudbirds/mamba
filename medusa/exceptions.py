class QueueException(Exception):
    def __init__(self, queue, msg=None):
        if msg is None:
            msg = "Queue exception: {}".format(queue)
        super(QueueException, self).__init__(msg)


class QueueWriteException(QueueException):
    def __init__(self, queue, msg=None):
        if msg is None:
            msg = "Queue write exception: {}".format(queue)
        super(QueueWriteException, self).__init__(msg)


class QueueReadException(QueueException):
    def __init__(self, queue, msg=None):
        if msg is None:
            msg = "Queue read exception: {}".format(queue)
        super(QueueReadException, self).__init__(msg)


class QueueRemoveException(QueueException):
    def __init__(self, queue, msg=None):
        if msg is None:
            msg = "Queue remove exception: {}".format(queue)
        super(QueueRemoveException, self).__init__(msg)


class DataStoreGetException(QueueException):
    def __init__(self, queue, msg=None):
        if msg is None:
            msg = "Data store get exception: {}".format(queue)
        super(DataStoreGetException, self).__init__(msg)


class DataStorePutException(QueueException):
    def __init__(self, queue, msg=None):
        if msg is None:
            msg = "Data store put exception: {}".format(queue)
        super(DataStorePutException, self).__init__(msg)


class DataStoreTimeout(QueueException):
    def __init__(self, queue, msg=None):
        if msg is None:
            msg = "Data store timeout exception: {}".format(queue)
        super(DataStoreTimeout, self).__init__(msg)


class ScheduleAddException(QueueException):
    def __init__(self, queue, msg=None):
        if msg is None:
            msg = "Schedule add exception: {}".format(queue)
        super(ScheduleAddException, self).__init__(msg)


class ScheduleReadException(QueueException):
    def __init__(self, queue, msg=None):
        if msg is None:
            msg = "Schedule read exception: {}".format(queue)
        super(ScheduleReadException, self).__init__(msg)


class ConfigurationError(QueueException):
    def __init__(self, queue, msg=None):
        if msg is None:
            msg = "Configuration error exception: {}".format(queue)
        super(ConfigurationError, self).__init__(msg)
