import datetime
import sys
import time


class UTC(datetime.tzinfo):
    """
    UTC implementation taken from Python's docs/
    Used only whn pytz isnt't available.
    """
    def __repr__(self):
        return "<UTC>"

    def utcoffset(self, dt):
        _ = (dt,)
        return datetime.timedelta(0)

    def tzname(self, dt):
        _ = (dt,)
        return "UTC"

    def dst(self, dt):
        _ = (dt,)
        return datetime.timedelta(0)


def is_naive(dt):
    """
    Determines if a given datetime.datetime is naive.
    The concept is defined in Python's docs:
    http://docs.python.org/library/datetime.html#datetime.tzinfo
    Assuming value.tzinfo is either None or a proper datetime.tzinfo,
    value.utcoffset() implements the appropriate logic.
    :param dt:
    :return:
    """
    return dt.utcoffset() is None


def is_aware(dt):
    """

    :param dt:
    :return:
    """
    return not is_naive(dt)


def load_class(s):
    """

    :param s:
    :return:
    """
    path, klass = s.rsplit('.', 1)
    __import__(path)
    mod = sys.modules[path]
    return getattr(mod, klass)


def wrap_exception(new_exc_class):
    """

    :param new_exc_class:
    :return:
    """
    exc_class,  exc, tb = sys.exc_info()
    raise new_exc_class("{}: {}".format(exc_class.__name__, exc))


def make_naive(dt):
    """
    Makes an aware datetime.datetime naive in its time zone.
    :param dt:
    :return:
    """
    return dt.replace(tzinfo=None)


def aware_to_utc(dt):
    """
    Converts an aware datetime.datetime naive in its time
    :param dt:
    :return:
    """
    dt = dt.astimezone(UTC())
    assert not is_naive(dt), 'Must be a time zone aware datetime'
    return make_naive(dt)


def local_to_utc(dt):
    """

    :param dt:
    :return:
    """
    return datetime.datetime(*time.gmtime(time.mktime(dt.timetuple())))[:6]

