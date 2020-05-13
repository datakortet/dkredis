import datetime, time

from dkredis.rediscache import cache, djangocache, cached


@cached(timeout=5)
def can_view_user():
    time.sleep(1)
    return time.time()   # will be cached for 1hr (3600 secs)


def test_cached():
    assert can_view_user() == can_view_user() == can_view_user()


def test_unit():
    "cache unit tests."
    # pylint:disable=W0212
    import time

    cache.put('foo', 42, 1)
    assert cache.get('foo') == 42

    cache.remove('bar')  # remove non-existing key.

    time.sleep(1.6)
    assert cache._raw_get('foo') is None

    cache.put('foo', 42)
    cache.remove('foo')
    assert cache._raw_get('foo') is None


def test_put():
    val = {42, 'hello'}
    key = 'tstput'
    cache.put(key, val, duration=datetime.timedelta(seconds=5))
    assert cache.get(key) == val


def test_ping():
    cache.ping()
    assert 1  # didn't throw


def test_get_value():
    assert cache.get_value('missing', 42) == 42


def test_djangocache():
    val = {42, 'hello'}
    key = 'testdjcache'
    djangocache.set(key, val, 15)
    assert djangocache.get(key) == val
