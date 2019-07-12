# -*- coding: utf-8 -*-

"""Interface to our redis instance.

   Redis command reference: http://redis.io/commands

   Usage::

       from dksys import dkredis
       r = dkredis.connect()
       r.set('foo', 'bar')
       r.get('foo')
       r.delete('foo')

   Python interface peculiarities are explained here:
   https://github.com/andymccurdy/redis-py

   The windows version of the redis server that we use is
   from https://github.com/rgl/redis/downloads
   ----
"""
import os
import pickle
import time
import redis as _redis
from contextlib import contextmanager


class Timeout(Exception):  # pragma: nocover
    """A timout limit was exceeded.
    """

    def __init__(self, retry=0):
        super(Timeout, self).__init__()
        self.retry = retry


# class Redis(object):
#     def __init__(self, host='localhost', port=6379, db=0, cn=None):
#         self.host = host
#         self.port = port
#         self.db = db
#         self._cn = cn
#
#     def _get_connection(self):
#         """Return a connection to the redis server.
#         """
#         if not self._cn:
#             self._cn = _redis.StrictRedis(
#                 host=self.host,
#                 port=self.port,
#                 db=self.db
#             )
#         return self._cn
#
#     @property
#     def cn(self):
#         return self._get_connection()
#
#
# class DurationMixin(object):
#     """Mixin class to set the duration of key lifetimes.
#     """
#
#
# class JsonRedis(Redis):
#     """Redis class that stores and retrieves values serialized as json.
#     """
#
#     def __getitem__(self, item):
#         pass
#
#     def set_pyval(key, val, secs=None, cn=None):
#         """Store any (picleable) value in Redis.
#         """
#         r = self.cn
#         pval = pickle.dumps(val)
#         if secs is None:
#             r.set(key, pval)
#         else:
#             r.setex(key, secs, pval)
#
#     def get_pyval(key, cn=None, missing_value=None):
#         """Get a Python value from Redis.
#         """
#         r = cn or connect()
#         val = r.get(key)
#         if val is None:  # pragma: nocover
#             return missing_value  # value if key is missing
#         # print "dkredis:get_pyval:VAL:%s:" % val
#         return pickle.loads(val)
#
#     def pop_pyval(key, cn=None):
#         """Get value and remove key.
#         """
#         r = cn or connect()
#         val = get_pyval(key, cn=r)
#         r.delete(key)
#         # with r.pipeline() as p:
#         #     val = get_pyval(key, p)  # can't get a val in the middle of a pipeline
#         #     p.delete(key)
#         #     p.execute()
#         return val
#
#     def update(self, key, fn):
#         """Atomic update of ``key`` with result of ``fn``.
#
#            Usage::
#
#                r = Redis()
#                r.set('foo', 40)
#                r.update('foo', lambda val: val + 2)
#                r.get('foo')   # ==> 42
#
#         """
#         with self.cn.pipeline() as p:
#             while 1:
#                 try:
#                     p.watch(key)  # --> immediate mode
#                     val = p.get(key)
#                     p.multi()  # --> back to buffered mode
#                     newval = fn(val)
#                     p.set(key, newval)
#                     p.execute()  # raises WatchError if anyone has changed `key`
#                     break  # success, break out of while loop
#                 except _redis.WatchError:  # pragma: nocover
#                     pass  # someone else got there before us, retry.
#         return newval


def connect(host=None, port=6379, db=0, password=None):
    """Return a connection to the redis server.
    """
    if host is None:
        host = os.environ.get('REDIS_HOST', 'localhost')
    if password is None:
        password = os.environ.get('REDIS_PASSWORD')
    return _redis.StrictRedis(host=host, port=port, db=db, password=password)


# def update_binop(binaryfn):
#     """Utility function for creating binary update functions.
#     """
#     def curry(key, val, cn=None):
#         return update(key, (lambda v: binaryfn(v, val)), cn=cn)
#     return curry
#
#
# #  Usage:  setmax(KEY, val)
# #          setmin(KEY, val)
# setmax = update_binop(max)
# setmin = update_binop(min)
#
#
# def setmin_cutoff(cutoff):
#     """setmin_cutoff(KEY, val, cutoff)
#        ==> min(r[KEY], val) if less than cutoff
#     """
#     def fn(a, b):
#         return max(cutoff, min(a, b))
#     return update_binop(fn)
#
# setmin_cut = lambda cut: lambda a, b: update_binop(lambda a,b: max(cut, min(a, b)))
# setmax_cut = lambda cut: lambda a, b: update_binop(lambda a,b: min(cut, max(a, b)))
#
# setmin0 = setmin_cut(0)
# setmax0 = setmax_cut(0)
#
# def setmax_cutoff(key,
#     """setmin_cutoff(KEY, val, cutoff)
#        ==> max(r[KEY], val) if greater than cutoff
#     """
#     return update_binop(lambda a,b: min(cutoff, max(a, b)))
#
#
# #: set to minimum of
# setmin0 = lambda a,b: setmin_cutoff(a, b


def update(key, fn, cn=None):
    """Usage::

           update(KEY, lambda val: val + 42)

    """
    r = cn or connect()
    with r.pipeline() as p:
        while 1:
            try:
                p.watch(key)  # --> immediate mode
                val = p.get(key)
                p.multi()  # --> back to buffered mode
                newval = fn(val)
                p.set(key, newval)
                p.execute()  # raises WatchError if anyone has changed `key`
                break  # success, break out of while loop
            except _redis.WatchError:  # pragma: nocover
                pass  # someone else got there before us, retry.
    return newval


def setmax(key, val, cn=None):
    """Update key with the max of the current value and `val`::

          r[key] := max(r[key], val)

       returns the maximum value.
    """
    return update(key, lambda v: max(v, val), cn=cn)


def setmin(key, val, cn=None):
    """Update key with the max of the current value and `val`::

          r[key] := min(r[key], val)

       returns the maximum value.
    """
    return update(key, lambda v: min(v, val), cn=cn)


def later(n=0.0):
    """Return timestamp ``n`` seconds from now.
    """
    return time.time() + n


def now():
    """Return timestamp.
    """
    return later(0)


def set_pyval(key, val, secs=None, cn=None):
    """Store any (picleable) value in Redis.
    """
    r = cn or connect()
    pval = pickle.dumps(val)
    if secs is None:
        r.set(key, pval)
    else:
        r.setex(key, secs, pval)


def get_pyval(key, cn=None, missing_value=None):
    """Get a Python value from Redis.
    """
    r = cn or connect()
    val = r.get(key)
    if val is None:  # pragma: nocover
        return missing_value  # value if key is missing
    # print "dkredis:get_pyval:VAL:%s:" % val
    return pickle.loads(val)


def pop_pyval(key, cn=None):
    """Get value and remove key.
    """
    r = cn or connect()
    val = get_pyval(key, cn=r)
    r.delete(key)
    # with r.pipeline() as p:
    #     val = get_pyval(key, p)  # can't get a val in the middle of a pipeline
    #     p.delete(key)
    #     p.execute()
    return val


def set_dict(key, dictval, secs=None, cn=None):
    """All values in `dictval` should be strings. They'll be read back
       as strings -- use `py_setval` to set dicts with any values.
    """
    r = cn or connect()
    r.hmset(key, dictval)
    if secs is not None:
        r.expire(key, secs)


def get_dict(key, cn=None):
    """Return a redis hash as a python dict.
    """
    r = cn or connect()
    res = {}
    for fieldname in r.hkeys(key):
        res[fieldname] = r.hget(key, fieldname)
    return res


def mhkeyget(keypattern, field, cn=None):
    """Get a field from multiple hashes.

       Usage::

         >>> r = connect()
         >>> r.hmset('lock.a', {'x': 1})
         True
         >>> r.hmset('lock.b', {'x': 2})
         True
         >>> r.hmset('lock.c', {'x': 3})
         True
         >>> mhkeyget('lock.*', 'x')
         {'lock.a': '1', 'lock.c': '3', 'lock.b': '2'}

         # cleanup
         >>> #r.delete('lock.a', 'lock.b', 'lock.c')
         True

    """
    result = {}
    r = cn or connect()

    hashes = r.keys(keypattern)
    for h in hashes:
        result[h] = r.hget(h, field)

    return result


def rate_limiting_lock(resources, seconds=30, cn=None):
    """Lock all the keys and keep them locked for ``seconds`` seconds.
       Useful e.g. to prevent sending email to the same domain more often
       than every 15 seconds.

       XXX: Currently doesn't recover from crashed clients (can be done as
            an else: clause to the if r.msetnx(), similarly to the mutex
            function (below).

    """
    if not resources:
        return True

    keys = dict(('rl-lock.' + r, later(seconds)) for r in resources)
    r = cn or connect()

    if r.msetnx(keys):
        with r.pipeline() as pipe:
            for key in keys:
                pipe.expire(key, seconds)
            pipe.execute()
        return True

    return False


@contextmanager
def mutex(name, seconds=30, timeout=60, unlock=True, waitsecs=3):
    """Lock the ``name`` for ``seconds``, waiting ``waitsecs`` seconds
       between each attempt at locking the name.  Locking means creating
       a key 'lock.' + key.

       It will raise a Timeout exception if more than ``timeout`` seconds
       has elapsed.

       Usage::

           from dksys import dkredis

           with dkredis.lock('mymutex'):
               # mutual exclusion zone ;-)

    """
    # the various time.time() calls can happen at different times.
    if timeout == 0:
        timeout = 60 * 60  # 1 hour

    start = now()
    r = connect()
    k = 'lock.' + name
    expire = start

    try:
        while 1:
            if start + timeout < now():
                raise Timeout()

            expire = later(seconds)
            if r.setnx(k, expire):
                # we have the lock, yield to the context, then exit.
                yield
                break

            # we didn't get the lock, but it exists...
            if float(r.get(k)) > now():
                # the lock is still valid (someone else has the lock).
                time.sleep(waitsecs)

            else:
                # lock has expired, try to grab it...
                expire = later(timeout)
                ts = float(r.getset(k, expire))
                if ts < now:
                    # we won, yield to the context, then exit.
                    yield
                    break
                # else start again, from the top.

    finally:
        if unlock and expire < now():
            # we should unlock and our lock hasn't expired.
            r.delete(k)
