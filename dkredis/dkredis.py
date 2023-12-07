"""
Interface to our redis instance.

Redis command reference: http://redis.io/commands

Usage::

   from dkredis import dkredis
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
import threading
import time
import uuid

import redis as _redis
from contextlib import contextmanager

PICLE_PROTOCOL = 1


class Timeout(Exception):  # pragma: nocover
    """A timout limit was exceeded.
    """

    def __init__(self, retry=0):
        super().__init__()
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
#         #     # can't get a val in the middle of a pipeline
#         #     val = get_pyval(key, p)
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
#                     # raises WatchError if anyone has changed `key`
#                     p.execute()
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
# setmin_cut = lambda cut: lambda a, b: update_binop(
#                                           lambda a,b: max(cut, min(a, b)))
# setmax_cut = lambda cut: lambda a, b: update_binop(
#                                           lambda a,b: min(cut, max(a, b)))
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
    """Usage
       ::

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
    if isinstance(newval, bytes):
        newval = newval.decode('u8')
    return newval


def setmax(key, val, cn=None):
    """Update key with the max of the current value and `val`::

          r[key] := max(r[key], val)

       returns the maximum value.
    """
    if isinstance(val, str):
        val = val.encode('u8')
    return update(key, lambda v: max(v, val), cn=cn)


def setmin(key, val, cn=None):
    """Update key with the max of the current value and `val`::

          r[key] := min(r[key], val)

       returns the maximum value.
    """
    if isinstance(val, str):
        val = val.encode('u8')
    return update(key, lambda v: min(v, val), cn=cn)


def later(n=0.0):
    """Return timestamp ``n`` seconds from now.
    """
    return time.time() + n


def now():
    """Return timestamp.
    """
    return later(0)


_last_ts = 0


def unique_id(fast=True):
    """Return a unique id.
    """
    if not fast:
        # this picks up randomness from /dev/urandom which can be very slow.
        return str(uuid.uuid4().hex)
    # machine-id:thread-id:time-in-ns
    global _last_ts
    _ts = time.time_ns() + _last_ts
    _last_ts += 1
    return f'{uuid.getnode()}:{threading.get_ident()}:{_ts}'


def is_valid_identifier(s: str) -> bool:
    """Return True if s is a valid python identifier.
    """
    return s.isidentifier() and s.islower()


def set_pyval(key, val, secs=None, cn=None):
    """Store any (picleable) value in Redis.
    """
    r = cn or connect()
    pval = pickle.dumps(val, protocol=PICLE_PROTOCOL)
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
    #     # can't get a val in the middle of a pipeline
    #     val = get_pyval(key, p)
    #     p.delete(key)
    #     p.execute()
    return val


def set_dict(key, dictval, secs=None, cn=None):
    """All values in `dictval` should be strings. They'll be read back
       as strings -- use `py_setval` to set dicts with any values.
    """
    r = cn or connect()
    r.hset(key, mapping=dictval)
    if secs is not None:
        r.expire(key, secs)


def get_dict(key, cn=None):
    """Return a redis hash as a python dict.
    """
    r = cn or connect()
    res = {}
    for fieldname in r.hkeys(key):
        res[fieldname] = r.hget(key, fieldname)
    return {k.decode('u8'): v.decode('u8') for k, v in res.items()}


def mhkeyget(keypattern, field, cn=None):
    """Get a field from multiple hashes.

       Usage::

         >>> r = connect()
         >>> r.hset('lock.a', {'x': 1})
         True
         >>> r.hset('lock.b', {'x': 2})
         True
         >>> r.hset('lock.c', {'x': 3})
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
    # XXX: redis always returns bytes, and we want unicode
    return {k.decode('u8'): v.decode('u8') for k, v in result.items()}


def convert_to_bytes(r):
    """Converts the input object to bytes.

       Parameters:
           r (object): The input object to convert.

       Returns:
           bytes: The converted object as bytes. If the input object is
                  already of type 'bytes', it is returned as is.

                  If the input object is of type 'str', it is encoded to
                  bytes using the 'utf-8' encoding.

                  For any other input object, it is converted to a string
                  and then encoded to bytes using the 'utf-8' encoding.
    """
    if isinstance(r, bytes):
        return r
    if isinstance(r, str):
        return r.encode('utf-8')
    return str(r).encode('utf-8')


@contextmanager
def fetch_lock(apiname: str, timeout=5, cn=None):
    """Use this lock to ensure that only one process is fetching
       expired cached data from an external api.

       It is important to have a timeout on the lock, so it will be released
       even if the process crashes.

       A process that doesn't get the lock should not wait for the lock, but
       should wait and try using the cached data instead.

       Usage::

            def get_weather_data():
                try:
                    return cache.get('weatherdata')
                except cache.DoesNotExist:
                    with fetch_lock('weatherapi') as should_fetch:
                        if should_fetch:
                            weatherdata = fetch_weather_data()
                            cache.put('weatherdata', weatherdata, 60)
                            return weatherdata
                        else:
                            # another process is fetching data, wait for it
                            time.sleep(1)
                            return cache.get_value('weatherdata', default=None)

    """
    if not is_valid_identifier(apiname):
        raise ValueError(
            f'`apiname` must be a valid lower-case python identifier, '
            f'got {apiname}'
        )
    key = f'dkredis:fetchlock:{apiname}'
    uniq = unique_id()
    r = cn or connect()
    if r.set(key, value=uniq, ex=timeout, nx=True):
        # We have the lock:
        # if set(..nx=True) returns True, then our value was set, and we have
        # the lock, yield to the context, then exit.

        try:
            yield True      # the client should do the fetch

        finally:
            # Release the lock:
            # The lock can have timed out while we were in the context, so we
            # need to check that we still have the lock before deleting it.
            # The get + del needs to be atomic, so we have to use a lua script.
            # See https://redis.io/commands/eval
            script = """
                if redis.call("get", KEYS[1]) == ARGV[1] then
                    return redis.call("del", KEYS[1])
                else
                    return 0
                end
            """
            r.eval(script, 1, key, uniq)
            return
    else:
        # Lock is already held by another process
        yield False    # the client should not do the fetch


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

    resources = [convert_to_bytes(r) for r in resources]
    keys = {b'rl-lock.' + r: later(seconds) for r in resources}

    r = cn or connect()

    if r.msetnx(keys):
        with r.pipeline() as pipe:
            for key in keys:
                pipe.expire(key, seconds)
            pipe.execute()
        return True

    return False


@contextmanager
def mutex(name, seconds: int=30, timeout: int=60, unlock: bool=True, waitsecs: int=3):
    """Lock the ``name`` for ``seconds``, waiting ``waitsecs`` seconds
       between each attempt at locking the name.  Locking means creating
       a key 'dkredis:mutex:' + key.

       It will raise a Timeout exception if more than ``timeout`` seconds
       has elapsed.

       Usage::

           from dkredis import dkredis

           with dkredis.lock('mymutex'):
               # mutual exclusion zone ;-)

    """
    # the various time.time() calls can happen at different times.
    if timeout == 0:
        timeout = 60 * 60  # 1 hour

    prefix = 'dkredis:mutex:'

    start = now()
    r = connect()
    k = f'{prefix}{name}'
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
                if ts < now():
                    # we won, yield to the context, then exit.
                    yield
                    break
                # else start again, from the top.

    finally:
        if unlock and expire < now():
            # we should unlock, and our lock hasn't expired.
            r.delete(k)
