import time
from contextlib import contextmanager

from .utils import (
    is_valid_identifier,
    unique_id,
    convert_to_bytes,
    later,
    now,
)
from .dkredis import connect, Timeout, remove_if


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
            remove_if(key, uniq, cn=r)
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


# XXX: [bp-2023-12-17] No idea what this is supposed to be used for, but it is
#      definitely not a mutex implementation...
@contextmanager
def mutex(name, seconds: int = 30, timeout: int = 60,
          unlock: bool = True, waitsecs: int = 3):
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
