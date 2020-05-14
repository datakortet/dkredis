# -*- coding: utf-8 -*-
"""Object cache implementation using redis as a backend.
"""
from __future__ import absolute_import
import pickle
import hashlib
import zlib
import base64
from . import dkredis


PICLE_PROTOCOL = 0


def _cache_serialize(val):
    """Serialize a python value to go into the cache.
    """
    return base64.b64encode(zlib.compress(pickle.dumps(val,protocol=PICLE_PROTOCOL)))


def _cache_unserialize(val):
    """Unserialize a python value from the cache.
    """
    return pickle.loads(zlib.decompress(base64.b64decode(val)))


class cache(object):
    """Usage::

         from dkredis.rediscache import cache

         try:
             v = cache.get(key)
         except cache.DoesNotExist:
             v = mk_object_value(...)
             cache.put(
                 key, v,
                 duration=secs)  # or datetime.duration()

          you can use a datetime value for the valid_until parameter,
          or anything the timeperiod.when() function accepts.

    """

    class DoesNotExist(Exception):
        "Value not in cache (possibly due to expiration)."

    @staticmethod
    def rediskey(key):
        "The redis key is obj-cache. + the md5 hexdigest of its serialization."
        k = _cache_serialize(key)
        return "obj-cache." + hashlib.md5(k).hexdigest()

    @classmethod
    def ping(cls):
        r = dkredis.connect()
        r.ping()

    @classmethod
    def remove(cls, key):
        "Remove key from cache."
        r = dkredis.connect()
        r.delete(cls.rediskey(key))

    @classmethod
    def put(cls, key, value, duration=None):
        "Put ``value`` in cache, under ``key``, for ``duration`` seconds."
        if duration is None:
            _duration = 30 * 60  # 30 minutes
        elif hasattr(duration, 'to_int'):
            # ttcal.Duration
            _duration = duration.to_int()   # pragma: nocover
        elif hasattr(duration, 'days') and hasattr(duration, 'seconds'):
            # datetime.timedelta
            _duration = duration.days * 24 * 60 * 60 + duration.seconds
        else:
            _duration = int(duration)  # try int conversion, throws ValueError
        assert _duration >= 1  # smallest cache duration is +1 second

        cls.remove(key)

        k = cls.rediskey(key)
        v = _cache_serialize(value)

        r = dkredis.connect()
        r.setex(k, _duration, v)

    @classmethod
    def _raw_get(cls, key):
        r = dkredis.connect()
        return r.get(cls.rediskey(key))
    
    @classmethod
    def get(cls, key):
        "Fetch value for ``key`` from redis."
        val = cls._raw_get(key)
        if val is not None:
            return _cache_unserialize(val)
        raise cls.DoesNotExist(
            "Value not in cache (possibly due to expiration).")

    @classmethod
    def get_value(cls, key, default=None):
        try:
            return cls.get(key)
        except cls.DoesNotExist:
            return default


class djangocache(object):
    "Django facade to the rediscache."
    
    @classmethod
    def get(cls, key, default=None):
        return cache.get_value(key, default)

    @classmethod
    def set(cls, key, value, duration):
        cache.put(key, value, duration)


class Cached(object):
    """Mixin class to invalidate cache keys on model.save().

       Usage::

           class MyModel(Cached, models.Model):   # models.Model must be last
               @property
               def cache_keys(self):
                   return [...]
               ...

       (that's it.  All cache keys will be removed whenever `MyModel.save()` is
       called).
    """
    cache_keys = []

    def save(self, *args, **kwargs):
        for ckey in self.cache_keys:
            cache.remove(ckey)
        super(Cached, self).save(*args, **kwargs)

    def get_cache_values(self):
        """Get all cached values for this object.

           .. Note:: returns None both for keys that are None and
                     non-existant keys!
        """
        return {ckey: cache.get_value(ckey) for ckey in self.cache_keys}


def cached(cache_key=None, timeout=3600):
    """Function result cache decorator.

       Usage::

           @cached()
           def can_view_user(user, username):
               ...
               return True   # will be cached for 1hr (3600 secs)

           class MenuItem(models.Model):
               @classmethod
               @cached('menu_root', 3600*24)
               def get_root(self):
                   return MenuItem.objects.get(pk=1)

           @cached(lambda u: 'user_privileges_%s' % u.username, 3600)
           def get_user_privileges(user):
               #...
    """
    def _cached(func):
        def do_cache(*args, **kws):
            if isinstance(cache_key, str):
                key = cache_key % locals()
            elif callable(cache_key):
                key = cache_key(*args, **kws)
            else:
                key = hashlib.sha1(
                    (str(func.__module__)
                    + str(func.__name__)
                    + str(args)
                    + str(kws)).encode('ascii')
                ).hexdigest()
            # key = "FNCACHED-" + key
            try:
                return cache.get(key)
            except cache.DoesNotExist:
                data = func(*args, **kws)
                cache.put(key, data, timeout)
                return data
        return do_cache
    return _cached
