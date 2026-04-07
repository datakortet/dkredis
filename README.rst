

dkredis - Python interface to Redis
====================================

.. image:: https://img.shields.io/pypi/v/dkredis.svg
   :target: https://pypi.python.org/pypi/dkredis
   :alt: Latest PyPI version

.. image:: https://github.com/datakortet/dkredis/actions/workflows/ci-cd.yml/badge.svg
   :target: https://github.com/datakortet/dkredis/actions/workflows/ci-cd.yml
   :alt: CI/CD Pipeline

.. image:: https://readthedocs.org/projects/dkredis/badge/?version=latest
    :target: https://dkredis.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status


.. image:: https://codecov.io/gh/datakortet/dkredis/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/datakortet/dkredis

A thin convenience wrapper around `redis-py <https://github.com/redis/redis-py>`_
for storing Python values (via pickle), working with Redis hashes, performing
atomic updates, and managing distributed locking primitives.

Installation
------------

::

    pip install dkredis

Usage
-----

Connecting
~~~~~~~~~~

.. code-block:: python

    from dkredis import dkredis

    r = dkredis.connect()

``connect()`` reads ``REDIS_HOST`` and ``REDIS_PASSWORD`` from environment
variables, defaulting to ``localhost:6379``.

Storing Python values
~~~~~~~~~~~~~~~~~~~~~

Any pickleable value can be stored and retrieved:

.. code-block:: python

    from dkredis.dkredis import set_pyval, get_pyval, pop_pyval

    set_pyval('mykey', {'answer': 42}, secs=300)  # expires in 5 minutes
    get_pyval('mykey')          # {'answer': 42}
    pop_pyval('mykey')          # {'answer': 42}, then deletes the key

Dict / hash operations
~~~~~~~~~~~~~~~~~~~~~~

Store and retrieve dicts as Redis hashes (string values only):

.. code-block:: python

    from dkredis.dkredis import set_dict, get_dict

    set_dict('user:1', {'name': 'Alice', 'role': 'admin'}, secs=600)
    get_dict('user:1')          # {'name': 'Alice', 'role': 'admin'}

Atomic updates
~~~~~~~~~~~~~~

``update()`` uses ``WATCH``/``MULTI`` pipelines for optimistic locking:

.. code-block:: python

    from dkredis.dkredis import update, setmax, setmin

    r.set('counter', 40)
    update('counter', lambda val: val + 2)  # atomically set to 42

    setmax('highscore', b'100')  # r[key] := max(r[key], val)
    setmin('lowscore', b'5')     # r[key] := min(r[key], val)

``remove_if()`` atomically deletes a key only if it holds an expected value
(implemented via a Lua script):

.. code-block:: python

    from dkredis.dkredis import remove_if

    remove_if('mykey', expected_value)

Locking primitives
~~~~~~~~~~~~~~~~~~

**fetch_lock** -- prevents thundering-herd on cache misses. Only one process
fetches fresh data; others receive ``False`` and should fall back to stale
cache:

.. code-block:: python

    from dkredis.dkredislocks import fetch_lock

    with fetch_lock('weatherapi', timeout=10) as should_fetch:
        if should_fetch:
            data = call_external_api()
            cache.set('weather', data, 60)
        else:
            time.sleep(1)
            data = cache.get('weather')

**rate_limiting_lock** -- sets multiple keys atomically with ``MSETNX`` to
enforce a per-resource cooldown period:

.. code-block:: python

    from dkredis.dkredislocks import rate_limiting_lock

    if rate_limiting_lock(['smtp.example.com'], seconds=15):
        send_email()

**mutex** -- a polling mutex using ``SETNX`` with expiry-based recovery:

.. code-block:: python

    from dkredis.dkredislocks import mutex

    with mutex('mylock', seconds=30, timeout=60):
        # mutual exclusion zone
        ...

Multi-hash field lookup
~~~~~~~~~~~~~~~~~~~~~~~

``mhkeyget()`` fetches one field from all hashes matching a key pattern:

.. code-block:: python

    from dkredis.dkredis import mhkeyget

    mhkeyget('lock.*', 'x')
    # {'lock.a': '1', 'lock.b': '2', 'lock.c': '3'}

Requirements
------------

- Python 3
- ``redis==5.0.1``
- A running Redis server (``localhost:6379`` by default)
