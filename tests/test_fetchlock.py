from multiprocessing.pool import ThreadPool
import time
from uuid import uuid1, uuid4
from dkredis.dkredis import unique_id, fetch_lock


import pytest
from unittest.mock import Mock, call, ANY


@pytest.fixture()
def redis_mock():
    return Mock()


def test_fetch_lock_acquired(redis_mock):
    redis_mock.set.return_value = True

    with fetch_lock('weatherapi', cn=redis_mock) as should_fetch:
        assert redis_mock.set.call_args == call('lock:si:weatherapi', value=ANY, ex=5, nx=True)
        assert should_fetch

    assert redis_mock.eval.called


def test_fetch_lock_not_acquired(redis_mock):
    redis_mock.set.return_value = False

    with fetch_lock('weatherapi', cn=redis_mock) as should_fetch:
        assert redis_mock.set.call_args == call('lock:si:weatherapi', value=ANY, ex=5, nx=True)
        assert not should_fetch

    assert not redis_mock.eval.called


def test_fetch_lock_invalid_apiname(redis_mock):
    with pytest.raises(ValueError) as exc_info:
        with fetch_lock('Invalid Apiname', cn=redis_mock):
            pass

    assert '`apiname` must be a valid lower-case python identifier, got Invalid Apiname' in str(exc_info.value)


def uniqueid_test(n=100000):
    s = set()
    for i in range(n):
        s.add(unique_id())
    assert len(s) == n


def fetch_lock_test(n=1):
    with fetch_lock('test', 4) as lock:
        if lock:
            print('have lock test', n)
            if n == 0:
                raise ValueError('test')
            time.sleep(1)
            print('test', n)
            return n
        else:
            print(f'no lock test: {n}')
    return 0


def test_gallopping_herd():
    with ThreadPool(4) as pool:
        res = pool.map(fetch_lock_test, range(5))
    assert sum(res) > 0
    time.sleep(2)
    with ThreadPool(4) as pool:
        res = pool.map(fetch_lock_test, range(5))
    assert sum(res) > 0
