# -*- coding: utf-8 -*-

"""Fast tests of dkredis (no timeout tests).
"""
import dkredis
import pytest


@pytest.fixture
def cn():
    return dkredis.connect()


def test_now_later():
    assert dkredis.now() < dkredis.later(5)


def test_mhkeyget(cn):
    "Test of mhkeyget-function."
    for key in cn.scan_iter("lock.*"):
        cn.delete(key)
    assert cn.hset('lock.a', key='x', value=1)
    assert cn.hset('lock.b', key='x', value=2)
    assert cn.hset('lock.c', key='x', value=3)
    # assert cn.hset('lock.b', {'x': 2})
    # assert cn.hset('lock.c', {'x': 3})
    all_locks = {'lock.a': '1', 'lock.c': '3', 'lock.b': '2'}
    assert dkredis.mhkeyget('lock.*', 'x') == all_locks
    assert cn.delete('lock.a', 'lock.b', 'lock.c')


def test_update(cn):
    cn.set('testupdate', 42)
    assert dkredis.update('testupdate', lambda x: int(x)+42, cn=cn) == 84
    assert cn.get('testupdate') == b'84'
    cn.delete('testupdate')


def test_setmax(cn):
    cn.set('testsetmax', 'hello')
    assert dkredis.setmax('testsetmax', 'world', cn=cn) == 'world'


def test_setmin(cn):
    cn.set('testsetmax', 'hello')
    assert dkredis.setmin('testsetmax', 'world', cn=cn) == 'hello'


def test_pyval():
    dkredis.set_pyval('pyval', 40, secs=5)
    assert dkredis.get_pyval('pyval') + 2 == 42


def test_pop_pyval(cn):
    dkredis.set_pyval('testpoppyval', 42)
    v = dkredis.pop_pyval('testpoppyval', cn=cn)
    assert v == 42
    assert cn.keys('testpoppyval') == []


def test_dict(cn):
    dkredis.set_dict('testdict', dict(hello='world'), secs=5, cn=cn)
    assert dkredis.get_dict('testdict', cn=cn) == {b'hello': b'world'}
