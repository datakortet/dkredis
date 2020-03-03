# -*- coding: utf-8 -*-

"""Fast tests of dkredis (no timeout tests).
"""
import dkredis
import pytest


@pytest.fixture
def cn():
    return dkredis.connect()


def test_mhkeyget(cn):
    "Test of mhkeyget-function."
    assert cn.hmset('lock.a', {'x': 1})
    assert cn.hmset('lock.b', {'x': 2})
    assert cn.hmset('lock.c', {'x': 3})
    all_locks = {b'lock.a': b'1', b'lock.c': b'3', b'lock.b': b'2'}
    assert dkredis.mhkeyget('lock.*', 'x') == all_locks
    assert cn.delete('lock.a', 'lock.b', 'lock.c')


def test_update(cn):
    cn.set('testupdate', 42)
    assert dkredis.update('testupdate', lambda x: int(x)+42, cn=cn) == 84
    assert cn.get('testupdate') == b'84'
    cn.delete('testupdate')


def test_setmax(cn):
    cn.set('testsetmax', b'hello')
    assert dkredis.setmax('testsetmax', b'world', cn=cn) == b'world'


def test_setmin(cn):
    cn.set('testsetmax', b'hello')
    assert dkredis.setmin('testsetmax', b'world', cn=cn) == b'hello'


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
