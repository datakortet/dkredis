"""Fast tests of dkredis (no timeout tests).
"""
import os
import dkredis
import pytest

import dkredis.utils


@pytest.fixture
def cn():
    return dkredis.connect()


def test_now_later():
    assert dkredis.utils.now() < dkredis.utils.later(5)


def test_convert_to_bytes():
    # Test when input is already bytes
    bytes_input = b'test bytes'
    assert dkredis.utils.convert_to_bytes(bytes_input) == bytes_input

    # Test when input is a string
    str_input = 'test string'
    assert dkredis.utils.convert_to_bytes(str_input) == str_input.encode('utf-8')

    # Test when input is an int
    int_input = 123
    assert dkredis.utils.convert_to_bytes(int_input) == str(int_input).encode('utf-8')

    # Test when input is a complex object e.g. a dictionary
    dict_input = {'key': 'value'}
    assert dkredis.utils.convert_to_bytes(dict_input) == str(dict_input).encode('utf-8')


def test_unique_id_is_unique():
    ids = {dkredis.utils.unique_id(fast=True) for _ in range(1000)}
    assert len(ids) == 1000

    ids = {dkredis.utils.unique_id(fast=False) for _ in range(1000)}
    assert len(ids) == 1000


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
    # cn.set('testsetmax', 1)
    # assert dkredis.setmax('testsetmax', 2, cn=cn) == 2


def test_setmin(cn):
    cn.set('testsetmax', 'hello')
    assert dkredis.setmin('testsetmax', 'world', cn=cn) == 'hello'
    # cn.set('testsetmax', 1)
    # assert dkredis.setmin('testsetmax', 2, cn=cn) == 1


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
    assert dkredis.get_dict('testdict', cn=cn) == {'hello': 'world'}
