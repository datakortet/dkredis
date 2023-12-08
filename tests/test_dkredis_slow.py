"dkredis"

# pylint:disable=R0904
# R0904: Too many public methods
import pytest
import dkredis
import time

import dkredis.dkredislocks


def test_rate_limiting_lock():
    "Test of rate_limiting_lock-function."
    assert dkredis.dkredislocks.rate_limiting_lock([], 0.5) is True
    assert dkredis.dkredislocks.rate_limiting_lock({'x2': 1}, 1) is True
    assert dkredis.dkredislocks.rate_limiting_lock({'x2': 1}, 0.5) is False
    time.sleep(1.5)
    assert dkredis.dkredislocks.rate_limiting_lock({'x2': 1}, 4) is True


def test_mutex():
    with dkredis.dkredislocks.mutex('d', 1) as lock:
        assert lock is None
