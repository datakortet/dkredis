# -*- coding: utf-8 -*-

"dkredis"

# pylint:disable=R0904
# R0904: Too many public methods
import pytest
import dkredis
import time



##@pytest.mark('very-slow')
# @pytest.mark.skip
# def test_rate_limiting_lock():
#     "Test of rate_limiting_lock-function."
#     assert dkredis.rate_limiting_lock({'x2': 1}, 4) is True
#     assert dkredis.rate_limiting_lock({'x2': 1}, 2) is False
#     time.sleep(5)
#     assert dkredis.rate_limiting_lock({'x2': 1}, 4) is True


# def xtest_mutex():
#     "Test of mutex-function"
#     # TODO: Not working as expected yet. Need to revisit this later.
#     try:
#         self.assert_(self.r.hmset('lock.d', {'x': 1}))
#         with dkredis.mutex('d', 10) as lock:
#             self.assertEqual(lock, None)
#         self.assertEqual(self.r.hmset('lock.d', {'x': 1}), False)
#         time.sleep(10)
#         self.assertEqual(self.r.hmset('lock.d', {'x': 5}), True)
#         with dkredis.mutex('e') as lock:
#             self.assertEqual(lock, True)
#     finally:
#         self.assert_(self.r.delete('lock.d', 'lock.e'))
