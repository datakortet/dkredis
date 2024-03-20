"""
Test that all modules are importable.
"""

import dkredis
import dkredis.dkredis
import dkredis.dkredislocks
import dkredis.rediscache
import dkredis.utils


def test_import_dkredis():
    """Test that all modules are importable.
    """
    
    assert dkredis
    assert dkredis.dkredis
    assert dkredis.dkredislocks
    assert dkredis.rediscache
    assert dkredis.utils
