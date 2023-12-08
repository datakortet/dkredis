"""
Test that all modules are importable.
"""

import dkredis
import dkredis.dkredis
import dkredis.rediscache


def test_import_dkredis():
    """Test that all modules are importable.
    """
    
    assert dkredis
    assert dkredis.dkredis
    assert dkredis.rediscache
