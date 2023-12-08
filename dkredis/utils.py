import threading
import time
import uuid


def later(n=0.0):
    """Return timestamp ``n`` seconds from now.
    """
    return time.time() + n


def now():
    """Return timestamp.
    """
    return later(0)


_last_ts = 0


def unique_id(fast=True):
    """Return a unique id.
    """
    if not fast:
        # this picks up randomness from /dev/urandom which can be very slow.
        return str(uuid.uuid4().hex)
    # machine-id:thread-id:time-in-ns
    global _last_ts
    _ts = time.time_ns() + _last_ts
    _last_ts += 1
    return f'{uuid.getnode()}:{threading.get_ident()}:{_ts}'


def is_valid_identifier(s: str) -> bool:
    """Return True if s is a valid python identifier.
    """
    return s.isidentifier() and s.islower()


def convert_to_bytes(r):
    """Converts the input object to bytes.

       Parameters:
           r (object): The input object to convert.

       Returns:
           bytes: The converted object as bytes. If the input object is
                  already of type 'bytes', it is returned as is.

                  If the input object is of type 'str', it is encoded to
                  bytes using the 'utf-8' encoding.

                  For any other input object, it is converted to a string
                  and then encoded to bytes using the 'utf-8' encoding.
    """
    if isinstance(r, bytes):
        return r
    if isinstance(r, str):
        return r.encode('utf-8')
    return str(r).encode('utf-8')
