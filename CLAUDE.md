# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is dkredis?

`dkredis` is a thin Python wrapper around the `redis-py` library, providing convenience functions for storing Python values (via pickle), working with Redis hashes, performing atomic updates, and managing distributed locking primitives. It is a foundation-level package with no intra-`/srv` dependencies.

Part of the `/srv` ecosystem (~150+ repos). See `/srv/CLAUDE.md` for environment setup and global conventions. See `/srv/DEVELOPING.md` for development guidelines.

## Commands

```bash
# Run tests (creates/uses the dkredis venv)
dk testpackage

# Run pytest directly
/srv/venv/dkredis/Scripts/pytest.exe tests/

# Run only fast tests (no sleep-based timing tests)
/srv/venv/dkredis/Scripts/pytest.exe tests/test_dkredis_fast.py tests/test_fetchlock.py tests/test_dkredis_import.py

# Lint
dk pylint
```

Tests require a running Redis instance on `localhost:6379`. Start it with `/srv/bin/start-redis.bat` if needed.

## Architecture

### Key modules

- **`dkredis/dkredis.py`** — Core module. `connect()` returns a `redis.StrictRedis` connection (reads `REDIS_HOST` / `REDIS_PASSWORD` from environment). Provides `set_pyval`/`get_pyval`/`pop_pyval` (pickle-based), `set_dict`/`get_dict` (Redis hashes), `update` (atomic read-modify-write via WATCH/MULTI pipeline), `setmax`/`setmin`, `remove`, `remove_if` (atomic Lua-script conditional delete), and `mhkeyget` (fetch a field from multiple hash keys matching a pattern).

- **`dkredis/dkredislocks.py`** — Distributed locking primitives:
  - `fetch_lock(apiname, timeout)` — context manager that prevents thundering-herd on cache misses. Only one process fetches; others receive `False` and should wait on stale cache.
  - `rate_limiting_lock(resources, seconds)` — sets multiple keys atomically with `msetnx` to enforce a per-resource rate limit.
  - `mutex(name, seconds, timeout)` — a polling mutex using `setnx`.

- **`dkredis/utils.py`** — Small helpers: `unique_id()` (machine+thread+nanosecond ID for lock tokens), `now()`/`later(n)` (Unix timestamps), `convert_to_bytes()`, `is_valid_identifier()`.

### Dependencies

None within `/srv`. Only runtime dependency is `redis==5.0.1`.

### Data flow

Callers call `connect()` to get a raw `StrictRedis` connection, then pass it as `cn=` to the module-level functions to avoid repeated reconnects. Values stored via `set_pyval` are pickled with protocol 1; `get_pyval` unpickles on read. Dict values are stored as Redis hashes (string fields only); `get_dict` decodes all keys and values from bytes to UTF-8 strings.

The `fetch_lock` context manager uses Redis `SET key value EX timeout NX` (atomic set-if-not-exists with expiry) for lock acquisition, and a Lua script for atomic conditional delete on release, preventing accidental release of an expired-and-reacquired lock.
