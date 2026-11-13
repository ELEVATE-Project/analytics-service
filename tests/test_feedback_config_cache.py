"""
Unit tests (in-memory) of the consumer's feedback config cache (app/services/feedback_config_cache.py):
one load per persona per TTL, a new config version replaces the old entry after the TTL, "no config" and an
invalid config are cached for the TTL too, an infrastructure error is never cached.
"""
import asyncio

import pytest

from app.services.feedback_config_cache import FeedbackConfigCache

KEY = ("mitra", "program-1", "Youth_Voice", "mitra")


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def _cache(results, ttl=60.0):
    """results: list of values / exceptions the loader hands out in order."""
    clock = Clock()
    calls = []

    async def loader(*key):
        calls.append(key)
        result = results[len(calls) - 1]
        if isinstance(result, Exception):
            raise result
        return result

    return FeedbackConfigCache(loader, ttl_seconds=ttl, clock=clock), clock, calls


def test_a_burst_inside_the_ttl_loads_once():
    cache, clock, calls = _cache([{"config_version_id": "v1"}])
    for _ in range(100):
        assert asyncio.run(cache.get(*KEY)) == {"config_version_id": "v1"}
        clock.now += 0.1
    assert calls == [KEY] and cache.loads == 1


def test_entry_expires_after_the_ttl_and_a_new_version_replaces_it():
    cache, clock, calls = _cache([{"config_version_id": "v1"}, {"config_version_id": "v2"}, {"config_version_id": "v2"}])
    assert asyncio.run(cache.get(*KEY))["config_version_id"] == "v1"
    clock.now += 59.9
    assert asyncio.run(cache.get(*KEY))["config_version_id"] == "v1"  # still inside the TTL
    clock.now += 0.2
    assert asyncio.run(cache.get(*KEY))["config_version_id"] == "v2"  # reloaded: the new version wins
    assert cache.replaced_versions == 1
    clock.now += 61
    assert asyncio.run(cache.get(*KEY))["config_version_id"] == "v2"
    assert cache.replaced_versions == 1  # same version again: not a replacement
    assert cache.loads == 3


def test_keys_are_independent():
    cache, _, calls = _cache([{"config_version_id": "a"}, {"config_version_id": "b"}])
    asyncio.run(cache.get("mitra", "p", "Youth_Voice", "mitra"))
    asyncio.run(cache.get("mitra", "p", "Youth_Voice", "google_forms"))
    asyncio.run(cache.get("mitra", "p", "Youth_Voice", "mitra"))
    assert [c[3] for c in calls] == ["mitra", "google_forms"]


def test_missing_config_is_cached_for_the_ttl():
    cache, clock, calls = _cache([None, {"config_version_id": "v1"}])
    assert asyncio.run(cache.get(*KEY)) is None
    assert asyncio.run(cache.get(*KEY)) is None
    assert len(calls) == 1
    clock.now += 61
    assert asyncio.run(cache.get(*KEY)) == {"config_version_id": "v1"}  # the config was added meanwhile


def test_invalid_config_is_cached_and_raises_every_time():
    cache, clock, calls = _cache([ValueError("bad schema"), {"config_version_id": "v2"}])
    for _ in range(3):
        with pytest.raises(ValueError, match="bad schema"):
            asyncio.run(cache.get(*KEY))
    assert len(calls) == 1
    clock.now += 61
    assert asyncio.run(cache.get(*KEY)) == {"config_version_id": "v2"}  # fixed config is picked up


def test_infrastructure_errors_are_not_cached():
    cache, _, calls = _cache([ConnectionError("db down"), {"config_version_id": "v1"}])
    with pytest.raises(ConnectionError):
        asyncio.run(cache.get(*KEY))
    assert asyncio.run(cache.get(*KEY)) == {"config_version_id": "v1"}  # next event tries again immediately
    assert len(calls) == 2


def test_invalidate_forces_a_reload():
    cache, _, calls = _cache([{"config_version_id": "v1"}, {"config_version_id": "v2"}])
    asyncio.run(cache.get(*KEY))
    cache.invalidate()
    assert asyncio.run(cache.get(*KEY))["config_version_id"] == "v2"


@pytest.mark.parametrize("ttl", [0, -1])
def test_ttl_must_be_positive(ttl):
    with pytest.raises(ValueError):
        FeedbackConfigCache(lambda *k: None, ttl_seconds=ttl)
