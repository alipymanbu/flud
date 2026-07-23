import pytest

from flud.protocol.ClientDHTPrimitives import (
    _AlphaController,
    _LookupFrontier,
    _TTLCache,
    _ValueAccumulator,
    _normalize_read_quorum,
    _normalize_write_quorum,
)


def test_alpha_controller_fixed_mode_stays_constant():
    controller = _AlphaController(4, "fixed")
    assert controller.alpha(frontier_size=20, in_flight=0) == 4
    controller.note_result(False)
    controller.note_result(True)
    assert controller.alpha(frontier_size=2, in_flight=0) == 4


def test_alpha_controller_adaptive_mode_moves_with_results():
    controller = _AlphaController(3, "adaptive")
    baseline = controller.alpha(frontier_size=10, in_flight=0)
    for _ in range(4):
        controller.note_result(True)
    increased = controller.alpha(frontier_size=10, in_flight=0)
    assert increased >= baseline
    for _ in range(4):
        controller.note_result(False)
    decreased = controller.alpha(frontier_size=10, in_flight=0)
    assert decreased <= increased
    assert decreased >= 1


def test_value_accumulator_first_returns_immediately():
    values = _ValueAccumulator()
    values.observe("v1")
    should_return, value = values.should_return("first")
    assert should_return is True
    assert value == "v1"


def test_value_accumulator_majority_waits_for_match():
    values = _ValueAccumulator()
    values.observe("v1")
    assert values.should_return("majority") == (False, None)
    values.observe("v2")
    assert values.should_return("majority") == (False, None)
    values.observe("v1")
    should_return, value = values.should_return("majority")
    assert should_return is True
    assert value == "v1"


def test_lookup_frontier_dedupes_and_orders_by_distance():
    key = 16
    frontier = _LookupFrontier(key)
    frontier.add_many([
        ("127.0.0.1", 1, 30),
        ("127.0.0.1", 2, 18),
        ("127.0.0.1", 3, 22),
        ("127.0.0.1", 9, 18),
    ])
    pending = frontier.pending(set(), set())
    assert [candidate[2] for candidate in pending] == [18, 22, 30]


def test_lookup_frontier_best_k_excludes_failed_nodes():
    key = 64
    frontier = _LookupFrontier(key)
    frontier.add_many([
        ("127.0.0.1", 1, 65),
        ("127.0.0.1", 2, 66),
        ("127.0.0.1", 3, 67),
    ])
    best = frontier.best_k(exclude_ids={65})
    assert [candidate[2] for candidate in best[:2]] == [66, 67]


def test_lookup_frontier_pending_excludes_failed_nodes():
    # Regression test: a candidate that has already failed must not keep
    # reappearing in pending() -- previously failed_ids wasn't passed to
    # pending()/has_better_pending() at all, so a permanently unreachable
    # node in the routing table caused an infinite retry loop (it's never
    # added to queried_ids since it never succeeds, and it's no longer
    # in_flight once its attempt completes).
    key = 16
    frontier = _LookupFrontier(key)
    frontier.add_many([
        ("127.0.0.1", 1, 30),
        ("127.0.0.1", 2, 18),  # this one will be "dead"
        ("127.0.0.1", 3, 22),
    ])
    pending = frontier.pending(set(), set(), failed_ids={18})
    assert [candidate[2] for candidate in pending] == [22, 30]


def test_lookup_frontier_has_better_pending_excludes_failed_nodes():
    key = 16
    frontier = _LookupFrontier(key)
    frontier.add_many([("127.0.0.1", 2, 18)])
    # Without failed_ids, the sole (dead) candidate looks like it's still
    # worth pursuing; with failed_ids, there's nothing left to try.
    assert frontier.has_better_pending(None, set(), set()) is True
    assert frontier.has_better_pending(None, set(), set(), failed_ids={18}) is False


# --- A1: write quorum -------------------------------------------------

def test_normalize_write_quorum_defaults_to_majority():
    assert _normalize_write_quorum(None, 12) == 7
    assert _normalize_write_quorum(None, 1) == 1
    assert _normalize_write_quorum(None, 2) == 2


def test_normalize_write_quorum_respects_explicit_value_and_caps_to_n():
    assert _normalize_write_quorum(3, 12) == 3
    assert _normalize_write_quorum(50, 12) == 12
    assert _normalize_write_quorum(0, 12) == 1


# --- A3: read quorum + read-repair -------------------------------------

def test_normalize_read_quorum_satisfies_w_plus_r_over_n():
    write_quorum = _normalize_write_quorum(None, 12)
    read_quorum = _normalize_read_quorum(None, 12, write_quorum)
    assert write_quorum + read_quorum > 12


def test_value_accumulator_quorum_policy_waits_for_explicit_count():
    values = _ValueAccumulator()
    values.observe("v1")
    assert values.should_return("quorum", quorum=3) == (False, None)
    values.observe("v1")
    assert values.should_return("quorum", quorum=3) == (False, None)
    values.observe("v1")
    should_return, value = values.should_return("quorum", quorum=3)
    assert should_return is True
    assert value == "v1"


def test_value_accumulator_tracks_stale_responders_for_read_repair():
    values = _ValueAccumulator()
    values.observe("stale", responder=("127.0.0.1", 1))
    values.observe("fresh", responder=("127.0.0.1", 2))
    values.observe("fresh", responder=("127.0.0.1", 3))
    assert values.stale_responders("fresh") == [("127.0.0.1", 1)]
    assert values.stale_responders("stale") == [("127.0.0.1", 2), ("127.0.0.1", 3)]


# --- A5: reputation-aware ordering --------------------------------------

def test_lookup_frontier_demotes_negative_reputation_nodes():
    key = 16
    reputations = {18: -5}
    frontier = _LookupFrontier(key, reputation_lookup=reputations.get)
    frontier.add_many([
        ("127.0.0.1", 1, 30),
        ("127.0.0.1", 2, 18),  # closest by distance, but bad reputation
        ("127.0.0.1", 3, 22),
    ])
    pending = frontier.pending(set(), set())
    # 18 is XOR-closest to key=16 but has negative reputation, so it's
    # demoted behind neutral-reputation candidates despite being farther.
    assert [candidate[2] for candidate in pending] == [22, 30, 18]


def test_lookup_frontier_no_reputation_lookup_is_pure_distance_order():
    key = 16
    frontier = _LookupFrontier(key)
    frontier.add_many([
        ("127.0.0.1", 1, 30),
        ("127.0.0.1", 2, 18),
        ("127.0.0.1", 3, 22),
    ])
    pending = frontier.pending(set(), set())
    assert [candidate[2] for candidate in pending] == [18, 22, 30]


# --- A4: client-side TTL cache -------------------------------------------

def test_ttl_cache_hit_and_expiry(monkeypatch):
    import flud.protocol.ClientDHTPrimitives as dht_primitives

    clock = {"now": 1000.0}
    monkeypatch.setattr(dht_primitives.time, "monotonic", lambda: clock["now"])

    cache = _TTLCache(ttl_seconds=10)
    assert cache.get("k1") == (False, None)

    cache.set("k1", "v1")
    assert cache.get("k1") == (True, "v1")

    clock["now"] += 11
    assert cache.get("k1") == (False, None)


def test_ttl_cache_disabled_when_ttl_is_zero():
    cache = _TTLCache(ttl_seconds=0)
    cache.set("k1", "v1")
    assert cache.get("k1") == (False, None)
