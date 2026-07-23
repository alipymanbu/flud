import pytest

from flud.protocol.ClientDHTPrimitives import (
    _AlphaController,
    _LookupFrontier,
    _ValueAccumulator,
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
