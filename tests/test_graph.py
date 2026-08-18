import pytest

from nanoflow import CycleError, task
from nanoflow.graph import Graph, Node


@task
def noop():
    return None


def test_topological_order_respects_definition_order_among_ready_nodes():
    g = Graph()
    g.add(Node("a", noop, (), {}))
    g.add(Node("b", noop, (), {}))
    g.add(Node("c", noop, (), {}, upstream={"a"}))
    assert g.topological_order() == ["a", "b", "c"]


def test_cycle_detection():
    g = Graph()
    g.add(Node("a", noop, (), {}))
    g.add(Node("b", noop, (), {}, upstream={"a"}))
    g.nodes["a"].upstream.add("b")
    with pytest.raises(CycleError):
        g.topological_order()


def test_duplicate_node_id_raises():
    g = Graph()
    g.add(Node("a", noop, (), {}))
    with pytest.raises(ValueError):
        g.add(Node("a", noop, (), {}))


def test_roots_returns_nodes_without_upstream_in_definition_order():
    g = Graph()
    g.add(Node("a", noop, (), {}))
    g.add(Node("b", noop, (), {}, upstream={"a"}))
    g.add(Node("c", noop, (), {}))
    assert g.roots() == ["a", "c"]


def test_to_dict_reports_nodes_upstream_and_tags():
    @task(tags={"team": "data"})
    def tagged():
        return None

    g = Graph()
    g.add(Node("a", tagged, (), {}))
    g.add(Node("b", tagged, (), {}, upstream={"a"}))
    assert g.to_dict() == {
        "nodes": [
            {"id": "a", "task": "tagged", "upstream": [], "tags": {"team": "data"}},
            {"id": "b", "task": "tagged", "upstream": ["a"], "tags": {"team": "data"}},
        ]
    }
