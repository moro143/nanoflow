"""Directed acyclic graph of task nodes."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from nanoflow.errors import CycleError


@dataclass
class Node:
    id: str
    task: Any  # nanoflow.task.Task
    args: tuple
    kwargs: dict
    upstream: set[str] = field(default_factory=set)
    downstream: set[str] = field(default_factory=set)


class Graph:
    def __init__(self) -> None:
        self.nodes: dict[str, Node] = {}
        self._order: list[str] = []  # insertion order, used for stable ids

    def add(self, node: Node) -> None:
        if node.id in self.nodes:
            raise ValueError(f"duplicate node id {node.id!r}")
        self.nodes[node.id] = node
        self._order.append(node.id)
        for up in node.upstream:
            self.nodes[up].downstream.add(node.id)

    def roots(self) -> list[str]:
        return [n for n in self._order if not self.nodes[n].upstream]

    def topological_order(self) -> list[str]:
        """Kahn's algorithm, preserving definition order among ready nodes."""
        indeg = {nid: len(n.upstream) for nid, n in self.nodes.items()}
        ready = [nid for nid in self._order if indeg[nid] == 0]
        out: list[str] = []
        while ready:
            nid = ready.pop(0)
            out.append(nid)
            for d in sorted(self.nodes[nid].downstream, key=self._order.index):
                indeg[d] -= 1
                if indeg[d] == 0:
                    ready.append(d)
        if len(out) != len(self.nodes):
            remaining = [n for n in self._order if n not in out]
            raise CycleError(f"cycle detected among nodes: {remaining}")
        return out

    def to_dict(self) -> dict[str, Any]:
        return {
            "nodes": [
                {
                    "id": n.id,
                    "task": n.task.name,
                    "upstream": sorted(n.upstream),
                    "tags": n.task.tags,
                }
                for n in (self.nodes[i] for i in self._order)
            ]
        }

    def to_mermaid(self) -> str:
        lines = ["graph TD"]
        for nid in self._order:
            n = self.nodes[nid]
            lines.append(f'    {nid}["{n.task.name}"]')
        for nid in self._order:
            for d in sorted(self.nodes[nid].downstream):
                lines.append(f"    {nid} --> {d}")
        return "\n".join(lines)
