# src/paxos.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True, order=True)
class Ballot:
    depth: int
    seq: int
    proc_id: int

    def to_dict(self) -> Dict[str, int]:
        return {
            "depth": self.depth,
            "seq": self.seq,
            "proc_id": self.proc_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, int]) -> "Ballot":
        return cls(
            depth=data["depth"],
            seq=data["seq"],
            proc_id=data["proc_id"],
        )

    def __str__(self) -> str:
        return f"(d={self.depth}, s={self.seq}, p={self.proc_id})"


@dataclass
class AcceptorState:
    promised: Optional[Ballot] = None
    accepted_ballot: Optional[Ballot] = None
    accepted_value: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "promised": self.promised.to_dict() if self.promised else None,
            "accepted_ballot": (
                self.accepted_ballot.to_dict()
                if self.accepted_ballot
                else None
            ),
            "accepted_value": self.accepted_value,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AcceptorState":
        promised = data.get("promised")
        accepted_ballot = data.get("accepted_ballot")
        accepted_value = data.get("accepted_value")

        return cls(
            promised=Ballot.from_dict(promised) if promised else None,
            accepted_ballot=(
                Ballot.from_dict(accepted_ballot)
                if accepted_ballot
                else None
            ),
            accepted_value=accepted_value,
        )


class PaxosState:
    def __init__(self, node_id: int):
        self.node_id = node_id
        # depth -> AcceptorState
        self._acceptors: Dict[int, AcceptorState] = {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_acceptor(self, depth: int) -> AcceptorState:
        if depth not in self._acceptors:
            self._acceptors[depth] = AcceptorState()
        return self._acceptors[depth]

    # ------------------------------------------------------------------
    # Acceptor logic
    # ------------------------------------------------------------------

    def on_prepare(
        self, depth: int, ballot: Ballot
    ) -> tuple[bool, Optional[Ballot], Optional[Dict[str, Any]]]:
        acc = self._get_acceptor(depth)
        if acc.promised is None or ballot >= acc.promised:
            acc.promised = ballot
            return True, acc.accepted_ballot, acc.accepted_value

        # Reject
        return False, acc.accepted_ballot, acc.accepted_value

    def on_accept(
        self, depth: int, ballot: Ballot, value: Dict[str, Any]
    ) -> bool:
        acc = self._get_acceptor(depth)
        if acc.promised is None or ballot >= acc.promised:
            acc.promised = ballot
            acc.accepted_ballot = ballot
            acc.accepted_value = value
            return True

        # Reject
        return False

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:

        depths: Dict[str, Any] = {}
        for depth, acc in self._acceptors.items():
            depths[str(depth)] = acc.to_dict()
        return {"depths": depths}

    def load_from_dict(self, data: Dict[str, Any]) -> None:
        """
        Load Paxos acceptor state from dict produced by to_dict().
        """
        self._acceptors.clear()
        depths = data.get("depths", {})
        for depth_str, acc_dict in depths.items():
            depth = int(depth_str)
            self._acceptors[depth] = AcceptorState.from_dict(acc_dict)

    # ------------------------------------------------------------------
    # Debug helpers
    # ------------------------------------------------------------------

    def dump_depth_state(self, depth: int) -> str:
        acc = self._acceptors.get(depth)
        if acc is None:
            return f"[Paxos {self.node_id}] Depth {depth}: <no state>"

        lines = [f"[Paxos {self.node_id}] Depth {depth} state:"]
        lines.append(f"  promised       : {acc.promised}")
        lines.append(f"  accepted_ballot: {acc.accepted_ballot}")
        lines.append(f"  accepted_value : {acc.accepted_value}")
        return "\n".join(lines)
