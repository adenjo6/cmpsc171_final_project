# src/paxos.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True, order=True)
class Ballot:
    """
    Ballot number for Paxos.

    We order by:
      depth (block index),
      then seq (per-node sequence),
      then proc_id (tie-breaker).

    This matches the spec's "compare depth first, then seq_num, then proc_id".
    """
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
    """
    Per-depth acceptor state.

    - promised: highest ballot we've promised not to go below
    - accepted_ballot: ballot of the value we have accepted (if any)
    - accepted_value: the accepted value (block data as dict) or None
    """
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


class PaxosState:
    """
    Holds acceptor state for all depths in this node.

    Proposer logic will live in the Node layer (using this state),
    not inside this class.
    """

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
        """
        Handle a PREPARE(depth, ballot) at this node.

        Returns:
            (ok, accepted_ballot, accepted_value)

        If ok == True:
          - We have promised not to accept ballots < 'ballot'
          - We return any previously accepted (accepted_ballot, accepted_value).

        If ok == False:
          - We reject; caller may optionally send a REJECT or ignore.
        """
        acc = self._get_acceptor(depth)
        if acc.promised is None or ballot >= acc.promised:
            acc.promised = ballot
            return True, acc.accepted_ballot, acc.accepted_value

        # Reject: promised ballot is higher than the incoming ballot
        return False, acc.accepted_ballot, acc.accepted_value

    def on_accept(
        self, depth: int, ballot: Ballot, value: Dict[str, Any]
    ) -> bool:
        """
        Handle an ACCEPT(depth, ballot, value) at this node.

        Returns:
            True if we accept the value for this ballot,
            False if we reject (ballot < promised).
        """
        acc = self._get_acceptor(depth)
        if acc.promised is None or ballot >= acc.promised:
            acc.promised = ballot
            acc.accepted_ballot = ballot
            acc.accepted_value = value
            return True

        # Reject
        return False

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
