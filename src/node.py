# src/node.py

from __future__ import annotations

from dataclasses import dataclass
import json
import os
import queue
import time
from typing import Any, Dict, Tuple

from blockchain import Blockchain
from block import Block, Transaction
from network import Network
from paxos import PaxosState, Ballot


@dataclass
class NodeConfig:
    node_id: int
    num_nodes: int = 5
    initial_balance: int = 100


class Node:
    """
    Node abstraction.

    Owns:
    - blockchain
    - network (TCP, JSON messages)
    - message queue (for PROMISE/ACCEPTED/etc.)
    - Paxos state (acceptor, and proposer helper logic here)

    For now we assume:
    - single leader per proposal (the node initiating moneyTransfer),
    - no failures,
    - nodes have the same initial state.
    """

    def __init__(self, config: NodeConfig):
        if config.node_id < 1 or config.node_id > config.num_nodes:
            raise ValueError(
                f"node_id must be between 1 and {config.num_nodes}, "
                f"got {config.node_id}"
            )

        self.node_id = config.node_id
        self.num_nodes = config.num_nodes

        # Local blockchain + accounts
        self.blockchain = Blockchain(
            num_nodes=config.num_nodes,
            initial_balance=config.initial_balance,
        )

        self.running = True

        # Incoming message queue (for PROMISE/ACCEPTED/etc. that the proposer consumes)
        self.incoming_messages: "queue.Queue[Dict[str, Any]]" = queue.Queue()

        # Paxos state (acceptor)
        self.paxos = PaxosState(self.node_id)

        # Proposer sequence number (for ballots)
        self._seq_counter: int = 0

        # Load cluster config and start network
        self.nodes_config = self._load_nodes_config()
        self.network = self._init_network()

        print(
            f"[Node {self.node_id}] Initialized with "
            f"{self.num_nodes} total nodes, initial balance "
            f"{config.initial_balance} each."
        )

    # ------------------------------------------------------------------
    # Config + network init
    # ------------------------------------------------------------------

    def _load_nodes_config(self) -> Dict[str, Dict[str, Any]]:
        """
        Load config/nodes.json relative to project root.
        """
        this_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(this_dir, "..", "config", "nodes.json")
        config_path = os.path.normpath(config_path)

        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _init_network(self) -> Network:
        node_entry = self.nodes_config.get(str(self.node_id))
        if not node_entry:
            raise ValueError(
                f"No entry for node_id={self.node_id} in nodes.json"
            )

        host = node_entry["host"]
        port = node_entry["port"]

        net = Network(
            node_id=self.node_id,
            host=host,
            port=port,
            nodes_config=self.nodes_config,
            on_message=self._on_network_message,
            delay=0.0,  # can set to 3.0 later to simulate network delay
        )
        net.start()
        return net

    # ------------------------------------------------------------------
    # Network message callback
    # ------------------------------------------------------------------

    def _on_network_message(
        self, message: Dict[str, Any], addr: Tuple[str, int]
    ) -> None:
        sender = message.get("from", "unknown")
        mtype = message.get("type", "UNKNOWN")

        if mtype == "PING":
            print(
                f"[Node {self.node_id}] Received PING from {sender} at {addr}: "
                f"{message}"
            )
            self.incoming_messages.put(message)
            return

        if mtype == "PAXOS_PREPARE":
            self._handle_paxos_prepare(message, addr)
            return

        if mtype == "PAXOS_ACCEPT":
            self._handle_paxos_accept(message, addr)
            return

        if mtype == "PAXOS_DECISION":
            self._handle_paxos_decision(message, addr)
            return

        # These are primarily for proposers to consume
        if mtype in ("PAXOS_PROMISE", "PAXOS_ACCEPTED", "PAXOS_REJECT"):
            print(
                f"[Node {self.node_id}] Received {mtype} from {sender} at {addr}: "
                f"{message}"
            )
            self.incoming_messages.put(message)
            return

        print(
            f"[Node {self.node_id}] Received UNKNOWN message type {mtype} "
            f"from {sender} at {addr}: {message}"
        )
        self.incoming_messages.put(message)

    # ------------------------------------------------------------------
    # Paxos message handlers (acceptor logic + DECISION)
    # ------------------------------------------------------------------

    def _handle_paxos_prepare(
        self, message: Dict[str, Any], addr: Tuple[str, int]
    ) -> None:
        sender = message.get("from")
        payload = message.get("payload", {})
        depth = payload.get("depth")
        ballot_dict = payload.get("ballot")

        if sender is None or depth is None or ballot_dict is None:
            print(
                f"[Node {self.node_id}] Malformed PAXOS_PREPARE from {addr}: "
                f"{message}"
            )
            return

        ballot = Ballot.from_dict(ballot_dict)

        # optional: depth sanity check vs local blockchain
        local_depth = self.blockchain.get_depth() - 1  # last committed index
        if depth < local_depth:
            print(
                f"[Node {self.node_id}] Ignoring PREPARE for depth {depth} "
                f"(local depth {local_depth}) from Node {sender}"
            )
            return

        ok, accepted_ballot, accepted_value = self.paxos.on_prepare(depth, ballot)

        if ok:
            print(
                f"[Node {self.node_id}] PREPARE accepted for depth {depth}, "
                f"ballot {ballot}, from Node {sender}"
            )
            acc_state = self.paxos._get_acceptor(depth)
            reply_payload: Dict[str, Any] = {
                "depth": depth,
                "ballot": ballot.to_dict(),
                "promised_ballot": (
                    acc_state.promised.to_dict() if acc_state.promised else None
                ),
                "accepted_ballot": (
                    accepted_ballot.to_dict() if accepted_ballot else None
                ),
                "accepted_value": accepted_value,
            }
            reply = {
                "from": self.node_id,
                "type": "PAXOS_PROMISE",
                "payload": reply_payload,
            }
            self.network.send_message(sender, reply)
        else:
            print(
                f"[Node {self.node_id}] PREPARE rejected for depth {depth}, "
                f"ballot {ballot}, from Node {sender}"
            )
            reply = {
                "from": self.node_id,
                "type": "PAXOS_REJECT",
                "payload": {
                    "depth": depth,
                    "ballot": ballot.to_dict(),
                },
            }
            self.network.send_message(sender, reply)

    def _handle_paxos_accept(
        self, message: Dict[str, Any], addr: Tuple[str, int]
    ) -> None:
        sender = message.get("from")
        payload = message.get("payload", {})
        depth = payload.get("depth")
        ballot_dict = payload.get("ballot")
        value = payload.get("value")

        if sender is None or depth is None or ballot_dict is None or value is None:
            print(
                f"[Node {self.node_id}] Malformed PAXOS_ACCEPT from {addr}: "
                f"{message}"
            )
            return

        ballot = Ballot.from_dict(ballot_dict)

        local_depth = self.blockchain.get_depth() - 1
        if depth < local_depth:
            print(
                f"[Node {self.node_id}] Ignoring ACCEPT for depth {depth} "
                f"(local depth {local_depth}) from Node {sender}"
            )
            return

        ok = self.paxos.on_accept(depth, ballot, value)

        if ok:
            print(
                f"[Node {self.node_id}] ACCEPT accepted for depth {depth}, "
                f"ballot {ballot}, from Node {sender}"
            )
            reply = {
                "from": self.node_id,
                "type": "PAXOS_ACCEPTED",
                "payload": {
                    "depth": depth,
                    "ballot": ballot.to_dict(),
                    "value": value,
                },
            }
            self.network.send_message(sender, reply)
        else:
            print(
                f"[Node {self.node_id}] ACCEPT rejected for depth {depth}, "
                f"ballot {ballot}, from Node {sender}"
            )
            reply = {
                "from": self.node_id,
                "type": "PAXOS_REJECT",
                "payload": {
                    "depth": depth,
                    "ballot": ballot.to_dict(),
                },
            }
            self.network.send_message(sender, reply)

    def _handle_paxos_decision(
        self, message: Dict[str, Any], addr: Tuple[str, int]
    ) -> None:
        sender = message.get("from")
        payload = message.get("payload", {})
        depth = payload.get("depth")
        value = payload.get("value")

        if sender is None or depth is None or value is None:
            print(
                f"[Node {self.node_id}] Malformed PAXOS_DECISION from {addr}: "
                f"{message}"
            )
            return

        print(
            f"[Node {self.node_id}] Received DECISION for depth {depth} "
            f"from Node {sender}: {value}"
        )

        try:
            self.blockchain.commit_block_from_value(value)
        except Exception as e:
            print(
                f"[Node {self.node_id}] Error committing decided block at depth "
                f"{depth}: {e}"
            )

    # ------------------------------------------------------------------
    # Paxos proposer helpers
    # ------------------------------------------------------------------

    def _majority(self) -> int:
        return self.num_nodes // 2 + 1

    def _wait_for_promises(
        self, depth: int, ballot: Ballot, timeout: float = 5.0
    ) -> tuple[list[Dict[str, Any]], list[Dict[str, Any]]]:
        """
        Wait for PROMISE or REJECT messages for a given (depth, ballot).
        Returns (promises, rejects).
        """
        promises: list[Dict[str, Any]] = []
        rejects: list[Dict[str, Any]] = []
        buffer: list[Dict[str, Any]] = []

        end = time.time() + timeout
        needed = self._majority()

        while time.time() < end and len(promises) < needed:
            remaining = end - time.time()
            if remaining <= 0:
                break
            try:
                msg = self.incoming_messages.get(timeout=remaining)
            except queue.Empty:
                break

            mtype = msg.get("type")
            payload = msg.get("payload", {})
            msg_depth = payload.get("depth")
            msg_ballot = payload.get("ballot")

            if mtype == "PAXOS_PROMISE" and msg_depth == depth and msg_ballot == ballot.to_dict():
                promises.append(msg)
            elif mtype == "PAXOS_REJECT" and msg_depth == depth and msg_ballot == ballot.to_dict():
                rejects.append(msg)
            else:
                buffer.append(msg)

        # Return unrelated messages to the queue
        for m in buffer:
            self.incoming_messages.put(m)

        return promises, rejects

    def _wait_for_accepteds(
        self, depth: int, ballot: Ballot, timeout: float = 5.0
    ) -> tuple[list[Dict[str, Any]], list[Dict[str, Any]]]:
        """
        Wait for ACCEPTED or REJECT messages for a given (depth, ballot).
        Returns (accepteds, rejects).
        """
        accepteds: list[Dict[str, Any]] = []
        rejects: list[Dict[str, Any]] = []
        buffer: list[Dict[str, Any]] = []

        end = time.time() + timeout
        needed = self._majority()

        while time.time() < end and len(accepteds) < needed:
            remaining = end - time.time()
            if remaining <= 0:
                break
            try:
                msg = self.incoming_messages.get(timeout=remaining)
            except queue.Empty:
                break

            mtype = msg.get("type")
            payload = msg.get("payload", {})
            msg_depth = payload.get("depth")
            msg_ballot = payload.get("ballot")

            if mtype == "PAXOS_ACCEPTED" and msg_depth == depth and msg_ballot == ballot.to_dict():
                accepteds.append(msg)
            elif mtype == "PAXOS_REJECT" and msg_depth == depth and msg_ballot == ballot.to_dict():
                rejects.append(msg)
            else:
                buffer.append(msg)

        for m in buffer:
            self.incoming_messages.put(m)

        return accepteds, rejects

    def _build_block_value(
        self, depth: int, sender_id: int, receiver_id: int, amount: int
    ) -> Dict[str, Any]:
        """
        Build a block value dict (transaction + nonce + hash pointer)
        to be proposed at a given depth.
        """
        tx = Transaction(sender_id=sender_id, receiver_id=receiver_id, amount=amount)
        print(f"[Node {self.node_id}] Building block value for depth {depth}: {tx}")

        nonce, pow_hash = Block.find_nonce(tx)
        print(
            f"[Node {self.node_id}] PoW for proposal: nonce={nonce}, hash={pow_hash}"
        )

        prev_block = self.blockchain.latest_block()
        hash_pointer = Block.compute_hash_pointer(prev_block)

        block_dict: Dict[str, Any] = {
            "index": depth,
            "transaction": tx.to_dict(),
            "nonce": nonce,
            "hash": hash_pointer,
            "status": "tentative",
        }
        return block_dict

    def _paxos_propose_transaction(
        self, sender_id: int, receiver_id: int, amount: int
    ) -> None:
        """
        Full Paxos round for a single transaction:
        PREPARE -> (majority PROMISE) -> choose value -> ACCEPT ->
        (majority ACCEPTED) -> DECISION.
        """
        depth = self.blockchain.get_depth()  # next block index
        self._seq_counter += 1
        ballot = Ballot(depth=depth, seq=self._seq_counter, proc_id=self.node_id)

        print(
            f"[Node {self.node_id}] Starting Paxos for depth {depth}, "
            f"ballot {ballot}, tx {sender_id}->{receiver_id} amount={amount}"
        )

        # PREPARE phase (include self as acceptor)
        ok, accepted_ballot, accepted_value = self.paxos.on_prepare(depth, ballot)
        if not ok:
            raise RuntimeError("Local acceptor rejected our own PREPARE")

        local_promise_payload = {
            "depth": depth,
            "ballot": ballot.to_dict(),
            "promised_ballot": (
                self.paxos._get_acceptor(depth).promised.to_dict()
            ),
            "accepted_ballot": (
                accepted_ballot.to_dict() if accepted_ballot else None
            ),
            "accepted_value": accepted_value,
        }
        local_promise_msg = {
            "from": self.node_id,
            "type": "PAXOS_PROMISE",
            "payload": local_promise_payload,
        }
        promises = [local_promise_msg]

        # Broadcast PREPARE to all other nodes
        prepare_payload = {
            "depth": depth,
            "ballot": ballot.to_dict(),
        }
        prepare_msg = {
            "from": self.node_id,
            "type": "PAXOS_PREPARE",
            "payload": prepare_payload,
        }
        self.network.broadcast(prepare_msg)

        remote_promises, remote_rejects = self._wait_for_promises(depth, ballot)
        promises.extend(remote_promises)

        if len(promises) < self._majority():
            raise RuntimeError(
                f"Did not receive majority PROMISEs "
                f"(got {len(promises)}, need {self._majority()})"
            )

        # Choose value: if any PROMISE has an accepted_value, use the one
        # with highest accepted_ballot; otherwise, use new transaction.
        highest_ab = None
        chosen_value = None

        for m in promises:
            pl = m["payload"]
            ab = pl.get("accepted_ballot")
            av = pl.get("accepted_value")
            if ab and av:
                b_obj = Ballot.from_dict(ab)
                if highest_ab is None or b_obj > highest_ab:
                    highest_ab = b_obj
                    chosen_value = av

        if chosen_value is None:
            chosen_value = self._build_block_value(depth, sender_id, receiver_id, amount)
        else:
            print(
                f"[Node {self.node_id}] Using previously accepted value "
                f"from ballot {highest_ab}"
            )

        # ACCEPT phase (include self as acceptor)
        ok = self.paxos.on_accept(depth, ballot, chosen_value)
        if not ok:
            raise RuntimeError("Local acceptor rejected our own ACCEPT")

        local_accepted_msg = {
            "from": self.node_id,
            "type": "PAXOS_ACCEPTED",
            "payload": {
                "depth": depth,
                "ballot": ballot.to_dict(),
                "value": chosen_value,
            },
        }
        accepteds = [local_accepted_msg]

        accept_payload = {
            "depth": depth,
            "ballot": ballot.to_dict(),
            "value": chosen_value,
        }
        accept_msg = {
            "from": self.node_id,
            "type": "PAXOS_ACCEPT",
            "payload": accept_payload,
        }
        self.network.broadcast(accept_msg)

        remote_accepteds, remote_rejects = self._wait_for_accepteds(depth, ballot)
        accepteds.extend(remote_accepteds)

        if len(accepteds) < self._majority():
            raise RuntimeError(
                f"Did not receive majority ACCEPTEDs "
                f"(got {len(accepteds)}, need {self._majority()})"
            )

        print(
            f"[Node {self.node_id}] Achieved majority ACCEPTED for depth {depth}, "
            f"ballot {ballot}"
        )

        # Commit locally
        self.blockchain.commit_block_from_value(chosen_value)

        # Broadcast DECISION so others commit too
        decision_msg = {
            "from": self.node_id,
            "type": "PAXOS_DECISION",
            "payload": {
                "depth": depth,
                "value": chosen_value,
            },
        }
        self.network.broadcast(decision_msg)

        print(
            f"[Node {self.node_id}] DECISION broadcast for depth {depth}, "
            f"tx {sender_id}->{receiver_id} amount={amount}"
        )

    # ------------------------------------------------------------------
    # Public operations (CLI calls these)
    # ------------------------------------------------------------------

    def money_transfer(self, debit_id: int, credit_id: int, amount: int) -> None:
        """
        Initiate a transfer from debit_id to credit_id, using Paxos
        to agree on the next block at the current depth.

        Requirement: debit_id must be this node's id.
        """
        if debit_id != self.node_id:
            raise ValueError(
                f"This node ({self.node_id}) can only initiate debits from its own "
                f"account. Got debit_id={debit_id}."
            )
        if debit_id == credit_id:
            raise ValueError("Sender and receiver must be different")

        # Validate funds against our local view before starting Paxos
        if not self.blockchain.validate_transaction(debit_id, amount):
            raise ValueError("Invalid or insufficient-balance transaction")

        print(
            f"[Node {self.node_id}] moneyTransfer: {debit_id} -> {credit_id}, "
            f"amount={amount} (starting Paxos)"
        )

        self._paxos_propose_transaction(debit_id, credit_id, amount)

    def print_balances(self) -> None:
        print(f"[Node {self.node_id}] Balances:")
        for nid in range(1, self.num_nodes + 1):
            bal = self.blockchain.get_balance(nid)
            print(f"  Node {nid}: ${bal}")
        print(f"  Total: ${self.blockchain.total_balance()}\n")

    def print_blockchain(self) -> None:
        print(f"[Node {self.node_id}] Blockchain state:")
        self.blockchain.print_blockchain()

    def validate_chain(self) -> None:
        print(f"[Node {self.node_id}] Validating blockchain:")
        ok = self.blockchain.validate_chain()
        print(f"[Node {self.node_id}] Chain valid? {ok}")

    # ----- PING + message inspection -----------------------------------

    def send_ping(self, target_id: int, text: str = "hello") -> None:
        msg = {
            "from": self.node_id,
            "type": "PING",
            "payload": {"text": text},
        }
        self.network.send_message(target_id, msg)

    def show_messages(self) -> None:
        """
        Drain and print incoming messages (PROMISE, ACCEPTED, PING, etc.).
        """
        print(f"[Node {self.node_id}] Incoming message queue:")
        if self.incoming_messages.empty():
            print("  (no messages)")
            return

        while not self.incoming_messages.empty():
            msg = self.incoming_messages.get_nowait()
            print(" ", msg)

    # ----- Paxos CLI helpers (manual testing still available) ----------

    def send_prepare(self, target_id: int, depth: int, seq: int) -> None:
        ballot = Ballot(depth=depth, seq=seq, proc_id=self.node_id)
        payload = {
            "depth": depth,
            "ballot": ballot.to_dict(),
        }
        msg = {
            "from": self.node_id,
            "type": "PAXOS_PREPARE",
            "payload": payload,
        }
        print(
            f"[Node {self.node_id}] Sending PREPARE to Node {target_id}, "
            f"depth={depth}, ballot={ballot}"
        )
        self.network.send_message(target_id, msg)

    def send_accept(
        self,
        target_id: int,
        depth: int,
        seq: int,
        sender_id: int,
        receiver_id: int,
        amount: int,
    ) -> None:
        ballot = Ballot(depth=depth, seq=seq, proc_id=self.node_id)
        value = self._build_block_value(depth, sender_id, receiver_id, amount)
        payload = {
            "depth": depth,
            "ballot": ballot.to_dict(),
            "value": value,
        }
        msg = {
            "from": self.node_id,
            "type": "PAXOS_ACCEPT",
            "payload": payload,
        }
        print(
            f"[Node {self.node_id}] Sending ACCEPT to Node {target_id}, "
            f"depth={depth}, ballot={ballot}, value={value}"
        )
        self.network.send_message(target_id, msg)

    def show_paxos_state(self, depth: int) -> None:
        print(self.paxos.dump_depth_state(depth))

    # ----- Failure stubs -----------------------------------------------

    def fail_process(self) -> None:
        """
        Stub for crash: mark not running and stop network listener.
        """
        print(
            f"[Node {self.node_id}] failProcess called. "
            f"Marking node as not running and stopping network."
        )
        self.running = False
        if self.network:
            self.network.stop()

    def fix_process(self) -> None:
        """
        Stub for recovery.

        For now, just mark running and (re)start network.
        """
        print(
            f"[Node {self.node_id}] fixProcess called. "
            f"Marking node as running and starting network."
        )
        self.running = True
        if self.network:
            self.network.start()
        else:
            self.network = self._init_network()
