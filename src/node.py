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
import persistence


@dataclass
class NodeConfig:
    node_id: int
    num_nodes: int = 5
    initial_balance: int = 100


class Node:
    def __init__(self, config: NodeConfig):
        if config.node_id < 1 or config.node_id > config.num_nodes:
            raise ValueError(
                f"node_id must be between 1 and {config.num_nodes}, "
                f"got {config.node_id}"
            )

        self.node_id = config.node_id
        self.num_nodes = config.num_nodes
        self.initial_balance = config.initial_balance

        self.running = True

        # Incoming message queue (for PROMISE/ACCEPTED/etc. that the proposer consumes)
        self.incoming_messages: "queue.Queue[Dict[str, Any]]" = queue.Queue()

        # Paxos state (acceptor)
        self.paxos = PaxosState(self.node_id)

        # Proposer sequence number (for ballots)
        self._seq_counter: int = 0

        # Load cluster config
        self.nodes_config = self._load_nodes_config()

        # Load or initialize blockchain + Paxos from disk *before* network starts
        self.blockchain = persistence.load_blockchain(
            self.node_id, self.num_nodes, self.initial_balance
        )
        persistence.load_paxos_state(self.node_id, self.paxos)

        # Start network listener
        self.network = self._init_network()

        print(
            f"[Node {self.node_id}] Initialized with "
            f"{self.num_nodes} total nodes, initial balance "
            f"{self.initial_balance} each. "
            f"Current depth={self.blockchain.get_depth() - 1}"
        )

    # ------------------------------------------------------------------
    # Config + network init
    # ------------------------------------------------------------------

    def _load_nodes_config(self) -> Dict[str, Dict[str, Any]]:
        this_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(this_dir, "..", "config", "nodes.json")
        config_path = os.path.normpath(config_path)

        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _nodes_entry_for_self(self) -> Dict[str, Any]:
        node_entry = self.nodes_config.get(str(self.node_id))
        if not node_entry:
            raise ValueError(
                f"No entry for node_id={self.node_id} in nodes.json"
            )
        return node_entry

    def _init_network(self) -> Network:
        node_entry = self._nodes_entry_for_self()
        host = node_entry["host"]
        port = node_entry["port"]

        net = Network(
            node_id=self.node_id,
            host=host,
            port=port,
            nodes_config=self.nodes_config,
            on_message=self._on_network_message,
            delay=3.0,  # 3-second send delay per spec; tweak to 0.0 while debugging if needed
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

        if mtype == "SYNC_REQUEST":
            self._handle_sync_request(message, addr)
            return

        if mtype == "SYNC_RESPONSE":
            print(
                f"[Node {self.node_id}] Received SYNC_RESPONSE from {sender} at {addr}"
            )
            # Let the sync logic pick it up from the queue
            self.incoming_messages.put(message)
            return

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

        local_depth = self.blockchain.get_depth() - 1
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
            # Persist latest state after commit
            persistence.save_blockchain(self.node_id, self.blockchain)
            persistence.save_paxos_state(self.node_id, self.paxos)
        except Exception as e:
            print(
                f"[Node {self.node_id}] Error committing decided block at depth "
                f"{depth}: {e}"
            )

    # ------------------------------------------------------------------
    # SYNC / recovery handlers
    # ------------------------------------------------------------------

    def _handle_sync_request(
        self, message: Dict[str, Any], addr: Tuple[str, int]
    ) -> None:
        requester = message.get("from")
        payload = message.get("payload", {})
        req_depth = payload.get("depth")
        local_depth = self.blockchain.get_depth() - 1

        print(
            f"[Node {self.node_id}] Received SYNC_REQUEST from Node {requester} "
            f"(their depth={req_depth}) at {addr}"
        )

        blocks_data = [b.to_dict() for b in self.blockchain.blocks]
        accounts_data = {str(k): v for k, v in self.blockchain.accounts.items()}

        resp = {
            "from": self.node_id,
            "type": "SYNC_RESPONSE",
            "payload": {
                "depth": local_depth,
                "blocks": blocks_data,
                "accounts": accounts_data,
            },
        }

        if requester is not None:
            self.network.send_message(requester, resp)

    def _wait_for_sync_response(
        self, from_id: int, timeout: float = 5.0
    ) -> Dict[str, Any] | None:
        """
        Wait for a SYNC_RESPONSE from a specific node.
        """
        end = time.time() + timeout
        buffer: list[Dict[str, Any]] = []

        while time.time() < end:
            remaining = end - time.time()
            if remaining <= 0:
                break
            try:
                msg = self.incoming_messages.get(timeout=remaining)
            except queue.Empty:
                break

            mtype = msg.get("type")
            sender = msg.get("from")
            if mtype == "SYNC_RESPONSE" and sender == from_id:
                # Put back other messages
                for m in buffer:
                    self.incoming_messages.put(m)
                return msg

            buffer.append(msg)

        for m in buffer:
            self.incoming_messages.put(m)
        return None

    def _apply_sync_state_from_payload(self, payload: Dict[str, Any]) -> None:
        """
        Replace local blockchain/accounts with the snapshot from payload.
        """
        blocks_data = payload.get("blocks", [])
        accounts_data = payload.get("accounts", {})

        if not blocks_data:
            print(
                f"[Node {self.node_id}] SYNC_RESPONSE has empty blockchain; "
                f"ignoring."
            )
            return

        new_bc = Blockchain(
            num_nodes=self.num_nodes,
            initial_balance=self.initial_balance,
        )
        new_bc.blocks = [Block.from_dict(bd) for bd in blocks_data]
        new_bc.accounts = {int(k): v for k, v in accounts_data.items()}
        self.blockchain = new_bc

        print(
            f"[Node {self.node_id}] Synced blockchain from peer; "
            f"new depth={self.blockchain.get_depth() - 1}, "
            f"balances={self.blockchain.accounts}"
        )

    def _sync_from_peers(self, timeout_per_peer: float = 5.0) -> None:
        """
        Simple recovery strategy: ask peers for their full blockchain and
        adopt the first one that is longer than ours.
        """
        current_depth = self.blockchain.get_depth() - 1
        peer_ids = sorted(
            int(k) for k in self.nodes_config.keys() if int(k) != self.node_id
        )

        if not peer_ids:
            print(f"[Node {self.node_id}] No peers configured for sync.")
            return

        print(
            f"[Node {self.node_id}] Attempting to sync from peers. "
            f"Current depth={current_depth}"
        )

        for peer_id in peer_ids:
            print(
                f"[Node {self.node_id}] Sending SYNC_REQUEST to Node {peer_id}"
            )
            req = {
                "from": self.node_id,
                "type": "SYNC_REQUEST",
                "payload": {"depth": current_depth},
            }
            self.network.send_message(peer_id, req)

            resp = self._wait_for_sync_response(peer_id, timeout_per_peer)
            if resp is None:
                print(
                    f"[Node {self.node_id}] No SYNC_RESPONSE from Node {peer_id} "
                    f"(timeout)."
                )
                continue

            pl = resp.get("payload", {})
            remote_depth = pl.get("depth")
            if remote_depth is None:
                print(
                    f"[Node {self.node_id}] Malformed SYNC_RESPONSE from "
                    f"Node {peer_id}: {resp}"
                )
                continue

            if remote_depth <= current_depth:
                print(
                    f"[Node {self.node_id}] Node {peer_id} not ahead "
                    f"(remote depth={remote_depth}); skipping."
                )
                continue

            print(
                f"[Node {self.node_id}] Syncing from Node {peer_id} "
                f"(remote depth={remote_depth})."
            )
            self._apply_sync_state_from_payload(pl)

            try:
                persistence.save_blockchain(self.node_id, self.blockchain)
                persistence.save_paxos_state(self.node_id, self.paxos)
            except Exception as e:
                print(
                    f"[Node {self.node_id}] Error saving state after sync: {e}"
                )
            return

        print(
            f"[Node {self.node_id}] Could not find a peer with a longer "
            f"blockchain to sync from."
        )

    # ------------------------------------------------------------------
    # Paxos proposer helpers (unchanged from previous step)
    # ------------------------------------------------------------------

    def _majority(self) -> int:
        return self.num_nodes // 2 + 1

    def _wait_for_promises(
        self, depth: int, ballot: Ballot, timeout: float = 5.0
    ) -> tuple[list[Dict[str, Any]], list[Dict[str, Any]]]:
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

            if (
                mtype == "PAXOS_PROMISE"
                and msg_depth == depth
                and msg_ballot == ballot.to_dict()
            ):
                promises.append(msg)
            elif (
                mtype == "PAXOS_REJECT"
                and msg_depth == depth
                and msg_ballot == ballot.to_dict()
            ):
                rejects.append(msg)
            else:
                buffer.append(msg)

        for m in buffer:
            self.incoming_messages.put(m)

        return promises, rejects

    def _wait_for_accepteds(
        self, depth: int, ballot: Ballot, timeout: float = 5.0
    ) -> tuple[list[Dict[str, Any]], list[Dict[str, Any]]]:
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

            if (
                mtype == "PAXOS_ACCEPTED"
                and msg_depth == depth
                and msg_ballot == ballot.to_dict()
            ):
                accepteds.append(msg)
            elif (
                mtype == "PAXOS_REJECT"
                and msg_depth == depth
                and msg_ballot == ballot.to_dict()
            ):
                rejects.append(msg)
            else:
                buffer.append(msg)

        for m in buffer:
            self.incoming_messages.put(m)

        return accepteds, rejects

    def _build_block_value(
        self, depth: int, sender_id: int, receiver_id: int, amount: int
    ) -> Dict[str, Any]:
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
        depth = self.blockchain.get_depth()  # next block index
        self._seq_counter += 1
        ballot = Ballot(depth=depth, seq=self._seq_counter, proc_id=self.node_id)

        print(
            f"[Node {self.node_id}] Starting Paxos for depth {depth}, "
            f"ballot {ballot}, tx {sender_id}->{receiver_id} amount={amount}"
        )

        # PREPARE (include self)
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

        # Choose value
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

        # ACCEPT (include self)
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

        # Commit locally + persist
        self.blockchain.commit_block_from_value(chosen_value)
        persistence.save_blockchain(self.node_id, self.blockchain)
        persistence.save_paxos_state(self.node_id, self.paxos)

        # Broadcast DECISION
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
    # Public operations (CLI)
    # ------------------------------------------------------------------

    def money_transfer(self, debit_id: int, credit_id: int, amount: int) -> None:
        if debit_id != self.node_id:
            raise ValueError(
                f"This node ({self.node_id}) can only initiate debits from its own "
                f"account. Got debit_id={debit_id}."
            )
        if debit_id == credit_id:
            raise ValueError("Sender and receiver must be different")

        if not self.blockchain.validate_transaction(debit_id, amount):
            raise ValueError("Invalid or insufficient-balance transaction")

        print(
            f"[Node {self.node_id}] moneyTransfer: {debit_id} -> {credit_id}, "
            f"amount={amount} (starting Paxos)"
        )

        self._paxos_propose_transaction(debit_id, credit_id, amount)

    def print_balances(self) -> None:
        """
        Print the balance of all accounts on this node.

        Spec wants all 5 accounts; we just iterate over whatever accounts
        exist in the blockchain.accounts dict, sorted by node id.
        """
        print(f"[Node {self.node_id}] Balances:")
        for nid in sorted(self.blockchain.accounts.keys()):
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
        print(f"[Node {self.node_id}] Incoming message queue:")
        if self.incoming_messages.empty():
            print("  (no messages)")
            return
        while not self.incoming_messages.empty():
            msg = self.incoming_messages.get_nowait()
            print(" ", msg)

    # ----- Paxos CLI helpers (manual testing) --------------------------

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

    # ----- Failure stubs now integrated with persistence + sync --------

    def fail_process(self) -> None:
        """
        Simulate a crash:

        - Save blockchain + Paxos state to disk
        - Stop network
        - Mark node as not running
        """
        print(
            f"[Node {self.node_id}] failProcess called. "
            f"Saving state and stopping network."
        )
        try:
            persistence.save_blockchain(self.node_id, self.blockchain)
            persistence.save_paxos_state(self.node_id, self.paxos)
        except Exception as e:
            print(f"[Node {self.node_id}] Error saving state on failProcess: {e}")

        self.running = False
        if self.network:
            self.network.stop()

    def fix_process(self) -> None:
        """
        Simulate a recovery:

        - Reload blockchain + Paxos state from disk
        - Restart network listener
        - Sync from any peer that has a longer chain
        - Mark node as running
        """
        print(
            f"[Node {self.node_id}] fixProcess called. "
            f"Reloading state and starting network."
        )

        self.blockchain = persistence.load_blockchain(
            self.node_id, self.num_nodes, self.initial_balance
        )
        self.paxos = PaxosState(self.node_id)
        persistence.load_paxos_state(self.node_id, self.paxos)

        self.running = True
        if self.network:
            self.network.start()
        else:
            self.network = self._init_network()

        self._sync_from_peers()
