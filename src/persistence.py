# src/persistence.py

import json
import os
from typing import Any, Dict, Optional

from block import Block
from blockchain import Blockchain
from paxos import PaxosState


def _project_root() -> str:
    """Return absolute path to project root (where data/ lives)."""
    this_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.normpath(os.path.join(this_dir, ".."))


def _node_data_dir(node_id: int) -> str:
    root = _project_root()
    path = os.path.join(root, "data", f"node_{node_id}")
    os.makedirs(path, exist_ok=True)
    return path


def _blockchain_path(node_id: int) -> str:
    return os.path.join(_node_data_dir(node_id), "blockchain.json")


def _paxos_path(node_id: int) -> str:
    return os.path.join(_node_data_dir(node_id), "paxos_state.json")


# ----------------------------------------------------------------------
# Blockchain + accounts persistence
# ----------------------------------------------------------------------

def save_blockchain(node_id: int, blockchain: Blockchain) -> None:
    """
    Save blockchain blocks + accounts to disk for this node.

    Format:
    {
      "blocks": [block_dict, ...],
      "accounts": {"1": 100, "2": 110, ...}
    }
    """
    data = {
        "blocks": [b.to_dict() for b in blockchain.blocks],
        "accounts": {str(k): v for k, v in blockchain.accounts.items()},
    }

    path = _blockchain_path(node_id)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"[Persistence {node_id}] Saved blockchain to {path}")


def load_blockchain(
    node_id: int,
    num_nodes: int,
    initial_balance: int,
) -> Blockchain:
    """
    Load blockchain + accounts from disk if available.
    If not present, create a fresh Blockchain.

    NOTE: This trusts the saved accounts and blocks; we do *not*
    recompute accounts by replaying blocks yet (that can be added later).
    """
    path = _blockchain_path(node_id)
    if not os.path.exists(path):
        print(
            f"[Persistence {node_id}] No blockchain file at {path}; "
            f"creating fresh blockchain"
        )
        return Blockchain(num_nodes=num_nodes, initial_balance=initial_balance)

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    bc = Blockchain(num_nodes=num_nodes, initial_balance=initial_balance)

    # Replace genesis and accounts with loaded data
    bc.blocks = [Block.from_dict(bdict) for bdict in data["blocks"]]
    bc.accounts = {int(k): v for k, v in data["accounts"].items()}

    print(
        f"[Persistence {node_id}] Loaded blockchain from {path} "
        f"(depth={len(bc.blocks)})"
    )
    return bc


# ----------------------------------------------------------------------
# Paxos persistence
# ----------------------------------------------------------------------

def save_paxos_state(node_id: int, paxos: PaxosState) -> None:
    """
    Save Paxos acceptor state to disk.
    """
    data = paxos.to_dict()
    path = _paxos_path(node_id)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"[Persistence {node_id}] Saved Paxos state to {path}")


def load_paxos_state(node_id: int, paxos: PaxosState) -> None:
    """
    Load Paxos acceptor state from disk into an existing PaxosState object.
    If file is missing, leave paxos as empty.
    """
    path = _paxos_path(node_id)
    if not os.path.exists(path):
        print(
            f"[Persistence {node_id}] No Paxos state file at {path}; "
            f"starting with empty Paxos state"
        )
        return

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    paxos.load_from_dict(data)
    print(f"[Persistence {node_id}] Loaded Paxos state from {path}")
