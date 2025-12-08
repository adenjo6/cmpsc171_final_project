# src/block.py

import hashlib
import json
import random
import string
from dataclasses import dataclass, asdict


@dataclass
class Transaction:
    sender_id: int
    receiver_id: int
    amount: int

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Transaction":
        return cls(
            sender_id=data["sender_id"],
            receiver_id=data["receiver_id"],
            amount=data["amount"],
        )

    def canonical_string(self) -> str:
        return f"{self.sender_id},{self.receiver_id},{self.amount}"


@dataclass
class Block:
    index: int
    transaction: Transaction
    nonce: str
    hash: str  
    status: str = "tentative"

    @staticmethod
    def compute_pow_hash(transaction: Transaction, nonce: str) -> str:

        data = f"{transaction.canonical_string()}|{nonce}"
        return hashlib.sha256(data.encode("utf-8")).hexdigest()

    @staticmethod
    def find_nonce(transaction: Transaction, max_attempts: int = None):
        alphabet = string.ascii_letters + string.digits
        attempts = 0

        while True:
            attempts += 1
            nonce = "".join(random.choices(alphabet, k=16))
            h = Block.compute_pow_hash(transaction, nonce)
            if h[-1] in "01234":
                print(
                    f"[PoW] Found nonce after {attempts} attempts: "
                    f"nonce={nonce}, hash={h}"
                )
                return nonce, h

            if max_attempts is not None and attempts >= max_attempts:
                raise RuntimeError(
                    f"Failed to find valid nonce within {max_attempts} attempts"
                )

    @staticmethod
    def compute_hash_pointer(prev_block: "Block | None") -> str:
        if prev_block is None:
            # Genesis "previous hash"
            return "0" * 64

        prev_tx_str = prev_block.transaction.canonical_string()
        data = f"{prev_tx_str}|{prev_block.nonce}|{prev_block.hash}"
        return hashlib.sha256(data.encode("utf-8")).hexdigest()

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "transaction": self.transaction.to_dict(),
            "nonce": self.nonce,
            "hash": self.hash,
            "status": self.status,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Block":
        return cls(
            index=data["index"],
            transaction=Transaction.from_dict(data["transaction"]),
            nonce=data["nonce"],
            hash=data["hash"],
            status=data.get("status", "tentative"),
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, s: str) -> "Block":
        return cls.from_dict(json.loads(s))
