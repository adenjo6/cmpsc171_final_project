# src/blockchain.py

from typing import Dict, List

from block import Block, Transaction


class Blockchain:
    """
    Blockchain + Bank Accounts Table.

    - blocks: list of Block objects starting with a genesis block
    - accounts: {node_id: balance}

    There are two main usage patterns:

    1) Local, no-Paxos testing:
       - use append_transaction() to create PoW, append, and update balances.

    2) Paxos-based commit:
       - leader builds a block value (dict) with PoW + hash pointer,
       - Paxos decides that value,
       - all nodes call commit_block_from_value() to append and update balances.
    """

    def __init__(self, num_nodes: int = 5, initial_balance: int = 100):
        self.blocks: List[Block] = []
        # Bank Accounts Table: all nodes start with the same initial balance
        self.accounts: Dict[int, int] = {
            node_id: initial_balance for node_id in range(1, num_nodes + 1)
        }

        # Create genesis block (no real transaction; just a placeholder)
        genesis_tx = Transaction(sender_id=0, receiver_id=0, amount=0)
        genesis_nonce = "GENESIS"
        genesis_hash = "0" * 64  # No previous block
        genesis_block = Block(
            index=0,
            transaction=genesis_tx,
            nonce=genesis_nonce,
            hash=genesis_hash,
            status="decided",
        )
        self.blocks.append(genesis_block)
        print("[Init] Created genesis block")

    # ------------------------------------------------------------------
    # Bank accounts operations
    # ------------------------------------------------------------------

    def get_balance(self, node_id: int) -> int:
        return self.accounts.get(node_id, 0)

    def total_balance(self) -> int:
        return sum(self.accounts.values())

    def validate_transaction(self, sender_id: int, amount: int) -> bool:
        """Check that sender exists and has sufficient funds."""
        if sender_id not in self.accounts:
            return False
        if amount <= 0:
            return False
        return self.accounts[sender_id] >= amount

    def apply_transaction(self, tx: Transaction) -> None:
        """
        Apply a transaction to the bank accounts table.

        In the full system, this should only be called when the block
        containing tx is decided and committed.
        """
        if tx.sender_id not in self.accounts or tx.receiver_id not in self.accounts:
            raise ValueError("Invalid sender or receiver id")

        if not self.validate_transaction(tx.sender_id, tx.amount):
            raise ValueError("Insufficient funds for transaction")

        self.accounts[tx.sender_id] -= tx.amount
        self.accounts[tx.receiver_id] += tx.amount

    # ------------------------------------------------------------------
    # Blockchain operations
    # ------------------------------------------------------------------

    def latest_block(self) -> Block:
        return self.blocks[-1]

    def get_depth(self) -> int:
        """Number of blocks (including genesis)."""
        return len(self.blocks)

    # === Local, no-Paxos helper (keep for debugging) ===================

    def append_transaction(
        self, sender_id: int, receiver_id: int, amount: int, status: str = "decided"
    ) -> Block:
        """
        Append a new block with the given transaction, computing PoW
        and hash pointer locally.

        This is useful for simple testing, but in the Paxos version the
        leader will instead build a block value and call
        commit_block_from_value() after consensus.
        """
        if not self.validate_transaction(sender_id, amount):
            raise ValueError("Invalid or insufficient-balance transaction")

        tx = Transaction(sender_id=sender_id, receiver_id=receiver_id, amount=amount)

        print(f"[Blockchain] Computing nonce for transaction {tx}")
        nonce, pow_hash = Block.find_nonce(tx)

        prev_block = self.latest_block()
        hash_pointer = Block.compute_hash_pointer(prev_block)

        new_index = len(self.blocks)
        new_block = Block(
            index=new_index,
            transaction=tx,
            nonce=nonce,
            hash=hash_pointer,
            status=status,
        )

        self.blocks.append(new_block)
        # In the full system, you would only apply the transaction once the block is "decided"
        self.apply_transaction(tx)

        print(
            f"[Blockchain] Appended block at index {new_index}: "
            f"nonce={nonce}, prev_hash_pointer={hash_pointer}"
        )
        print(f"[Blockchain] New balances: {self.accounts}")
        print(f"[Blockchain] Total balance: {self.total_balance()}")

        return new_block

    # === Paxos-based commit helper =====================================

    def commit_block_from_value(self, block_data: dict) -> Block:
        """
        Commit a decided block represented as a value dict that came
        from Paxos (accepted_value / DECISION).

        block_data is expected to have keys:
          - index
          - transaction: {"sender_id", "receiver_id", "amount"}
          - nonce
          - hash  (hash pointer to previous block)
          - status (ignored; we force 'decided')
        """
        index = block_data["index"]
        tx = Transaction.from_dict(block_data["transaction"])
        nonce = block_data["nonce"]
        hash_pointer = block_data["hash"]

        # If we already have a block at this index, check if it's the same.
        if index < len(self.blocks):
            existing = self.blocks[index]
            if (
                existing.transaction.to_dict() == tx.to_dict()
                and existing.nonce == nonce
                and existing.hash == hash_pointer
            ):
                print(
                    f"[Blockchain] Block at index {index} already committed; skipping"
                )
                return existing
            else:
                # Conflicting history – in a real system we’d need recovery.
                print(
                    f"[Blockchain] WARNING: conflicting block at index {index}; "
                    f"existing={existing.to_dict()}, new={block_data}"
                )
                return existing

        # Expect the new block to be appended at the end.
        if index != len(self.blocks):
            raise ValueError(
                f"Cannot commit block with index {index}; "
                f"expected next index {len(self.blocks)}"
            )

        # Verify hash pointer matches our current last block.
        prev_block = self.latest_block()
        expected_hash = Block.compute_hash_pointer(prev_block)
        if hash_pointer != expected_hash:
            raise ValueError(
                "Hash pointer mismatch in commit_block_from_value: "
                f"expected {expected_hash}, got {hash_pointer}"
            )

        # Apply transaction and append block as decided.
        self.apply_transaction(tx)

        block = Block(
            index=index,
            transaction=tx,
            nonce=nonce,
            hash=hash_pointer,
            status="decided",
        )
        self.blocks.append(block)

        print(
            f"[Blockchain] Committed decided block at index {index}: "
            f"tx={tx}, nonce={nonce}"
        )
        print(
            f"[Blockchain] Balances after commit: {self.accounts}, "
            f"total={self.total_balance()}"
        )

        return block

    # ------------------------------------------------------------------
    # Validation / printing
    # ------------------------------------------------------------------

    def validate_chain(self) -> bool:
        """
        Validate the blockchain based on hash pointers.

        For each block at index i > 0, ensure:
        blocks[i].hash == SHA256(T_{i-1}.Txns || T_{i-1}.Nonce || T_{i-1}.Hash)
        """
        if not self.blocks:
            print("[Validate] No blocks found")
            return False

        for i in range(1, len(self.blocks)):
            prev = self.blocks[i - 1]
            current = self.blocks[i]
            expected_hash = Block.compute_hash_pointer(prev)
            if current.hash != expected_hash:
                print(
                    f"[Validate] Hash mismatch at index {i}: "
                    f"expected {expected_hash}, got {current.hash}"
                )
                return False

        print("[Validate] Blockchain hash pointers are consistent")
        return True

    def print_blockchain(self) -> None:
        """Pretty-print the blockchain contents."""
        print("=== Blockchain ===")
        for block in self.blocks:
            tx = block.transaction
            print(
                f"Index: {block.index}, "
                f"Tx: <{tx.sender_id}->{tx.receiver_id}, ${tx.amount}>, "
                f"Nonce: {block.nonce}, "
                f"HashPtr(prev): {block.hash}, "
                f"Status: {block.status}"
            )
        print("==================")
