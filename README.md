# Implementation Plan: Distributed Blockchain with Paxos Consensus

## Project Overview
Building a peer-to-peer money exchange application using a private blockchain with Paxos consensus protocol. The system consists of 5 nodes (P1-P5), each starting with $100 balance. Nodes use Paxos to agree on blocks added to the blockchain, with crash-fault tolerance.

## System Architecture

### Core Components
1. **Blockchain** - Linked list of blocks containing transactions
2. **Bank Accounts Table** - Balance tracking for all 5 nodes
3. **Paxos Consensus** - Leader election and agreement protocol
4. **Persistence Layer** - Disk storage for blockchain and balances
5. **Network Layer** - Node-to-node communication with simulated delays
6. **User Interface** - Command-line interface for user commands

### Block Structure
Each block contains:
- **Transaction**: `<sender_id, receiver_id, amount>`
- **Hash Pointer**: Pointer to previous block + SHA256 hash of previous block
- **Nonce**: Random string ensuring hash ends with digit 0-4 (Proof of Work)

## Implementation Steps

### Phase 1: Project Setup and Basic Infrastructure

**1.1 Choose Programming Language & Setup**
- Recommend: Python (good libraries for networking, hashing, serialization)
- Alternative: Go, Java, or Node.js
- Create directory structure:
  ```
  final_project/
  ├── config/
  │   └── nodes.json          # IP and port configuration
  ├── src/
  │   ├── main.py             # Entry point
  │   ├── node.py             # Node class
  │   ├── blockchain.py       # Blockchain implementation
  │   ├── block.py            # Block structure
  │   ├── paxos.py            # Paxos protocol
  │   ├── network.py          # Network communication
  │   ├── persistence.py      # Disk I/O
  │   └── cli.py              # Command-line interface
  ├── data/
  │   └── node_*/             # Per-node data directories
  └── README.md
  ```

**1.2 Configuration File**
- Create `config/nodes.json` with IP:port for all 5 nodes
- Support localhost testing (127.0.0.1 with different ports)

### Phase 2: Core Data Structures

**2.1 Block Class** (`src/block.py`)
- Attributes:
  - `transaction`: dict with sender_id, receiver_id, amount
  - `previous_hash`: SHA256 hash string
  - `nonce`: random string
  - `hash`: current block's hash
  - `depth`: block index in chain
  - `status`: "tentative" or "decided"
- Methods:
  - `calculate_nonce()`: Find nonce where SHA256(transaction||nonce) ends with 0-4
  - `calculate_hash()`: Compute hash pointer using previous block data
  - `serialize()`: Convert to storable format
  - `deserialize()`: Reconstruct from stored data

**2.2 Blockchain Class** (`src/blockchain.py`)
- Attributes:
  - `blocks`: list of Block objects
  - `depth`: current chain length
- Methods:
  - `append_block(block)`: Add block to chain
  - `get_latest_block()`: Return most recent block
  - `validate_chain()`: Verify hash integrity
  - `get_depth()`: Return chain length

**2.3 Bank Accounts Table** (`src/blockchain.py` or separate)
- Dictionary: `{node_id: balance}`
- Initialize all 5 nodes with $100
- Methods:
  - `update_balance(sender, receiver, amount)`
  - `get_balance(node_id)`
  - `validate_transaction(sender, amount)`: Check sufficient funds

### Phase 3: Paxos Consensus Protocol

**3.1 Paxos State** (`src/paxos.py`)
- **Ballot Number**: `<seq_num, proc_id, depth>`
  - Compare: depth first, then seq_num, then proc_id
  - Reinitialize seq_num for each new depth
- **Proposer State**:
  - `ballot_num`: current ballot number
  - `proposal_value`: Block being proposed
- **Acceptor State** (per depth):
  - `promised_ballot`: highest ballot promised
  - `accept_num`: highest ballot accepted
  - `accept_val`: value accepted for that ballot

**3.2 Paxos Messages**
- `PREPARE(ballot_num)`: Leader election request
- `PROMISE(ballot_num, accept_num, accept_val)`: Response to prepare
- `ACCEPT(ballot_num, value)`: Propose block
- `ACCEPTED(ballot_num)`: Accept acknowledgment
- `DECISION(block)`: Commit instruction

**3.3 Paxos Protocol Flow**
1. **Leader Election**:
   - Node receives transaction → increments seq_num
   - Sends PREPARE with `<seq_num, self.id, current_depth>`
   - Collects PROMISE from majority
   - If any PROMISE has accept_val, use highest ballot's value

2. **Proof of Work**:
   - Compute nonce for proposed block
   - If takes too long, another node may timeout and start election

3. **Proposal Phase**:
   - Send ACCEPT with ballot_num and block
   - Collect ACCEPTED from majority
   - Append block locally, update balances

4. **Decision Phase**:
   - Send DECISION to all nodes
   - Nodes append block, update balances, persist to disk

**3.4 Depth Checking**
- Acceptors reject PREPARE/ACCEPT if message depth < local blockchain depth
- Triggers recovery protocol

### Phase 4: Network Communication

**4.1 Network Layer** (`src/network.py`)
- TCP sockets for reliable communication
- Message format: JSON with type and payload
- **Message Types**: PREPARE, PROMISE, ACCEPT, ACCEPTED, DECISION, SYNC_REQUEST, SYNC_RESPONSE
- **Simulated Delay**: 3-second delay on message send
- Methods:
  - `send_message(node_id, message_type, payload)`
  - `broadcast(message_type, payload)`: Send to all nodes
  - `receive_message()`: Non-blocking message receipt
  - `start_listener()`: Background thread for incoming messages

**4.2 Message Handling**
- Message queue for incoming messages
- Handler thread processes messages sequentially
- Dispatch to appropriate Paxos phase handler

### Phase 5: Persistence Layer

**5.1 Disk Storage** (`src/persistence.py`)
- **Blockchain File**: `data/node_X/blockchain.json`
  - Store all blocks with status (tentative/decided)
  - Format: JSON array of blocks
- **Bank Accounts File**: `data/node_X/accounts.json`
  - Format: `{"1": 100, "2": 100, ...}`
- **Paxos State File**: `data/node_X/paxos_state.json`
  - Store promised_ballot, accept_num, accept_val per depth

**5.2 Persistence Methods**
- `save_blockchain(blockchain)`
- `load_blockchain() -> Blockchain`
- `save_accounts(accounts)`
- `load_accounts() -> dict`
- `save_paxos_state(state)`
- `load_paxos_state() -> dict`

**5.3 Write Strategy**
- **Leader**: Write block as "decided" when majority accepts
- **Participant**: Write block as "tentative" on ACCEPT, change to "decided" on DECISION
- **Bank Accounts**: Update only after block marked "decided"

### Phase 6: Node Implementation

**6.1 Node Class** (`src/node.py`)
- Attributes:
  - `node_id`: 1-5
  - `blockchain`: Blockchain instance
  - `accounts`: Bank accounts dict
  - `paxos`: Paxos state
  - `network`: Network handler
  - `is_leader`: boolean
  - `running`: boolean for process state

**6.2 Core Node Methods**
- `start()`: Initialize, load from disk, start network listener
- `stop()`: Graceful shutdown, persist state
- `handle_transaction(sender, receiver, amount)`:
  - Validate transaction
  - Start Paxos leader election
- `handle_message(message)`: Route to appropriate handler
- `become_leader()`: Execute Paxos prepare phase
- `propose_block(block)`: Execute Paxos accept phase
- `commit_block(block)`: Append to chain, update accounts

### Phase 7: User Interface

**7.1 CLI Commands** (`src/cli.py`)
- `moneyTransfer(debit_node, credit_node, amount)`
  - Validate: debit_node == current node
  - Validate: amount <= current balance
  - Initiate Paxos with transaction

- `failProcess`
  - Stop network listener
  - Set running = False
  - Persist state

- `fixProcess`
  - Reload state from disk
  - Restart network listener
  - Start recovery if blockchain outdated

- `printBlockchain`
  - Display all blocks with: depth, transaction, nonce, hash, status

- `printBalance`
  - Display all 5 node balances

**7.2 Input Parsing**
- Read from stdin
- Parse command and arguments
- Validate arguments
- Execute command

### Phase 8: Failure Handling and Recovery

**8.1 Timeouts**
- Paxos timeout: If no majority PROMISE/ACCEPTED within X seconds
- Increment seq_num and retry
- Other nodes may also timeout and compete

**8.2 Leader Failure**
- Other nodes timeout waiting for DECISION
- Start new leader election with higher seq_num
- New leader may propose different value

**8.3 Node Crash**
- On restart: Load state from disk
- Check if blockchain is behind

**8.4 Basic Recovery (Non-Extra Credit)**
- When node receives message with higher depth:
  - Request blockchain from sender
  - Replace local blockchain
  - Replay all transactions to rebuild accounts table

**8.5 Advanced Recovery (Extra Credit - 10%)**
- Maintain `first_uncommitted_index` in messages
- Attach to ACCEPT, ACCEPTED, DECISION messages
- **Leader receives ACCEPTED with old index**:
  - Send missing blocks to that acceptor
- **Leader receives ACCEPTED with newer index**:
  - Request missing blocks from that acceptor
- Incremental repair instead of full replacement

### Phase 9: Testing Scenarios

**9.1 Basic Tests**
1. **Sequential Transfers**: Node1 → Node2 → Node3 → Node4 → Node5
2. **Concurrent Transfers**: Multiple nodes initiate transactions simultaneously
3. **Balance Validation**: Verify balances sum to $500 always
4. **Blockchain Consistency**: All nodes have identical chains

**9.2 Failure Tests**
1. **Single Node Failure**: Fail 1 node, system continues
2. **Leader Failure**: Fail current leader during proposal
3. **Majority Failure**: Fail 3 nodes, system stalls (expected)
4. **Recovery**: Restart failed node, verify catches up

**9.3 Edge Cases**
1. **Insufficient Funds**: Try to transfer more than balance
2. **Concurrent Leaders**: Multiple nodes become leader simultaneously
3. **Network Partition**: Simulate with message dropping
4. **Nonce Computation Timeout**: One node slow, another becomes leader

### Phase 10: Debugging and Logging

**10.1 Console Logging**
Required logs:
- "Node X: Computing nonce for transaction <T>"
- "Node X: Found nonce: {nonce}, hash: {hash}"
- "Node X: Sending PREPARE with ballot <seq, proc, depth>"
- "Node X: Received PROMISE from Node Y"
- "Node X: Elected as leader"
- "Node X: Sending ACCEPT for block at depth Z"
- "Node X: Received ACCEPTED from Node Y"
- "Node X: Committing block at depth Z"
- "Node X: Blockchain depth: Z, Balance: $X"

**10.2 Debug Mode**
- Verbose logging for all message sends/receives
- State dumps on significant events

## Critical Files to Create

1. **[src/block.py](src/block.py)** - Block data structure and nonce calculation
2. **[src/blockchain.py](src/blockchain.py)** - Blockchain and accounts management
3. **[src/paxos.py](src/paxos.py)** - Paxos protocol implementation
4. **[src/network.py](src/network.py)** - Network communication with delays
5. **[src/persistence.py](src/persistence.py)** - Disk I/O for state
6. **[src/node.py](src/node.py)** - Main node logic and orchestration
7. **[src/cli.py](src/cli.py)** - Command-line interface
8. **[src/main.py](src/main.py)** - Entry point and initialization
9. **[config/nodes.json](config/nodes.json)** - Network configuration

## Implementation Order (Recommended)

1. **Start Simple**: Block, Blockchain, Bank Accounts (no network, no Paxos)
2. **Add Hashing**: Implement nonce calculation and hash pointers
3. **Add Network**: Basic message passing between nodes
4. **Add Paxos**: Implement consensus without failures
5. **Add Persistence**: Disk storage and recovery
6. **Add Failure Handling**: Crash failures and timeouts
7. **Add Recovery**: Basic blockchain sync
8. **Polish**: CLI, logging, testing
9. **Extra Credit**: Advanced recovery with first_uncommitted_index

## Key Design Decisions

### Language Choice
- **Recommended**: Python 3.8+
  - Libraries: `hashlib` (SHA256), `socket` (networking), `json` (serialization), `threading` (concurrency)
  - Fast development, good debugging

### Concurrency Model
- Main thread: CLI input
- Listener thread: Incoming messages
- Handler thread: Process messages sequentially (avoid race conditions)
- Timeout thread: Detect leader failures

### Ballot Number Comparison
```python
def compare_ballot(b1, b2):
    # b1 = (seq1, proc1, depth1)
    # b2 = (seq2, proc2, depth2)
    if b1[2] != b2[2]:  # depth
        return b1[2] - b2[2]
    if b1[0] != b2[0]:  # seq_num
        return b1[0] - b2[0]
    return b1[1] - b2[1]  # proc_id
```

### Nonce Calculation Strategy
```python
import hashlib
import random
import string

def find_nonce(transaction):
    txn_str = f"{transaction['sender']},{transaction['receiver']},{transaction['amount']}"
    while True:
        nonce = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        hash_input = txn_str + nonce
        hash_output = hashlib.sha256(hash_input.encode()).hexdigest()
        if hash_output[-1] in '01234':
            return nonce, hash_output
```

## Testing Strategy

### Local Testing Setup
- Run all 5 nodes on localhost: ports 5001-5005
- Open 5 terminal windows
- Execute: `python src/main.py --node-id 1 --port 5001`

### Demo Preparation
1. Start all 5 nodes
2. Execute sequential transfer: N1→N2 $10
3. Execute concurrent transfers: N3→N4 $20, N5→N1 $15
4. Print balances on all nodes (should be consistent)
5. Fail N2 (failProcess)
6. Execute transfer: N1→N3 $5 (should succeed)
7. Print blockchain on remaining nodes
8. Fix N2 (fixProcess)
9. Print blockchain on N2 (should catch up)
10. Print all balances (should total $500)

## Potential Challenges

1. **Race Conditions**: Multiple concurrent leaders
   - Solution: Proper ballot number comparison and Paxos protocol adherence

2. **Deadlocks**: Waiting for messages that never arrive
   - Solution: Timeouts and retry logic

3. **State Inconsistency**: Blockchain/accounts mismatch
   - Solution: Rebuild accounts by replaying all decided blocks

4. **Nonce Computation Delay**: Blocking other operations
   - Solution: Compute in separate thread, allow timeouts

5. **Recovery Complexity**: Syncing after multiple blocks missed
   - Solution: Start with simple full-chain replacement, optimize later

## Success Criteria

- [ ] 5 nodes can start and communicate
- [ ] Sequential money transfers work correctly
- [ ] Concurrent transfers handled via Paxos
- [ ] Blockchain consistent across all nodes
- [ ] Balances consistent across all nodes
- [ ] Nonce correctly ends hash with 0-4
- [ ] Hash pointers correctly link blocks
- [ ] Node can fail and restart
- [ ] Failed node catches up on restart
- [ ] System progresses with 3+ nodes alive
- [ ] System stalls with <3 nodes alive
- [ ] All CLI commands work
- [ ] Proper logging for demo
- [ ] [Extra Credit] Advanced recovery with first_uncommitted_index

## Timeline Estimate

- **Phase 1-2** (Setup & Data Structures): 2-3 hours
- **Phase 3** (Paxos Core): 4-6 hours
- **Phase 4** (Networking): 3-4 hours
- **Phase 5** (Persistence): 2-3 hours
- **Phase 6** (Node Integration): 3-4 hours
- **Phase 7** (CLI): 1-2 hours
- **Phase 8** (Failure Handling): 4-5 hours
- **Phase 9** (Testing): 3-4 hours
- **Phase 10** (Logging & Polish): 2-3 hours
- **Extra Credit**: 3-4 hours

**Total**: ~25-35 hours for base implementation

## Notes

- Skip extra credit recovery until base system works
- Test incrementally after each phase
- Use print statements liberally for debugging
- Start with 2 nodes for initial testing, scale to 5
- Demo is on Monday, December 8, 2025 via Zoom
