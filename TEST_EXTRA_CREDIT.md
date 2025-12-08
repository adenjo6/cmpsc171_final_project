# Extra Credit Incremental Repair - Test Guide

## Test Scenario: Node Falls Behind Without fixProcess

This demonstrates the extra credit feature where nodes are repaired incrementally during Paxos (NOT via fixProcess full sync).

---

## Setup (Clean Start)

```bash
# Terminal 0: Clean data
cd /Users/adenj/CS/171/final_project
rm -rf data/node_*
```

---

## Step 1: Start All 5 Nodes

**Terminal 1:**
```bash
cd /Users/adenj/CS/171/final_project/src
python main.py --node-id 1 --num-nodes 5
```

**Terminal 2:**
```bash
cd /Users/adenj/CS/171/final_project/src
python main.py --node-id 2 --num-nodes 5
```

**Terminal 3:**
```bash
cd /Users/adenj/CS/171/final_project/src
python main.py --node-id 3 --num-nodes 5
```

**Terminal 4:**
```bash
cd /Users/adenj/CS/171/final_project/src
python main.py --node-id 4 --num-nodes 5
```

**Terminal 5:**
```bash
cd /Users/adenj/CS/171/final_project/src
python main.py --node-id 5 --num-nodes 5
```

---

## Step 2: Do 3 Transactions (All Nodes Participate)

**On Node 1 terminal:**
```
moneyTransfer 1 2 10
```
*Wait ~10 seconds for completion*

**On Node 2 terminal:**
```
moneyTransfer 2 3 5
```
*Wait ~10 seconds*

**On Node 3 terminal:**
```
moneyTransfer 3 1 8
```
*Wait ~10 seconds*

**Verify all nodes at depth 3:**
```
printBlockchain
```
Should show 4 blocks (0=genesis, 1-3=transactions)

---

## Step 3: Stop Node 4 (Simulate Crash)

**On Node 4 terminal:**
Press `Ctrl+C` to kill the process

Node 4 is now offline with blockchain at depth 3 saved to disk.

---

## Step 4: Do 2 More Transactions (Node 4 Misses These)

**On Node 1 terminal:**
```
moneyTransfer 1 5 7
```
*Wait ~10 seconds*

**On Node 2 terminal:**
```
moneyTransfer 2 1 3
```
*Wait ~10 seconds*

**Verify nodes 1-3,5 are at depth 5:**
```
printBlockchain
```
Should show 6 blocks (0-5)

Node 4 is still offline at depth 3.

---

## Step 5: Restart Node 4 (Without fixProcess!)

**On Node 4 terminal:**
```bash
cd /Users/adenj/CS/171/final_project/src
python main.py --node-id 4 --num-nodes 5
```

**IMPORTANT:** Do NOT type `fixProcess`!

Node 4 loads from disk → depth 3
Node 4 is now online but 2 blocks behind (missing depths 4-5)

---

## Step 6: Do Another Transaction → Extra Credit Repair Triggers!

**On Node 1 terminal:**
```
moneyTransfer 1 2 5
```

**WATCH Node 1's terminal for:**
```
[Node 1] Extra Credit: Detected Node 4 is behind (index 4 < 6). Sending repair...
[Node 1] Sent 2 blocks to Node 4
```

**WATCH Node 4's terminal for:**
```
[Node 4] Received REPAIR_BLOCKS from Node 1: start_index=4, count=2
[Node 4] Repaired block 4: Transaction(...)
[Node 4] Repaired block 5: Transaction(...)
[Node 4] Repair complete. New depth=5
```

---

## Step 7: Verify Success

**On Node 4 terminal:**
```
printBlockchain
```

Should show **7 blocks** (0-6), including the 2 repaired blocks!

**On Node 1 terminal:**
```
printBalance
```

**On Node 4 terminal:**
```
printBalance
```

Both should show identical balances!

---

## Expected Output Summary

### What You Should See:

1. **Node 1 (leader) detects Node 4 is behind:**
   - `first_uncommitted_index` comparison: `4 < 6`
   - Sends REPAIR_BLOCKS with blocks [4, 5]

2. **Node 4 receives incremental repair:**
   - Gets only 2 missing blocks (not full blockchain)
   - Applies them and catches up
   - Persists to disk

3. **All nodes end at depth 6 with identical blockchains**

---

## Key Differences from fixProcess

| Mechanism | Triggered | Blocks Sent | Use Case |
|-----------|-----------|-------------|----------|
| **fixProcess** | Manual command | ENTIRE blockchain (O(n)) | Full recovery after crash |
| **Extra Credit** | Automatic during Paxos | ONLY missing blocks (O(k)) | Catch up after missing messages |

---

## If Extra Credit Doesn't Trigger

If you don't see the "Extra Credit" messages, Node 4 might have caught up via DECISION messages. Try:

1. Stop Node 4 again (`Ctrl+C`)
2. Do 3-4 more transactions
3. Restart Node 4 (without fixProcess)
4. Do another transaction immediately

The more blocks Node 4 is behind, the easier to observe the repair.

---

## Clean Up After Test

```bash
rm -rf /Users/adenj/CS/171/final_project/data/node_*
```