#!/usr/bin/env python3
"""
Test script for extra credit incremental repair.

This script simulates a scenario where a node falls behind and
the extra credit repair mechanism kicks in during Paxos.
"""

import sys
import os
import time
import shutil

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from node import Node, NodeConfig
from block import Block

def cleanup_data():
    """Remove all persisted data."""
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    if os.path.exists(data_dir):
        for item in os.listdir(data_dir):
            if item.startswith('node_'):
                path = os.path.join(data_dir, item)
                if os.path.isdir(path):
                    shutil.rmtree(path)
    print("✓ Cleaned data directory\n")

def test_extra_credit_repair():
    """
    Test scenario:
    1. Start 5 nodes
    2. Do 3 transactions (nodes at depth 3)
    3. Manually stop Node 4 (simulate crash)
    4. Do 2 more transactions (other nodes at depth 5, Node 4 at depth 3)
    5. Restart Node 4 (loads from disk, still at depth 3)
    6. Do another transaction -> Extra credit repair should trigger!
    """

    print("=" * 70)
    print("EXTRA CREDIT REPAIR TEST")
    print("=" * 70)
    print()

    # Clean start
    cleanup_data()

    # Initialize all 5 nodes
    print("Step 1: Starting 5 nodes...")
    nodes = {}
    for i in range(1, 6):
        config = NodeConfig(node_id=i, num_nodes=5, initial_balance=100)
        nodes[i] = Node(config)
        print(f"  ✓ Node {i} started")

    time.sleep(2)  # Let network initialize
    print()

    # Step 2: Do 3 transactions (all nodes participate)
    print("Step 2: Doing 3 transactions (all nodes at depth 3)...")
    try:
        nodes[1].money_transfer(1, 2, 10)
        time.sleep(8)
        print(f"  ✓ Transaction 1: Node 1 -> Node 2, $10 (depth 1)")

        nodes[2].money_transfer(2, 3, 5)
        time.sleep(8)
        print(f"  ✓ Transaction 2: Node 2 -> Node 3, $5 (depth 2)")

        nodes[3].money_transfer(3, 1, 8)
        time.sleep(8)
        print(f"  ✓ Transaction 3: Node 3 -> Node 1, $8 (depth 3)")
    except Exception as e:
        print(f"  ✗ Error during transactions: {e}")

    print()

    # Step 3: Stop Node 4 (simulate crash without calling failProcess)
    print("Step 3: Stopping Node 4 network (simulating crash)...")
    nodes[4].network.stop()
    nodes[4].running = False
    print("  ✓ Node 4 is now offline (network stopped)")
    print()

    # Step 4: Do 2 more transactions (Node 4 misses these)
    print("Step 4: Doing 2 more transactions (Node 4 misses these)...")
    try:
        nodes[1].money_transfer(1, 5, 7)
        time.sleep(8)
        print(f"  ✓ Transaction 4: Node 1 -> Node 5, $7 (depth 4)")

        nodes[2].money_transfer(2, 1, 3)
        time.sleep(8)
        print(f"  ✓ Transaction 5: Node 2 -> Node 1, $3 (depth 5)")
    except Exception as e:
        print(f"  ✗ Error during transactions: {e}")

    print()

    # Check depths
    print("Current blockchain depths:")
    for i in range(1, 6):
        if nodes[i].running:
            depth = nodes[i].blockchain.get_depth() - 1
            print(f"  Node {i}: depth {depth}")
        else:
            print(f"  Node {i}: OFFLINE (depth 3 on disk)")
    print()

    # Step 5: Restart Node 4 (but DON'T call fixProcess - no sync)
    print("Step 5: Restarting Node 4 (loads from disk, no sync)...")
    nodes[4].running = True
    nodes[4].network.start()
    print(f"  ✓ Node 4 restarted (still at depth 3 from disk)")
    print()

    time.sleep(2)  # Let network stabilize

    # Step 6: Do another transaction - EXTRA CREDIT REPAIR SHOULD TRIGGER!
    print("Step 6: Doing another transaction (should trigger extra credit repair)...")
    print("=" * 70)
    print("WATCH FOR: 'Extra Credit: Detected Node 4 is behind'")
    print("=" * 70)
    try:
        nodes[1].money_transfer(1, 2, 5)
        time.sleep(10)  # Give time for repair messages
        print(f"  ✓ Transaction 6: Node 1 -> Node 2, $5 (depth 6)")
    except Exception as e:
        print(f"  ✗ Error during transaction: {e}")

    print()

    # Verify final state
    print("=" * 70)
    print("FINAL VERIFICATION")
    print("=" * 70)
    print()

    print("Final blockchain depths:")
    for i in range(1, 6):
        if nodes[i].running:
            depth = nodes[i].blockchain.get_depth() - 1
            print(f"  Node {i}: depth {depth}")
    print()

    print("Final balances (Node 1):")
    nodes[1].print_balances()
    print()

    # Check if Node 4 caught up
    node4_depth = nodes[4].blockchain.get_depth() - 1
    if node4_depth == 6:
        print("✓ SUCCESS: Node 4 caught up to depth 6 via extra credit repair!")
    else:
        print(f"✗ ISSUE: Node 4 is at depth {node4_depth}, expected 6")

    print()

    # Cleanup
    print("Stopping all nodes...")
    for i in range(1, 6):
        try:
            nodes[i].network.stop()
        except:
            pass
    print("✓ Test complete")
    print()

if __name__ == "__main__":
    test_extra_credit_repair()