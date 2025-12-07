# src/main.py

import argparse

from node import Node, NodeConfig
from cli import run_cli


def main():
    parser = argparse.ArgumentParser(
        description="Private Blockchain Node (no Paxos yet)"
    )
    parser.add_argument("--node-id", type=int, required=True, help="Node id (1..N)")
    parser.add_argument(
        "--num-nodes", type=int, default=5, help="Total number of nodes"
    )
    parser.add_argument(
        "--initial-balance",
        type=int,
        default=100,
        help="Initial balance for each node",
    )

    args = parser.parse_args()

    config = NodeConfig(
        node_id=args.node_id,
        num_nodes=args.num_nodes,
        initial_balance=args.initial_balance,
    )

    node = Node(config)
    run_cli(node)


if __name__ == "__main__":
    main()
