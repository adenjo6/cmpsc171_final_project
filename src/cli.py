# src/cli.py

from node import Node


def run_cli(node: Node):
    """Simple blocking CLI loop for a single node."""
    print(
        f"[CLI] Node {node.node_id} ready. Commands:\n"
        "  moneyTransfer <debit_id> <credit_id> <amount>\n"
        "  printBalance\n"
        "  printBlockchain\n"
        "  validate\n"
        "  sendPing <target_id> [text...]\n"
        "  showMessages\n"
        "  sendPrepare <target_id> <depth> <seq>\n"
        "  sendAccept <target_id> <depth> <seq> <sender_id> <receiver_id> <amount>\n"
        "  showPaxos <depth>\n"
        "  failProcess\n"
        "  fixProcess\n"
        "  quit / exit\n"
    )

    while True:
        try:
            raw = input(f"node{node.node_id}> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n[CLI] Exiting.")
            break

        if not raw:
            continue

        parts = raw.split()
        cmd = parts[0]

        try:
            if cmd == "moneyTransfer":
                if len(parts) != 4:
                    print("Usage: moneyTransfer <debit_id> <credit_id> <amount>")
                    continue
                debit_id = int(parts[1])
                credit_id = int(parts[2])
                amount = int(parts[3])
                node.money_transfer(debit_id, credit_id, amount)

            elif cmd == "printBalance":
                node.print_balances()

            elif cmd == "printBlockchain":
                node.print_blockchain()

            elif cmd == "validate":
                node.validate_chain()

            elif cmd == "sendPing":
                if len(parts) < 2:
                    print("Usage: sendPing <target_id> [text...]")
                    continue
                target_id = int(parts[1])
                text = " ".join(parts[2:]) if len(parts) > 2 else "hello"
                node.send_ping(target_id, text)

            elif cmd == "showMessages":
                node.show_messages()

            elif cmd == "sendPrepare":
                if len(parts) != 4:
                    print("Usage: sendPrepare <target_id> <depth> <seq>")
                    continue
                target_id = int(parts[1])
                depth = int(parts[2])
                seq = int(parts[3])
                node.send_prepare(target_id, depth, seq)

            elif cmd == "sendAccept":
                if len(parts) != 7:
                    print(
                        "Usage: sendAccept <target_id> <depth> <seq> "
                        "<sender_id> <receiver_id> <amount>"
                    )
                    continue
                target_id = int(parts[1])
                depth = int(parts[2])
                seq = int(parts[3])
                sender_id = int(parts[4])
                receiver_id = int(parts[5])
                amount = int(parts[6])
                node.send_accept(
                    target_id, depth, seq, sender_id, receiver_id, amount
                )

            elif cmd == "showPaxos":
                if len(parts) != 2:
                    print("Usage: showPaxos <depth>")
                    continue
                depth = int(parts[1])
                node.show_paxos_state(depth)

            elif cmd == "failProcess":
                node.fail_process()

            elif cmd == "fixProcess":
                node.fix_process()

            elif cmd in ("quit", "exit"):
                print("[CLI] Quitting.")
                break

            else:
                print(f"Unknown command: {cmd}")

        except Exception as e:
            print(f"[Error] {e}")
