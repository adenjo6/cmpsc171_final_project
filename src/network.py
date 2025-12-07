# src/network.py

import json
import socket
import threading
import time
from typing import Any, Callable, Dict, Tuple, Optional

Message = Dict[str, Any]
OnMessageCallback = Callable[[Message, Tuple[str, int]], None]


class Network:
    """
    Simple TCP-based network layer.

    - Each node listens on its (host, port)
    - Messages are JSON {"from": int, "type": str, "payload": {...}}
    - One message per connection (for simplicity)
    - 'delay' introduces a constant delay before sending (in seconds)
    """

    def __init__(
        self,
        node_id: int,
        host: str,
        port: int,
        nodes_config: Dict[str, Dict[str, Any]],
        on_message: OnMessageCallback,
        delay: float = 3.0,  # <-- default 3-second delay per spec
    ):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.nodes_config = nodes_config
        self.on_message = on_message
        self.delay = delay

        self.running: bool = False
        self._server_thread: Optional[threading.Thread] = None
        self._server_sock: Optional[socket.socket] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        if self.running:
            return
        self.running = True
        self._server_thread = threading.Thread(
            target=self._server_loop, daemon=True
        )
        self._server_thread.start()
        print(f"[Network {self.node_id}] Listening on {self.host}:{self.port}")

    def stop(self) -> None:
        self.running = False
        if self._server_sock is not None:
            try:
                self._server_sock.close()
            except OSError:
                pass
        print(f"[Network {self.node_id}] Stopped")

    def send_message(self, target_id: int, message: Message) -> None:
        """
        Connect to target node and send a single JSON message.
        """
        target_info = self.nodes_config.get(str(target_id))
        if not target_info:
            raise ValueError(f"Unknown target node id {target_id}")

        host = target_info["host"]
        port = target_info["port"]

        # Constant artificial delay
        if self.delay > 0:
            time.sleep(self.delay)

        try:
            with socket.create_connection((host, port), timeout=2.0) as sock:
                raw = json.dumps(message).encode("utf-8")
                sock.sendall(raw)
            print(
                f"[Network {self.node_id}] Sent message to Node {target_id}: "
                f"{message}"
            )
        except OSError as e:
            print(
                f"[Network {self.node_id}] Failed to send to Node {target_id} "
                f"({host}:{port}): {e}"
            )

    def broadcast(self, message: Message) -> None:
        """
        Send a message to all other nodes.
        """
        for nid_str in self.nodes_config.keys():
            nid = int(nid_str)
            if nid == self.node_id:
                continue
            self.send_message(nid, message)

    # ------------------------------------------------------------------
    # Internal server loop
    # ------------------------------------------------------------------

    def _server_loop(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            self._server_sock = s
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.host, self.port))
            s.listen()

            while self.running:
                try:
                    conn, addr = s.accept()
                except OSError:
                    # Socket closed while stopping
                    break

                threading.Thread(
                    target=self._handle_client, args=(conn, addr), daemon=True
                ).start()

    def _handle_client(self, conn: socket.socket, addr: Tuple[str, int]) -> None:
        with conn:
            try:
                data = conn.recv(4096)
            except ConnectionError:
                return

            if not data:
                return

            try:
                msg = json.loads(data.decode("utf-8"))
            except json.JSONDecodeError as e:
                print(
                    f"[Network {self.node_id}] Failed to decode message from {addr}: {e}"
                )
                return

            # Dispatch to node callback
            self.on_message(msg, addr)
