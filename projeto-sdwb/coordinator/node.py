import socket
import threading
import time
from typing import Callable

from common import config
from common.protocol import (
    MessageStream,
    make_message,
    PING,
    PONG,
    ELECTION,
    ELECTION_OK,
    COORDINATOR_ANNOUNCE,
)

class PeerInfo:
    __slots__ = ("node_id", "ip", "porta")

    def __init__(self, node_id: int, ip: str, porta: int):
        self.node_id = node_id
        self.ip = ip
        self.porta = porta

    def as_dict(self) -> dict:
        return {"node_id": self.node_id, "ip": self.ip, "porta": self.porta}

    @staticmethod
    def from_dict(d: dict) -> "PeerInfo":
        return PeerInfo(d["node_id"], d["ip"], d["porta"])

    def __repr__(self):
        return f"PeerInfo(id={self.node_id}, {self.ip}:{self.porta})"

class Node:
    def __init__(self, ip: str, porta: int,
                 on_become_coordinator: Callable[[], None] | None = None,
                 on_coordinator_changed: Callable[["PeerInfo"], None] | None = None):
        self.ip = ip
        self.porta = porta
        self.node_id = porta  # porta como id: única e comparável, exigido pelo Bully

        self._coordinator: PeerInfo | None = None
        self._peers: dict[int, PeerInfo] = {}
        self._peers_lock = threading.Lock()

        self._is_coordinator = False
        self._running = False

        self._on_become_coordinator = on_become_coordinator
        self._on_coordinator_changed = on_coordinator_changed

        self._extra_handlers: dict[str, Callable[[dict, MessageStream], None]] = {}

        self._election_in_progress = False
        self._election_lock = threading.Lock()

        self._server_socket: socket.socket | None = None

    def register_handler(self, msg_type: str, handler: Callable[[dict, MessageStream], None]) -> None:
        self._extra_handlers[msg_type] = handler

    def set_coordinator(self, peer: PeerInfo) -> None:
        self._coordinator = peer
        self._is_coordinator = peer.node_id == self.node_id
        print(f"[Node {self.node_id}] coordenador atual: {peer}")
        if self._on_coordinator_changed:
            self._on_coordinator_changed(peer)

    def get_coordinator(self) -> PeerInfo | None:
        return self._coordinator

    def is_coordinator(self) -> bool:
        return self._is_coordinator

    def known_peers(self) -> list[PeerInfo]:
        with self._peers_lock:
            return list(self._peers.values())

    def add_peer(self, peer: PeerInfo) -> None:
        if peer.node_id == self.node_id:
            return
        with self._peers_lock:
            self._peers[peer.node_id] = peer

    def remove_peer(self, node_id: int) -> None:
        with self._peers_lock:
            self._peers.pop(node_id, None)

    def set_peers(self, peers: list[PeerInfo]) -> None:
        with self._peers_lock:
            self._peers = {p.node_id: p for p in peers if p.node_id != self.node_id}

    def start(self) -> None:
        self._running = True
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.ip, self.porta))
        srv.listen(config.LISTEN_BACKLOG)
        self._server_socket = srv
        print(f"[Node {self.node_id}] servidor escutando em {self.ip}:{self.porta}")

        threading.Thread(target=self._accept_loop, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

    def stop(self) -> None:
        # desliga a flag ANTES de fechar o socket: evita processar conexões
        # que já estavam pendentes no backlog do SO no momento do close()
        self._running = False
        if self._server_socket:
            try:
                self._server_socket.close()
            except OSError:
                pass

    def _accept_loop(self) -> None:
        while self._running:
            try:
                conn, addr = self._server_socket.accept()
            except OSError:
                break
            threading.Thread(target=self._handle_conn, args=(conn, addr), daemon=True).start()

    def _handle_conn(self, conn: socket.socket, addr) -> None:
        stream = MessageStream(conn)
        try:
            while True:
                self._dispatch(stream.recv(), stream)
        except (ConnectionError, OSError):
            pass
        finally:
            stream.close()

    def _dispatch(self, msg: dict, stream: MessageStream) -> None:
        if not self._running:
            return

        mtype = msg.get("type")

        if mtype == PING:
            stream.send(make_message(PONG, str(self.node_id)))
        elif mtype == ELECTION:
            self._handle_election(msg, stream)
        elif mtype == COORDINATOR_ANNOUNCE:
            self._handle_coordinator_announce(msg)
        elif mtype in self._extra_handlers:
            self._extra_handlers[mtype](msg, stream)
        else:
            print(f"[Node {self.node_id}] mensagem sem handler: {mtype}")

    def _heartbeat_loop(self) -> None:
        while self._running:
            time.sleep(config.HEARTBEAT_INTERVAL)
            if self._is_coordinator or self._coordinator is None:
                continue
            if not self._ping_coordinator():
                print(f"[Node {self.node_id}] coordenador {self._coordinator} não respondeu, iniciando eleição.")
                self.start_election()

    def _ping_coordinator(self) -> bool:
        coord = self._coordinator
        if coord is None:
            return False
        try:
            with socket.create_connection((coord.ip, coord.porta), timeout=config.HEARTBEAT_TIMEOUT) as s:
                stream = MessageStream(s)
                stream.send(make_message(PING, str(self.node_id)))
                return stream.recv().get("type") == PONG
        except (OSError, ConnectionError, TimeoutError):
            return False

    # eleição Bully

    def start_election(self) -> None:
        with self._election_lock:
            if self._election_in_progress:
                return
            self._election_in_progress = True

        try:
            maiores = [p for p in self.known_peers() if p.node_id > self.node_id]
            algum_respondeu = any(self._send_election(peer) for peer in maiores)

            if algum_respondeu:
                print(f"[Node {self.node_id}] eleição: nó(s) de id maior responderam OK, aguardando anúncio.")
            else:
                print(f"[Node {self.node_id}] eleição: nenhum nó maior respondeu, assumindo coordenação.")
                self._become_coordinator()
        finally:
            with self._election_lock:
                self._election_in_progress = False

    def _send_election(self, peer: PeerInfo) -> bool:
        try:
            with socket.create_connection((peer.ip, peer.porta), timeout=config.ELECTION_OK_TIMEOUT) as s:
                stream = MessageStream(s)
                stream.send(make_message(ELECTION, str(self.node_id)))
                return stream.recv().get("type") == ELECTION_OK
        except (OSError, ConnectionError, TimeoutError):
            return False

    def _handle_election(self, msg: dict, stream: MessageStream) -> None:
        stream.send(make_message(ELECTION_OK, str(self.node_id)))
        threading.Thread(target=self.start_election, daemon=True).start()

    def _become_coordinator(self) -> None:
        self._is_coordinator = True
        self._coordinator = PeerInfo(self.node_id, self.ip, self.porta)

        for peer in self.known_peers():
            self._announce_to(peer)

        if self._on_become_coordinator:
            self._on_become_coordinator()

    def _announce_to(self, peer: PeerInfo) -> None:
        try:
            with socket.create_connection((peer.ip, peer.porta), timeout=config.ELECTION_OK_TIMEOUT) as s:
                stream = MessageStream(s)
                stream.send(make_message(
                    COORDINATOR_ANNOUNCE, str(self.node_id),
                    {"node_id": self.node_id, "ip": self.ip, "porta": self.porta},
                ))
        except (OSError, ConnectionError, TimeoutError):
            pass

    def _handle_coordinator_announce(self, msg: dict) -> None:
        self.set_coordinator(PeerInfo.from_dict(msg["payload"]))
