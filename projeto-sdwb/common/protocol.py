import json
import socket

# Serviço de Nomes
REGISTER = "REGISTER"
LOOKUP = "LOOKUP"
LOOKUP_RESPONSE = "LOOKUP_RESPONSE"
UNREGISTER = "UNREGISTER"

# Onboarding / sincronização de estado
JOIN_BOARD = "JOIN_BOARD"
JOIN_ACCEPTED = "JOIN_ACCEPTED"
STATE_SYNC = "STATE_SYNC"
MEMBER_LIST = "MEMBER_LIST"
LEAVE_BOARD = "LEAVE_BOARD"

# Desenho
DRAW_LINE = "DRAW_LINE"
DRAW_SQUARE = "DRAW_SQUARE"
OBJECT_CREATED = "OBJECT_CREATED"

# Exclusão mútua
SELECT_REQUEST = "SELECT_REQUEST"
SELECT_GRANTED = "SELECT_GRANTED"
SELECT_DENIED = "SELECT_DENIED"
RELEASE_SELECT = "RELEASE_SELECT"
RECOLOR = "RECOLOR"
DELETE_OBJECT = "DELETE_OBJECT"

# 2PC
TX_BEGIN = "TX_BEGIN"
TX_PREPARE = "TX_PREPARE"
TX_VOTE_YES = "TX_VOTE_YES"
TX_VOTE_NO = "TX_VOTE_NO"
TX_COMMIT = "TX_COMMIT"
TX_ABORT = "TX_ABORT"

# Heartbeat e eleição (Bully)
PING = "PING"
PONG = "PONG"
ELECTION = "ELECTION"
ELECTION_OK = "ELECTION_OK"
COORDINATOR_ANNOUNCE = "COORDINATOR_ANNOUNCE"
WHO_IS_COORDINATOR = "WHO_IS_COORDINATOR"

# Recuperação de estado pós-eleição
STATE_REQUEST = "STATE_REQUEST"
STATE_REPLY = "STATE_REPLY"

ERROR = "ERROR"

def make_message(msg_type: str, sender_id: str, payload: dict | None = None) -> dict:
    return {"type": msg_type, "from": sender_id, "payload": payload or {}}

def send_msg(sock: socket.socket, msg: dict) -> None:
    sock.sendall(json.dumps(msg).encode("utf-8") + b"\n")

def recv_msg(sock: socket.socket, buffer: bytes = b"") -> tuple[dict, bytes]:
    # TCP é um stream de bytes; acumula até achar o delimitador '\n'
    while b"\n" not in buffer:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("Conexão fechada pelo outro lado.")
        buffer += chunk
    line, _, rest = buffer.partition(b"\n")
    return json.loads(line.decode("utf-8")), rest

class MessageStream:
    def __init__(self, sock: socket.socket):
        self.sock = sock
        self._buffer = b""

    def send(self, msg: dict) -> None:
        send_msg(self.sock, msg)

    def recv(self) -> dict:
        msg, self._buffer = recv_msg(self.sock, self._buffer)
        return msg

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass
