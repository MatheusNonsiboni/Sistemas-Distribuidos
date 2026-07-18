import socket
import threading

from common import config
from common.protocol import (
    MessageStream,
    REGISTER,
    UNREGISTER,
    LOOKUP,
    LOOKUP_RESPONSE,
    ERROR,
    make_message,
)

class NameService:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self._table: dict[str, dict] = {}
        self._lock = threading.Lock()

    def register(self, nome: str, ip: str, porta: int, node_id: str | None = None) -> None:
        with self._lock:
            self._table[nome] = {"ip": ip, "porta": porta, "node_id": node_id}
        print(f"[NameService] REGISTER  {nome} -> {ip}:{porta} (node_id={node_id})")

    def unregister(self, nome: str) -> None:
        with self._lock:
            removido = self._table.pop(nome, None)
        if removido:
            print(f"[NameService] UNREGISTER {nome}")

    def lookup(self, nome: str | None) -> list[dict]:
        with self._lock:
            if not nome:
                return [{"nome": n, **info} for n, info in self._table.items()]
            info = self._table.get(nome)
            return [{"nome": nome, **info}] if info else []

    def start(self) -> None:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(config.LISTEN_BACKLOG)
        print(f"[NameService] escutando em {self.host}:{self.port}")

        try:
            while True:
                conn, addr = srv.accept()
                threading.Thread(target=self._handle_client, args=(conn, addr), daemon=True).start()
        except KeyboardInterrupt:
            print("\n[NameService] encerrando.")
        finally:
            srv.close()

    def _handle_client(self, conn: socket.socket, addr) -> None:
        stream = MessageStream(conn)
        try:
            self._dispatch(stream, stream.recv())
        except (ConnectionError, OSError) as e:
            print(f"[NameService] conexão com {addr} encerrada: {e}")
        finally:
            stream.close()

    def _dispatch(self, stream: MessageStream, msg: dict) -> None:
        mtype = msg.get("type")
        payload = msg.get("payload", {})

        if mtype == REGISTER:
            self.register(payload["nome"], payload["ip"], payload["porta"], payload.get("node_id"))
            stream.send(make_message(LOOKUP_RESPONSE, "name_service", {"ok": True}))

        elif mtype == UNREGISTER:
            self.unregister(payload["nome"])
            stream.send(make_message(LOOKUP_RESPONSE, "name_service", {"ok": True}))

        elif mtype == LOOKUP:
            resultados = self.lookup(payload.get("nome"))
            stream.send(make_message(LOOKUP_RESPONSE, "name_service", {"resultados": resultados}))

        else:
            stream.send(make_message(ERROR, "name_service", {"erro": f"tipo desconhecido: {mtype}"}))

def main():
    NameService(config.NAME_SERVICE_HOST, config.NAME_SERVICE_PORT).start()

if __name__ == "__main__":
    main()
