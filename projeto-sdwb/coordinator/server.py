import socket
import threading

from common import config
from common.protocol import (
    MessageStream,
    make_message,
    REGISTER,
    JOIN_BOARD,
    JOIN_ACCEPTED,
    STATE_SYNC,
    MEMBER_LIST,
    LEAVE_BOARD,
    DRAW_LINE,
    DRAW_SQUARE,
    OBJECT_CREATED,
    SELECT_REQUEST,
    SELECT_GRANTED,
    SELECT_DENIED,
    RELEASE_SELECT,
    RECOLOR,
    DELETE_OBJECT,
    TX_BEGIN,
    TX_PREPARE,
    TX_COMMIT,
    TX_ABORT,
    STATE_REQUEST,
    STATE_REPLY,
)
from coordinator.board_state import BoardState, ObjetoJaTravado, ObjetoInexistente
from coordinator.node import Node, PeerInfo

class BoardCoordinatorServer:
    def __init__(self, ip: str, porta: int, board_name: str, ui_callback=None):
        self.board_name = board_name
        self.board = BoardState()
        self.node = Node(
            ip, porta,
            on_become_coordinator=self._on_become_coordinator,
            on_coordinator_changed=self._on_coordinator_changed,
        )
        self.ui_callback = ui_callback

        self._client_addrs: dict[str, PeerInfo] = {}
        self._lock = threading.Lock()

        self._register_handlers()

    def _notify_ui(self, evento: str, dados: dict) -> None:
        if self.ui_callback is not None:
            try:
                self.ui_callback(evento, dados)
            except Exception as e:
                print(f"[Coordinator {self.node.node_id}] erro no ui_callback: {e}")

    def _on_coordinator_changed(self, peer: PeerInfo) -> None:
        self._notify_ui("coordinator_changed", {"coordinator": peer.as_dict()})

    def start(self, is_initial_coordinator: bool = True) -> None:
        self.node.start()
        if is_initial_coordinator:
            self.node.set_coordinator(PeerInfo(self.node.node_id, self.node.ip, self.node.porta))
            self._register_in_name_service()

    def stop(self) -> None:
        self.node.stop()

    def _register_in_name_service(self) -> None:
        try:
            with socket.create_connection(
                (config.NAME_SERVICE_HOST, config.NAME_SERVICE_PORT), timeout=3.0
            ) as s:
                stream = MessageStream(s)
                stream.send(make_message(REGISTER, str(self.node.node_id), {
                    "nome": self.board_name,
                    "ip": self.node.ip,
                    "porta": self.node.porta,
                    "node_id": self.node.node_id,
                }))
                stream.recv()
            print(f"[Coordinator {self.node.node_id}] registrado no Serviço de Nomes como '{self.board_name}'.")
        except (OSError, ConnectionError) as e:
            print(f"[Coordinator {self.node.node_id}] falha ao registrar no Serviço de Nomes: {e}")

    def _on_become_coordinator(self) -> None:
        print(f"[Coordinator {self.node.node_id}] assumindo papel de coordenador após eleição.")
        self._request_state_recovery()
        self._register_in_name_service()
        self._broadcast_member_list()

    def _register_handlers(self) -> None:
        self.node.register_handler(JOIN_BOARD, self._handle_join)
        self.node.register_handler(LEAVE_BOARD, self._handle_leave)
        self.node.register_handler(DRAW_LINE, self._handle_draw_line)
        self.node.register_handler(DRAW_SQUARE, self._handle_draw_square)
        self.node.register_handler(SELECT_REQUEST, self._handle_select_request)
        self.node.register_handler(RELEASE_SELECT, self._handle_release_select)
        self.node.register_handler(RECOLOR, self._handle_recolor)
        self.node.register_handler(DELETE_OBJECT, self._handle_delete_object)
        self.node.register_handler(TX_BEGIN, self._handle_tx_begin)
        self.node.register_handler(STATE_REQUEST, self._handle_state_request)
        self.node.register_handler(OBJECT_CREATED, self._handle_replicated_object_created)
        self.node.register_handler(MEMBER_LIST, self._handle_member_list_update)
        self.node.register_handler(TX_COMMIT, self._handle_replicated_tx_commit)
        self.node.register_handler(TX_ABORT, self._handle_replicated_tx_abort)

    # onboarding

    def _handle_join(self, msg: dict, stream: MessageStream) -> None:
        payload = msg["payload"]
        client_id = payload["client_id"]
        peer = PeerInfo(payload["node_id"], payload["ip"], payload["porta"])

        with self._lock:
            self._client_addrs[client_id] = peer
        self.node.add_peer(peer)
        self.board.adicionar_membro(client_id, payload["node_id"], payload["ip"], payload["porta"])

        stream.send(make_message(JOIN_ACCEPTED, str(self.node.node_id), {
            "coordinator": PeerInfo(self.node.node_id, self.node.ip, self.node.porta).as_dict(),
        }))
        stream.send(make_message(STATE_SYNC, str(self.node.node_id), {
            "objetos": self.board.snapshot_objetos(),
            "membros": [p.as_dict() for p in self.node.known_peers()] + [
                PeerInfo(self.node.node_id, self.node.ip, self.node.porta).as_dict()
            ],
        }))

        print(f"[Coordinator {self.node.node_id}] cliente '{client_id}' ingressou ({payload['ip']}:{payload['porta']}).")
        self._broadcast_member_list()

    def _handle_leave(self, msg: dict, stream: MessageStream) -> None:
        client_id = msg["payload"]["client_id"]
        with self._lock:
            peer = self._client_addrs.pop(client_id, None)
        if peer:
            self.node.remove_peer(peer.node_id)
        self.board.remover_membro(client_id)
        print(f"[Coordinator {self.node.node_id}] cliente '{client_id}' saiu.")
        self._broadcast_member_list()

    def _broadcast_member_list(self) -> None:
        membros = [p.as_dict() for p in self.node.known_peers()]
        membros.append(PeerInfo(self.node.node_id, self.node.ip, self.node.porta).as_dict())
        self._broadcast(make_message(MEMBER_LIST, str(self.node.node_id), {"membros": membros}))

    # desenho

    def _handle_draw_line(self, msg: dict, stream: MessageStream) -> None:
        payload = msg["payload"]
        obj_id = self.board.novo_obj_id()
        obj = self.board.adicionar_objeto(obj_id, "linha", {
            "p1": payload["p1"], "p2": payload["p2"], "cor": payload.get("cor", "preto"),
        })
        stream.send(make_message(OBJECT_CREATED, str(self.node.node_id), {"objeto": obj}))
        self._broadcast(make_message(OBJECT_CREATED, str(self.node.node_id), {"objeto": obj}), exceto=msg.get("from"))
        self._notify_ui("object_created", {"objeto": obj})

    def _handle_draw_square(self, msg: dict, stream: MessageStream) -> None:
        payload = msg["payload"]
        obj_id = self.board.novo_obj_id()
        obj = self.board.adicionar_objeto(obj_id, "quadrado", {
            "p1": payload["p1"], "p2": payload["p2"], "cor": payload.get("cor", "preto"),
        })
        stream.send(make_message(OBJECT_CREATED, str(self.node.node_id), {"objeto": obj}))
        self._broadcast(make_message(OBJECT_CREATED, str(self.node.node_id), {"objeto": obj}), exceto=msg.get("from"))
        self._notify_ui("object_created", {"objeto": obj})

    # exclusão mútua

    def _handle_select_request(self, msg: dict, stream: MessageStream) -> None:
        payload = msg["payload"]
        obj_id = payload["obj_id"]
        try:
            self.board.selecionar(obj_id, payload["client_id"])
            stream.send(make_message(SELECT_GRANTED, str(self.node.node_id), {"obj_id": obj_id}))
        except ObjetoJaTravado as e:
            stream.send(make_message(SELECT_DENIED, str(self.node.node_id), {"obj_id": obj_id, "motivo": str(e)}))
        except ObjetoInexistente:
            stream.send(make_message(SELECT_DENIED, str(self.node.node_id), {"obj_id": obj_id, "motivo": "objeto não existe"}))

    def _handle_release_select(self, msg: dict, stream: MessageStream) -> None:
        payload = msg["payload"]
        self.board.liberar_selecao(payload["obj_id"], payload["client_id"])

    def _handle_recolor(self, msg: dict, stream: MessageStream) -> None:
        # esta mensagem chega tanto como pedido de cliente (autoridade) quanto
        # como broadcast do coordenador atual para os demais peers (réplica)
        payload = msg["payload"]
        obj_id = payload["obj_id"]
        veio_do_coordenador = self._veio_do_coordenador_atual(msg)

        if veio_do_coordenador:
            try:
                self.board.recolorir_objeto(obj_id, payload["cor"])
                self._notify_ui("object_recolored", {"obj_id": obj_id, "cor": payload["cor"]})
            except ObjetoInexistente:
                pass
            return

        try:
            self.board.recolorir_objeto(obj_id, payload["cor"])
            self.board.liberar_selecao(obj_id, payload.get("client_id", ""))
            self._broadcast(make_message(RECOLOR, str(self.node.node_id), {"obj_id": obj_id, "cor": payload["cor"]}))
            stream.send(make_message(RECOLOR, str(self.node.node_id), {"obj_id": obj_id, "cor": payload["cor"]}))
            self._notify_ui("object_recolored", {"obj_id": obj_id, "cor": payload["cor"]})
        except ObjetoInexistente:
            stream.send(make_message(SELECT_DENIED, str(self.node.node_id), {"obj_id": obj_id, "motivo": "objeto não existe"}))

    def _handle_delete_object(self, msg: dict, stream: MessageStream) -> None:
        payload = msg["payload"]
        obj_id = payload["obj_id"]

        if self._veio_do_coordenador_atual(msg):
            self.board.remover_objeto(obj_id)
            self._notify_ui("object_deleted", {"obj_id": obj_id})
            return

        self.board.remover_objeto(obj_id)
        self._broadcast(make_message(DELETE_OBJECT, str(self.node.node_id), {"obj_id": obj_id}))
        stream.send(make_message(DELETE_OBJECT, str(self.node.node_id), {"obj_id": obj_id}))
        self._notify_ui("object_deleted", {"obj_id": obj_id})

    def _veio_do_coordenador_atual(self, msg: dict) -> bool:
        coord = self.node.get_coordinator()
        return (coord is not None and str(coord.node_id) == str(msg.get("from"))
                and not self.node.is_coordinator())

    # transações 2PC 

    def _handle_tx_begin(self, msg: dict, stream: MessageStream) -> None:
        payload = msg["payload"]
        operacao = payload["operacao"]
        autor = payload["autor"]
        tx_id = self.board.novo_tx_id()

        if operacao == "criar_atomico":
            objetos = payload["objetos_a_criar"]
            obj_ids = [o["id"] for o in objetos]
            voto = not any(self.board.existe(oid) for oid in obj_ids)
            if voto:
                self.board._transactions[tx_id] = {
                    "obj_ids": obj_ids, "operacao": operacao, "autor": autor, "estado": "preparado",
                }
                self.board.tx_set_objetos_a_criar(tx_id, objetos)
        else:
            obj_ids = payload["obj_ids"]
            voto = self.board.tx_prepare(tx_id, obj_ids, operacao, autor)

        if voto:
            grupo_id = f"grupo-{tx_id}" if operacao == "agrupar" else None
            self.board.tx_commit(tx_id, grupo_id=grupo_id)
            resultado = make_message(TX_COMMIT, str(self.node.node_id), {
                "tx_id": tx_id, "operacao": operacao, "obj_ids": obj_ids, "grupo_id": grupo_id,
                "objetos_criados": self.board._transactions[tx_id].get("objetos_a_criar", []) if operacao == "criar_atomico" else [],
            })
        else:
            self.board.tx_abort(tx_id)
            resultado = make_message(TX_ABORT, str(self.node.node_id), {
                "tx_id": tx_id, "operacao": operacao, "obj_ids": obj_ids,
                "motivo": "conflito: um ou mais objetos não estão disponíveis",
            })

        stream.send(resultado)
        self._broadcast(resultado, exceto=msg.get("from"))
        self._notify_ui("tx_committed" if voto else "tx_aborted", resultado["payload"])

    def _handle_replicated_tx_commit(self, msg: dict, stream: MessageStream) -> None:
        if not self._veio_do_coordenador_atual(msg):
            return
        payload = msg["payload"]
        operacao = payload["operacao"]
        obj_ids = payload["obj_ids"]

        if operacao == "agrupar":
            for oid in obj_ids:
                if self.board.existe(oid):
                    self.board._objects[oid]["grupo_id"] = payload.get("grupo_id")
        elif operacao == "deletar_conjunto":
            for oid in obj_ids:
                self.board.remover_objeto(oid)
        elif operacao == "criar_atomico":
            for obj in payload.get("objetos_criados", []):
                self.board.adicionar_objeto(obj["id"], obj["tipo"], {k: v for k, v in obj.items() if k not in ("id", "tipo")})

        self._notify_ui("tx_committed", payload)

    def _handle_replicated_tx_abort(self, msg: dict, stream: MessageStream) -> None:
        if not self._veio_do_coordenador_atual(msg):
            return
        self._notify_ui("tx_aborted", msg["payload"])

    # recuperação de estado pós-eleição

    def _handle_state_request(self, msg: dict, stream: MessageStream) -> None:
        stream.send(make_message(STATE_REPLY, str(self.node.node_id), {
            "objetos": self.board.snapshot_objetos(),
            "membros": self.board.snapshot_membros(),
        }))

    def _handle_replicated_object_created(self, msg: dict, stream: MessageStream) -> None:
        if not self._veio_do_coordenador_atual(msg):
            return
        obj = msg["payload"]["objeto"]
        self.board.adicionar_objeto(obj["id"], obj["tipo"], {k: v for k, v in obj.items() if k not in ("id", "tipo")})
        self._notify_ui("object_created", {"objeto": obj})

    def _handle_member_list_update(self, msg: dict, stream: MessageStream) -> None:
        for m in msg["payload"]["membros"]:
            self.node.add_peer(PeerInfo(m["node_id"], m["ip"], m["porta"]))
        self._notify_ui("member_list_updated", {"membros": msg["payload"]["membros"]})

    def _request_state_recovery(self) -> None:
        for peer in self.node.known_peers():
            estado = self._request_state_from_peer(peer)
            if estado is not None:
                for obj in estado["objetos"]:
                    self.board.adicionar_objeto(obj["id"], obj["tipo"], {k: v for k, v in obj.items() if k not in ("id", "tipo")})
                for client_id, info in estado.get("membros", {}).items():
                    self.board.adicionar_membro(client_id, info["node_id"], info["ip"], info["porta"])
                print(f"[Coordinator {self.node.node_id}] estado recuperado de peer {peer.node_id}: {len(estado['objetos'])} objeto(s).")
                return
        print(f"[Coordinator {self.node.node_id}] nenhum peer respondeu ao pedido de recuperação de estado.")

    def _request_state_from_peer(self, peer: PeerInfo) -> dict | None:
        try:
            with socket.create_connection((peer.ip, peer.porta), timeout=2.0) as s:
                stream = MessageStream(s)
                stream.send(make_message(STATE_REQUEST, str(self.node.node_id)))
                resp = stream.recv()
                if resp.get("type") == STATE_REPLY:
                    return resp["payload"]
        except (OSError, ConnectionError, TimeoutError):
            pass
        return None

    # broadcast

    def _broadcast(self, msg: dict, exceto: str | None = None) -> None:
        for peer in self.node.known_peers():
            if exceto is not None and str(peer.node_id) == str(exceto):
                continue
            self._send_to_peer(peer, msg)

    def _send_to_peer(self, peer: PeerInfo, msg: dict) -> None:
        try:
            with socket.create_connection((peer.ip, peer.porta), timeout=2.0) as s:
                MessageStream(s).send(msg)
        except (OSError, ConnectionError, TimeoutError):
            pass

def main():
    import sys
    import time
    porta = int(sys.argv[1]) if len(sys.argv) > 1 else 9100
    server = BoardCoordinatorServer("127.0.0.1", porta, config.BOARD_COORDINATOR_NAME)
    server.start(is_initial_coordinator=True)
    print("Coordenador rodando. Ctrl+C para encerrar.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        server.stop()

if __name__ == "__main__":
    main()
