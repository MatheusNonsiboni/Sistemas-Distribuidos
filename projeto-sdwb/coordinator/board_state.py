import itertools
import threading
import time

class ObjetoJaTravado(Exception):
    pass

class ObjetoInexistente(Exception):
    pass

class BoardState:
    def __init__(self):
        self._lock = threading.RLock()
        self._objects: dict[str, dict] = {}
        self._selection: dict[str, str] = {}
        self._members: dict[str, dict] = {}
        self._id_counter = itertools.count(1)
        self._transactions: dict[str, dict] = {}
        self._tx_counter = itertools.count(1)

    # objetos 

    def novo_obj_id(self) -> str:
        return f"obj-{next(self._id_counter)}"

    def novo_tx_id(self) -> str:
        return f"tx-{next(self._tx_counter)}"

    def recalibrar_contador_apos_id(self, obj_id: str) -> None:
        # necessário quando um objeto chega via réplica/recuperação de outro
        # BoardState, cujo contador interno não é compartilhado com o nosso
        prefixo = "obj-"
        if not obj_id.startswith(prefixo):
            return
        try:
            numero = int(obj_id[len(prefixo):])
        except ValueError:
            return
        with self._lock:
            atual = next(self._id_counter)
            self._id_counter = itertools.count(max(atual, numero + 1))

    def adicionar_objeto(self, obj_id: str, tipo: str, dados: dict) -> dict:
        with self._lock:
            obj = {"id": obj_id, "tipo": tipo, **dados}
            self._objects[obj_id] = obj
            self.recalibrar_contador_apos_id(obj_id)
            return obj

    def remover_objeto(self, obj_id: str) -> None:
        with self._lock:
            self._objects.pop(obj_id, None)
            self._selection.pop(obj_id, None)

    def recolorir_objeto(self, obj_id: str, cor: str) -> dict:
        with self._lock:
            if obj_id not in self._objects:
                raise ObjetoInexistente(obj_id)
            self._objects[obj_id]["cor"] = cor
            return self._objects[obj_id]

    def snapshot_objetos(self) -> list[dict]:
        with self._lock:
            return list(self._objects.values())

    def existe(self, obj_id: str) -> bool:
        with self._lock:
            return obj_id in self._objects

    # exclusão mútua

    def selecionar(self, obj_id: str, client_id: str) -> None:
        with self._lock:
            if obj_id not in self._objects:
                raise ObjetoInexistente(obj_id)
            dono_atual = self._selection.get(obj_id)
            if dono_atual is not None and dono_atual != client_id:
                raise ObjetoJaTravado(f"{obj_id} já selecionado por {dono_atual}")
            self._selection[obj_id] = client_id

    def liberar_selecao(self, obj_id: str, client_id: str) -> None:
        with self._lock:
            if self._selection.get(obj_id) == client_id:
                del self._selection[obj_id]

    def liberar_todas_de(self, client_id: str) -> None:
        with self._lock:
            obsoletos = [oid for oid, c in self._selection.items() if c == client_id]
            for oid in obsoletos:
                del self._selection[oid]

    # membros

    def adicionar_membro(self, client_id: str, node_id: str, ip: str, porta: int) -> None:
        with self._lock:
            self._members[client_id] = {"node_id": node_id, "ip": ip, "porta": porta}

    def remover_membro(self, client_id: str) -> None:
        with self._lock:
            self._members.pop(client_id, None)
        self.liberar_todas_de(client_id)

    def snapshot_membros(self) -> dict[str, dict]:
        with self._lock:
            return dict(self._members)

    # transações 2PC
    # a decisão é local: o coordenador já é a fonte de verdade do estado,
    # então "posso travar estes objetos?" é resolvido sem perguntar a
    # nenhum nó remoto.

    def tx_prepare(self, tx_id: str, obj_ids: list[str], operacao: str, autor: str) -> bool:
        with self._lock:
            for oid in obj_ids:
                if oid not in self._objects:
                    return False
                dono = self._selection.get(oid)
                if dono is not None and dono != autor:
                    return False

            for oid in obj_ids:
                self._selection[oid] = autor

            self._transactions[tx_id] = {
                "obj_ids": list(obj_ids),
                "operacao": operacao,
                "autor": autor,
                "estado": "preparado",
                "criada_em": time.time(),
            }
            return True

    def tx_commit(self, tx_id: str, grupo_id: str | None = None) -> None:
        with self._lock:
            tx = self._transactions.get(tx_id)
            if tx is None:
                return
            if tx["operacao"] == "agrupar":
                for oid in tx["obj_ids"]:
                    if oid in self._objects:
                        self._objects[oid]["grupo_id"] = grupo_id
            elif tx["operacao"] == "deletar_conjunto":
                for oid in tx["obj_ids"]:
                    self._objects.pop(oid, None)
            elif tx["operacao"] == "criar_atomico":
                for obj in tx.get("objetos_a_criar", []):
                    self._objects[obj["id"]] = obj

            for oid in tx["obj_ids"]:
                self._selection.pop(oid, None)
            tx["estado"] = "commitado"

    def tx_abort(self, tx_id: str) -> None:
        with self._lock:
            tx = self._transactions.get(tx_id)
            if tx is None:
                return
            for oid in tx["obj_ids"]:
                if self._selection.get(oid) == tx["autor"]:
                    del self._selection[oid]
            tx["estado"] = "abortada"

    def tx_set_objetos_a_criar(self, tx_id: str, objetos: list[dict]) -> None:
        with self._lock:
            if tx_id in self._transactions:
                self._transactions[tx_id]["objetos_a_criar"] = objetos
