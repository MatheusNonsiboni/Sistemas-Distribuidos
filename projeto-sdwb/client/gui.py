import socket
import threading
import queue
import tkinter as tk
from tkinter import messagebox, simpledialog
import random
import time

from common import config
from common.protocol import (
    MessageStream, make_message,
    LOOKUP,
    JOIN_BOARD, JOIN_ACCEPTED, STATE_SYNC,
    DRAW_LINE, DRAW_SQUARE, OBJECT_CREATED,
    SELECT_REQUEST, SELECT_GRANTED, SELECT_DENIED, RELEASE_SELECT,
    RECOLOR, DELETE_OBJECT,
    TX_BEGIN, TX_COMMIT, TX_ABORT,
    LEAVE_BOARD,
)
from coordinator.server import BoardCoordinatorServer
from coordinator.node import PeerInfo

CORES_DISPONIVEIS = ["#E74C3C", "#2980B9"]
COR_PADRAO = "#1B1B1B"


def listar_quadros() -> list[dict]:
    with socket.create_connection((config.NAME_SERVICE_HOST, config.NAME_SERVICE_PORT), timeout=3.0) as s:
        stream = MessageStream(s)
        stream.send(make_message(LOOKUP, "client-discovery", {"nome": None}))
        return stream.recv()["payload"]["resultados"]


def porta_livre_aleatoria() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    porta = s.getsockname()[1]
    s.close()
    return porta


class TelaInicial(tk.Frame):
    def __init__(self, master, on_quadro_pronto):
        super().__init__(master, padx=24, pady=24)
        self.on_quadro_pronto = on_quadro_pronto

        tk.Label(self, text="Shared Distributed Write Board", font=("Helvetica", 16, "bold")).pack(pady=(0, 4))
        tk.Label(self, text="Sistemas Distribuídos — Projeto Final", font=("Helvetica", 10)).pack(pady=(0, 20))
        tk.Button(self, text="CRIAR NOVO QUADRO", width=30, height=2, command=self._criar_quadro).pack(pady=6)
        tk.Button(self, text="INGRESSAR EM QUADRO EXISTENTE", width=30, height=2, command=self._ingressar_quadro).pack(pady=6)

    def _criar_quadro(self) -> None:
        nome = simpledialog.askstring("Novo quadro", "Nome do novo quadro:", parent=self)
        if not nome:
            return
        client_id = simpledialog.askstring("Identificação", "Seu nome de usuário:", parent=self) or f"user-{random.randint(1000, 9999)}"

        porta = porta_livre_aleatoria()
        coord = BoardCoordinatorServer("127.0.0.1", porta, nome)
        try:
            coord.start(is_initial_coordinator=True)
        except OSError as e:
            messagebox.showerror("Erro", f"Não foi possível iniciar o quadro: {e}")
            return

        self.on_quadro_pronto(coord, client_id, nome)

    def _ingressar_quadro(self) -> None:
        try:
            quadros = listar_quadros()
        except (OSError, ConnectionError) as e:
            messagebox.showerror("Serviço de Nomes indisponível",
                                  f"Não foi possível consultar {config.NAME_SERVICE_HOST}:{config.NAME_SERVICE_PORT}.\n\n{e}")
            return
        if not quadros:
            messagebox.showinfo("Nenhum quadro", "Não há quadros registrados no momento.")
            return
        JanelaEscolherQuadro(self, quadros, self._on_quadro_escolhido).grab_set()

    def _on_quadro_escolhido(self, quadro: dict) -> None:
        client_id = simpledialog.askstring("Identificação", "Seu nome de usuário:", parent=self) or f"user-{random.randint(1000, 9999)}"

        porta = porta_livre_aleatoria()
        coord = BoardCoordinatorServer("127.0.0.1", porta, quadro["nome"])
        try:
            coord.start(is_initial_coordinator=False)
        except OSError as e:
            messagebox.showerror("Erro", f"Não foi possível iniciar o cliente: {e}")
            return

        try:
            with socket.create_connection((quadro["ip"], quadro["porta"]), timeout=4.0) as s:
                stream = MessageStream(s)
                stream.send(make_message(JOIN_BOARD, str(porta), {
                    "client_id": client_id, "node_id": porta, "ip": "127.0.0.1", "porta": porta,
                }))
                accepted = stream.recv()
                state = stream.recv()
        except (OSError, ConnectionError, TimeoutError) as e:
            messagebox.showerror("Erro ao ingressar", f"Falha ao contatar o coordenador: {e}")
            coord.stop()
            return

        if accepted.get("type") != JOIN_ACCEPTED:
            messagebox.showerror("Erro", "Coordenador rejeitou o ingresso.")
            coord.stop()
            return

        ci = accepted["payload"]["coordinator"]
        coord.node.set_coordinator(PeerInfo(ci["node_id"], ci["ip"], ci["porta"]))
        for m in state["payload"].get("membros", []):
            coord.node.add_peer(PeerInfo(m["node_id"], m["ip"], m["porta"]))
        for obj in state["payload"].get("objetos", []):
            coord.board.adicionar_objeto(obj["id"], obj["tipo"], {k: v for k, v in obj.items() if k not in ("id", "tipo")})

        self.on_quadro_pronto(coord, client_id, quadro["nome"], state["payload"].get("objetos", []))


class JanelaEscolherQuadro(tk.Toplevel):
    def __init__(self, master, quadros: list[dict], on_escolha):
        super().__init__(master)
        self.title("Quadros existentes")
        self.on_escolha = on_escolha
        self.geometry("420x260")

        tk.Label(self, text="Quadros disponíveis (via Serviço de Nomes):", font=("Helvetica", 10, "bold")).pack(pady=(12, 6))

        frame_lista = tk.Frame(self)
        frame_lista.pack(fill="both", expand=True, padx=12)
        self.listbox = tk.Listbox(frame_lista, font=("Courier", 10))
        self.listbox.pack(side="left", fill="both", expand=True)
        scrollbar = tk.Scrollbar(frame_lista, command=self.listbox.yview)
        scrollbar.pack(side="right", fill="y")
        self.listbox.config(yscrollcommand=scrollbar.set)

        self._quadros = quadros
        for q in quadros:
            self.listbox.insert("end", f"{q['nome']}  —  {q['ip']}:{q['porta']}")

        tk.Button(self, text="Ingressar", command=self._confirmar).pack(pady=10)

    def _confirmar(self) -> None:
        sel = self.listbox.curselection()
        if not sel:
            messagebox.showwarning("Seleção necessária", "Escolha um quadro na lista.", parent=self)
            return
        quadro = self._quadros[sel[0]]
        self.destroy()
        self.on_escolha(quadro)


class TelaQuadro(tk.Frame):
    def __init__(self, master, coord: BoardCoordinatorServer, client_id: str, nome_quadro: str,
                 objetos_iniciais: list[dict] | None = None):
        super().__init__(master)
        self.coord = coord
        self.client_id = client_id
        self.nome_quadro = nome_quadro

        self._canvas_items: dict[str, int] = {}
        self._selecionados: set[str] = set()
        self._pontos_pendentes: list[tuple[int, int]] = []
        self.modo = tk.StringVar(value="linha")
        self.cor_atual = tk.StringVar(value=CORES_DISPONIVEIS[0])

        # Fila thread-safe: a thread de rede só enfileira eventos; só a
        # thread principal do Tkinter lê e toca em widgets (chamar métodos
        # do Tkinter fora dela causa "main thread is not in main loop").
        self._fila_eventos: queue.Queue = queue.Queue()

        self._montar_layout()
        coord.ui_callback = self._on_network_event

        if objetos_iniciais:
            for obj in objetos_iniciais:
                self._desenhar_objeto(obj)

        self._atualizar_titulo()
        self.after(80, self._drenar_fila_eventos)

    def _montar_layout(self) -> None:
        barra = tk.Frame(self, padx=8, pady=8)
        barra.pack(side="top", fill="x")

        tk.Label(barra, text="Modo:").pack(side="left")
        for valor, rotulo in (("linha", "Linha"), ("quadrado", "Quadrado"), ("selecionar", "Selecionar")):
            tk.Radiobutton(barra, text=rotulo, variable=self.modo, value=valor,
                           command=self._limpar_pontos_pendentes).pack(side="left", padx=4)

        tk.Label(barra, text="   Cor:").pack(side="left")
        for cor in CORES_DISPONIVEIS:
            tk.Radiobutton(barra, variable=self.cor_atual, value=cor, bg=cor, width=3, indicatoron=False).pack(side="left", padx=2)

        tk.Button(barra, text="Colorir selecionado(s)", command=self._aplicar_cor).pack(side="left", padx=(12, 4))
        tk.Button(barra, text="Remover selecionado(s)", command=self._remover_selecionados).pack(side="left", padx=4)
        tk.Button(barra, text="Agrupar selecionados (2PC)", command=self._agrupar_selecionados).pack(side="left", padx=4)
        tk.Button(barra, text="Boneco palito (2PC atômico)", command=self._criar_boneco_palito).pack(side="left", padx=4)

        self.label_status = tk.Label(self, text="", anchor="w", padx=8)
        self.label_status.pack(side="bottom", fill="x")

        self.canvas = tk.Canvas(self, bg="white", width=800, height=560, highlightthickness=1, highlightbackground="#999")
        self.canvas.pack(side="top", fill="both", expand=True, padx=8, pady=(0, 8))
        self.canvas.bind("<Button-1>", self._on_canvas_click)

    def _atualizar_titulo(self) -> None:
        papel = "coordenador" if self.coord.node.is_coordinator() else "cliente"
        self.label_status.config(text=f"Quadro: {self.nome_quadro}  |  Você: {self.client_id} ({papel}, node_id={self.coord.node.node_id})")

    def _limpar_pontos_pendentes(self) -> None:
        self._pontos_pendentes = []

    def _on_canvas_click(self, event) -> None:
        modo = self.modo.get()
        if modo in ("linha", "quadrado"):
            self._pontos_pendentes.append((event.x, event.y))
            if len(self._pontos_pendentes) == 2:
                p1, p2 = self._pontos_pendentes
                self._pontos_pendentes = []
                (self._enviar_draw_line if modo == "linha" else self._enviar_draw_square)(p1, p2)
        elif modo == "selecionar":
            obj_id = self._achar_objeto_proximo(event.x, event.y)
            if obj_id:
                self._toggle_selecao(obj_id)

    def _achar_objeto_proximo(self, x: int, y: int, raio: int = 6) -> str | None:
        itens = self.canvas.find_overlapping(x - raio, y - raio, x + raio, y + raio)
        for item in itens:
            for obj_id, canvas_item in self._canvas_items.items():
                if canvas_item == item:
                    return obj_id
        return None

    def _toggle_selecao(self, obj_id: str) -> None:
        if obj_id in self._selecionados:
            self._selecionados.discard(obj_id)
            self._enviar_release_select(obj_id)
            self._redesenhar_destaque(obj_id, selecionado=False)
            return
        self._enviar_select_request(obj_id)

    def _coord_addr(self) -> tuple[str, int]:
        c = self.coord.node.get_coordinator()
        if c is None:
            raise RuntimeError("Coordenador desconhecido (sem conexão estabelecida).")
        return c.ip, c.porta

    def _send(self, msg: dict) -> dict | None:
        try:
            ip, porta = self._coord_addr()
            with socket.create_connection((ip, porta), timeout=4.0) as s:
                stream = MessageStream(s)
                stream.send(msg)
                return stream.recv()
        except (OSError, ConnectionError, TimeoutError, RuntimeError) as e:
            self.label_status.config(text=f"Erro de rede: {e}")
            return None

    def _enviar_draw_line(self, p1, p2) -> None:
        resp = self._send(make_message(DRAW_LINE, str(self.coord.node.node_id), {"p1": list(p1), "p2": list(p2), "cor": self.cor_atual.get()}))
        if resp and resp.get("type") == OBJECT_CREATED:
            self._desenhar_objeto(resp["payload"]["objeto"])

    def _enviar_draw_square(self, p1, p2) -> None:
        resp = self._send(make_message(DRAW_SQUARE, str(self.coord.node.node_id), {"p1": list(p1), "p2": list(p2), "cor": self.cor_atual.get()}))
        if resp and resp.get("type") == OBJECT_CREATED:
            self._desenhar_objeto(resp["payload"]["objeto"])

    def _enviar_select_request(self, obj_id: str) -> None:
        resp = self._send(make_message(SELECT_REQUEST, str(self.coord.node.node_id), {"obj_id": obj_id, "client_id": self.client_id}))
        if resp and resp.get("type") == SELECT_GRANTED:
            self._selecionados.add(obj_id)
            self._redesenhar_destaque(obj_id, selecionado=True)
        elif resp and resp.get("type") == SELECT_DENIED:
            messagebox.showwarning("Objeto indisponível", resp["payload"].get("motivo", "objeto já selecionado por outro usuário"))

    def _enviar_release_select(self, obj_id: str) -> None:
        self._send(make_message(RELEASE_SELECT, str(self.coord.node.node_id), {"obj_id": obj_id, "client_id": self.client_id}))

    def _aplicar_cor(self) -> None:
        if not self._selecionados:
            messagebox.showinfo("Nada selecionado", "Selecione ao menos um objeto primeiro.")
            return
        cor = self.cor_atual.get()
        for obj_id in list(self._selecionados):
            resp = self._send(make_message(RECOLOR, str(self.coord.node.node_id), {"obj_id": obj_id, "cor": cor, "client_id": self.client_id}))
            if resp and resp.get("type") == RECOLOR:
                self._aplicar_cor_no_canvas(obj_id, cor)
                self._selecionados.discard(obj_id)

    def _aplicar_cor_no_canvas(self, obj_id: str, cor: str) -> None:
        item = self._canvas_items.get(obj_id)
        if item is None:
            return
        # create_line só aceita -fill; create_rectangle aceita -fill e -outline
        if self.canvas.type(item) == "line":
            self.canvas.itemconfig(item, fill=cor)
        else:
            self.canvas.itemconfig(item, fill=cor, outline=cor)

    def _remover_selecionados(self) -> None:
        if not self._selecionados:
            messagebox.showinfo("Nada selecionado", "Selecione ao menos um objeto primeiro.")
            return
        for obj_id in list(self._selecionados):
            resp = self._send(make_message(DELETE_OBJECT, str(self.coord.node.node_id), {"obj_id": obj_id, "client_id": self.client_id}))
            if resp and resp.get("type") == DELETE_OBJECT:
                self._remover_do_canvas(obj_id)
                self._selecionados.discard(obj_id)

    def _agrupar_selecionados(self) -> None:
        if len(self._selecionados) < 2:
            messagebox.showinfo("Seleção insuficiente", "Selecione ao menos 2 objetos para agrupar.")
            return
        obj_ids = list(self._selecionados)
        resp = self._send(make_message(TX_BEGIN, str(self.coord.node.node_id), {"operacao": "agrupar", "obj_ids": obj_ids, "autor": self.client_id}))
        if resp is None:
            return
        if resp.get("type") == TX_COMMIT:
            messagebox.showinfo("Transação concluída", f"Grupo {resp['payload']['grupo_id']} criado com sucesso.")
        else:
            messagebox.showwarning("Transação abortada", resp["payload"].get("motivo", "conflito detectado"))
        self._selecionados.clear()

    def _criar_boneco_palito(self) -> None:
        base_id = f"palito-{int(time.time() * 1000)}"
        cx, cy = 400, 280
        linhas = [
            {"id": f"{base_id}-0", "tipo": "linha", "p1": [cx, cy - 60], "p2": [cx, cy], "cor": COR_PADRAO},
            {"id": f"{base_id}-1", "tipo": "linha", "p1": [cx, cy], "p2": [cx - 30, cy + 60], "cor": COR_PADRAO},
            {"id": f"{base_id}-2", "tipo": "linha", "p1": [cx, cy], "p2": [cx + 30, cy + 60], "cor": COR_PADRAO},
            {"id": f"{base_id}-3", "tipo": "linha", "p1": [cx, cy - 30], "p2": [cx - 35, cy], "cor": COR_PADRAO},
            {"id": f"{base_id}-4", "tipo": "linha", "p1": [cx, cy - 30], "p2": [cx + 35, cy], "cor": COR_PADRAO},
            {"id": f"{base_id}-5", "tipo": "linha", "p1": [cx, cy - 90], "p2": [cx, cy - 60], "cor": COR_PADRAO},
        ]
        resp = self._send(make_message(TX_BEGIN, str(self.coord.node.node_id), {"operacao": "criar_atomico", "autor": self.client_id, "objetos_a_criar": linhas}))
        if resp is None:
            return
        if resp.get("type") == TX_COMMIT:
            for obj in resp["payload"]["objetos_criados"]:
                self._desenhar_objeto(obj)
        else:
            messagebox.showwarning("Transação abortada", resp["payload"].get("motivo", "conflito detectado"))

    def _desenhar_objeto(self, obj: dict) -> None:
        if obj["id"] in self._canvas_items:
            return
        x1, y1 = obj["p1"]
        x2, y2 = obj["p2"]
        cor = obj.get("cor", COR_PADRAO)
        if obj["tipo"] == "linha":
            item = self.canvas.create_line(x1, y1, x2, y2, fill=cor, width=3)
        else:
            item = self.canvas.create_rectangle(x1, y1, x2, y2, outline=cor, width=3)
        self._canvas_items[obj["id"]] = item

    def _remover_do_canvas(self, obj_id: str) -> None:
        item = self._canvas_items.pop(obj_id, None)
        if item is not None:
            self.canvas.delete(item)

    def _redesenhar_destaque(self, obj_id: str, selecionado: bool) -> None:
        item = self._canvas_items.get(obj_id)
        if item is not None:
            self.canvas.itemconfig(item, width=5 if selecionado else 3)

    def _on_network_event(self, evento: str, dados: dict) -> None:
        self._fila_eventos.put((evento, dados))

    def _drenar_fila_eventos(self) -> None:
        try:
            while True:
                evento, dados = self._fila_eventos.get_nowait()
                self._processar_evento_rede(evento, dados)
        except queue.Empty:
            pass
        finally:
            self.after(80, self._drenar_fila_eventos)

    def _processar_evento_rede(self, evento: str, dados: dict) -> None:
        if evento == "object_created":
            self._desenhar_objeto(dados["objeto"])
        elif evento == "object_recolored":
            self._aplicar_cor_no_canvas(dados["obj_id"], dados["cor"])
        elif evento == "object_deleted":
            self._remover_do_canvas(dados["obj_id"])
        elif evento == "tx_committed":
            if dados.get("operacao") == "criar_atomico":
                for obj in dados.get("objetos_criados", []):
                    self._desenhar_objeto(obj)
            elif dados.get("operacao") == "deletar_conjunto":
                for oid in dados.get("obj_ids", []):
                    self._remover_do_canvas(oid)
        elif evento == "coordinator_changed":
            self._atualizar_titulo()


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("SDWB — Shared Distributed Write Board")
        self.geometry("860x700")
        self._tela_atual = None
        self._mostrar_tela_inicial()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _mostrar_tela_inicial(self) -> None:
        self._tela_atual = TelaInicial(self, self._on_quadro_pronto)
        self._tela_atual.pack(fill="both", expand=True)

    def _on_quadro_pronto(self, coord: BoardCoordinatorServer, client_id: str, nome_quadro: str,
                           objetos_iniciais: list[dict] | None = None) -> None:
        self._coord = coord
        self._tela_atual.destroy()
        self._tela_atual = TelaQuadro(self, coord, client_id, nome_quadro, objetos_iniciais)
        self._tela_atual.pack(fill="both", expand=True)

    def _on_close(self) -> None:
        if hasattr(self, "_coord"):
            try:
                self._coord.stop()
            except Exception:
                pass
        self.destroy()


def main():
    App().mainloop()


if __name__ == "__main__":
    main()
