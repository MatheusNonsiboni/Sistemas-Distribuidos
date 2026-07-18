# SDWB — Shared Distributed Write Board

Projeto final de Sistemas Distribuídos: quadro branco colaborativo distribuído,
sem servidor fixo, com descoberta via Serviço de Nomes e coordenação via um
nó migrante eleito pelo algoritmo Bully.


## Requisitos

- Python 3.10 ou superior
- Tkinter (no Windows vem junto com o instalador oficial do Python;
  no Ubuntu/Debian, se não estiver: `sudo apt-get install python3-tk`)

Nenhuma biblioteca externa — só a biblioteca padrão do Python.

## Como executar

Abra **um terminal para cada processo**. Todos os comandos devem ser
executados a partir da pasta raiz do projeto (`sdwb/`).

### 1. Serviço de Nomes (sempre o primeiro a subir)

```
python -m name_service.server
```

### 2. Clientes

```
python -m client.gui
```

Abra esta janela para cada usuário que quiser participar do quadro.

Na tela inicial, escolha:
- **CRIAR NOVO QUADRO** — o primeiro cliente a criar um quadro se torna o
  Coordenador automaticamente. Não é necessário subir um processo de
  coordenador separado.
- **INGRESSAR EM QUADRO EXISTENTE** — consulta o Serviço de Nomes, lista
  os quadros disponíveis e conecta ao escolhido. O novo cliente recebe
  imediatamente todos os desenhos já feitos.


## Cenários de demonstração obrigatórios

### 1. Entrada Dinâmica
Suba o Serviço de Nomes, depois um cliente com "Criar Novo Quadro", depois
mais clientes com "Ingressar em Quadro Existente". Cada novo cliente deve
ver instantaneamente todos os desenhos já feitos.

### 2. Concorrência Transacional
Com dois clientes no modo **Selecionar**, clique no mesmo objeto nos dois
ao mesmo tempo — o segundo deve receber uma mensagem de erro (exclusão
mútua). Para a demonstração de 2PC: selecione 3 objetos em um cliente,
clique **Agrupar selecionados (2PC)** e, antes de confirmar, tente deletar
um desses objetos no outro cliente. A operação que chegar primeiro ao
Coordenador vence; a outra é abortada com mensagem de erro.

### 3. Morte do Coordenador
A barra de status de cada janela mostra quem é o Coordenador atual. Feche
ou encerre com força (Ctrl+C no terminal) o processo que está como
Coordenador. Após alguns segundos os demais clientes elegem um novo
Coordenador (Bully) e o sistema volta a funcionar normalmente, mantendo
todos os desenhos.

> **Nota de timing:** a detecção de falha depende do heartbeat (intervalo
> de 2s, timeout de 5s) mais o tempo de eleição. Espere aproximadamente
> 8–10 segundos entre matar o Coordenador e tentar usar o sistema
> novamente — isso é esperado e pode ser mencionado no relatório.

## Estrutura do projeto

```
sdwb/
├── common/
│   ├── protocol.py      protocolo de mensagens (JSON + '\n')
│   └── config.py        portas, timeouts, endereço do Serviço de Nomes
├── name_service/
│   └── server.py        "páginas amarelas": tabela (nome, IP, porta)
├── coordinator/
│   ├── board_state.py   objetos do quadro, exclusão mútua, 2PC
│   ├── node.py          servidor TCP, heartbeat, eleição Bully
│   └── server.py        integra os dois módulos acima + handlers de rede
└── client/
    └── gui.py           interface gráfica Tkinter
```
