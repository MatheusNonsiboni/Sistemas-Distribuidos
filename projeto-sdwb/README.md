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

## Executando com dois ou mais computadores

O sistema funciona tanto numa única máquina (tudo via `127.0.0.1`, o padrão)
quanto em várias máquinas na mesma rede local (Wi-Fi ou cabo). Para várias
máquinas, siga os passos abaixo.

### 1. Descubra o IP de cada máquina na rede local

- **Windows**: abra o `cmd` e rode `ipconfig`. Use o "Endereço IPv4" da
  rede em uso (Wi-Fi ou Ethernet), algo como `192.168.0.x` ou `10.0.0.x`.
- **Linux/Mac**: rode `ip a` ou `ifconfig` no terminal.

Todas as máquinas precisam estar na **mesma rede** (mesmo Wi-Fi, por
exemplo — rede de convidados costuma isolar os dispositivos entre si e
não funciona).

### 2. Escolha uma máquina para hospedar o Serviço de Nomes

Essa máquina roda o Serviço de Nomes normalmente:

```
python -m name_service.server
```

Anote o IP dela (ex.: `192.168.0.10`) — todas as outras máquinas vão
precisar apontar para ele.

### 3. Nas demais máquinas, aponte para o IP do Serviço de Nomes

Antes de abrir a GUI, defina a variável de ambiente com o IP anotado no
passo 2:

**Windows (cmd):**
```
set SDWB_NAME_SERVICE_HOST=192.168.0.10
python -m client.gui
```

**Windows (PowerShell):**
```
$env:SDWB_NAME_SERVICE_HOST="192.168.0.10"
python -m client.gui
```

**Linux/Mac:**
```
SDWB_NAME_SERVICE_HOST=192.168.0.10 python -m client.gui
```

> Na própria máquina que hospeda o Serviço de Nomes, isso não é
> necessário — o padrão (`127.0.0.1`) já funciona para ela mesma.

### 4. Na tela inicial da GUI, confirme o campo "Meu IP nesta rede"

O campo já vem preenchido com uma sugestão automática do IP local da
máquina. Confira se bate com o IP descoberto no passo 1 (se a máquina
tiver mais de uma interface de rede — Wi-Fi e cabo ao mesmo tempo, por
exemplo — a sugestão pode escolher a errada; ajuste manualmente se for
o caso). Esse é o IP que os outros nós vão usar para contatar este
cliente/coordenador, então precisa ser o IP real na rede, nunca
`127.0.0.1` quando a máquina não é a mesma que a de quem vai se conectar.

### 5. Firewall

Se uma máquina não conseguir enxergar as outras, o motivo mais comum é o
firewall do Windows bloqueando conexões de entrada em portas Python. Ao
rodar `python -m name_service.server` ou `python -m client.gui` pela
primeira vez, o Windows normalmente pergunta se deve permitir o acesso —
escolha **Permitir** (redes privadas). Se não perguntar, pode ser preciso
liberar manualmente em Firewall do Windows Defender → Configurações
avançadas → Regra de Entrada.

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

## Estrutura do projeto

```
sdwb/
├── common/
│   ├── protocol.py      protocolo de mensagens (JSON + '\n')
│   └── config.py        portas, timeouts, endereço do Serviço de Nomes
│                         (configurável via SDWB_NAME_SERVICE_HOST)
├── name_service/
│   └── server.py        "páginas amarelas": tabela (nome, IP, porta)
├── coordinator/
│   ├── board_state.py   objetos do quadro, exclusão mútua, 2PC
│   ├── node.py          servidor TCP, heartbeat, eleição Bully
│   └── server.py        integra os dois módulos acima + handlers de rede
└── client/
    └── gui.py           interface gráfica Tkinter
```
