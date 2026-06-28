# P2PCHAT

P2PCHAT é um sistema de chat Peer-to-Peer (P2P) desenvolvido em Python para a disciplina de Redes de Computadores.

O projeto utiliza um servidor Rendezvous para descoberta de peers e conexões TCP diretas para comunicação entre os participantes da rede. Após a descoberta inicial, as mensagens são trocadas diretamente entre os peers, sem intermediários.

## Funcionalidades

* Registro de peers em servidor Rendezvous
* Descoberta automática de peers ativos
* Comunicação direta via TCP
* Handshake de conexão (`HELLO` / `HELLO_OK`)
* Mensagens diretas (`SEND` / `ACK`)
* Mensagens de broadcast (`PUB`)
* Keep-alive com `PING` / `PONG`
* Monitoramento de RTT
* Encerramento controlado de conexões (`BYE` / `BYE_OK`)
* Reconexão automática de peers

## Tecnologias Utilizadas

* Python 3
* TCP Sockets
* JSON
* Threads

## Estrutura do Projeto

```text
P2PCHAT/
├── main.py
├── config.py
├── rendezvous_connection.py
├── peer.py
├── peer_table.py
├── peer_connection.py
├── message_router.py
├── keep_alive.py
├── cli.py
└── utils.py
```

## Arquitetura

O sistema segue uma arquitetura Peer-to-Peer baseada em descoberta de peers através de um servidor Rendezvous.

```text
Peer A
   │
   ├── REGISTER
   ▼
Rendezvous Server
   ▲
   └── DISCOVER
   │
Peer B
```

Após a descoberta, a comunicação ocorre diretamente entre os peers através de conexões TCP persistentes.

## Comandos Disponíveis

| Comando                        | Descrição                        |
| ------------------------------ | -------------------------------- |
| `/peers`                       | Lista peers conhecidos           |
| `/msg <peer_id> <mensagem>`    | Envia mensagem direta            |
| `/pub * <mensagem>`            | Envia broadcast global           |
| `/pub #<namespace> <mensagem>` | Envia mensagem para um namespace |
| `/conn`                        | Lista conexões ativas            |
| `/rtt`                         | Exibe RTT médio dos peers        |
| `/reconnect`                   | Força nova descoberta de peers   |
| `/log <nível>`                 | Ajusta o nível de log            |
| `/quit`                        | Encerra a aplicação              |

## Como usar/testar o chat

Para executar um peer com o arquivo `config.json` padrão:

```bash
python main.py
```

Também é possível passar um arquivo de configuração específico:

```bash
python main.py alice.json
python main.py bob.json
```

Exemplo de teste manual com dois peers locais:

1. Crie dois arquivos de configuração, por exemplo `alice.json` e `bob.json`.
2. Use nomes e portas diferentes em cada arquivo:

```json
{
  "app_name": "P2P-Chat",
  "name": "alice",
  "namespace": "UnB",
  "port": 5001,
  "rendezvous_host": "pyp2p.mfcaetano.cc",
  "rendezvous_port": 8080,
  "listen_host": "0.0.0.0",
  "discover_interval": 20,
  "ping_interval": 30,
  "rdv_ttl": 3600,
  "fixed_msg_ttl": 1,
  "log_level": "INFO",
  "features": ["ack", "metrics"],
  "autonomous_mode": false,
  "max_reconnect_attempts": 5,
  "ack_timeout": 5
}
```

3. Em um terminal, inicie o primeiro peer:

```bash
python main.py alice.json
```

4. Em outro terminal, inicie o segundo peer:

```bash
python main.py bob.json
```

5. Em um dos peers, force a descoberta/conexão:

```text
/reconnect
```

6. Teste os comandos principais:

```text
/peers
/conn
/msg bob@UnB ola bob
/pub * ola pessoal
/rtt
/quit
```

Observações:

* O `peer_id` usa o formato `nome@namespace`, por exemplo `alice@UnB`.
* O comando `/rtt` só mostra valores depois que houver troca de `PING`/`PONG`.
* O comando `/pub *` envia para os peers conectados diretamente.
* Com `autonomous_mode` como `false`, use `/reconnect` para descobrir e conectar peers manualmente.

## Objetivos

* Aplicar conceitos de arquitetura Peer-to-Peer
* Implementar protocolos de aplicação sobre TCP
* Desenvolver mecanismos de descoberta e comunicação entre peers
* Trabalhar com programação concorrente e comunicação em rede
* Implementar tolerância a falhas e reconexão automática

## Integrantes

| Nome | Github |
|------|--------|
| Carlos Eduardo Pires Gomes | @carlosk25 |
| Dannyeclisson Rodrigo Martins da Costa | @dannyeclisson |
| Augusto Queiroz Alves Silva | @augustoqas |

## Disciplina
Redes de Computadores  
Universidade de Brasília (UnB)
