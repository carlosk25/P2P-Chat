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
