##controle das conexões TCP e mensagens entre peers

#1. conexões inbound
 #abrir socket local
 #aceitar conexões de outros peers
 #receber HELLO
 #responder HELLO_OK

#2. conexões outbound
 #conectar em outro peer
 #enviar HELLO
 #esperar HELLO_OK
 #manter socket aberto

#abrir servidor TCP local
#aceitar conexões de entrada
#conectar em outros peers
#enviar HELLO
#receber HELLO
#responder HELLO_OK
#guardar sockets conectados
#fechar conexões

import socket
import threading
import json
import logging
from typing import Dict, Optional


class PeerConnectionManager:
    def __init__(self, my_peer_id: str, listen_host: str, listen_port: int):
        self.my_peer_id = my_peer_id
        self.listen_host = listen_host
        self.listen_port = listen_port

        self.server_socket: Optional[socket.socket] = None
        self.connections: Dict[str, socket.socket] = {}

        self.running = False