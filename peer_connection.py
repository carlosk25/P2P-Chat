# Grupo 12
# Augusto Queiroz Alves Silva - 232024302
# Carlos Eduardo Pires Gomes - 232045895
# Dannyeclisson Rodrigo Martins da Costa - 211061592

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

from __future__ import annotations

import json
import logging
import socket
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, TextIO, Tuple


MAX_MESSAGE_BYTES = 32768
PROTOCOL_VERSION = "1.0"
DEFAULT_FEATURES = ["ack", "metrics"]

# Tipos de callback usados para integrar com outros módulos depois.
# Exemplo: message_router.py pode passar uma função para tratar SEND/PUB/ACK.
MessageCallback = Callable[[str, dict], None]
ConnectionCallback = Callable[[str], None]


@dataclass
class PeerConnection:
    """
    Representa uma conexão TCP aberta com um peer.

    peer_id:
        Identidade do peer remoto, no formato name@namespace.

    sock:
        Socket TCP conectado com o peer remoto.

    reader:
        Objeto de leitura em cima do socket. Ele facilita ler mensagem por linha,
        já que o protocolo usa JSON terminado por \n.

    direction:
        Indica se a conexão foi aberta por nós ("outbound") ou recebida de outro
        peer ("inbound").

    address:
        Endereço remoto no formato (ip, porta).

    connected_at:
        Timestamp local de quando a conexão foi registrada.

    send_lock:
        Lock para impedir duas threads de enviarem dados ao mesmo tempo pelo
        mesmo socket e misturarem bytes.
    """

    peer_id: str
    sock: socket.socket
    reader: TextIO
    direction: str
    address: Tuple[str, int]
    connected_at: float = field(default_factory=time.time)
    send_lock: threading.Lock = field(default_factory=threading.Lock)


class PeerConnectionManager:
    """
    Gerencia as conexões TCP diretas entre este peer e os outros peers.

    Esta classe é a parte do peer que atua como cliente e servidor ao mesmo tempo:
    - servidor: start_server() abre uma porta local e aceita conexões;
    - cliente: connect_to_peer() conecta em outro peer descoberto pelo Rendezvous.
    """

    def __init__(
        self,
        my_peer_id: str,
        listen_host: str = "0.0.0.0",
        listen_port: int = 0,
        features: Optional[List[str]] = None,
        on_message: Optional[MessageCallback] = None,
        on_connect: Optional[ConnectionCallback] = None,
        on_disconnect: Optional[ConnectionCallback] = None,
        max_message_bytes: int = MAX_MESSAGE_BYTES,
        handshake_timeout: float = 5.0,
    ):
        """
        Cria o gerenciador de conexões P2P.

        my_peer_id:
            Identidade deste peer, por exemplo "alice@CIC".

        listen_host:
            Endereço local onde o servidor TCP vai escutar. Normalmente "0.0.0.0",
            para aceitar conexões por qualquer interface de rede.

        listen_port:
            Porta TCP local. Deve ser a mesma porta registrada no Rendezvous.
            Se for 0, o sistema operacional escolhe uma porta livre automaticamente.

        features:
            Lista de recursos anunciados no HELLO/HELLO_OK.

        on_message:
            Função opcional chamada quando chegar uma mensagem que não pertence
            diretamente à parte de conexão, por exemplo SEND, ACK, PUB, PING, PONG.

        on_connect:
            Função opcional chamada quando um peer conecta com sucesso.

        on_disconnect:
            Função opcional chamada quando um peer desconecta.
        """

        self.my_peer_id = my_peer_id
        self.listen_host = listen_host
        self.listen_port = int(listen_port)
        self.features = features or DEFAULT_FEATURES.copy()
        self.max_message_bytes = int(max_message_bytes)
        self.handshake_timeout = float(handshake_timeout)

        self.on_message = on_message
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect

        self.server_socket: Optional[socket.socket] = None
        self.accept_thread: Optional[threading.Thread] = None
        self.running = False

        # Dicionário peer_id -> PeerConnection.
        # Exemplo: {"bob@CIC": PeerConnection(...)}
        self.connections: Dict[str, PeerConnection] = {}
        self.connections_lock = threading.RLock()

        self.logger = logging.getLogger(self.__class__.__name__)

    # ---------------------------------------------------------------------
    # Servidor TCP local/inbound
    # ---------------------------------------------------------------------

    def start_server(self) -> int:
        """
        Abre o servidor TCP local e começa a aceitar conexões inbound.

        Retorna a porta real usada. Isso é útil quando listen_port=0, porque nesse
        caso o sistema operacional escolhe a porta automaticamente.
        """

        if self.running:
            return self.listen_port

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.listen_host, self.listen_port))
        self.server_socket.listen()
        self.server_socket.settimeout(1.0)

        # Se a porta era 0, aqui descobrimos qual porta o SO escolheu.
        self.listen_port = self.server_socket.getsockname()[1]
        self.running = True

        self.accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self.accept_thread.start()

        self.logger.info(
            "Servidor P2P escutando em %s:%s", self.listen_host, self.listen_port
        )
        return self.listen_port

    def _accept_loop(self) -> None:
        """
        Loop que fica aceitando conexões de outros peers.

        Cada conexão aceita é tratada em uma thread separada para que o servidor
        continue livre para aceitar novos peers.
        """

        assert self.server_socket is not None

        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            thread = threading.Thread(
                target=self._handle_inbound_connection,
                args=(client_socket, address),
                daemon=True,
            )
            thread.start()

    def _handle_inbound_connection(
        self, client_socket: socket.socket, address: Tuple[str, int]
    ) -> None:
        """
        Trata uma conexão que chegou de outro peer.

        Fluxo esperado:
        1. O peer remoto conecta no nosso servidor TCP.
        2. Ele envia HELLO.
        3. Nós validamos o HELLO.
        4. Nós respondemos HELLO_OK.
        5. A conexão fica aberta e entra no loop de leitura.
        """

        connection: Optional[PeerConnection] = None

        try:
            client_socket.settimeout(self.handshake_timeout)
            reader = client_socket.makefile("r", encoding="utf-8", newline="\n")

            hello = self._recv_json_from_reader(reader)
            self._validate_hello(hello)

            remote_peer_id = str(hello["peer_id"])
            if remote_peer_id == self.my_peer_id:
                raise ValueError("peer tentou conectar nele mesmo")

            self._send_json_to_socket(client_socket, self._build_hello_ok())
            client_socket.settimeout(None)

            connection = PeerConnection(
                peer_id=remote_peer_id,
                sock=client_socket,
                reader=reader,
                direction="inbound",
                address=address,
            )

            self._store_connection(connection)
            self.logger.info("Inbound conectado: %s de %s:%s", remote_peer_id, *address)
            self._notify_connect(remote_peer_id)

            self._read_loop(connection)

        except Exception as exc:
            self.logger.warning("Falha ao aceitar conexão inbound de %s: %s", address, exc)
            if connection is not None:
                self._remove_connection(connection.peer_id, connection)
            else:
                self._close_socket(client_socket)

    # ---------------------------------------------------------------------
    # Conexão outbound
    # ---------------------------------------------------------------------

    def connect_to_peer(self, peer_id: str, host: str, port: int) -> bool:
        """
        Conecta em outro peer usando IP/host e porta.

        Fluxo esperado:
        1. Abrimos conexão TCP com host:port.
        2. Enviamos HELLO.
        3. Esperamos HELLO_OK.
        4. Guardamos a conexão como ativa.
        5. Começamos uma thread para ler mensagens recebidas desse peer.
        """

        if peer_id == self.my_peer_id:
            self.logger.debug("Ignorando tentativa de conectar em si mesmo: %s", peer_id)
            return False

        if self.is_connected(peer_id):
            self.logger.debug("Peer já conectado: %s", peer_id)
            return True

        connection: Optional[PeerConnection] = None

        try:
            sock = socket.create_connection((host, int(port)), timeout=self.handshake_timeout)
            reader = sock.makefile("r", encoding="utf-8", newline="\n")

            self._send_json_to_socket(sock, self._build_hello())
            response = self._recv_json_from_reader(reader)
            self._validate_hello_ok(response)

            remote_peer_id = str(response["peer_id"])
            if remote_peer_id == self.my_peer_id:
                raise ValueError("HELLO_OK retornou o próprio peer_id")

            if peer_id and remote_peer_id != peer_id:
                raise ValueError(
                    f"peer_id esperado era {peer_id}, mas conexão respondeu {remote_peer_id}"
                )

            sock.settimeout(None)
            connection = PeerConnection(
                peer_id=remote_peer_id,
                sock=sock,
                reader=reader,
                direction="outbound",
                address=(host, int(port)),
            )

            self._store_connection(connection)
            self.logger.info("Outbound conectado: %s em %s:%s", remote_peer_id, host, port)
            self._notify_connect(remote_peer_id)

            thread = threading.Thread(
                target=self._read_loop,
                args=(connection,),
                daemon=True,
            )
            thread.start()

            return True

        except Exception as exc:
            self.logger.warning("Erro ao conectar em %s (%s:%s): %s", peer_id, host, port, exc)
            if connection is not None:
                self._remove_connection(connection.peer_id, connection)
            return False

    # ---------------------------------------------------------------------
    # Envio e recebimento de JSON
    # ---------------------------------------------------------------------

    def send_to_peer(self, peer_id: str, message: dict) -> bool:
        """
        Envia uma mensagem JSON para um peer já conectado.

        Este método é o principal ponto de integração com message_router.py e
        keep_alive.py. Ele aceita qualquer mensagem do protocolo, por exemplo:
        SEND, ACK, PUB, PING ou PONG.
        """

        with self.connections_lock:
            connection = self.connections.get(peer_id)

        if connection is None:
            self.logger.warning("Não há conexão ativa com %s", peer_id)
            return False

        try:
            with connection.send_lock:
                self._send_json_to_socket(connection.sock, message)
            return True
        except Exception as exc:
            self.logger.warning("Falha ao enviar mensagem para %s: %s", peer_id, exc)
            self._remove_connection(peer_id, connection)
            return False

    def _send_json_to_socket(self, sock: socket.socket, message: dict) -> None:
        """
        Serializa um dicionário como JSON UTF-8 e envia com \n no final.
        """

        line = json.dumps(message, ensure_ascii=False, separators=(",", ":")) + "\n"
        data = line.encode("utf-8")

        if len(data) > self.max_message_bytes:
            raise ValueError(
                f"mensagem excede o limite de {self.max_message_bytes} bytes"
            )

        sock.sendall(data)

    def _recv_json_from_reader(self, reader: TextIO) -> dict:
        """
        Lê uma linha terminada por \n e converte JSON para dict.
        """

        line = reader.readline(self.max_message_bytes + 1)

        if line == "":
            raise ConnectionError("conexão fechada pelo peer remoto")

        if len(line.encode("utf-8")) > self.max_message_bytes:
            raise ValueError(
                f"mensagem recebida excede {self.max_message_bytes} bytes"
            )

        line = line.rstrip("\n")
        message = json.loads(line)

        if not isinstance(message, dict):
            raise ValueError("mensagem JSON precisa ser um objeto/dict")

        return message

    def _read_loop(self, connection: PeerConnection) -> None:
        """
        Loop de leitura de mensagens de um peer já conectado.

        Mensagens da camada de conexão, como BYE e BYE_OK, são tratadas aqui.
        Outras mensagens são repassadas para on_message, se existir.
        """

        peer_id = connection.peer_id

        try:
            while self.running:
                message = self._recv_json_from_reader(connection.reader)
                message_type = message.get("type")

                if message_type == "BYE":
                    self._handle_bye(connection, message)
                    break

                if message_type == "BYE_OK":
                    self.logger.info("BYE_OK recebido de %s", peer_id)
                    break

                if message_type in {"HELLO", "HELLO_OK"}:
                    self.logger.warning(
                        "Mensagem %s inesperada depois do handshake de %s",
                        message_type,
                        peer_id,
                    )
                    continue

                self.logger.debug("Mensagem recebida de %s: %s", peer_id, message)
                if self.on_message is not None:
                    self.on_message(peer_id, message)

        except Exception as exc:
            self.logger.info("Conexão com %s encerrada: %s", peer_id, exc)
        finally:
            self._remove_connection(peer_id, connection)

    # ---------------------------------------------------------------------
    # HELLO / HELLO_OK
    # ---------------------------------------------------------------------

    def _build_hello(self) -> dict:
        """Monta HELLO do handshake; usa my_peer_id/features e retorna dict."""
        return {
            "type": "HELLO",
            "peer_id": self.my_peer_id,
            "version": PROTOCOL_VERSION,
            "features": self.features,
            "ttl": 1,
        }

    def _build_hello_ok(self) -> dict:
        """Monta HELLO_OK de resposta; usa my_peer_id/features e retorna dict."""
        return {
            "type": "HELLO_OK",
            "peer_id": self.my_peer_id,
            "version": PROTOCOL_VERSION,
            "features": self.features,
            "ttl": 1,
        }

    def _validate_hello(self, message: dict) -> None:
        """Valida HELLO recebido; le campos obrigatorios e retorna None ou levanta ValueError."""
        if message.get("type") != "HELLO":
            raise ValueError("primeira mensagem precisa ser HELLO")
        if not message.get("peer_id"):
            raise ValueError("HELLO sem peer_id")
        if message.get("ttl", 1) != 1:
            raise ValueError("HELLO precisa ter ttl = 1")

    def _validate_hello_ok(self, message: dict) -> None:
        """Valida HELLO_OK recebido; le campos obrigatorios e retorna None ou levanta ValueError."""
        if message.get("type") != "HELLO_OK":
            raise ValueError("resposta do handshake precisa ser HELLO_OK")
        if not message.get("peer_id"):
            raise ValueError("HELLO_OK sem peer_id")
        if message.get("ttl", 1) != 1:
            raise ValueError("HELLO_OK precisa ter ttl = 1")

    # ---------------------------------------------------------------------
    # BYE / BYE_OK
    # ---------------------------------------------------------------------

    def send_bye(self, peer_id: str, reason: str = "Encerrando sessão") -> bool:
        """
        Envia BYE para um peer conectado.

        A conexão só será removida de verdade quando chegar BYE_OK ou quando o
        socket fechar. Se o envio falhar, a conexão é removida.
        """

        bye = {
            "type": "BYE",
            "msg_id": str(uuid.uuid4()),
            "src": self.my_peer_id,
            "dst": peer_id,
            "reason": reason,
            "ttl": 1,
        }
        return self.send_to_peer(peer_id, bye)

    def _handle_bye(self, connection: PeerConnection, message: dict) -> None:
        """
        Responde BYE com BYE_OK.
        """

        peer_id = connection.peer_id
        bye_ok = {
            "type": "BYE_OK",
            "msg_id": message.get("msg_id"),
            "src": self.my_peer_id,
            "dst": peer_id,
            "ttl": 1,
        }

        try:
            with connection.send_lock:
                self._send_json_to_socket(connection.sock, bye_ok)
            self.logger.info("BYE recebido de %s; BYE_OK enviado", peer_id)
        except Exception as exc:
            self.logger.warning("Falha ao responder BYE para %s: %s", peer_id, exc)

    # ---------------------------------------------------------------------
    # Estado das conexões e encerramento
    # ---------------------------------------------------------------------

    def is_connected(self, peer_id: str) -> bool:
        """Verifica conexao ativa; consulta connections com lock e retorna bool."""
        with self.connections_lock:
            return peer_id in self.connections

    def get_connection_ids(self) -> List[str]:
        """Retorna os peer_id atualmente conectados."""

        with self.connections_lock:
            return list(self.connections.keys())

    def get_connections_info(self) -> List[dict]:
        """Retorna dados das conexões ativas, útil para o comando /conn."""

        with self.connections_lock:
            return [
                {
                    "peer_id": conn.peer_id,
                    "direction": conn.direction,
                    "address": f"{conn.address[0]}:{conn.address[1]}",
                    "connected_for_seconds": round(time.time() - conn.connected_at, 2),
                }
                for conn in self.connections.values()
            ]

    def close_connection(self, peer_id: str) -> None:
        """Fecha uma conexão específica sem enviar BYE."""

        with self.connections_lock:
            connection = self.connections.get(peer_id)

        if connection is not None:
            self._remove_connection(peer_id, connection)

    def stop(self, reason: str = "Aplicação encerrada") -> None:
        """
        Encerra o servidor TCP local e fecha todas as conexões ativas.
        """

        self.running = False

        # Tenta avisar os peers antes de fechar.
        for peer_id in self.get_connection_ids():
            self.send_bye(peer_id, reason=reason)

        # Fecha o servidor para liberar accept().
        if self.server_socket is not None:
            self._close_socket(self.server_socket)
            self.server_socket = None

        # Fecha todas as conexões locais.
        with self.connections_lock:
            connections = list(self.connections.values())

        for connection in connections:
            self._remove_connection(connection.peer_id, connection)

        self.logger.info("PeerConnectionManager encerrado")

    def _store_connection(self, connection: PeerConnection) -> None:
        """
        Salva uma conexão ativa.

        Se já existir conexão com o mesmo peer_id, a antiga é fechada e substituída.
        Isso evita duplicidade quando dois peers tentam conectar um no outro ao
        mesmo tempo.
        """

        with self.connections_lock:
            old_connection = self.connections.get(connection.peer_id)
            self.connections[connection.peer_id] = connection

        if old_connection is not None and old_connection is not connection:
            self.logger.debug("Substituindo conexão antiga com %s", connection.peer_id)
            self._close_connection_object(old_connection)

    def _remove_connection(self, peer_id: str, connection: PeerConnection) -> None:
        """Remove e fecha conexao; chama _close_connection_object/_notify_disconnect e retorna None."""
        removed = False

        with self.connections_lock:
            current = self.connections.get(peer_id)
            if current is connection:
                del self.connections[peer_id]
                removed = True

        self._close_connection_object(connection)

        if removed:
            self.logger.info("Peer desconectado: %s", peer_id)
            self._notify_disconnect(peer_id)

    def _close_connection_object(self, connection: PeerConnection) -> None:
        """Fecha reader e socket de uma conexao; chama _close_socket e retorna None."""
        try:
            connection.reader.close()
        except Exception:
            pass
        self._close_socket(connection.sock)

    def _close_socket(self, sock: socket.socket) -> None:
        """Fecha socket com shutdown defensivo; ignora erros e retorna None."""
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        try:
            sock.close()
        except Exception:
            pass

    def _notify_connect(self, peer_id: str) -> None:
        """Executa callback on_connect se existir; captura excecoes e retorna None."""
        if self.on_connect is not None:
            try:
                self.on_connect(peer_id)
            except Exception as exc:
                self.logger.warning("Callback on_connect falhou para %s: %s", peer_id, exc)

    def _notify_disconnect(self, peer_id: str) -> None:
        """Executa callback on_disconnect se existir; captura excecoes e retorna None."""
        if self.on_disconnect is not None:
            try:
                self.on_disconnect(peer_id)
            except Exception as exc:
                self.logger.warning(
                    "Callback on_disconnect falhou para %s: %s", peer_id, exc
                )
