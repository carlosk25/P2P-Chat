# Grupo 12
# Augusto Queiroz Alves Silva - 232024302
# Carlos Eduardo Pires Gomes - 232045895
# Dannyeclisson Rodrigo Martins da Costa - 211061592

"""Main application orchestration for the P2P chat."""

from __future__ import annotations

import logging
import threading

import rendezvous_connection
from cli import CLI
from config import Config
from keep_alive import KeepAliveManager
from message_router import MessageRouter
from peer_connection import PeerConnectionManager
from peer_table import PeerTable


log = logging.getLogger("P2PClient")


class P2PClient:
    """Integra configuracao, Rendezvous, conexoes P2P, keep-alive e CLI."""

    def __init__(self, config_path: str = "config.json") -> None:
        """Carrega Config e cria gerenciadores; registra callbacks e retorna None."""
        self.config = Config.from_file(config_path)
        self.peer_table = PeerTable()
        self._stop_lock = threading.RLock()
        self._stopped = False

        self.connection_manager = PeerConnectionManager(
            my_peer_id=self.config.peer_id,
            listen_host=self.config.listen_host,
            listen_port=self.config.listen_port,
            features=self.config.features,
            on_message=self._on_message,
            on_connect=self._on_connect,
            on_disconnect=self._on_disconnect,
        )
        self.message_router = MessageRouter(
            self.config,
            self.connection_manager,
            self.peer_table,
        )
        self.keep_alive_manager = KeepAliveManager(
            self.config,
            self.connection_manager,
            self.peer_table,
        )
        self.cli = CLI(
            self.config,
            self.peer_table,
            self.connection_manager,
            self.message_router,
            self.keep_alive_manager,
            reconnect_callback=self.reconnect,
            shutdown_callback=self.stop,
        )

    def start(self) -> None:
        """Sobe servidor, registra no Rendezvous, inicia keep-alive/CLI e retorna None."""
        try:
            actual_port = self.connection_manager.start_server()
            self.config.listen_port = actual_port
            rendezvous_connection.register(self.config)
            self.reconnect(connect_discovered=True)
            self.keep_alive_manager.start()
            self.cli.start()
        except KeyboardInterrupt:
            log.info("Interrompido pelo usuario")
        finally:
            self.stop()

    def stop(self) -> None:
        """Encerra CLI, timers, conexoes e unregister; chama stops em ordem e retorna None."""
        with self._stop_lock:
            if self._stopped:
                return
            self._stopped = True

        try:
            self.cli.stop()
        except Exception:
            pass

        try:
            self.message_router.stop()
        except Exception as exc:
            log.warning("Falha ao parar MessageRouter: %s", exc)

        try:
            self.keep_alive_manager.stop()
        except Exception as exc:
            log.warning("Falha ao parar keep-alive: %s", exc)

        try:
            self.connection_manager.stop()
        except Exception as exc:
            log.warning("Falha ao encerrar conexoes P2P: %s", exc)

        try:
            rendezvous_connection.unregister(self.config)
        except Exception as exc:
            log.warning("Falha ao desregistrar no Rendezvous: %s", exc)

    def reconnect(self, connect_discovered: bool = True) -> None:
        """Descobre peers do namespace; atualiza PeerTable, conecta quando pedido e retorna None."""
        peers = rendezvous_connection.discover(self.config, namespace=self.config.namespace)
        self.peer_table.update_from_discovery(peers)

        if not connect_discovered:
            return

        for peer in self.peer_table.get_all():
            if peer.peer_id == self.config.peer_id:
                continue
            if peer.namespace != self.config.namespace:
                continue
            if self.connection_manager.is_connected(peer.peer_id):
                continue
            if self.connection_manager.connect_to_peer(peer.peer_id, peer.ip, peer.port):
                self.peer_table.mark_connected(peer.peer_id)

    def _on_message(self, peer_id: str, message: dict) -> None:
        """Callback de mensagens P2P; despacha para MessageRouter/KeepAlive e retorna None."""
        message_type = message.get("type")
        if message_type in {"SEND", "ACK", "PUB"}:
            self.message_router.handle_message(peer_id, message)
        elif message_type in {"PING", "PONG"}:
            self.keep_alive_manager.handle_message(peer_id, message)
        else:
            log.warning("Mensagem de tipo desconhecido recebida de %s: %s", peer_id, message_type)

    def _on_connect(self, peer_id: str) -> None:
        """Callback de conexao aberta; chama PeerTable.mark_connected e retorna None."""
        self.peer_table.mark_connected(peer_id)

    def _on_disconnect(self, peer_id: str) -> None:
        """Callback de conexao fechada; chama PeerTable.mark_disconnected e retorna None."""
        self.peer_table.mark_disconnected(peer_id)
