# Grupo 12
# Augusto Queiroz Alves Silva - 232024302
# Carlos Eduardo Pires Gomes - 232045895
# Dannyeclisson Rodrigo Martins da Costa - 211061592

"""PING/PONG keep-alive and RTT accounting."""

from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone


log = logging.getLogger("KeepAlive")


def utc_now_iso() -> str:
    """Gera timestamp UTC ISO 8601; nao chama outros modulos do projeto e retorna str."""
    return datetime.now(timezone.utc).isoformat()


@dataclass
class PendingPing:
    """Registro de PING aguardando PONG, com peer, horario local e timer."""

    peer_id: str
    sent_at: float
    timer: threading.Timer


class KeepAliveManager:
    """Sends periodic PINGs, answers PINGs and computes RTT from PONGs."""

    def __init__(self, config, connection_manager, peer_table=None) -> None:
        """Inicializa estado de PING/RTT; recebe dependencias e retorna None."""
        self.config = config
        self.connection_manager = connection_manager
        self.peer_table = peer_table
        self.interval = float(getattr(config, "keepalive_interval", 30))
        self.timeout = float(getattr(config, "ack_timeout", 5))
        self.pending_pings: dict[str, PendingPing] = {}
        self.rtt_history: dict[str, list[float]] = {}
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def build_ping(self) -> dict:
        """Monta mensagem PING; chama uuid/utc_now_iso e retorna dict do protocolo."""
        return {
            "type": "PING",
            "msg_id": str(uuid.uuid4()),
            "timestamp": utc_now_iso(),
            "ttl": 1,
        }

    def build_pong(self, msg_id: str) -> dict:
        """Monta PONG para um PING; chama utc_now_iso e retorna dict do protocolo."""
        return {
            "type": "PONG",
            "msg_id": msg_id,
            "timestamp": utc_now_iso(),
            "ttl": 1,
        }

    def start(self) -> None:
        """Inicia thread periodica; chama _run em background e retorna None."""
        if self._thread is not None and self._thread.is_alive():
            return

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        log.info("Keep-alive iniciado (intervalo %.1fs)", self.interval)

    def stop(self) -> None:
        """Para thread e timers pendentes; chama join/cancel e retorna None."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2)

        with self._lock:
            pending = list(self.pending_pings.values())
            self.pending_pings.clear()

        for item in pending:
            item.timer.cancel()

        log.info("Keep-alive encerrado")

    def send_ping_all(self) -> list[str]:
        """Envia PING aos peers conectados; chama get_connection_ids/send_ping e retorna lista enviada."""
        sent_to: list[str] = []
        for peer_id in self.connection_manager.get_connection_ids():
            if self.send_ping(peer_id):
                sent_to.append(peer_id)
        return sent_to

    def send_ping(self, peer_id: str) -> bool:
        """Envia PING a um peer; chama build_ping/send_to_peer e retorna sucesso bool."""
        ping = self.build_ping()
        msg_id = ping["msg_id"]
        timer = threading.Timer(self.timeout, self._ping_timeout, args=(msg_id,))
        timer.daemon = True

        pending = PendingPing(peer_id=peer_id, sent_at=time.monotonic(), timer=timer)
        with self._lock:
            self.pending_pings[msg_id] = pending

        if not self.connection_manager.send_to_peer(peer_id, ping):
            with self._lock:
                self.pending_pings.pop(msg_id, None)
            timer.cancel()
            return False

        timer.start()
        log.debug("PING enviado para %s (msg_id=%s)", peer_id, msg_id)
        return True

    def handle_message(self, peer_id: str, message: dict) -> None:
        """Despacha PING/PONG recebido; chama handle_ping/handle_pong e retorna None."""
        message_type = message.get("type")
        if message_type == "PING":
            self.handle_ping(peer_id, message)
        elif message_type == "PONG":
            self.handle_pong(peer_id, message)
        else:
            log.warning("KeepAlive recebeu tipo desconhecido de %s: %s", peer_id, message_type)

    def handle_ping(self, peer_id: str, message: dict) -> None:
        """Responde PING com PONG; chama build_pong/send_to_peer e retorna None."""
        msg_id = message.get("msg_id")
        if not msg_id:
            log.warning("PING sem msg_id recebido de %s", peer_id)
            return

        pong = self.build_pong(str(msg_id))
        if self.connection_manager.send_to_peer(peer_id, pong):
            log.debug("PONG enviado para %s (msg_id=%s)", peer_id, msg_id)

    def handle_pong(self, peer_id: str, message: dict) -> None:
        """Calcula RTT de PONG; atualiza historico, cancela timer e retorna None."""
        msg_id = message.get("msg_id")
        if not msg_id:
            log.warning("PONG sem msg_id recebido de %s", peer_id)
            return

        with self._lock:
            pending = self.pending_pings.pop(str(msg_id), None)

        if pending is None:
            log.debug("PONG desconhecido ou tardio de %s (msg_id=%s)", peer_id, msg_id)
            return

        pending.timer.cancel()
        rtt_ms = (time.monotonic() - pending.sent_at) * 1000.0
        with self._lock:
            history = self.rtt_history.setdefault(peer_id, [])
            history.append(rtt_ms)
            del history[:-20]

        log.info("PONG recebido de %s (RTT %.2f ms)", peer_id, rtt_ms)

    def get_average_rtt(self) -> dict[str, float]:
        """Calcula medias de RTT por peer; le rtt_history com lock e retorna dict."""
        with self._lock:
            return {
                peer_id: sum(values) / len(values)
                for peer_id, values in self.rtt_history.items()
                if values
            }

    def _run(self) -> None:
        """Loop interno da thread; chama send_ping_all a cada intervalo e retorna None."""
        while not self._stop_event.is_set():
            try:
                self.send_ping_all()
            except Exception as exc:
                log.warning("Erro no keep-alive: %s", exc)

            self._stop_event.wait(self.interval)

    def _ping_timeout(self, msg_id: str) -> None:
        """Trata PING sem PONG; chama PeerTable.mark_stale quando houver e retorna None."""
        with self._lock:
            pending = self.pending_pings.pop(msg_id, None)

        if pending is None:
            return

        log.warning(
            "PONG não recebido de %s após %.1fs (msg_id=%s)",
            pending.peer_id,
            self.timeout,
            msg_id,
        )
        if self.peer_table is not None and hasattr(self.peer_table, "mark_stale"):
            try:
                self.peer_table.mark_stale(pending.peer_id)
            except Exception as exc:
                log.warning("Falha ao marcar %s como STALE: %s", pending.peer_id, exc)
