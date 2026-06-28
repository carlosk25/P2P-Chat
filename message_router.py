"""Routing for chat messages, ACKs and publications."""

from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional


log = logging.getLogger("MessageRouter")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class PendingAck:
    peer_id: str
    payload: str
    sent_at: float
    timer: threading.Timer


class MessageRouter:
    """Builds, sends and handles SEND/ACK/PUB messages."""

    def __init__(
        self,
        config,
        connection_manager,
        peer_table=None,
        output: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.config = config
        self.connection_manager = connection_manager
        self.peer_table = peer_table
        self.output = output or print
        self.pending_acks: dict[str, PendingAck] = {}
        self._lock = threading.RLock()

    def build_send(self, peer_id: str, payload: str) -> dict:
        return {
            "type": "SEND",
            "msg_id": str(uuid.uuid4()),
            "src": self.config.peer_id,
            "dst": peer_id,
            "payload": payload,
            "require_ack": True,
            "ttl": 1,
        }

    def build_ack(self, msg_id: str) -> dict:
        return {
            "type": "ACK",
            "msg_id": msg_id,
            "timestamp": utc_now_iso(),
            "ttl": 1,
        }

    def build_pub(self, target: str, payload: str) -> dict:
        return {
            "type": "PUB",
            "msg_id": str(uuid.uuid4()),
            "src": self.config.peer_id,
            "dst": target,
            "payload": payload,
            "require_ack": False,
            "ttl": 1,
        }

    def send_direct(self, peer_id: str, payload: str) -> Optional[str]:
        message = self.build_send(peer_id, payload)
        if message.get("require_ack"):
            self._register_pending_ack(message["msg_id"], peer_id, payload)

        sent = self.connection_manager.send_to_peer(peer_id, message)
        if not sent:
            self._cancel_pending_ack(message["msg_id"])
            return None

        log.info("SEND enviado para %s (msg_id=%s)", peer_id, message["msg_id"])
        return message["msg_id"]

    def send_publication(self, target: str, payload: str) -> list[str]:
        message = self.build_pub(target, payload)
        recipients = self._publication_recipients(target)
        sent_to: list[str] = []

        for peer_id in recipients:
            if self.connection_manager.send_to_peer(peer_id, message):
                sent_to.append(peer_id)

        log.info(
            "PUB enviado para %d peer(s), destino=%s, msg_id=%s",
            len(sent_to),
            target,
            message["msg_id"],
        )
        return sent_to

    def handle_message(self, peer_id: str, message: dict) -> None:
        message_type = message.get("type")
        if message_type == "SEND":
            self.handle_send(peer_id, message)
        elif message_type == "ACK":
            self.handle_ack(peer_id, message)
        elif message_type == "PUB":
            self.handle_pub(peer_id, message)
        else:
            log.warning("MessageRouter recebeu tipo desconhecido de %s: %s", peer_id, message_type)

    def handle_send(self, peer_id: str, message: dict) -> None:
        msg_id = message.get("msg_id")
        src = message.get("src", peer_id)
        payload = message.get("payload", "")

        log.info("Mensagem recebida de %s: %s", src, payload)
        self.output(f"[msg] {src}: {payload}")

        if message.get("require_ack") and msg_id:
            ack = self.build_ack(str(msg_id))
            if self.connection_manager.send_to_peer(peer_id, ack):
                log.debug("ACK enviado para %s (msg_id=%s)", peer_id, msg_id)

    def handle_ack(self, peer_id: str, message: dict) -> None:
        msg_id = message.get("msg_id")
        if not msg_id:
            log.warning("ACK sem msg_id recebido de %s", peer_id)
            return

        with self._lock:
            pending = self.pending_acks.pop(str(msg_id), None)

        if pending is None:
            log.debug("ACK desconhecido ou tardio recebido de %s (msg_id=%s)", peer_id, msg_id)
            return

        pending.timer.cancel()
        elapsed = time.monotonic() - pending.sent_at
        log.info("ACK recebido de %s (msg_id=%s, %.3fs)", peer_id, msg_id, elapsed)

    def handle_pub(self, peer_id: str, message: dict) -> None:
        src = message.get("src", peer_id)
        dst = message.get("dst", "*")
        payload = message.get("payload", "")
        log.info("PUB recebido de %s para %s: %s", src, dst, payload)
        self.output(f"[pub {dst}] {src}: {payload}")

    def _register_pending_ack(self, msg_id: str, peer_id: str, payload: str) -> None:
        timer = threading.Timer(
            float(getattr(self.config, "ack_timeout", 5)),
            self._ack_timeout,
            args=(msg_id,),
        )
        timer.daemon = True
        pending = PendingAck(
            peer_id=peer_id,
            payload=payload,
            sent_at=time.monotonic(),
            timer=timer,
        )

        with self._lock:
            self.pending_acks[msg_id] = pending

        timer.start()

    def _ack_timeout(self, msg_id: str) -> None:
        with self._lock:
            pending = self.pending_acks.pop(msg_id, None)

        if pending is None:
            return

        log.warning(
            "ACK não recebido de %s após %.1fs (msg_id=%s)",
            pending.peer_id,
            float(getattr(self.config, "ack_timeout", 5)),
            msg_id,
        )

    def _cancel_pending_ack(self, msg_id: str) -> None:
        with self._lock:
            pending = self.pending_acks.pop(msg_id, None)

        if pending is not None:
            pending.timer.cancel()

    def _publication_recipients(self, target: str) -> list[str]:
        connected = set(self.connection_manager.get_connection_ids())
        if target == "*":
            return sorted(connected)

        if not target.startswith("#"):
            return []

        namespace = target[1:]
        suffix = f"@{namespace}"
        if self.peer_table is not None:
            peers = self.peer_table.get_by_namespace(namespace)
            from_table = {peer.peer_id for peer in peers if peer.peer_id in connected}
            from_connected_ids = {peer_id for peer_id in connected if peer_id.endswith(suffix)}
            return sorted(from_table | from_connected_ids)

        return sorted(peer_id for peer_id in connected if peer_id.endswith(suffix))

    def stop(self) -> None:
        with self._lock:
            pending = list(self.pending_acks.values())
            self.pending_acks.clear()

        for item in pending:
            item.timer.cancel()
