"""Interactive command-line interface for the P2P chat."""

from __future__ import annotations

import logging
from typing import Callable, Optional


VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR"}


class CLI:
    def __init__(
        self,
        config,
        peer_table,
        connection_manager,
        message_router,
        keep_alive_manager,
        reconnect_callback: Optional[Callable[[], None]] = None,
        shutdown_callback: Optional[Callable[[], None]] = None,
        input_func: Callable[[str], str] = input,
        output_func: Callable[[str], None] = print,
    ) -> None:
        self.config = config
        self.peer_table = peer_table
        self.connection_manager = connection_manager
        self.message_router = message_router
        self.keep_alive_manager = keep_alive_manager
        self.reconnect_callback = reconnect_callback
        self.shutdown_callback = shutdown_callback
        self.input_func = input_func
        self.output = output_func
        self.running = False

    def start(self) -> None:
        self.running = True
        self.output(f"Chat P2P iniciado como {self.config.peer_id}. Use /quit para sair.")

        while self.running:
            try:
                line = self.input_func("> ")
            except (EOFError, KeyboardInterrupt):
                self.output("")
                self.handle_line("/quit")
                break

            self.handle_line(line)

    def stop(self) -> None:
        self.running = False

    def handle_line(self, line: str) -> bool:
        line = line.strip()
        if not line:
            return True

        if not line.startswith("/"):
            self.output("Comando invalido. Use /msg, /pub, /peers, /conn, /rtt ou /quit.")
            return True

        command, _, rest = line.partition(" ")
        command = command.lower()
        rest = rest.strip()

        try:
            if command == "/peers":
                self._cmd_peers(rest)
            elif command == "/msg":
                self._cmd_msg(rest)
            elif command == "/pub":
                self._cmd_pub(rest)
            elif command == "/conn":
                self._cmd_conn()
            elif command == "/rtt":
                self._cmd_rtt()
            elif command == "/reconnect":
                self._cmd_reconnect()
            elif command == "/log":
                self._cmd_log(rest)
            elif command == "/quit":
                self._cmd_quit()
                return False
            else:
                self.output(f"Comando desconhecido: {command}")
        except Exception as exc:
            logging.getLogger("CLI").warning("Erro ao executar %s: %s", command, exc)
            self.output(f"Erro: {exc}")

        return self.running

    def _cmd_peers(self, arg: str) -> None:
        if not arg or arg == "*":
            peers = self.peer_table.get_all()
        elif arg.startswith("#") and len(arg) > 1:
            peers = self.peer_table.get_by_namespace(arg[1:])
        else:
            self.output("Uso: /peers [* | #namespace]")
            return

        if not peers:
            self.output("Nenhum peer conhecido.")
            return

        for peer in peers:
            state = getattr(peer.state, "value", peer.state)
            self.output(f"{peer.peer_id} {peer.ip}:{peer.port} {state}")

    def _cmd_msg(self, rest: str) -> None:
        parts = rest.split(maxsplit=1)
        if len(parts) != 2 or not parts[0] or not parts[1].strip():
            self.output("Uso: /msg <peer_id> <mensagem>")
            return

        peer_id, payload = parts[0], parts[1].strip()
        msg_id = self.message_router.send_direct(peer_id, payload)
        if msg_id is None:
            self.output(f"Nao foi possivel enviar mensagem para {peer_id}.")
        else:
            self.output(f"Mensagem enviada para {peer_id} (msg_id={msg_id}).")

    def _cmd_pub(self, rest: str) -> None:
        parts = rest.split(maxsplit=1)
        if len(parts) != 2 or not parts[1].strip():
            self.output("Uso: /pub * <mensagem> ou /pub #namespace <mensagem>")
            return

        target, payload = parts[0], parts[1].strip()
        if target != "*" and not (target.startswith("#") and len(target) > 1):
            self.output("Destino invalido. Use * ou #namespace.")
            return

        sent_to = self.message_router.send_publication(target, payload)
        self.output(f"Publicacao enviada para {len(sent_to)} peer(s).")

    def _cmd_conn(self) -> None:
        connections = self.connection_manager.get_connections_info()
        if not connections:
            self.output("Nenhuma conexao ativa.")
            return

        for conn in connections:
            self.output(
                "{direction} {peer_id} {address} conectado ha {seconds}s".format(
                    direction=conn.get("direction", "?"),
                    peer_id=conn.get("peer_id", "?"),
                    address=conn.get("address", "?"),
                    seconds=conn.get("connected_for_seconds", "?"),
                )
            )

    def _cmd_rtt(self) -> None:
        averages = self.keep_alive_manager.get_average_rtt()
        if not averages:
            self.output("Ainda nao ha medicoes de RTT.")
            return

        for peer_id, rtt_ms in sorted(averages.items()):
            self.output(f"{peer_id}: {rtt_ms:.2f} ms")

    def _cmd_reconnect(self) -> None:
        if self.reconnect_callback is None:
            self.output("Reconexao nao esta disponivel neste cliente.")
            return

        self.reconnect_callback()
        self.output("Reconexao/discovery executado.")

    def _cmd_log(self, arg: str) -> None:
        level = arg.strip().upper()
        if level not in VALID_LOG_LEVELS:
            self.output("Nivel invalido. Use DEBUG, INFO, WARNING ou ERROR.")
            return

        logging.getLogger().setLevel(getattr(logging, level))
        self.output(f"Nivel de log alterado para {level}.")

    def _cmd_quit(self) -> None:
        self.running = False
        self.output("Encerrando...")
        if self.shutdown_callback is not None:
            self.shutdown_callback()
