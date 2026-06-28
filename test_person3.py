# Grupo 12
# Augusto Queiroz Alves Silva - 232024302
# Carlos Eduardo Pires Gomes - 232045895
# Dannyeclisson Rodrigo Martins da Costa - 211061592

import logging
import time
import unittest
from dataclasses import dataclass

from cli import CLI
from keep_alive import KeepAliveManager
from message_router import MessageRouter
from peer_table import PeerTable


@dataclass
class FakeConfig:
    """Config falsa para testes da Pessoa 3, sem ler arquivos JSON."""

    name: str = "alice"
    namespace: str = "CIC"
    listen_port: int = 5000
    ack_timeout: float = 0.05
    keepalive_interval: float = 60

    @property
    def peer_id(self):
        """Monta peer_id de teste; usa name/namespace e retorna str."""
        return f"{self.name}@{self.namespace}"


class FakeConnectionManager:
    """Gerenciador falso que registra envios em memoria para testes unitarios."""

    def __init__(self, connected=None):
        """Inicializa lista de conectados e mensagens enviadas; retorna None."""
        self.connected = list(connected or ["bob@CIC"])
        self.sent = []

    def send_to_peer(self, peer_id, message):
        """Simula envio; grava mensagem se peer conectado e retorna bool."""
        if peer_id not in self.connected:
            return False
        self.sent.append((peer_id, message))
        return True

    def get_connection_ids(self):
        """Retorna peers conectados simulados; nao chama rede e retorna list[str]."""
        return list(self.connected)

    def get_connections_info(self):
        """Monta info fake de conexoes; usa connected e retorna list[dict]."""
        return [
            {
                "peer_id": peer_id,
                "direction": "outbound",
                "address": "127.0.0.1:5001",
                "connected_for_seconds": 1.2,
            }
            for peer_id in self.connected
        ]

    def connect_to_peer(self, peer_id, host, port):
        """Simula conexao manual; adiciona peer em connected e retorna True."""
        if peer_id not in self.connected:
            self.connected.append(peer_id)
        return True


class Person3Tests(unittest.TestCase):
    """Suite de testes da CLI, MessageRouter e KeepAliveManager."""

    def test_builds_send_ack_pub_ping_pong(self):
        """Valida construcao de mensagens; chama builders e confere campos retornados."""
        config = FakeConfig()
        conn = FakeConnectionManager()
        router = MessageRouter(config, conn, output=lambda _: None)
        keepalive = KeepAliveManager(config, conn)

        send = router.build_send("bob@CIC", "oi")
        self.assertEqual(send["type"], "SEND")
        self.assertEqual(send["src"], "alice@CIC")
        self.assertEqual(send["dst"], "bob@CIC")
        self.assertTrue(send["require_ack"])
        self.assertEqual(send["ttl"], 1)

        ack = router.build_ack(send["msg_id"])
        self.assertEqual(ack["type"], "ACK")
        self.assertEqual(ack["msg_id"], send["msg_id"])
        self.assertEqual(ack["ttl"], 1)

        pub = router.build_pub("*", "todos")
        self.assertEqual(pub["type"], "PUB")
        self.assertEqual(pub["dst"], "*")
        self.assertFalse(pub["require_ack"])
        self.assertEqual(pub["ttl"], 1)

        ping = keepalive.build_ping()
        self.assertEqual(ping["type"], "PING")
        self.assertEqual(ping["ttl"], 1)

        pong = keepalive.build_pong(ping["msg_id"])
        self.assertEqual(pong["type"], "PONG")
        self.assertEqual(pong["msg_id"], ping["msg_id"])
        self.assertEqual(pong["ttl"], 1)

    def test_receiving_send_sends_ack(self):
        """Valida SEND recebido; chama handle_message e espera ACK enviado."""
        config = FakeConfig()
        conn = FakeConnectionManager()
        output = []
        router = MessageRouter(config, conn, output=output.append)

        router.handle_message(
            "bob@CIC",
            {
                "type": "SEND",
                "msg_id": "m1",
                "src": "bob@CIC",
                "dst": "alice@CIC",
                "payload": "ola",
                "require_ack": True,
                "ttl": 1,
            },
        )

        self.assertEqual(conn.sent[-1][0], "bob@CIC")
        self.assertEqual(conn.sent[-1][1]["type"], "ACK")
        self.assertEqual(conn.sent[-1][1]["msg_id"], "m1")
        self.assertIn("[msg] bob@CIC: ola", output)

    def test_ack_removes_pending(self):
        """Valida ACK recebido; chama send_direct/handle_message e verifica pendencia removida."""
        config = FakeConfig(ack_timeout=1)
        conn = FakeConnectionManager()
        router = MessageRouter(config, conn, output=lambda _: None)

        msg_id = router.send_direct("bob@CIC", "ola")
        self.assertIn(msg_id, router.pending_acks)

        router.handle_message("bob@CIC", {"type": "ACK", "msg_id": msg_id, "ttl": 1})
        self.assertNotIn(msg_id, router.pending_acks)
        router.stop()

    def test_ack_timeout_logs_warning(self):
        """Valida timeout de ACK; chama send_direct, aguarda timer e verifica warning."""
        config = FakeConfig(ack_timeout=0.02)
        conn = FakeConnectionManager()
        router = MessageRouter(config, conn, output=lambda _: None)

        with self.assertLogs("MessageRouter", level="WARNING") as captured:
            msg_id = router.send_direct("bob@CIC", "sem ack")
            time.sleep(0.08)

        self.assertNotIn(msg_id, router.pending_acks)
        self.assertTrue(any("ACK" in line for line in captured.output))
        router.stop()

    def test_publication_filters_namespace(self):
        """Valida PUB por namespace; chama send_publication e confere destinatarios."""
        config = FakeConfig()
        conn = FakeConnectionManager(["bob@CIC", "carol@MAT"])
        table = PeerTable()
        table.update_from_discovery(
            [
                {"name": "bob", "namespace": "CIC", "ip": "127.0.0.1", "port": 5001},
                {"name": "carol", "namespace": "MAT", "ip": "127.0.0.1", "port": 5002},
            ]
        )
        router = MessageRouter(config, conn, table, output=lambda _: None)

        sent_to = router.send_publication("#CIC", "turma")
        self.assertEqual(sent_to, ["bob@CIC"])
        self.assertEqual(conn.sent[-1][1]["type"], "PUB")

    def test_receiving_ping_sends_pong_and_pong_computes_rtt(self):
        """Valida PING/PONG; chama handlers e confere RTT medio calculado."""
        config = FakeConfig()
        conn = FakeConnectionManager()
        keepalive = KeepAliveManager(config, conn)

        keepalive.handle_message("bob@CIC", {"type": "PING", "msg_id": "p1", "ttl": 1})
        self.assertEqual(conn.sent[-1][1]["type"], "PONG")
        self.assertEqual(conn.sent[-1][1]["msg_id"], "p1")

        self.assertTrue(keepalive.send_ping("bob@CIC"))
        ping_msg_id = conn.sent[-1][1]["msg_id"]
        keepalive.handle_message("bob@CIC", {"type": "PONG", "msg_id": ping_msg_id, "ttl": 1})
        self.assertIn("bob@CIC", keepalive.get_average_rtt())
        keepalive.stop()

    def test_cli_parses_main_commands(self):
        """Valida comandos principais da CLI; chama handle_line e confere saidas/efeitos."""
        config = FakeConfig()
        conn = FakeConnectionManager()
        router = MessageRouter(config, conn, output=lambda _: None)
        keepalive = KeepAliveManager(config, conn)
        table = PeerTable()
        table.update_from_discovery(
            [{"name": "bob", "namespace": "CIC", "ip": "127.0.0.1", "port": 5001}]
        )
        output = []
        reconnected = []
        cli = CLI(
            config,
            table,
            conn,
            router,
            keepalive,
            reconnect_callback=lambda: reconnected.append(True),
            shutdown_callback=lambda: None,
            output_func=output.append,
        )
        cli.running = True

        cli.handle_line("/peers")
        cli.handle_line("/conn")
        cli.handle_line("/connect carol@CIC 127.0.0.1 5003")
        cli.handle_line("/msg bob@CIC oi bob")
        cli.handle_line("/pub * oi todos")
        cli.handle_line("/rtt")
        cli.handle_line("/log DEBUG")
        cli.handle_line("/reconnect")

        self.assertTrue(any("bob@CIC" in line for line in output))
        self.assertTrue(any("Conectado a carol@CIC" in line for line in output))
        self.assertTrue(any("Nivel de log alterado" in line for line in output))
        self.assertEqual(reconnected, [True])
        self.assertEqual(logging.getLogger().level, logging.DEBUG)
        router.stop()
        keepalive.stop()


if __name__ == "__main__":
    unittest.main()
