import threading
import unittest
from dataclasses import dataclass
from unittest.mock import patch

from p2p_client import P2PClient
from peer_table import PeerTable
from state import PeerState


@dataclass
class FakeConfig:
    name: str = "alice"
    namespace: str = "CIC"
    listen_port: int = 5000
    listen_host: str = "127.0.0.1"
    discover_interval: int = 10
    autonomous_mode: bool = False
    features: tuple = ()

    @property
    def peer_id(self):
        return f"{self.name}@{self.namespace}"


class FakeConnectionManager:
    def __init__(self, connected=None):
        self.connected = set(connected or [])
        self.attempts = []

    def is_connected(self, peer_id):
        return peer_id in self.connected

    def connect_to_peer(self, peer_id, host, port):
        self.attempts.append((peer_id, host, port))
        self.connected.add(peer_id)
        return True

    def start_server(self):
        return 6000


class FakeStarter:
    def __init__(self):
        self.started = False

    def start(self):
        self.started = True


def make_client(config=None, connection_manager=None):
    client = P2PClient.__new__(P2PClient)
    client.config = config or FakeConfig()
    client.peer_table = PeerTable()
    client.connection_manager = connection_manager or FakeConnectionManager()
    client._stop_lock = threading.RLock()
    client._stopped = False
    client._reconnect_lock = threading.RLock()
    return client


class P2PClientDiscoveryTests(unittest.TestCase):
    def test_start_forces_initial_auto_connect(self):
        client = make_client()
        keep_alive = FakeStarter()
        cli = FakeStarter()
        reconnect_calls = []
        stop_calls = []
        client.keep_alive_manager = keep_alive
        client.cli = cli
        client.reconnect = lambda connect_discovered=True: reconnect_calls.append(
            connect_discovered
        )
        client.stop = lambda: stop_calls.append(True)

        with patch("p2p_client.rendezvous_connection.register") as register:
            P2PClient.start(client)

        register.assert_called_once_with(client.config)
        self.assertEqual(client.config.listen_port, 6000)
        self.assertEqual(reconnect_calls, [True])
        self.assertTrue(keep_alive.started)
        self.assertTrue(cli.started)
        self.assertEqual(stop_calls, [True])
        self.assertFalse(hasattr(P2PClient, "_start_discovery_loop"))
        self.assertFalse(hasattr(P2PClient, "_discovery_loop"))

    def test_reconnect_connects_discovered_peers_without_autonomous_mode(self):
        conn = FakeConnectionManager(connected={"dave@CIC"})
        client = make_client(connection_manager=conn)
        discovered = [
            {"name": "alice", "namespace": "CIC", "ip": "127.0.0.1", "port": 5000},
            {"name": "bob", "namespace": "CIC", "ip": "127.0.0.1", "port": 5001},
            {"name": "carol", "namespace": "MAT", "ip": "127.0.0.1", "port": 5002},
            {"name": "dave", "namespace": "CIC", "ip": "127.0.0.1", "port": 5003},
        ]

        with patch("p2p_client.rendezvous_connection.discover", return_value=discovered):
            client.reconnect(connect_discovered=True)

        self.assertEqual(conn.attempts, [("bob@CIC", "127.0.0.1", 5001)])
        self.assertEqual(client.peer_table.get("bob@CIC").state, PeerState.CONNECTED)

    def test_reconnect_only_connects_peers_from_current_discovery(self):
        conn = FakeConnectionManager()
        client = make_client(connection_manager=conn)
        client.peer_table.update_from_discovery(
            [{"name": "bob", "namespace": "CIC", "ip": "127.0.0.1", "port": 5001}]
        )

        with patch("p2p_client.rendezvous_connection.discover", return_value=[]):
            client.reconnect(connect_discovered=True)

        self.assertEqual(conn.attempts, [])


if __name__ == "__main__":
    unittest.main()
