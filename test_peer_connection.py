import logging
import time
import threading

from peer_connection import PeerConnectionManager


logging.basicConfig(
    level=logging.DEBUG,
    format="[%(levelname)s] %(name)s: %(message)s"
)


mensagem_recebida = threading.Event()


def on_message_alice(peer_id, message):
    print(f"[ALICE] recebeu mensagem de {peer_id}: {message}")
    mensagem_recebida.set()


def on_connect(peer_id):
    print(f"[CALLBACK] conectado com {peer_id}")


def on_disconnect(peer_id):
    print(f"[CALLBACK] desconectado de {peer_id}")


def main():
    alice = PeerConnectionManager(
        my_peer_id="alice@CIC",
        listen_host="127.0.0.1",
        listen_port=0,
        on_message=on_message_alice,
        on_connect=on_connect,
        on_disconnect=on_disconnect
    )

    bob = PeerConnectionManager(
        my_peer_id="bob@CIC",
        listen_host="127.0.0.1",
        listen_port=0,
        on_connect=on_connect,
        on_disconnect=on_disconnect
    )

    alice_port = alice.start_server()
    bob_port = bob.start_server()

    print(f"Alice escutando em 127.0.0.1:{alice_port}")
    print(f"Bob escutando em 127.0.0.1:{bob_port}")

    time.sleep(0.5)

    print("\n[TESTE] Bob tentando conectar na Alice...")
    conectado = bob.connect_to_peer(
        peer_id="alice@CIC",
        host="127.0.0.1",
        port=alice_port
    )

    assert conectado, "Bob não conseguiu conectar na Alice"

    time.sleep(0.5)

    print("\n[TESTE] Conexões ativas da Alice:")
    print(alice.get_connections_info())

    print("\n[TESTE] Conexões ativas do Bob:")
    print(bob.get_connections_info())

    assert alice.is_connected("bob@CIC"), "Alice não registrou conexão com Bob"
    assert bob.is_connected("alice@CIC"), "Bob não registrou conexão com Alice"

    print("\n[TESTE] Bob enviando mensagem genérica para Alice...")
    bob.send_to_peer("alice@CIC", {
        "type": "SEND",
        "msg_id": "teste-1",
        "src": "bob@CIC",
        "dst": "alice@CIC",
        "payload": "Oi Alice!",
        "require_ack": True,
        "ttl": 1
    })

    recebeu = mensagem_recebida.wait(timeout=3)
    assert recebeu, "Alice não recebeu a mensagem enviada por Bob"

    print("\n[TESTE] Bob enviando BYE para Alice...")
    bob.send_bye("alice@CIC", reason="Fim do teste")

    time.sleep(1)

    print("\n[TESTE] Conexões após BYE:")
    print("Alice:", alice.get_connections_info())
    print("Bob:", bob.get_connections_info())

    alice.stop()
    bob.stop()

    print("\n✅ Teste concluído com sucesso!")


if __name__ == "__main__":
    main()