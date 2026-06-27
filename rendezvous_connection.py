##comunicação com servidor Rendezvous
# Cada operação abre uma nova conexão TCP, envia um comando JSON, lê a resposta e fecha.
# O servidor aceita exatamente um comando por conexão.

import json
import logging
import socket

from config import Config

log = logging.getLogger("Rendezvous")

_TIMEOUT = 5          # segundos até timeout de conexão/leitura
_MAX_LINE = 32 * 1024 # limite de 32 KiB por linha (imposto pelo servidor)


def register(config: Config) -> None:
    # Registra o peer no servidor Rendezvous.
    # Deve ser chamado antes de discover() e unregister().
    payload = {
        "type": "REGISTER",
        "namespace": config.namespace,
        "name": config.name,
        "port": config.listen_port,
        "ttl": config.rdv_ttl,
    }
    response = _send_command(config.rdv_host, config.rdv_port, payload)
    _check_ok(response, "REGISTER")
    log.info("Registrado como %s (ip=%s, port=%s, ttl=%s)",
             config.peer_id, response.get("ip"), response.get("port"), response.get("ttl"))


def discover(config: Config, namespace: str | None = None) -> list[dict]:
    # Descobre peers registrados no servidor Rendezvous.
    # Se namespace for None, retorna peers de todos os namespaces.
    # Requer REGISTER prévio, servidor retorna erro caso contrário.
    payload: dict = {"type": "DISCOVER"}
    if namespace is not None:
        payload["namespace"] = namespace

    response = _send_command(config.rdv_host, config.rdv_port, payload)
    _check_ok(response, "DISCOVER")

    peers = response.get("peers", [])
    # Remove o próprio peer da lista
    peers = [p for p in peers if not (p["name"] == config.name and p["namespace"] == config.namespace)]
    log.info("DISCOVER: %d peer(s) encontrado(s) (namespace=%s)", len(peers), namespace or "*")
    return peers


def unregister(config: Config) -> None:
    # Remove o registro do peer no servidor Rendezvous.
    # Chamado no /quit, antes de encerrar o programa.
    payload = {
        "type": "UNREGISTER",
        "namespace": config.namespace,
        "name": config.name,
        "port": config.listen_port,
    }
    response = _send_command(config.rdv_host, config.rdv_port, payload)
    _check_ok(response, "UNREGISTER")
    log.info("Desregistrado: %s", config.peer_id)


# Funções internas

def _send_command(host: str, port: int, payload: dict) -> dict:
    # Abre conexão TCP, envia JSON+\n, lê resposta JSON+\n, fecha.
    # O protocolo exige \n como delimitador de mensagem.
    line = json.dumps(payload) + "\n"
    if len(line.encode("utf-8")) > _MAX_LINE:
        raise ValueError("Payload excede 32 KiB")

    with socket.create_connection((host, port), timeout=_TIMEOUT) as sock:
        sock.sendall(line.encode("utf-8"))

        # Lê em loop porque recv() pode retornar dados em pedaços
        buf = b""
        while b"\n" not in buf:
            chunk = sock.recv(4096)
            if not chunk:
                break
            buf += chunk
            if len(buf) > _MAX_LINE:
                raise RuntimeError("Resposta do Rendezvous excede 32 KiB")

    raw = buf.split(b"\n", 1)[0].decode("utf-8")
    response = json.loads(raw)

    if response.get("status") == "ERROR":
        msg = response.get("message", "erro desconhecido")
        # Rate limit: IP bloqueado por exceder 50 req/min
        if "blocked" in msg:
            log.warning("Rate limit atingido no Rendezvous: %s", msg)
        else:
            log.error("Rendezvous erro (%s): %s", payload.get("type"), msg)

    return response


def _check_ok(response: dict, command: str) -> None:
    # Verifica se a resposta do servidor foi OK, lança erro caso contrário
    if response.get("status") != "OK":
        msg = response.get("message", "erro desconhecido")
        raise RuntimeError(f"{command} falhou: {msg}")
