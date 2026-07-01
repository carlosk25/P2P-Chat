# Grupo 12
# Augusto Queiroz Alves Silva - 232024302
# Carlos Eduardo Pires Gomes - 232045895
# Dannyeclisson Rodrigo Martins da Costa - 211061592

##controle de estado e reconexões
# Repositório central de todos os peers conhecidos.
# Thread-safe: múltiplas threads (servidor TCP, keep-alive, CLI) acessam simultaneamente.

import logging
import threading

from state import Peer, PeerState

log = logging.getLogger("PeerTable")


class PeerTable:
    """Tabela thread-safe de peers conhecidos e seus estados."""

    def __init__(self) -> None:
        """Cria dicionario de peers e lock; nao chama outros modulos e retorna None."""
        # Dicionário principal: peer_id -> Peer (ex: "bob@UnB" -> Peer(...))
        self._peers: dict[str, Peer] = {}
        # Lock garante que apenas uma thread acessa o dicionário por vez
        self._lock = threading.Lock()

    # ── leitura ──────────────────────────────────────────────────────────────

    def get(self, peer_id: str) -> Peer | None:
        """Busca peer por id; le dicionario com lock e retorna Peer ou None."""
        # Retorna o peer pelo ID ou None se não existir
        with self._lock:
            return self._peers.get(peer_id)

    def get_all(self) -> list[Peer]:
        """Lista todos os peers; le dicionario com lock e retorna list[Peer]."""
        # Retorna todos os peers conhecidos (usado em /conn e /peers)
        with self._lock:
            return list(self._peers.values())

    def get_by_namespace(self, namespace: str) -> list[Peer]:
        """Filtra peers por namespace; le dicionario com lock e retorna list[Peer]."""
        # Retorna apenas os peers de um namespace específico (usado em /pub #namespace)
        with self._lock:
            return [p for p in self._peers.values() if p.namespace == namespace]

    # ── escrita de estado ─────────────────────────────────────────────────────

    def mark_connected(self, peer_id: str) -> None:
        """Marca peer como CONNECTED; chama _set_state e retorna None."""
        # Chamado pela Pessoa 2 ao receber HELLO_OK com sucesso
        self._set_state(peer_id, PeerState.CONNECTED)

    def mark_disconnected(self, peer_id: str) -> None:
        """Marca peer como DISCONNECTED; chama _set_state e retorna None."""
        # Chamado pela Pessoa 2 ao receber BYE ou ao detectar erro de conexão
        self._set_state(peer_id, PeerState.DISCONNECTED)

    def mark_stale(self, peer_id: str) -> None:
        """Marca peer como STALE; chama _set_state e retorna None."""
        # Chamado pela Pessoa 3 quando o peer não responde ao PING (sem PONG)
        self._set_state(peer_id, PeerState.STALE)

    def remove(self, peer_id: str) -> None:
        """Remove peer da tabela; altera dicionario com lock e retorna None."""
        # Remove o peer completamente da tabela
        with self._lock:
            if peer_id in self._peers:
                del self._peers[peer_id]
                log.debug("Removido: %s", peer_id)

    # ── integração com Rendezvous ─────────────────────────────────────────────

    def update_from_discovery(self, peer_list: list[dict]) -> None:
        """Mescla resposta do Rendezvous; cria/atualiza Peer e retorna None."""
        # Faz merge da lista retornada pelo discover() na tabela:
        # - Peer novo: cria com state=UNKNOWN
        # - Peer já conhecido: atualiza ttl/expires_in, preserva o estado atual
        # - Peer que sumiu do Rendezvous: NÃO é removido (pode estar CONNECTED)
        with self._lock:
            for data in peer_list:
                peer_id = f"{data['name']}@{data['namespace']}"
                if peer_id in self._peers:
                    # Atualiza tempo de vida sem sobrescrever o estado de conexão
                    self._peers[peer_id].ttl = data.get("ttl", 7200)
                    self._peers[peer_id].expires_in = data.get("expires_in", 0)
                    log.debug("Atualizado: %s", peer_id)
                else:
                    peer = Peer(
                        name=data["name"],
                        namespace=data["namespace"],
                        ip=data["ip"],
                        port=data["port"],
                        ttl=data.get("ttl", 7200),
                        expires_in=data.get("expires_in", 0),
                    )
                    self._peers[peer_id] = peer
                    log.info("Novo peer descoberto: %s", peer)

    # ── helpers ───────────────────────────────────────────────────────────────

    def _set_state(self, peer_id: str, state: PeerState) -> None:
        """Altera estado interno de um peer; usa lock e retorna None."""
        # Altera o estado de um peer; loga warning se o peer não existir na tabela
        with self._lock:
            if peer_id in self._peers:
                self._peers[peer_id].state = state
                log.debug("%s → %s", peer_id, state.value)
            else:
                log.warning("_set_state: peer desconhecido '%s'", peer_id)

    def __len__(self) -> int:
        """Conta peers conhecidos; le dicionario com lock e retorna int."""
        # Permite usar len(peer_table) para saber quantos peers estão na tabela
        with self._lock:
            return len(self._peers)

    def __repr__(self) -> str:
        """Gera representacao de debug; usa lock e retorna str."""
        with self._lock:
            return f"PeerTable({len(self._peers)} peers)"
