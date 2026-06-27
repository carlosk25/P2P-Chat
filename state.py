##armazenamento dos peers conhecidos
# Define as estruturas de dados que representam um peer e seu estado na rede.

from dataclasses import dataclass
from enum import Enum


class PeerState(Enum):
    # Estados possíveis de um peer conhecido
    UNKNOWN = "unknown"           # descoberto pelo Rendezvous, sem tentativa de conexão ainda
    CONNECTED = "connected"       # conexão TCP ativa estabelecida
    DISCONNECTED = "disconnected" # conexão encerrada (BYE ou erro)
    STALE = "stale"               # peer parou de responder (sem PONG)


@dataclass
class Peer:
    # Representa um único peer conhecido na rede
    name: str        # identificador dentro do namespace (ex: "alice")
    namespace: str   # grupo lógico do peer (ex: "UnB")
    ip: str          # endereço IP para conexão TCP direta
    port: int        # porta TCP para conexão direta
    state: PeerState = PeerState.UNKNOWN
    ttl: int = 7200        # tempo de vida do registro no Rendezvous (segundos)
    expires_in: int = 0    # segundos restantes até o registro expirar

    @property
    def peer_id(self) -> str:
        # Identificador único do peer no formato name@namespace (ex: "alice@UnB")
        return f"{self.name}@{self.namespace}"

    def __repr__(self) -> str:
        # Representação legível para debug (ex: Peer(alice@UnB | 192.168.0.1:5000 | connected))
        return f"Peer({self.peer_id} | {self.ip}:{self.port} | {self.state.value})"
