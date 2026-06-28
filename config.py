# Grupo 12
# Augusto Queiroz Alves Silva - 232024302
# Carlos Eduardo Pires Gomes - 232045895
# Dannyeclisson Rodrigo Martins da Costa - 211061592

##lê o arquivo de configuração config.json e transforma os dados em um objeto
# que o resto do programa consegue usar.

import json
from dataclasses import dataclass, field


@dataclass
class Config:
    """Representa as configuracoes carregadas do JSON para os outros modulos."""

    # Campos obrigatórios — o programa não sobe sem eles
    name: str        # identificador do peer (ex: "alice")
    namespace: str   # grupo lógico (ex: "UnB")
    listen_port: int # porta TCP local que este peer vai escutar

    # Campos opcionais — possuem valores padrão se não estiverem no JSON
    app_name: str = "pyp2p-chat"
    rdv_host: str = "pyp2p.mfcaetano.cc"
    rdv_port: int = 8080
    listen_host: str = "0.0.0.0"        # interface de escuta (0.0.0.0 = aceita de qualquer IP)
    discover_interval: int = 20          # segundos entre cada DISCOVER automático
    keepalive_interval: int = 30         # segundos entre cada PING de keep-alive
    rdv_ttl: int = 3600                  # tempo de vida do registro no Rendezvous (segundos)
    fixed_msg_ttl: int = 1               # TTL fixo das mensagens P2P (sempre 1, conforme a spec)
    log_level: str = "INFO"
    features: list = field(default_factory=lambda: ["ack", "metrics"])
    autonomous_mode: bool = False        # se True, conecta automaticamente a todos os peers descobertos
    max_reconnect_attempts: int = 5      # tentativas antes de marcar o peer como stale
    ack_timeout: int = 5                 # segundos esperando ACK antes de logar timeout

    @property
    def peer_id(self) -> str:
        """Monta identificador name@namespace; usa campos name/namespace e retorna str."""
        # Identificador único no formato name@namespace (ex: "alice@UnB")
        return f"{self.name}@{self.namespace}"

    @classmethod
    def from_file(cls, path: str = "config.json") -> "Config":
        """Le JSON de configuracao; chama json.load e retorna uma instancia Config."""
        # Lê o JSON, valida campos obrigatórios e retorna um objeto Config pronto
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        for required in ("name", "namespace", "port"):
            if required not in data:
                raise ValueError(f"config.json: campo obrigatório ausente: '{required}'")

        return cls(
            name=data["name"],
            namespace=data["namespace"],
            listen_port=int(data["port"]),                                    # "port" no JSON
            app_name=data.get("app_name", "pyp2p-chat"),
            rdv_host=data.get("rendezvous_host", "pyp2p.mfcaetano.cc"),      # "rendezvous_host" no JSON
            rdv_port=int(data.get("rendezvous_port", 8080)),                  # "rendezvous_port" no JSON
            listen_host=data.get("listen_host", "0.0.0.0"),
            discover_interval=int(data.get("discover_interval", 20)),
            keepalive_interval=int(data.get("ping_interval", 30)),            # "ping_interval" no JSON
            rdv_ttl=int(data.get("rdv_ttl", 3600)),
            fixed_msg_ttl=int(data.get("fixed_msg_ttl", 1)),
            log_level=data.get("log_level", "INFO"),
            features=data.get("features", ["ack", "metrics"]),
            autonomous_mode=bool(data.get("autonomous_mode", False)),
            max_reconnect_attempts=int(data.get("max_reconnect_attempts", 5)),
            ack_timeout=int(data.get("ack_timeout", 5)),
        )
