import json
from dataclasses import dataclass, field


@dataclass
class Config:
    name: str
    namespace: str
    port: int
    rendezvous_host: str = "pyp2p.mfcaetano.cc"
    rendezvous_port: int = 8080
    ping_interval: int = 30
    max_reconnect_attempts: int = 5
    ack_timeout: int = 5
    log_level: str = "INFO"
    log_file: str | None = None

    @property
    def peer_id(self) -> str:
        return f"{self.name}@{self.namespace}"

    @classmethod
    def from_file(cls, path: str = "config.json") -> "Config":
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        for required in ("name", "namespace", "port"):
            if required not in data:
                raise ValueError(f"config.json: campo obrigatório ausente: '{required}'")

        return cls(
            name=data["name"],
            namespace=data["namespace"],
            port=int(data["port"]),
            rendezvous_host=data.get("rendezvous_host", "pyp2p.mfcaetano.cc"),
            rendezvous_port=int(data.get("rendezvous_port", 8080)),
            ping_interval=int(data.get("ping_interval", 30)),
            max_reconnect_attempts=int(data.get("max_reconnect_attempts", 5)),
            ack_timeout=int(data.get("ack_timeout", 5)),
            log_level=data.get("log_level", "INFO"),
            log_file=data.get("log_file", None),
        )
