"""Application entry point."""

from __future__ import annotations

import logging
import sys

from config import Config
from p2p_client import P2PClient


def configure_logging(config_path: str = "config.json") -> None:
    try:
        config = Config.from_file(config_path)
        level_name = config.log_level.upper()
    except Exception:
        level_name = "INFO"

    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="[%(levelname)s] %(name)s: %(message)s",
    )


def main() -> None:
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.json"
    configure_logging(config_path)
    client = P2PClient(config_path)
    client.start()


if __name__ == "__main__":
    main()
