# =============================================================================
# LOGGER - Configuration centralisée du logging
# =============================================================================

from __future__ import annotations

import logging
import os
import sys
from typing import Optional


def _get_level() -> int:
    level_name = (os.environ.get("LOG_LEVEL") or "INFO").upper().strip()
    return getattr(logging, level_name, logging.INFO)


def _build_logger(name: str, level: Optional[int] = None) -> logging.Logger:
    """Construit un logger idempotent (évite doublons de handlers)."""
    logger_obj = logging.getLogger(name)

    # Déjà configuré
    if logger_obj.handlers:
        return logger_obj

    logger_obj.setLevel(level if level is not None else _get_level())

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level if level is not None else _get_level())

    formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    logger_obj.addHandler(handler)
    logger_obj.propagate = False

    return logger_obj


# Logger applicatif générique
logger = _build_logger("app")

# Logger dédié scraping (utile pour filtrer dans Railway logs)
scraping_logger = _build_logger("scraping")
