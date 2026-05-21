"""Logging estruturado com loguru. JSON em prod (Cloud Logging), legivel em dev."""

from __future__ import annotations

import sys

from loguru import logger


def setup_logging(level: str = "INFO", json: bool = False) -> None:
    """Configura o sink unico do loguru.

    Em prod (`json=True`) emite uma linha JSON por evento, consumivel pelo
    Cloud Logging. Em dev, formato colorido e legivel.
    """
    logger.remove()
    if json:
        logger.add(sys.stdout, level=level, serialize=True, backtrace=False, diagnose=False)
    else:
        logger.add(
            sys.stdout,
            level=level,
            colorize=True,
            format=(
                "<green>{time:HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
            ),
        )
