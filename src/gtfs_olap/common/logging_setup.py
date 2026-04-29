"""Konfiguracja logowania (loguru) - wspólna dla wszystkich entry pointów."""

import sys

from loguru import logger


def setup_logging(level: str = "INFO") -> None:
    """Konfiguruje loguru: kolorowy output, czas + level + funkcja + wiadomość."""
    logger.remove()
    logger.add(
        sys.stderr,
        format=(
            "<green>{time:HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}.{function}</cyan> | "
            "{message}"
        ),
        level=level,
    )
