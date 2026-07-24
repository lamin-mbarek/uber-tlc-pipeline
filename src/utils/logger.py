"""Configuration centralisée du logging pour l'ensemble du pipeline.

Chaque module obtient son logger via :

    from src.utils.logger import get_logger
    logger = get_logger(__name__)
"""

import logging


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Crée et configure un logger nommé pour un module du pipeline.

    Args:
        name: Nom du logger, typiquement ``__name__`` du module appelant.
        level: Niveau de verbosité minimal (par défaut ``logging.INFO``).

    Returns:
        Un logger prêt à l'emploi, écrivant sur la sortie standard avec un
        format horodaté incluant le nom du module et le niveau.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Ne pas ré-attacher de handler si le logger en possède déjà un : évite les
    # lignes dupliquées lors des imports multiples (notamment sous Airflow).
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s")
        )
        logger.addHandler(handler)

    return logger
