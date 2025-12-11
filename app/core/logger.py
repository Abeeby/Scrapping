# =============================================================================
# LOGGER - Gestion centralisee des logs
# =============================================================================
# Systeme de logging robuste avec support UTF-8 pour Windows 10/11
# =============================================================================

import sys
import os
import io
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

# Creer le dossier logs s'il n'existe pas
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Format des logs
FORMATTER = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def get_console_handler():
    """
    Handler pour la console avec gestion robuste de l'encodage Windows.
    
    Sur Windows, sys.stdout utilise souvent cp1252 qui ne supporte pas 
    les caracteres Unicode etendus (emojis, etc.). On cree un wrapper 
    UTF-8 avec errors='replace' pour eviter les UnicodeEncodeError.
    """
    try:
        # Creer un stream UTF-8 robuste
        wrapped_stream = io.TextIOWrapper(
            sys.stdout.buffer,
            encoding='utf-8',
            errors='replace',  # Remplace les caracteres non encodables par '?'
            line_buffering=True
        )
        console_handler = logging.StreamHandler(wrapped_stream)
    except (AttributeError, TypeError):
        # Fallback si sys.stdout n'a pas de buffer (certains environnements)
        console_handler = logging.StreamHandler(sys.stdout)
    
    console_handler.setFormatter(FORMATTER)
    return console_handler


def get_file_handler(filename):
    """
    Handler pour fichier avec rotation.
    
    Les fichiers sont toujours encodes en UTF-8 pour supporter
    tous les caracteres sans probleme.
    """
    file_handler = RotatingFileHandler(
        LOG_DIR / filename,
        maxBytes=10*1024*1024,  # 10MB par fichier
        backupCount=5,         # Garder 5 fichiers de backup
        encoding='utf-8'
    )
    file_handler.setFormatter(FORMATTER)
    return file_handler


def get_logger(logger_name):
    """
    Recupere ou cree un logger avec handlers console et fichier.
    
    Chaque logger a:
    - Un handler console (tous les logs)
    - Un handler fichier specifique (tous les logs)
    - Un handler fichier errors.log (erreurs uniquement)
    """
    logger = logging.getLogger(logger_name)
    
    # Eviter les doublons de handlers
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Console (tous les logs)
        logger.addHandler(get_console_handler())
        
        # Fichier specifique (tous les logs)
        logger.addHandler(get_file_handler(f"{logger_name}.log"))
        
        # Fichier erreurs (erreurs uniquement)
        error_handler = get_file_handler("errors.log")
        error_handler.setLevel(logging.ERROR)
        logger.addHandler(error_handler)
    
    return logger


# =============================================================================
# LOGGERS PREDEFINIS
# =============================================================================

logger = get_logger("app")
scraping_logger = get_logger("scraping")
bots_logger = get_logger("bots")
db_logger = get_logger("database")
