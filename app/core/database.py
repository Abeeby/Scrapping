# =============================================================================
# DATABASE - SQLAlchemy Async avec support PostgreSQL et SQLite
# =============================================================================
# En local: SQLite (fichier)
# En production: PostgreSQL (Railway, Heroku, etc.)
# =============================================================================

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Text, JSON, text
from datetime import datetime
import os

# =============================================================================
# CONFIGURATION - Lecture depuis variables d'environnement
# =============================================================================

def get_database_url():
    """
    Retourne l'URL de la base de donnees.
    - En production: utilise DATABASE_URL (PostgreSQL)
    - En local: utilise SQLite
    """
    database_url = os.environ.get("DATABASE_URL")
    
    if database_url:
        # Railway/Heroku fournissent postgres:// mais SQLAlchemy 2.0 veut postgresql://
        if database_url.startswith("postgres://"):
            database_url = database_url.replace("postgres://", "postgresql+asyncpg://", 1)
        elif database_url.startswith("postgresql://"):
            database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        return database_url
    
    # Fallback local: SQLite
    return "sqlite+aiosqlite:///./data/prospection.db"


DATABASE_URL = get_database_url()
IS_POSTGRES = "postgresql" in DATABASE_URL

# Configuration du moteur selon le type de base
if IS_POSTGRES:
    engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True  # Verifie les connexions avant utilisation
    )
else:
    engine = create_async_engine(DATABASE_URL, echo=False)

AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

# =============================================================================
# MODELS
# =============================================================================

class Prospect(Base):
    __tablename__ = "prospects"
    
    id = Column(String, primary_key=True)
    nom = Column(String, nullable=False)
    prenom = Column(String)
    telephone = Column(String)
    email = Column(String)
    adresse = Column(String)
    code_postal = Column(String)
    ville = Column(String)
    canton = Column(String, default="GE")
    type_bien = Column(String)
    surface = Column(Float)
    prix = Column(Float)
    score = Column(Integer, default=0)
    statut = Column(String, default="nouveau")
    source = Column(String)
    notes = Column(Text)
    tags = Column(JSON, default=[])
    rappel_date = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class EmailAccount(Base):
    __tablename__ = "email_accounts"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String, unique=True, nullable=False)
    password = Column(String, nullable=False)
    imap_server = Column(String)
    smtp_server = Column(String)
    quota_daily = Column(Integer, default=50)
    sent_today = Column(Integer, default=0)
    is_active = Column(Boolean, default=True)
    last_used = Column(DateTime)
    error_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)


class Bot(Base):
    __tablename__ = "bots"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    type = Column(String)  # comparis, immoscout, homegate
    status = Column(String, default="idle")  # idle, running, paused, error
    proxy_id = Column(Integer)
    email_id = Column(Integer)
    requests_count = Column(Integer, default=0)
    success_count = Column(Integer, default=0)
    error_count = Column(Integer, default=0)
    last_run = Column(DateTime)
    config = Column(JSON, default={})
    created_at = Column(DateTime, default=datetime.utcnow)


class Proxy(Base):
    __tablename__ = "proxies"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    host = Column(String, nullable=False)
    port = Column(Integer, nullable=False)
    username = Column(String)
    password = Column(String)
    protocol = Column(String, default="http")  # http, https, socks5
    country = Column(String, default="CH")
    is_active = Column(Boolean, default=True)
    is_valid = Column(Boolean, default=True)
    latency_ms = Column(Integer)
    success_rate = Column(Float, default=100.0)
    last_checked = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)


class Campaign(Base):
    __tablename__ = "campaigns"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    type = Column(String)  # brochure, contact
    target_portal = Column(String)  # comparis, immoscout
    target_city = Column(String)
    target_radius = Column(Integer, default=10)
    status = Column(String, default="draft")  # draft, running, paused, completed
    total_targets = Column(Integer, default=0)
    sent_count = Column(Integer, default=0)
    response_count = Column(Integer, default=0)
    error_count = Column(Integer, default=0)
    config = Column(JSON, default={})
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)


class Activity(Base):
    __tablename__ = "activities"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    type = Column(String)  # bot_start, email_sent, prospect_found, error
    message = Column(String)
    details = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)


class InteractionLog(Base):
    """Historique des interactions avec les prospects (appels, emails, RDV)"""
    __tablename__ = "interaction_logs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    prospect_id = Column(String, nullable=False, index=True)
    type = Column(String, nullable=False)  # appel, email, rdv, note
    notes = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)


# =============================================================================
# INITIALIZATION
# =============================================================================

async def init_db():
    """Cree les tables si elles n'existent pas"""
    # Creer le dossier data uniquement pour SQLite local
    if not IS_POSTGRES:
        os.makedirs("data", exist_ok=True)
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        
        # Migration pour les colonnes manquantes (SQLite uniquement)
        if not IS_POSTGRES:
            await _ensure_column_sqlite(conn, "prospects", "rappel_date", "rappel_date TIMESTAMP")


async def _ensure_column_sqlite(conn, table_name: str, column_name: str, column_definition: str):
    """
    Ajoute dynamiquement une colonne si elle n'existe pas (SQLite uniquement).
    PostgreSQL gere cela differemment via les migrations.
    """
    try:
        pragma = await conn.execute(text(f"PRAGMA table_info({table_name})"))
        columns = [row[1] for row in pragma.fetchall()]
        if column_name not in columns:
            await conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_definition}"))
    except Exception:
        pass  # Ignorer les erreurs sur PostgreSQL


async def get_db():
    """Dependency pour obtenir une session DB"""
    async with AsyncSessionLocal() as session:
        yield session
