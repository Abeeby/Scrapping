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

# Compat: certains modules attendent `async_session` (factory)
async_session = AsyncSessionLocal

# =============================================================================
# MODELS
# =============================================================================

class Prospect(Base):
    __tablename__ = "prospects"
    
    id = Column(String, primary_key=True)
    nom = Column(String, nullable=False)
    prenom = Column(String)
    telephone = Column(String)
    telephone_norm = Column(String)
    email = Column(String)
    email_norm = Column(String)
    adresse = Column(String)
    adresse_norm = Column(String)
    code_postal = Column(String)
    ville = Column(String)
    canton = Column(String, default="GE")
    lien_rf = Column(String)
    type_bien = Column(String)
    surface = Column(Float)
    prix = Column(Float)
    score = Column(Integer, default=0)
    # Score de "qualité de donnée" (complétude/joignabilité/dédup)
    quality_score = Column(Integer, default=0)
    # Drapeaux/raisons de qualité (dict JSON)
    quality_flags = Column(JSON, default=dict)
    enrichment_status = Column(String, default="pending")  # pending, ok, error, rate_limited
    last_enriched_at = Column(DateTime, nullable=True)
    last_enrichment_error = Column(Text)
    # Déduplication
    is_duplicate = Column(Boolean, default=False)
    duplicate_group_id = Column(String)
    merged_into_id = Column(String)
    statut = Column(String, default="nouveau")
    source = Column(String)
    notes = Column(Text)
    tags = Column(JSON, default=list)
    rappel_date = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ProspectDuplicateCandidate(Base):
    """Candidats doublons (suggestions) pour dédup hybride."""
    __tablename__ = "prospect_duplicate_candidates"

    id = Column(Integer, primary_key=True, autoincrement=True)
    prospect_id = Column(String, nullable=False, index=True)
    candidate_id = Column(String, nullable=False, index=True)
    reason = Column(String)
    confidence = Column(Float, default=0.0)
    status = Column(String, default="pending")  # pending, ignored, merged
    created_at = Column(DateTime, default=datetime.utcnow)


class ProspectMergeLog(Base):
    """Audit des fusions de prospects."""
    __tablename__ = "prospect_merge_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(String, nullable=False, index=True)
    target_id = Column(String, nullable=False, index=True)
    reason = Column(String)
    merged_fields = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)


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


class BrochureRequest(Base):
    """Demandes de brochure envoyées via les portails immobiliers."""
    __tablename__ = "brochure_requests"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    prospect_id = Column(String, index=True)  # Lien optionnel vers prospect
    email_account_id = Column(Integer, index=True)  # Email utilisé pour l'envoi
    portal = Column(String, nullable=False)  # comparis, immoscout24, homegate
    listing_url = Column(String, nullable=False)  # URL de l'annonce
    listing_title = Column(String)  # Titre de l'annonce
    listing_address = Column(String)  # Adresse du bien
    requester_name = Column(String)  # Nom utilisé dans la demande
    requester_email = Column(String)  # Email utilisé
    requester_phone = Column(String)  # Téléphone (optionnel)
    requester_message = Column(Text)  # Message personnalisé
    status = Column(String, default="pending")  # pending, sent, delivered, error, responded
    sent_at = Column(DateTime)
    response_received = Column(Boolean, default=False)
    response_at = Column(DateTime)
    error_message = Column(Text)
    retry_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class BrochureSchedule(Base):
    """Planification d'envois automatiques de demandes de brochure."""
    __tablename__ = "brochure_schedules"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    cron_expression = Column(String, default="0 9 * * *")  # Par défaut 9h tous les jours
    portal_filter = Column(JSON, default=list)  # ["comparis", "immoscout24"]
    canton_filter = Column(JSON, default=list)  # ["GE", "VD"]
    max_requests_per_run = Column(Integer, default=10)  # Limite par exécution
    delay_between_requests = Column(Integer, default=30)  # Secondes entre chaque demande
    is_active = Column(Boolean, default=True)
    last_run = Column(DateTime)
    last_run_count = Column(Integer, default=0)
    total_sent = Column(Integer, default=0)
    total_responses = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ScrapedListing(Base):
    """Annonces immobilières scrapées en attente de traitement brochure."""
    __tablename__ = "scraped_listings"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    portal = Column(String, nullable=False, index=True)  # comparis, immoscout24, homegate
    listing_id = Column(String, index=True)  # ID unique sur le portail
    url = Column(String, nullable=False)
    title = Column(String)
    address = Column(String)
    city = Column(String, index=True)
    canton = Column(String, index=True)
    price = Column(Float)
    rooms = Column(Float)
    surface = Column(Float)
    property_type = Column(String)  # apartment, house, etc.
    transaction_type = Column(String)  # rent, buy
    agency_name = Column(String)
    agency_phone = Column(String)
    agency_email = Column(String)
    brochure_requested = Column(Boolean, default=False)
    brochure_request_id = Column(Integer)
    scraped_at = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)


class MassScrapingJob(Base):
    """Jobs de scraping massif (rues, quartiers)."""
    __tablename__ = "mass_scraping_jobs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    canton = Column(String, nullable=False)  # GE, VD
    commune = Column(String)  # Commune cible ou NULL pour toutes
    source = Column(String, default="searchch")  # searchch, localch, rf
    status = Column(String, default="pending")  # pending, running, paused, completed, error
    total_streets = Column(Integer, default=0)
    processed_streets = Column(Integer, default=0)
    total_found = Column(Integer, default=0)
    current_street = Column(String)
    error_message = Column(Text)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
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
        
        # Mini-migrations idempotentes (sans Alembic)
        # Objectif: éviter les "0 résultat silencieux" et permettre les features qualité
        # même sur une base déjà existante.
        await _ensure_column(conn, "prospects", "rappel_date", "rappel_date TIMESTAMP")

        # Champs qualité / dédup (prospects)
        await _ensure_column(conn, "prospects", "telephone_norm", "telephone_norm VARCHAR")
        await _ensure_column(conn, "prospects", "email_norm", "email_norm VARCHAR")
        await _ensure_column(conn, "prospects", "adresse_norm", "adresse_norm VARCHAR")
        await _ensure_column(conn, "prospects", "lien_rf", "lien_rf VARCHAR")
        await _ensure_column(conn, "prospects", "quality_score", "quality_score INTEGER DEFAULT 0")
        await _ensure_column(conn, "prospects", "quality_flags", "quality_flags JSON")
        await _ensure_column(conn, "prospects", "enrichment_status", "enrichment_status VARCHAR DEFAULT 'pending'")
        await _ensure_column(conn, "prospects", "last_enriched_at", "last_enriched_at TIMESTAMP")
        await _ensure_column(conn, "prospects", "last_enrichment_error", "last_enrichment_error TEXT")
        if IS_POSTGRES:
            await _ensure_column(conn, "prospects", "is_duplicate", "is_duplicate BOOLEAN DEFAULT false")
        else:
            await _ensure_column(conn, "prospects", "is_duplicate", "is_duplicate BOOLEAN DEFAULT 0")
        await _ensure_column(conn, "prospects", "duplicate_group_id", "duplicate_group_id VARCHAR")
        await _ensure_column(conn, "prospects", "merged_into_id", "merged_into_id VARCHAR")

        # Index utiles (idempotents)
        await _ensure_index(conn, "idx_prospects_telephone_norm", "CREATE INDEX IF NOT EXISTS idx_prospects_telephone_norm ON prospects (telephone_norm)")
        await _ensure_index(conn, "idx_prospects_email_norm", "CREATE INDEX IF NOT EXISTS idx_prospects_email_norm ON prospects (email_norm)")
        await _ensure_index(conn, "idx_prospects_lien_rf", "CREATE INDEX IF NOT EXISTS idx_prospects_lien_rf ON prospects (lien_rf)")
        await _ensure_index(conn, "idx_prospects_duplicate_group_id", "CREATE INDEX IF NOT EXISTS idx_prospects_duplicate_group_id ON prospects (duplicate_group_id)")
        await _ensure_index(conn, "idx_prospects_merged_into_id", "CREATE INDEX IF NOT EXISTS idx_prospects_merged_into_id ON prospects (merged_into_id)")
        await _ensure_index(conn, "idx_prospects_statut", "CREATE INDEX IF NOT EXISTS idx_prospects_statut ON prospects (statut)")
        await _ensure_index(conn, "idx_prospects_ville", "CREATE INDEX IF NOT EXISTS idx_prospects_ville ON prospects (ville)")
        await _ensure_index(conn, "idx_prospects_quality_score", "CREATE INDEX IF NOT EXISTS idx_prospects_quality_score ON prospects (quality_score)")


async def _ensure_column(conn, table_name: str, column_name: str, column_definition: str):
    """Ajoute dynamiquement une colonne si elle n'existe pas (SQLite + PostgreSQL)."""
    if IS_POSTGRES:
        return await _ensure_column_postgres(conn, table_name, column_name, column_definition)
    return await _ensure_column_sqlite(conn, table_name, column_name, column_definition)


async def _ensure_column_sqlite(conn, table_name: str, column_name: str, column_definition: str):
    """
    Ajoute dynamiquement une colonne si elle n'existe pas (SQLite uniquement).
    """
    try:
        pragma = await conn.execute(text(f"PRAGMA table_info({table_name})"))
        columns = [row[1] for row in pragma.fetchall()]
        if column_name not in columns:
            await conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_definition}"))
    except Exception:
        pass


async def _ensure_column_postgres(conn, table_name: str, column_name: str, column_definition: str):
    """Ajoute dynamiquement une colonne si elle n'existe pas (PostgreSQL)."""
    try:
        exists = await conn.execute(
            text(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = :table_name
                  AND column_name = :column_name
                """
            ),
            {"table_name": table_name, "column_name": column_name},
        )
        if exists.first() is None:
            await conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_definition}"))
    except Exception:
        # Tolérance: on ne veut pas bloquer le boot.
        pass


async def _ensure_index(conn, index_name: str, create_sql: str):
    """Crée un index si absent (idempotent)."""
    try:
        await conn.execute(text(create_sql))
    except Exception:
        # Tolérance: index peut déjà exister ou ne pas être supporté
        pass


async def get_db():
    """Dependency pour obtenir une session DB"""
    async with AsyncSessionLocal() as session:
        yield session
