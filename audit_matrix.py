"""Matrice d’audit (use cases + critères) pour ProspectionPro.

But:
- Centraliser les flux critiques UI -> API -> DB.
- Servir de base aux smoke tests automatisés.

Ce fichier est volontairement *exécutable* (python audit_matrix.py) afin d’imprimer
un résumé lisible sans dépendances.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Literal, Optional

HttpMethod = Literal["GET", "POST", "PUT", "DELETE"]


@dataclass(frozen=True)
class ApiCase:
    id: str
    category: str
    title: str
    method: HttpMethod
    path: str
    description: str
    query: Optional[Dict[str, Any]] = None
    json: Optional[Dict[str, Any]] = None
    expect_status: int = 200
    # Clés minimales attendues dans le JSON de réponse (si JSON)
    expect_json_keys: Optional[List[str]] = None
    # Pour les endpoints qui peuvent légitimement retourner une liste vide
    allow_empty: bool = True


# --- Routes UI principales (React) et leur intention métier ---
UI_ROUTES: List[Dict[str, str]] = [
    {
        "route": "/dashboard",
        "title": "Dashboard",
        "intent": "KPIs + activité temps réel (stats + websocket)",
    },
    {
        "route": "/scraping",
        "title": "Scraping",
        "intent": "Extraction (Scanner/SITG/RF/Search.ch/Local.ch/VD) + export + ajout prospects",
    },
    {
        "route": "/speed-entry",
        "title": "Speed Entry",
        "intent": "Saisie manuelle + enrichissement auto (téléphone/email) sur prospects incomplets",
    },
    {
        "route": "/prospects",
        "title": "Prospects",
        "intent": "Pipeline CRM (list/search, drag&drop statut, interactions, rappels, import/export)",
    },
    {
        "route": "/emails",
        "title": "Emails",
        "intent": "Pool emails (CRUD, quotas, import/export)",
    },
    {
        "route": "/proxies",
        "title": "Proxies",
        "intent": "Pool proxies (CRUD, test, import/export)",
    },
    {
        "route": "/bots",
        "title": "Bots",
        "intent": "Bots (CRUD, start/stop/pause/resume, suivi temps réel)",
    },
    {
        "route": "/campaigns",
        "title": "Campagnes",
        "intent": "Campagnes (CRUD, start/pause/resume/stop, stats)",
    },
]


# --- Modèles DB (tables) et rôle fonctionnel ---
DB_MODELS: List[Dict[str, str]] = [
    {"table": "prospects", "purpose": "CRM prospects + scoring + statut + tags + rappel"},
    {"table": "interaction_logs", "purpose": "Historique des interactions (appel/email/rdv/note)"},
    {"table": "email_accounts", "purpose": "Pool d’emails (quotas, activation, erreurs)"},
    {"table": "proxies", "purpose": "Pool de proxies (validité, latence, activation)"},
    {"table": "bots", "purpose": "Bots (statut, compteurs, config)"},
    {"table": "campaigns", "purpose": "Campagnes (ciblage, statut, progression)"},
    {"table": "activities", "purpose": "Journal d’activité pour dashboard/notifications"},
]

# --- Événements Socket.IO utilisés par le frontend ---
SOCKET_EVENTS: List[Dict[str, str]] = [
    {"event": "connected", "purpose": "Handshake côté client"},
    {"event": "activity", "purpose": "Notifications générales (type/message)"},
    {"event": "scraping_progress", "purpose": "Progress scraping (source/progress/total/message)"},
    {"event": "bot_status", "purpose": "Statut bot (running/paused/idle/error)"},
    {"event": "bot_log", "purpose": "Logs bot en temps réel"},
    {"event": "prospect_found", "purpose": "Prospect trouvé par un bot"},
    {"event": "campaign_progress", "purpose": "Progress campagne"},
    {"event": "stats_update", "purpose": "Push de stats (optionnel)"},
]


# --- Cas API (base de la campagne de tests) ---
# Remarque: la plupart de ces cas nécessitent une base de données fonctionnelle.
API_CASES: List[ApiCase] = [
    # Infra
    ApiCase(
        id="health",
        category="infra",
        title="Healthcheck",
        method="GET",
        path="/api/health",
        description="Le backend répond et expose la version.",
        expect_status=200,
        expect_json_keys=["status", "version"],
        allow_empty=True,
    ),
    # Dashboard
    ApiCase(
        id="stats_dashboard",
        category="dashboard",
        title="Stats dashboard",
        method="GET",
        path="/api/stats/dashboard",
        description="KPIs (prospects, emails, bots, campagnes, score moyen).",
        expect_status=200,
        expect_json_keys=["prospects", "emails", "bots", "campaigns", "score_moyen"],
    ),
    ApiCase(
        id="stats_prospects_by_day",
        category="dashboard",
        title="Prospects by day",
        method="GET",
        path="/api/stats/prospects/by-day",
        description="Courbe prospects/jour.",
        query={"days": 7},
        expect_status=200,
        expect_json_keys=["labels", "values"],
    ),
    # Scraping - référentiels
    ApiCase(
        id="scraping_communes",
        category="scraping",
        title="Liste communes",
        method="GET",
        path="/api/scraping/communes",
        description="Retourne les communes GE/VD et les communes scanner.",
        expect_status=200,
        expect_json_keys=["geneve", "vaud", "scanner_ge", "scanner_vd"],
    ),
    ApiCase(
        id="scraping_rues_geneve",
        category="scraping",
        title="Rues pour une commune (scanner)",
        method="GET",
        path="/api/scraping/rues",
        description="Retourne la liste des rues pour une commune (doit être non vide sur des communes supportées).",
        query={"commune": "Genève"},
        expect_status=200,
        expect_json_keys=["rues", "commune"],
        allow_empty=False,
    ),
    # Scraping - actions
    ApiCase(
        id="scraping_searchch_person",
        category="scraping",
        title="Search.ch (person)",
        method="POST",
        path="/api/scraping/searchch",
        description="Recherche annuaire Search.ch (particuliers).",
        json={"source": "searchch", "commune": "Genève", "query": "Muller", "limit": 5, "type_recherche": "person"},
        expect_status=200,
        expect_json_keys=["status", "count", "results"],
        allow_empty=False,
    ),
    ApiCase(
        id="scraping_searchch_business",
        category="scraping",
        title="Search.ch (business)",
        method="POST",
        path="/api/scraping/searchch",
        description="Recherche annuaire Search.ch (entreprises).",
        json={"source": "searchch", "commune": "Genève", "query": "Restaurant", "limit": 5, "type_recherche": "business"},
        expect_status=200,
        expect_json_keys=["status", "count", "results"],
    ),
    ApiCase(
        id="scraping_localch_person",
        category="scraping",
        title="Local.ch (person)",
        method="POST",
        path="/api/scraping/localch",
        description="Recherche annuaire Local.ch (particuliers).",
        json={"source": "localch", "commune": "Genève", "query": "Muller", "limit": 5, "type_recherche": "person"},
        expect_status=200,
        expect_json_keys=["status", "count", "results"],
        allow_empty=False,
    ),
    ApiCase(
        id="scraping_scanner_all",
        category="scraping",
        title="Scanner quartier (all)",
        method="POST",
        path="/api/scraping/scanner",
        description="Scanner une commune (rue=all) — dépend du dataset streets.json.",
        json={"source": "scanner", "commune": "Genève", "query": "all", "limit": 10, "type_recherche": "person"},
        expect_status=200,
        expect_json_keys=["status", "count", "results"],
        allow_empty=False,
    ),
    ApiCase(
        id="scraping_sitg",
        category="scraping",
        title="SITG (GE)",
        method="POST",
        path="/api/scraping/sitg",
        description="Cadastre Genève (SITG) + fallback liens RF si l’API est indispo.",
        json={"source": "sitg", "commune": "Genève", "query": "", "limit": 10, "type_recherche": "all"},
        expect_status=200,
        expect_json_keys=["status", "count", "results"],
        allow_empty=False,
    ),
    ApiCase(
        id="scraping_rf_links",
        category="scraping",
        title="Génération liens RF (GE)",
        method="POST",
        path="/api/scraping/rf-links",
        description="Génère des liens Registre Foncier (sans scraping tiers).",
        json={"source": "rf", "commune": "Genève", "query": "", "limit": 10, "type_recherche": "all"},
        expect_status=200,
        expect_json_keys=["status", "count", "results"],
        allow_empty=False,
    ),
    ApiCase(
        id="scraping_vaud",
        category="scraping",
        title="Cadastre Vaud (VD)",
        method="POST",
        path="/api/scraping/vaud",
        description="Cadastre VD (API + fallback).",
        json={"source": "vaud", "commune": "Lausanne", "query": "", "limit": 10, "type_recherche": "all"},
        expect_status=200,
        expect_json_keys=["status", "count", "results"],
        allow_empty=False,
    ),
    # Prospects (CRM)
    ApiCase(
        id="prospects_list",
        category="prospects",
        title="Lister prospects",
        method="GET",
        path="/api/prospects/",
        description="Liste prospects (filtre search optionnel).",
        query={"limit": 50},
        expect_status=200,
        expect_json_keys=None,  # liste JSON
    ),
    ApiCase(
        id="prospects_pipeline",
        category="prospects",
        title="Pipeline prospects",
        method="GET",
        path="/api/prospects/pipeline",
        description="Compte par statut.",
        expect_status=200,
        expect_json_keys=None,  # dict JSON
    ),
    ApiCase(
        id="prospects_rappels_today",
        category="prospects",
        title="Rappels du jour",
        method="GET",
        path="/api/prospects/rappels/today",
        description="Retourne les prospects dont le rappel est prévu aujourd’hui.",
        expect_status=200,
        expect_json_keys=None,
    ),
    ApiCase(
        id="prospects_create",
        category="prospects",
        title="Créer un prospect",
        method="POST",
        path="/api/prospects/",
        description="Création prospect via API (utilisé par scripts/tests).",
        json={"nom": "Test", "prenom": "Prospect", "ville": "Genève", "source": "SmokeTest"},
        expect_status=200,
        expect_json_keys=["id", "nom", "created_at"],
        allow_empty=False,
    ),
    # Emails
    ApiCase(
        id="emails_list",
        category="emails",
        title="Lister emails",
        method="GET",
        path="/api/emails/",
        description="Liste des comptes email.",
        expect_status=200,
        expect_json_keys=None,
    ),
    ApiCase(
        id="emails_stats",
        category="emails",
        title="Stats emails",
        method="GET",
        path="/api/emails/stats",
        description="KPIs quotas / envois.",
        expect_status=200,
        expect_json_keys=["total_accounts", "active_accounts", "total_sent_today", "total_quota", "available_quota"],
    ),
    # Proxies
    ApiCase(
        id="proxies_list",
        category="proxies",
        title="Lister proxies",
        method="GET",
        path="/api/proxies/",
        description="Liste des proxies.",
        expect_status=200,
        expect_json_keys=None,
    ),
    ApiCase(
        id="proxies_stats",
        category="proxies",
        title="Stats proxies",
        method="GET",
        path="/api/proxies/stats",
        description="KPIs proxies.",
        expect_status=200,
        expect_json_keys=["total", "active", "valid", "swiss", "avg_latency"],
    ),
    # Bots
    ApiCase(
        id="bots_list",
        category="bots",
        title="Lister bots",
        method="GET",
        path="/api/bots/",
        description="Liste des bots.",
        expect_status=200,
        expect_json_keys=None,
    ),
    ApiCase(
        id="bots_stats",
        category="bots",
        title="Stats bots",
        method="GET",
        path="/api/bots/stats",
        description="KPIs bots.",
        expect_status=200,
        expect_json_keys=["total", "running", "idle", "error", "total_requests", "total_success", "total_errors"],
    ),
    # Campaigns
    ApiCase(
        id="campaigns_list",
        category="campaigns",
        title="Lister campagnes",
        method="GET",
        path="/api/campaigns/",
        description="Liste des campagnes.",
        expect_status=200,
        expect_json_keys=None,
    ),
    ApiCase(
        id="campaigns_stats",
        category="campaigns",
        title="Stats campagnes",
        method="GET",
        path="/api/campaigns/stats",
        description="KPIs campagnes.",
        expect_status=200,
        expect_json_keys=["total", "running", "completed", "total_sent", "total_responses", "avg_response_rate"],
    ),
    # Exports (binary)
    ApiCase(
        id="export_prospects_xlsx",
        category="export",
        title="Export prospects (xlsx)",
        method="GET",
        path="/api/export/prospects",
        description="Export Excel (doit retourner un binaire).",
        query={"format": "xlsx"},
        expect_status=200,
        expect_json_keys=None,
        allow_empty=True,
    ),
]


def _print_section(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def print_matrix() -> None:
    _print_section("UI ROUTES")
    for r in UI_ROUTES:
        print(f"- {r['route']}: {r['title']} — {r['intent']}")

    _print_section("DB MODELS")
    for m in DB_MODELS:
        print(f"- {m['table']}: {m['purpose']}")

    _print_section("SOCKET EVENTS")
    for e in SOCKET_EVENTS:
        print(f"- {e['event']}: {e['purpose']}")

    _print_section("API CASES")
    for c in API_CASES:
        q = f" query={c.query}" if c.query else ""
        print(f"- [{c.category}] {c.id}: {c.method} {c.path}{q} — {c.title}")


def export_as_dict() -> Dict[str, Any]:
    return {
        "ui_routes": UI_ROUTES,
        "db_models": DB_MODELS,
        "socket_events": SOCKET_EVENTS,
        "api_cases": [asdict(c) for c in API_CASES],
    }


if __name__ == "__main__":
    print_matrix()
