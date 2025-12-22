# =============================================================================
# SCANNER DE QUARTIER - Scraping par adresse
# =============================================================================

import asyncio
import uuid
import json
import os
import time
import unicodedata
from typing import List, Dict, Optional
from app.scrapers.searchch import SearchChScraper, SearchChScraperError
from app.core.websocket import sio, emit_activity
from app.core.logger import scraping_logger

# Chargement de la base de données des rues
import sys

def get_data_path():
    """Determine le chemin vers le dossier data"""
    # Option 1: PyInstaller frozen
    if getattr(sys, 'frozen', False):
        base = os.path.dirname(sys.executable)
        data_path = os.path.join(base, "data", "streets.json")
        if os.path.exists(data_path):
            return data_path
        # Fallback: app/data
        data_path = os.path.join(base, "app", "data", "streets.json")
        if os.path.exists(data_path):
            return data_path
    
    # Option 2: Execution normale (Railway, local dev)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_dir, "data", "streets.json")

DATA_FILE = get_data_path()
print(f"[Scanner] Fichier streets.json: {DATA_FILE}")

def load_streets_data():
    """Charge les données des rues depuis le fichier JSON"""
    try:
        if not os.path.exists(DATA_FILE):
            print(f"[Scanner] Fichier non trouve: {DATA_FILE}")
            return {"GE": {}, "VD": {}}
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            print(f"[Scanner] Donnees chargees: {len(data.get('GE', {}))} communes GE, {len(data.get('VD', {}))} communes VD")
            return data
    except Exception as e:
        print(f"[Scanner] Erreur chargement streets.json: {e}")
        return {"GE": {}, "VD": {}}

STREETS_DB = load_streets_data()

# region agent log
_AGENT_DEBUG_LOG_PATH = r"c:\Users\admin10\Desktop\Scrapping data\.cursor\debug.log"

def _agent_dbg(hypothesisId: str, location: str, message: str, data: dict | None = None, run_id: str = "pre-fix"):
    try:
        payload = {
            "sessionId": "debug-session",
            "runId": run_id,
            "hypothesisId": hypothesisId,
            "location": location,
            "message": message,
            "data": data or {},
            "timestamp": int(time.time() * 1000),
        }
        with open(_AGENT_DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass
# endregion

# =============================================================================
# NORMALISATION - Tolérance aux problèmes d'encodage (GenÃ¨ve, etc.)
# =============================================================================

def _repair_mojibake(text: str) -> str:
    """Répare les cas fréquents de double-encodage (UTF-8 lu en latin-1)."""
    if not text:
        return text
    try:
        repaired = text.encode("latin-1").decode("utf-8")
        return repaired if repaired else text
    except Exception:
        return text


def _normalize_commune_name(text: str) -> str:
    text = _repair_mojibake(text or "")
    text = text.replace("\ufffd", "")  # caractère de remplacement
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.casefold().strip()


# Fusion des données pour l'accès facile
ALL_RUES = {}
COMMUNES_GE = []
COMMUNES_VD = []

if "GE" in STREETS_DB:
    ALL_RUES.update(STREETS_DB["GE"])
    COMMUNES_GE = list(STREETS_DB["GE"].keys())

if "VD" in STREETS_DB:
    ALL_RUES.update(STREETS_DB["VD"])
    COMMUNES_VD = list(STREETS_DB["VD"].keys())

# Mapping normalisé -> clé canonique
_COMMUNE_KEY_BY_NORM = {_normalize_commune_name(k): k for k in ALL_RUES.keys()}

def _resolve_commune_key(commune: str) -> str:
    return _COMMUNE_KEY_BY_NORM.get(_normalize_commune_name(commune), commune)

# Liste de numeros a tester par defaut (1 a 100)
ALL_NUMEROS = [str(i) for i in range(1, 101)]

def get_canton(commune: str) -> str:
    """Determine le canton d'une commune"""
    if commune in COMMUNES_VD:
        return "VD"
    return "GE"

async def scrape_neighborhood(
    commune: str, 
    rue: str, 
    limit: int = 50,
    canton: Optional[str] = None,
    type_recherche: str = "person"
) -> List[Dict]:
    """
    Scanne une rue numero par numero pour trouver les residents.
    Supporte Geneve (GE) et Vaud (VD).
    type_recherche: "person" (prives), "business" (entreprises), "all" (tous)
    """
    results = []
    seen = set()  # Deduplication locale
    success_calls = 0
    error_calls = 0
    last_error: SearchChScraperError | None = None
    
    # Normaliser/repair entrée utilisateur (accents + mojibake)
    commune = _resolve_commune_key(_repair_mojibake(commune))
    rue = _repair_mojibake(rue)

    # Determiner le canton si non specifie
    if canton is None:
        canton = get_canton(commune)
    
    type_label = "prives" if type_recherche == "person" else "entreprises" if type_recherche == "business" else "tous"
    print(f"[Scanner] Demarrage scanner ({type_label}): {rue}, {commune} ({canton})")
    
    async with SearchChScraper() as scraper:
        # Generer les adresses a tester
        adresses_a_tester = []
        
        # Si la rue est "all", on prend toutes les rues de la commune
        if rue == "all":
            rues = ALL_RUES.get(commune, [])
            # On prend max 5 numeros par rue pour commencer (echantillonnage)
            for r in rues:
                for n in range(1, 6):
                    adresses_a_tester.append(f"{r} {n}")
        else:
            # Rue specifique : on teste tous les numeros
            for n in ALL_NUMEROS[:100]:  # Max 100 numeros
                adresses_a_tester.append(f"{rue} {n}")
        
        # Limiter le nombre de requetes
        adresses_a_tester = adresses_a_tester[:limit]
        total = len(adresses_a_tester)
        
        print(f"[Scanner] {total} adresses a tester")
        # region agent log
        _agent_dbg(
            hypothesisId="H1",
            location="app/scrapers/scanner.py:scanner_setup",
            message="scanner prepared",
            data={
                "commune": commune,
                "canton": canton,
                "rue_mode": "all" if rue == "all" else "single",
                "limit": limit,
                "total_addresses": total,
                "type_recherche": type_recherche,
                "sleep_s": 0.25,
            },
        )
        # endregion
        
        if total == 0:
            print(f"[Scanner] Aucune rue trouvee pour {commune}")
            return results
        
        for i, adresse_base in enumerate(adresses_a_tester):
            adresse_complete = f"{adresse_base}, {commune}"
            
            # Utiliser le champ 'wo' pour l'adresse et laisser 'was' vide pour tout trouver
            # C'est la technique cle pour le reverse search
            try:
                scan_results = await scraper.search(
                    query="", 
                    ville=adresse_complete, 
                    # Important: 5 est souvent trop bas (les entrées avec téléphone
                    # ne sont pas forcément dans les 5 premiers résultats).
                    limit=20,
                    type_recherche=type_recherche  # Passer le filtre prive/entreprise
                )
                success_calls += 1
            except SearchChScraperError as e:
                error_calls += 1
                last_error = e
                scraping_logger.warning(f"[Scanner] Search.ch erreur pour {adresse_complete}: {e}")
                continue
            except Exception as e:
                error_calls += 1
                last_error = SearchChScraperError(str(e))
                scraping_logger.error(f"[Scanner] Erreur pour {adresse_complete}: {e}", exc_info=True)
                continue
            
            for res in scan_results:
                # Le scraper searchch.py a deja un filtre anti-entreprise
                # On ajoute une couche supplementaire de validation
                # Ne pas filtrer sur le téléphone: beaucoup d'entrées privées n'ont
                # pas de numéro public. On garde la fiche et l'utilisateur peut
                # enrichir ensuite.
                if res and res.get('nom'):
                    # Ajouter l'info "Source: Scanner"
                    res['source'] = f"Scanner {canton}"
                    res['notes'] = f"Trouve a l'adresse: {adresse_complete}"
                    res['canton'] = canton
                    
                    # Generer un ID unique si absent
                    if not res.get('id'):
                        res['id'] = str(uuid.uuid4())
                    
                    # Deduplication locale: privilégier le lien (souvent unique),
                    # sinon fallback sur un identifiant composite.
                    dedup_key = res.get('lien_rf') or (
                        f"{res.get('nom', '')}|{res.get('adresse', '')}|{res.get('code_postal', '')}|{res.get('ville', '')}"
                    )
                    if dedup_key in seen:
                        continue
                    seen.add(dedup_key)
                    results.append(res)
            
            # Progression
            if i % 2 == 0:
                await sio.emit('scraping_progress', {
                    'source': 'scanner',
                    'progress': i + 1,
                    'total': total,
                    'message': f"Scan: {adresse_complete}"
                })
                
            # Petit delai pour ne pas spammer
            await asyncio.sleep(0.25)
            
            # Si on a assez de resultats, on arrete
            if len(results) >= limit:
                break
                
    # Si tout a échoué côté Search.ch, remonter une erreur explicite (au lieu de 0 résultat silencieux)
    # region agent log
    _agent_dbg(
        hypothesisId="H1",
        location="app/scrapers/scanner.py:scanner_end",
        message="scanner finished",
        data={
            "results_count": len(results),
            "success_calls": success_calls,
            "error_calls": error_calls,
            "last_error_status": getattr(last_error, "status_code", None) if last_error else None,
            "last_error": str(last_error)[:160] if last_error else None,
        },
    )
    # endregion
    if len(results) == 0 and error_calls > 0 and last_error is not None:
        # Si Search.ch nous rate-limit (429) même après quelques appels "OK mais 0 résultat",
        # on remonte l'erreur au lieu de renvoyer 0 silencieux.
        if getattr(last_error, "status_code", None) == 429:
            raise SearchChScraperError(
                f"Scanner: rate limit Search.ch (429). Réduisez le limit ou attendez 1-2 minutes. ({last_error})",
                status_code=429,
            )
        if success_calls == 0:
            raise SearchChScraperError(
                f"Scanner: impossible de contacter Search.ch ({last_error})",
                status_code=getattr(last_error, "status_code", None),
            )

    print(f"[Scanner] Termine: {len(results)} residents trouves")
    return results


def get_available_communes(canton: Optional[str] = None) -> List[str]:
    """Retourne la liste des communes disponibles pour le scanner"""
    if canton == "GE":
        return COMMUNES_GE
    elif canton == "VD":
        return COMMUNES_VD
    else:
        return COMMUNES_GE + COMMUNES_VD


def get_rues_for_commune(commune: str) -> List[str]:
    """Retourne les rues disponibles pour une commune"""
    return ALL_RUES.get(commune, [])
