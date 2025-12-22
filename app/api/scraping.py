# =============================================================================
# API SCRAPING - Routes pour le scraping cadastre et annuaires
# =============================================================================

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime
import asyncio
import aiohttp
import json
import time

from app.core.database import get_db, Prospect, async_session
from app.core.websocket import sio, emit_activity
from app.core.logger import scraping_logger
from app.services.enrichment import run_quality_pipeline_task

# Import des scrapers réels (Search.ch API + Local.ch)
from app.scrapers.searchch import SearchChScraper, SearchChScraperError
from app.scrapers.localch import LocalChScraper
from app.scrapers.scanner import scrape_neighborhood, get_available_communes, get_rues_for_commune, COMMUNES_GE as SCANNER_COMMUNES_GE, COMMUNES_VD as SCANNER_COMMUNES_VD

router = APIRouter()

# region agent log
import logging
_agent_dbg_logger = logging.getLogger("debug_agent")
def _agent_dbg(hypothesisId: str, location: str, message: str, data: dict | None = None, run_id: str = "pre-fix"):
    _agent_dbg_logger.info(f"[{hypothesisId}] {location}: {message} | {data or {}}")
# endregion

# =============================================================================
# UTILS
# =============================================================================

async def fetch_with_retry(url, params=None, retries=3, delay=1, timeout=30):
    """Exécute une requête HTTP avec retry automatique"""
    for attempt in range(retries):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=timeout) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        scraping_logger.warning(f"HTTP {response.status} pour {url} (Tentative {attempt+1}/{retries})")
        except asyncio.TimeoutError:
            scraping_logger.warning(f"Timeout pour {url} (Tentative {attempt+1}/{retries})")
        except Exception as e:
            scraping_logger.error(f"Erreur requête {url}: {e} (Tentative {attempt+1}/{retries})")
        
        if attempt < retries - 1:
            await asyncio.sleep(delay * (attempt + 1))
            
    return None

# =============================================================================
# SCHEMAS
# =============================================================================

class ScrapingRequest(BaseModel):
    source: str  # sitg, rf, searchch, localch, vaud, scanner
    commune: Optional[str] = "Genève"
    limit: Optional[int] = 100
    query: Optional[str] = ""  # Pour annuaires
    type_recherche: Optional[str] = "person"  # person (prives), business (entreprises), all

class ScrapingResult(BaseModel):
    id: str
    nom: Optional[str] = ""
    prenom: Optional[str] = ""
    adresse: Optional[str] = ""
    code_postal: Optional[str] = ""
    ville: Optional[str] = ""
    telephone: Optional[str] = ""
    email: Optional[str] = ""
    parcelle: Optional[str] = ""
    surface: Optional[float] = 0
    zone: Optional[str] = ""
    lien_rf: Optional[str] = ""
    # URL annonce (Comparis / autres portails)
    url_annonce: Optional[str] = ""
    # Détails bien (portails)
    titre: Optional[str] = ""
    type_bien: Optional[str] = ""
    pieces: Optional[float] = None
    nombre_etages: Optional[int] = None
    surface_habitable_m2: Optional[float] = None
    surface_terrain_m2: Optional[float] = None
    annee_construction: Optional[int] = None
    annee_renovation: Optional[int] = None
    disponibilite: Optional[str] = ""
    prix_vente_chf: Optional[int] = None
    source: str

class ScrapingResponse(BaseModel):
    status: str
    count: int
    results: List[ScrapingResult]


class ComparisDetailsRequest(BaseModel):
    url: str

# =============================================================================
# COMMUNES GENEVE
# =============================================================================

COMMUNES_GE = {
    "Genève": 1, "Aïre-la-Ville": 2, "Anières": 3, "Avusy": 4, "Bardonnex": 5,
    "Bellevue": 6, "Bernex": 7, "Carouge": 8, "Cartigny": 9, "Céligny": 10,
    "Chancy": 11, "Chêne-Bougeries": 12, "Chêne-Bourg": 13, "Choulex": 14,
    "Collex-Bossy": 15, "Collonge-Bellerive": 16, "Cologny": 17, "Confignon": 18,
    "Corsier": 19, "Dardagny": 20, "Genthod": 21, "Grand-Saconnex": 22,
    "Gy": 23, "Hermance": 24, "Jussy": 25, "Laconnex": 26, "Lancy": 27,
    "Meinier": 28, "Meyrin": 29, "Onex": 30, "Perly-Certoux": 31,
    "Plan-les-Ouates": 32, "Pregny-Chambésy": 33, "Presinge": 34, "Puplinge": 35,
    "Russin": 36, "Satigny": 37, "Soral": 38, "Thônex": 39, "Troinex": 40,
    "Vandœuvres": 41, "Vernier": 42, "Versoix": 43, "Veyrier": 44
}

# =============================================================================
# SCRAPER SITG (CADASTRE GENEVE)
# =============================================================================

async def scrape_sitg(commune: str, limit: int) -> List[dict]:
    """Scrape les parcelles via l'API SITG ou genere les liens RF"""
    results = []
    commune_id = COMMUNES_GE.get(commune, 19)  # 19 = Geneve par defaut
    
    # Essayer plusieurs APIs SITG
    api_urls = [
        "https://ge.ch/sitgags2/rest/services/MENSURATION/MapServer/0/query",
        "https://ge.ch/sitgags1/rest/services/VECTOR/SITG_OPENDATA_02/MapServer/6296/query",
    ]
    
    data = None
    for api_url in api_urls:
        params = {
            "where": f"NOM_COMMUNE='{commune}'" if "sitgags2" in api_url else f"COMMUNE='{commune}'",
            "outFields": "*",
            "returnGeometry": "false",
            "f": "json",
            "resultRecordCount": limit
        }
        
        try:
            data = await fetch_with_retry(api_url, params, timeout=15)
            if data and "features" in data and len(data.get("features", [])) > 0:
                scraping_logger.info(f"SITG: API {api_url} fonctionnelle")
                break
        except Exception as e:
            scraping_logger.warning(f"SITG: API {api_url} indisponible: {e}")
            data = None
    
    if data and "features" in data:
        features = data.get("features", [])
        
        for i, feature in enumerate(features[:limit]):
            attrs = feature.get("attributes", {})
            
            no_parcelle = attrs.get("NO_PARCELLE", "") or attrs.get("NUMERO", "") or str(i + 1)
            egrid = attrs.get("EGRID", "")
            surface = attrs.get("SHAPE_Area", 0) or attrs.get("SURFACE", 0)
            
            lien_rf = f"https://ge.ch/terextraitfoncier/rapport.aspx?commune={commune_id}&parcelle={no_parcelle}"
            lien_sitg = f"https://ge.ch/sitg/sitg_catalog/geodataid/{commune_id}"
            
            results.append({
                "id": f"sitg-{commune_id}-{no_parcelle}",
                "nom": f"Parcelle {no_parcelle}",
                "parcelle": str(no_parcelle),
                "adresse": "",
                "ville": commune,
                "code_postal": "",
                "surface": surface,
                "zone": attrs.get("NATURE", ""),
                "lien_rf": lien_rf,
                "lien_source": lien_sitg,
                "source": "SITG Geneve"
            })
            
            if i % 10 == 0:
                await sio.emit('scraping_progress', {
                    'source': 'sitg',
                    'progress': i + 1,
                    'total': min(len(features), limit)
                })
    else:
        # Fallback: generer directement les liens RF (plus utiles que rien)
        scraping_logger.warning(f"SITG API indisponible, generation des liens RF pour {commune}")
        for i in range(1, min(limit + 1, 201)):
            lien_rf = f"https://ge.ch/terextraitfoncier/rapport.aspx?commune={commune_id}&parcelle={i}"
            results.append({
                "id": f"sitg-{commune_id}-{i}",
                "nom": f"Parcelle {i}",
                "parcelle": str(i),
                "adresse": "",
                "ville": commune,
                "code_postal": "",
                "surface": 0,
                "zone": "",
                "lien_rf": lien_rf,
                "lien_source": lien_rf,
                "source": f"Registre Foncier {commune}"
            })
            
            if i % 20 == 0:
                await sio.emit('scraping_progress', {
                    'source': 'sitg',
                    'progress': i,
                    'total': min(limit, 200)
                })
    
    return results

def calculate_centroid(ring):
    """Calcule le centroïde d'un polygone"""
    if not ring:
        return [6.1432, 46.2044]  # Genève par défaut
    x_sum = sum(p[0] for p in ring)
    y_sum = sum(p[1] for p in ring)
    n = len(ring)
    return [x_sum / n, y_sum / n]

async def get_address_from_coords(x: float, y: float) -> str:
    """Obtient l'adresse via Swisstopo API"""
    try:
        url = f"https://api3.geo.admin.ch/rest/services/api/MapServer/identify"
        # L'endpoint Identify requiert imageDisplay + mapExtent
        extent = f"{x-200},{y-200},{x+200},{y+200}"
        params = {
            "geometry": f"{x},{y}",
            "geometryType": "esriGeometryPoint",
            "sr": "2056",
            "layers": "all:ch.bfs.gebaeude_wohnungs_register",
            "tolerance": 50,
            "returnGeometry": "false",
            "imageDisplay": "600,550,96",
            "mapExtent": extent,
            "f": "json"
        }
        
        data = await fetch_with_retry(url, params, retries=2, timeout=5)
        
        if data and "results" in data:
            results = data.get("results", [])
            if results:
                attrs = results[0].get("attributes", {})
                rue = attrs.get("strname_deinr", "")
                if rue:
                    return rue
    except:
        pass
    return ""

# =============================================================================
# LIENS REGISTRE FONCIER
# =============================================================================

async def generate_rf_links(commune: str, limit: int) -> List[dict]:
    """Genere les liens vers le registre foncier de Geneve"""
    results = []
    commune_id = COMMUNES_GE.get(commune, 19)  # 19 = Geneve par defaut
    
    print(f"[RF] Generation de {limit} liens pour {commune} (ID: {commune_id})")
    
    # Generer les liens pour les parcelles
    for i in range(1, limit + 1):
        lien = f"https://ge.ch/terextraitfoncier/rapport.aspx?commune={commune_id}&parcelle={i}"
        results.append({
            "id": f"rf-{commune_id}-{i}",
            "nom": f"Parcelle {i} - {commune}",
            "parcelle": str(i),
            "adresse": "",
            "code_postal": "",
            "ville": commune,
            "telephone": "",
            "email": "",
            "lien_rf": lien,  # Lien direct vers le RF
            "surface": 0,
            "source": f"Registre Foncier GE"
        })
        
        if i % 50 == 0:
            await sio.emit('scraping_progress', {
                'source': 'rf',
                'progress': i,
                'total': limit
            })
    
    print(f"[RF] {len(results)} liens generes")
    return results

# =============================================================================
# SCRAPER SEARCH.CH - REEL AVEC PLAYWRIGHT
# =============================================================================

async def scrape_searchch(query: str, ville: str, limit: int, type_recherche: str = "person") -> List[dict]:
    """Scrape les particuliers sur Search.ch via API"""
    results = []
    
    # Generer le lien de verification pour l'utilisateur
    import urllib.parse
    search_term = f"{query} {ville}".strip() if query else ville
    lien_verification = f"https://search.ch/tel/?was={urllib.parse.quote(search_term)}"
    
    type_label = "prives" if type_recherche == "person" else "entreprises" if type_recherche == "business" else "tous"
    scraping_logger.info(f"[Search.ch] Demarrage scraping ({type_label}): {query} a {ville} (limit: {limit})")
    scraping_logger.info(f"[Search.ch] Lien verification: {lien_verification}")
    
    try:
        async with SearchChScraper() as scraper:
            raw_results = await scraper.search(query, ville, limit, type_recherche=type_recherche)
            
            await sio.emit('scraping_progress', {
                'source': 'searchch',
                'progress': len(raw_results),
                'total': limit,
                'message': f"Extraction de {len(raw_results)} resultats..."
            })
            
            for i, entry in enumerate(raw_results):
                nom = entry.get("nom", "")
                # Generer un lien direct vers la fiche
                nom_encoded = urllib.parse.quote(nom) if nom else ""
                ville_encoded = urllib.parse.quote(entry.get("ville", ville))
                lien_source = f"https://search.ch/tel/?was={nom_encoded}&wo={ville_encoded}"
                
                results.append({
                    "id": f"search-{i}",
                    "nom": nom,
                    "prenom": entry.get("prenom", ""),
                    "adresse": entry.get("adresse", ""),
                    "code_postal": entry.get("code_postal", ""),
                    "ville": entry.get("ville", ville),
                    "telephone": entry.get("telephone", ""),
                    "email": entry.get("email", ""),
                    "lien_rf": lien_source,  # Utilise lien_rf pour le lien source (compatible frontend)
                    "source": entry.get("source", "Search.ch")
                })
                
        scraping_logger.info(f"[Search.ch] Termine: {len(results)} resultats")
        
    except SearchChScraperError:
        # Remonter l'erreur au routeur (pour afficher un toast rouge côté UI)
        raise
    except Exception as e:
        scraping_logger.error(f"[Search.ch] Erreur interne: {e}", exc_info=True)
        raise SearchChScraperError(f"Search.ch: erreur interne ({e})")
    
    return results

# =============================================================================
# SCRAPER LOCAL.CH - REEL AVEC PLAYWRIGHT
# =============================================================================

async def scrape_localch(query: str, ville: str, limit: int, type_recherche: str = "person") -> List[dict]:
    """Scrape les entreprises et particuliers sur Local.ch"""
    results = []
    
    import urllib.parse
    
    # Generer le lien de verification
    ville_slug = ville.lower().replace(' ', '-').replace('è', 'e').replace('é', 'e')
    query_slug = query.replace(' ', '-') if query else ''
    lien_verification = f"https://www.local.ch/fr/q/{ville_slug}/{query_slug}" if query else f"https://www.local.ch/fr/q/{ville_slug}"
    
    type_label = "prives" if type_recherche == "person" else "entreprises" if type_recherche == "business" else "tous"
    scraping_logger.info(f"[Local.ch] Demarrage scraping ({type_label}): {query} a {ville} (limit: {limit})")
    scraping_logger.info(f"[Local.ch] Lien verification: {lien_verification}")
    
    try:
        async with LocalChScraper() as scraper:
            raw_results = await scraper.search(query, ville, limit, type_recherche=type_recherche)
            
            await sio.emit('scraping_progress', {
                'source': 'localch',
                'progress': len(raw_results),
                'total': limit,
                'message': f"Extraction de {len(raw_results)} resultats..."
            })
            
            for i, entry in enumerate(raw_results):
                nom = entry.get("nom", "")
                nom_slug = nom.lower().replace(' ', '-').replace(',', '').replace('.', '') if nom else ''
                lien_source = f"https://www.local.ch/fr/q/{ville_slug}/{nom_slug}" if nom else lien_verification
                
                results.append({
                    "id": f"local-{i}",
                    "nom": nom,
                    "prenom": entry.get("prenom", ""),
                    "adresse": entry.get("adresse", ""),
                    "code_postal": entry.get("code_postal", ""),
                    "ville": entry.get("ville", ville),
                    "telephone": entry.get("telephone", ""),
                    "email": entry.get("email", ""),
                    "lien_rf": lien_source,  # Lien vers la fiche
                    "source": entry.get("source", "Local.ch")
                })
                
        scraping_logger.info(f"[Local.ch] Termine: {len(results)} resultats")
        
    except SearchChScraperError:
        raise
    except Exception as e:
        scraping_logger.error(f"[Local.ch] Erreur interne: {e}", exc_info=True)
        raise SearchChScraperError(f"Local.ch: erreur interne ({e})")
    
    return results

# =============================================================================
# SCRAPER VAUD
# =============================================================================

async def scrape_vaud(commune: str, limit: int) -> List[dict]:
    """Scrape les parcelles du cadastre vaudois"""
    results = []
    
    # Lien vers le geoportail VD
    lien_geoportail = f"https://www.geo.vd.ch/?commune={commune}"
    
    # API Geodonnees VD - plusieurs endpoints possibles
    api_urls = [
        "https://map.geo.vd.ch/ws/cad_bien_fonds/query",
        "https://map.geo.vd.ch/arcgis/rest/services/CadPublic/CadPublic_parcelles/MapServer/0/query",
    ]
    
    data = None
    for api_url in api_urls:
        try:
            params = {
                "where": f"nom_commune='{commune}'" if "cad_bien" in api_url else f"COMMUNE='{commune}'",
                "outFields": "*",
                "f": "json",
                "resultRecordCount": limit
            }
            
            data = await fetch_with_retry(api_url, params, timeout=15)
            if data and "features" in data and len(data.get("features", [])) > 0:
                scraping_logger.info(f"Cadastre VD: API fonctionnelle")
                break
        except Exception as e:
            scraping_logger.warning(f"Cadastre VD: API {api_url} indisponible: {e}")
            data = None
    
    if data and "features" in data:
        features = data.get("features", [])
        
        for i, feature in enumerate(features[:limit]):
            attrs = feature.get("attributes", {})
            no_parcelle = attrs.get("no_parcelle", "") or attrs.get("NUMERO", "") or str(i + 1)
            
            # Lien vers le registre foncier VD
            lien_rf = f"https://prestations.vd.ch/pub/RF/recherche?commune={commune}&parcelle={no_parcelle}"
            
            results.append({
                "id": f"vd-{i}",
                "nom": f"Parcelle {no_parcelle}",
                "parcelle": str(no_parcelle),
                "adresse": "",
                "ville": commune,
                "code_postal": "",
                "surface": attrs.get("surface", 0) or attrs.get("SURFACE", 0),
                "lien_rf": lien_rf,
                "source": "Cadastre VD"
            })
            
            if i % 20 == 0:
                await sio.emit('scraping_progress', {
                    'source': 'vaud',
                    'progress': i + 1,
                    'total': min(len(features), limit)
                })
    else:
        # Fallback: generer des liens vers le geoportail
        scraping_logger.warning(f"API VD indisponible, generation des liens pour {commune}")
        for i in range(1, min(limit + 1, 51)):
            lien_rf = f"https://prestations.vd.ch/pub/RF/recherche?commune={commune}&parcelle={i}"
            results.append({
                "id": f"vd-{i}",
                "nom": f"Parcelle {i}",
                "parcelle": str(i),
                "adresse": "",
                "ville": commune,
                "code_postal": "",
                "surface": 0,
                "lien_rf": lien_rf,
                "source": f"Registre Foncier VD"
            })
    
    return results

# =============================================================================
# ROUTES
# =============================================================================

@router.get("/debug")
async def debug_scraping():
    """Endpoint de debug pour diagnostiquer les problemes de scraping"""
    import os
    import sys
    
    # Verifier le fichier streets.json
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_path = os.path.join(base_dir, "data", "streets.json")
    
    # Lister les fichiers dans le dossier data
    data_dir = os.path.join(base_dir, "data")
    files_in_data = []
    if os.path.exists(data_dir):
        files_in_data = os.listdir(data_dir)
    
    return {
        "status": "debug",
        "paths": {
            "cwd": os.getcwd(),
            "base_dir": base_dir,
            "data_path": data_path,
            "data_exists": os.path.exists(data_path),
            "data_dir_exists": os.path.exists(data_dir),
            "files_in_data": files_in_data,
            "__file__": os.path.abspath(__file__)
        },
        "scanner_state": {
            "COMMUNES_GE_count": len(SCANNER_COMMUNES_GE),
            "COMMUNES_VD_count": len(SCANNER_COMMUNES_VD),
            "sample_GE": SCANNER_COMMUNES_GE[:5] if SCANNER_COMMUNES_GE else [],
            "sample_VD": SCANNER_COMMUNES_VD[:5] if SCANNER_COMMUNES_VD else []
        },
        "api_communes_count": len(COMMUNES_GE)
    }

@router.get("/communes")
async def get_communes():
    """Liste des communes disponibles pour tous les cantons"""
    from app.scrapers.cadastre_ch import COMMUNES_NE, COMMUNES_FR, COMMUNES_VS, COMMUNES_BE
    
    return {
        "geneve": list(COMMUNES_GE.keys()),
        "vaud": SCANNER_COMMUNES_VD,
        "scanner_ge": get_available_communes("GE"),
        "scanner_vd": get_available_communes("VD"),
        # Nouveaux cantons
        "neuchatel": COMMUNES_NE,
        "fribourg": COMMUNES_FR,
        "valais": COMMUNES_VS,
        "berne": COMMUNES_BE,
    }

@router.get("/rues")
async def get_rues(commune: str):
    """Liste des rues pour une commune donnee (Scanner GE + VD)"""
    rues = get_rues_for_commune(commune)
    return {"rues": rues, "commune": commune}

@router.post("/scanner", response_model=ScrapingResponse)
async def scrape_scanner_endpoint(request: ScrapingRequest):
    """
    Scanner de quartier residentiel
    Utilise une liste d'adresses residentielles pour trouver les occupants prives
    """
    # Le parametre 'query' contient le nom de la rue (ou 'all')
    rue = request.query or "all"
    commune = request.commune or "Bernex"
    type_recherche = request.type_recherche or "person"
    
    type_label = "prives" if type_recherche == "person" else "entreprises" if type_recherche == "business" else "tous"
    await emit_activity("scraping", f"Demarrage Scanner ({type_label}): {rue}, {commune}")
    
    try:
        results = await scrape_neighborhood(commune, rue, request.limit, type_recherche=type_recherche)
    except SearchChScraperError as e:
        status = e.status_code if getattr(e, "status_code", None) else 502
        if status not in (400, 401, 403, 404, 408, 409, 422, 429, 500, 502, 503, 504):
            status = 502
        raise HTTPException(status_code=status, detail=str(e))
    
    await emit_activity("scraping", f"Scanner termine: {len(results)} {type_label} trouves")
    
    return ScrapingResponse(
        status="completed",
        count=len(results),
        results=[ScrapingResult(**r) for r in results]
    )

@router.post("/sitg-api", response_model=ScrapingResponse)
async def scrape_sitg_api_endpoint(
    request: ScrapingRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Lance le scraper SITG complet (API + RF) en mode headless via le script Python.
    C'est la méthode recommandée pour une automatisation complète.
    """
    import subprocess
    import sys
    import os
    
    await emit_activity("scraping", f"Lancement Scraper SITG Complet - {request.commune}")
    
    # Chemin vers le script scraper
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    scraper_path = os.path.join(base_dir, "scraper", "scraper_sitg.py")
    
    # Commande
    cmd = [
        sys.executable,
        scraper_path,
        "--commune", request.commune,
        "--limite", str(request.limit),
        "--yes"  # Auto-confirm
    ]
    
    try:
        # Lancer le processus en arrière-plan
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8'
        )
        
        # Lire la sortie en temps réel (simplifié pour l'API)
        # Idéalement, on utiliserait une tâche de fond pour lire stdout et envoyer via WebSocket
        
        await emit_activity("scraping", "Scraper lancé en arrière-plan...")
        
        return ScrapingResponse(
            status="started",
            count=0,
            results=[]
        )
        
    except Exception as e:
        scraping_logger.error(f"Erreur lancement scraper: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/sitg", response_model=ScrapingResponse)
async def scrape_sitg_endpoint(request: ScrapingRequest, background_tasks: BackgroundTasks):
    """Scrape le cadastre SITG (Genève)"""
    await emit_activity("scraping", f"Démarrage scraping SITG - {request.commune}")
    
    results = await scrape_sitg(request.commune, request.limit)
    
    await emit_activity("scraping", f"SITG terminé: {len(results)} parcelles trouvées")
    
    return ScrapingResponse(
        status="completed",
        count=len(results),
        results=[ScrapingResult(**r) for r in results]
    )

@router.post("/rf-links", response_model=ScrapingResponse)
async def generate_rf_links_endpoint(request: ScrapingRequest):
    """Génère les liens vers le Registre Foncier"""
    await emit_activity("scraping", f"Génération liens RF - {request.commune}")
    
    results = await generate_rf_links(request.commune, request.limit)
    
    await emit_activity("scraping", f"RF terminé: {len(results)} liens générés")
    
    return ScrapingResponse(
        status="completed",
        count=len(results),
        results=[ScrapingResult(**r) for r in results]
    )

@router.post("/searchch", response_model=ScrapingResponse)
async def scrape_searchch_endpoint(request: ScrapingRequest):
    """Scrape l'annuaire Search.ch"""
    type_label = "prives" if request.type_recherche == "person" else "entreprises" if request.type_recherche == "business" else "tous"
    await emit_activity("scraping", f"Démarrage Search.ch ({type_label}) - {request.query}")
    
    try:
        results = await scrape_searchch(
            request.query or "", 
            request.commune, 
            request.limit,
            type_recherche=request.type_recherche or "person"
        )
    except SearchChScraperError as e:
        status = e.status_code if getattr(e, "status_code", None) else 502
        # 429 -> rate limit, 504 -> timeout, sinon 502
        if status not in (400, 401, 403, 404, 408, 409, 422, 429, 500, 502, 503, 504):
            status = 502
        raise HTTPException(status_code=status, detail=str(e))
    
    await emit_activity("scraping", f"Search.ch terminé: {len(results)} {type_label} trouvés")
    
    return ScrapingResponse(
        status="completed",
        count=len(results),
        results=[ScrapingResult(**r) for r in results]
    )

@router.post("/localch", response_model=ScrapingResponse)
async def scrape_localch_endpoint(request: ScrapingRequest):
    """Scrape l'annuaire Local.ch"""
    type_label = "prives" if request.type_recherche == "person" else "entreprises" if request.type_recherche == "business" else "tous"
    await emit_activity("scraping", f"Démarrage Local.ch ({type_label}) - {request.query}")
    
    try:
        results = await scrape_localch(
            request.query or "", 
            request.commune, 
            request.limit,
            type_recherche=request.type_recherche or "person"
        )
    except SearchChScraperError as e:
        status = e.status_code if getattr(e, "status_code", None) else 502
        if status not in (400, 401, 403, 404, 408, 409, 422, 429, 500, 502, 503, 504):
            status = 502
        raise HTTPException(status_code=status, detail=str(e))
    
    await emit_activity("scraping", f"Local.ch terminé: {len(results)} {type_label} trouvés")
    
    return ScrapingResponse(
        status="completed",
        count=len(results),
        results=[ScrapingResult(**r) for r in results]
    )


@router.post("/comparis-details", response_model=ScrapingResponse)
async def scrape_comparis_details_endpoint(request: ComparisDetailsRequest):
    """Récupère les caractéristiques d'une annonce Comparis via son URL."""
    await emit_activity("scraping", f"Démarrage Comparis (détails annonce) - {request.url}")

    try:
        from app.scrapers.comparis import ComparisScraper, ComparisScraperError

        async with ComparisScraper() as scraper:
            details = await scraper.extract_details(request.url)
    except Exception as e:
        if getattr(e, "status_code", None):
            status = int(getattr(e, "status_code") or 502)
        else:
            status = 502
        if status not in (400, 401, 403, 404, 408, 409, 422, 429, 500, 501, 502, 503, 504):
            status = 502
        raise HTTPException(status_code=status, detail=str(e))

    result = {
        "id": details.get("listing_id") or "comparis",
        "source": "Comparis",
        "url_annonce": details.get("url_annonce") or request.url,
        # Pour l’affichage existant (table), on met le titre dans `nom`
        "nom": details.get("titre") or f"Annonce Comparis {details.get('listing_id') or ''}".strip(),
        # Détails
        "titre": details.get("titre") or "",
        "type_bien": details.get("type_bien") or "",
        "pieces": details.get("pieces"),
        "nombre_etages": details.get("nombre_etages"),
        "surface_habitable_m2": details.get("surface_habitable_m2"),
        "surface_terrain_m2": details.get("surface_terrain_m2"),
        "annee_construction": details.get("annee_construction"),
        "annee_renovation": details.get("annee_renovation"),
        "disponibilite": details.get("disponibilite") or "",
        "prix_vente_chf": details.get("prix_vente_chf"),
        # `surface` = surface habitable pour compat export/table
        "surface": details.get("surface_habitable_m2") or 0,
        # Compat action UI (ouvrir un lien)
        "lien_rf": details.get("url_annonce") or request.url,
    }

    await emit_activity("scraping", "Comparis terminé: 1 annonce")

    return ScrapingResponse(status="completed", count=1, results=[ScrapingResult(**result)])

@router.post("/vaud", response_model=ScrapingResponse)
async def scrape_vaud_endpoint(request: ScrapingRequest):
    """Scrape le cadastre vaudois"""
    await emit_activity("scraping", f"Démarrage scraping Cadastre VD - {request.commune}")
    
    results = await scrape_vaud(request.commune, request.limit)
    
    await emit_activity("scraping", f"Cadastre VD terminé: {len(results)} parcelles")
    
    return ScrapingResponse(
        status="completed",
        count=len(results),
        results=[ScrapingResult(**r) for r in results]
    )

@router.post("/add-to-prospects")
async def add_results_to_prospects(
    results: List[ScrapingResult],
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """Ajoute les résultats de scraping aux prospects avec déduplication intelligente"""
    from sqlalchemy import select, and_, or_
    
    added = 0
    duplicates = 0
    created_ids: List[str] = []
    
    for result in results:
        # Critères de déduplication : Nom + Ville OU Id identique
        # Si le nom est vide (ex: juste parcelle), on vérifie l'ID
        
        # 1. Vérifier par ID technique
        existing = await db.execute(select(Prospect).where(Prospect.id == result.id))
        if existing.scalar_one_or_none():
            duplicates += 1
            continue

        # 2. Vérifier par Nom/Prénom/Ville (Déduplication Métier)
        if result.nom and result.ville:
            query = select(Prospect).where(
                and_(
                    Prospect.nom == result.nom,
                    Prospect.ville == result.ville,
                    # Si prénom existe, on l'utilise, sinon on ignore ce critère
                    or_(Prospect.prenom == result.prenom, Prospect.prenom.is_(None), result.prenom == "")
                )
            )
            existing_biz = await db.execute(query)
            if existing_biz.scalar_one_or_none():
                duplicates += 1
                continue
        
        # Insertion si pas de doublon
        prospect = Prospect(
            id=result.id,
            nom=result.nom or "",
            prenom=result.prenom or "",
            adresse=result.adresse or "",
            code_postal=result.code_postal or "",
            ville=result.ville or "",
            telephone=result.telephone or "",
            email=result.email or "",
            lien_rf=result.lien_rf or "",
            source=result.source,
            notes=f"Parcelle: {result.parcelle}\nLien RF: {result.lien_rf}" if result.parcelle else ""
        )
        db.add(prospect)
        added += 1
        created_ids.append(prospect.id)
    
    await db.commit()

    # Pipeline qualité post-import (asynchrone)
    for pid in created_ids:
        background_tasks.add_task(run_quality_pipeline_task, pid, async_session)
    
    # Notifier
    if added > 0:
        await emit_activity("import", f"Import terminé : {added} ajoutés, {duplicates} doublons ignorés")
    else:
        await emit_activity("info", f"Aucun nouveau prospect ({duplicates} doublons)")

    return {"added": added, "duplicates": duplicates}


# =============================================================================
# NOUVEAUX SCRAPERS - Zefix, Immoscout24, Homegate
# =============================================================================

class ZefixSearchRequest(BaseModel):
    name: str
    canton: Optional[str] = None
    limit: Optional[int] = 50


class PropertySearchRequest(BaseModel):
    location: str
    transaction_type: Optional[str] = "rent"  # rent, buy
    property_type: Optional[str] = "apartment"
    limit: Optional[int] = 50
    price_max: Optional[int] = None


@router.post("/zefix", response_model=ScrapingResponse)
async def scrape_zefix_endpoint(request: ZefixSearchRequest):
    """Recherche dans le registre du commerce suisse (Zefix)."""
    await emit_activity("scraping", f"Démarrage Zefix - {request.name}")
    
    try:
        from app.scrapers.zefix import ZefixClient, ZefixError
        
        async with ZefixClient() as client:
            companies = await client.search(
                name=request.name,
                canton=request.canton,
                limit=request.limit or 50
            )
        
        results = []
        for i, company in enumerate(companies):
            # Convertir en format ScrapingResult
            results.append({
                "id": f"zefix-{company.uid.replace('.', '-')}",
                "nom": company.name,
                "prenom": "",
                "adresse": company.address or "",
                "code_postal": company.zip_code or "",
                "ville": company.city or "",
                "telephone": "",
                "email": "",
                "lien_rf": f"https://www.zefix.admin.ch/fr/search/entity/list?name={company.name.replace(' ', '+')}&searchType=exact",
                "source": f"Zefix ({company.canton})",
                "zone": company.legal_form,
                "notes": f"UID: {company.uid}\nStatut: {company.status}"
            })
        
        await emit_activity("scraping", f"Zefix terminé: {len(results)} entreprises")
        
        return ScrapingResponse(
            status="completed",
            count=len(results),
            results=[ScrapingResult(**r) for r in results]
        )
        
    except Exception as e:
        scraping_logger.error(f"[Zefix] Erreur: {e}")
        raise HTTPException(status_code=502, detail=str(e))


@router.post("/immoscout24", response_model=ScrapingResponse)
async def scrape_immoscout24_endpoint(request: PropertySearchRequest):
    """Recherche sur Immoscout24.ch (annonces immobilières)."""
    await emit_activity("scraping", f"Démarrage Immoscout24 - {request.location} ({request.transaction_type})")
    
    try:
        from app.scrapers.immoscout24 import Immoscout24Scraper, Immoscout24Error
        
        async with Immoscout24Scraper() as scraper:
            listings = await scraper.search(
                location=request.location,
                transaction_type=request.transaction_type or "rent",
                property_type=request.property_type or "apartment",
                limit=request.limit or 50,
                price_max=request.price_max
            )
        
        results = []
        for listing in listings:
            prospect_data = listing.to_prospect_format()
            results.append(prospect_data)
        
        await emit_activity("scraping", f"Immoscout24 terminé: {len(results)} annonces")
        
        return ScrapingResponse(
            status="completed",
            count=len(results),
            results=[ScrapingResult(**r) for r in results]
        )
        
    except Exception as e:
        scraping_logger.error(f"[Immoscout24] Erreur: {e}")
        status = getattr(e, "status_code", None) or 502
        raise HTTPException(status_code=status, detail=str(e))


@router.post("/homegate", response_model=ScrapingResponse)
async def scrape_homegate_endpoint(request: PropertySearchRequest):
    """Recherche sur Homegate.ch (annonces immobilières)."""
    await emit_activity("scraping", f"Démarrage Homegate - {request.location} ({request.transaction_type})")
    
    try:
        from app.scrapers.homegate import HomegateScraper, HomegateError
        
        async with HomegateScraper() as scraper:
            listings = await scraper.search(
                location=request.location,
                transaction_type=request.transaction_type or "rent",
                property_type=request.property_type or "apartment",
                limit=request.limit or 50,
                price_max=request.price_max
            )
        
        results = []
        for listing in listings:
            prospect_data = listing.to_prospect_format()
            results.append(prospect_data)
        
        await emit_activity("scraping", f"Homegate terminé: {len(results)} annonces")
        
        return ScrapingResponse(
            status="completed",
            count=len(results),
            results=[ScrapingResult(**r) for r in results]
        )
        
    except Exception as e:
        scraping_logger.error(f"[Homegate] Erreur: {e}")
        status = getattr(e, "status_code", None) or 502
        raise HTTPException(status_code=status, detail=str(e))


# =============================================================================
# SWISS REALESTATE - Alternative via APIs publiques (GeoAdmin)
# =============================================================================

@router.post("/swiss-addresses", response_model=ScrapingResponse)
async def scrape_swiss_addresses(request: PropertySearchRequest):
    """
    Alternative stable via GeoAdmin/Swisstopo.
    Utilise les APIs fédérales suisses (pas de blocage anti-bot).
    """
    await emit_activity("scraping", f"Démarrage Swiss Addresses - {request.location}")
    
    try:
        from app.scrapers.swiss_realestate import SwissRealestateClient
        
        # Déterminer le canton depuis la location
        location = request.location.lower()
        canton = ""
        if "genève" in location or "geneve" in location or location in ["ge", "geneva"]:
            canton = "GE"
        elif "lausanne" in location or "vaud" in location or location == "vd":
            canton = "VD"
        
        async with SwissRealestateClient() as client:
            if canton:
                # Recherche par commune
                properties = await client.search_addresses_in_commune(
                    commune=request.location,
                    canton=canton,
                    limit=request.limit or 100
                )
            else:
                # Recherche générale
                properties = await client.search_by_location(
                    city=request.location,
                    canton=canton,
                    limit=request.limit or 50
                )
        
        results = [p.to_prospect_format() for p in properties]
        
        await emit_activity("success", f"Swiss Addresses terminé: {len(results)} adresses trouvées")
        
        return ScrapingResponse(
            status="completed",
            count=len(results),
            results=[ScrapingResult(**r) for r in results]
        )
        
    except Exception as e:
        scraping_logger.error(f"[SwissAddresses] Erreur: {e}")
        await emit_activity("error", f"Erreur Swiss Addresses: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# ANIBIS.CH - Petites annonces suisses (PRIORITAIRE pour particuliers)
# Plus de 68'000 annonces immobilières, majoritairement de particuliers
# =============================================================================

class AnibisRequest(BaseModel):
    canton: Optional[str] = "GE"
    transaction_type: Optional[str] = "vente"  # vente, location
    property_type: Optional[str] = ""  # appartement, maison, villa, terrain
    only_private: Optional[bool] = True  # Ne garder que les particuliers
    limit: Optional[int] = 50


@router.post("/anibis", response_model=ScrapingResponse)
async def scrape_anibis_endpoint(request: AnibisRequest):
    """
    Scrape les annonces immobilières sur anibis.ch.
    """
    # region agent log
    _agent_dbg("H2", "scraping.py:anibis_entry", "anibis endpoint called", {"canton": request.canton, "limit": request.limit})
    # endregion
    await emit_activity("scraping", f"Démarrage Anibis - {request.canton} ({request.transaction_type})")
    
    try:
        from app.scrapers.anibis import scrape_anibis
        # region agent log
        _agent_dbg("H2", "scraping.py:anibis_import", "anibis import SUCCESS", {})
        # endregion
        
        results = await scrape_anibis(
            canton=request.canton or "GE",
            transaction_type=request.transaction_type or "vente",
            only_private=request.only_private if request.only_private is not None else True,
            limit=request.limit or 50,
        )
        
        await emit_activity("success", f"Anibis terminé: {len(results)} annonces (particuliers: {sum(1 for r in results if r.get('is_private', True))})")
        
        return ScrapingResponse(
            status="completed",
            count=len(results),
            results=[ScrapingResult(**r) for r in results]
        )
        
    except Exception as e:
        scraping_logger.error(f"[Anibis] Erreur: {e}")
        await emit_activity("error", f"Erreur Anibis: {str(e)}")
        status = getattr(e, "status_code", None)
        if isinstance(status, int) and status in (403, 429):
            raise HTTPException(status_code=status, detail=str(e))
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# TUTTI.CH - Petites annonces suisses (populaire en Suisse alémanique)
# =============================================================================

class TuttiRequest(BaseModel):
    canton: Optional[str] = "GE"
    transaction_type: Optional[str] = "vente"
    property_type: Optional[str] = "appartement"
    only_private: Optional[bool] = True
    limit: Optional[int] = 50


@router.post("/tutti", response_model=ScrapingResponse)
async def scrape_tutti_endpoint(request: TuttiRequest):
    """
    Scrape les annonces immobilières sur tutti.ch.
    
    tutti.ch est une des plus grandes plateformes de petites annonces suisses,
    très populaire en Suisse alémanique.
    
    Fonctionnalités:
    - Détection automatique particulier vs agence
    - Filtrage par canton, type de bien, type de transaction
    - Extraction des coordonnées vendeur
    """
    await emit_activity("scraping", f"Démarrage Tutti - {request.canton} ({request.transaction_type})")
    
    try:
        from app.scrapers.tutti import scrape_tutti
        
        results = await scrape_tutti(
            canton=request.canton or "GE",
            transaction_type=request.transaction_type or "vente",
            property_type=request.property_type or "appartement",
            only_private=request.only_private if request.only_private is not None else True,
            limit=request.limit or 50,
        )
        
        await emit_activity("success", f"Tutti terminé: {len(results)} annonces (particuliers: {sum(1 for r in results if r.get('is_private', True))})")
        
        return ScrapingResponse(
            status="completed",
            count=len(results),
            results=[ScrapingResult(**r) for r in results]
        )
        
    except Exception as e:
        scraping_logger.error(f"[Tutti] Erreur: {e}")
        await emit_activity("error", f"Erreur Tutti: {str(e)}")
        status = getattr(e, "status_code", None)
        if isinstance(status, int) and status in (403, 429):
            raise HTTPException(status_code=status, detail=str(e))
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# STEALTH BROWSER - Scraping avec Playwright anti-détection
# =============================================================================

class StealthScrapingRequest(BaseModel):
    source: str  # immoscout24, homegate
    location: str
    transaction_type: Optional[str] = "rent"
    limit: Optional[int] = 50
    proxy_server: Optional[str] = None  # host:port
    proxy_username: Optional[str] = None
    proxy_password: Optional[str] = None


@router.post("/stealth", response_model=ScrapingResponse)
async def scrape_with_stealth_browser(request: StealthScrapingRequest):
    """
    Scraping avec navigateur Playwright anti-détection.
    
    Techniques utilisées:
    - Fingerprint randomization
    - Comportement humain (scroll, délais, mouvements souris)
    - Scripts anti-détection
    - Support proxy résidentiel
    
    Sources supportées: immoscout24, homegate
    """
    # region agent log
    _agent_dbg(
        hypothesisId="H3",
        location="app/api/scraping.py:stealth_enter",
        message="stealth endpoint called",
        data={
            "source": request.source,
            "location": request.location,
            "transaction_type": request.transaction_type,
            "limit": request.limit,
            "proxy_enabled": bool(request.proxy_server),
        },
    )
    # endregion
    await emit_activity("scraping", f"Démarrage Stealth Browser - {request.source} - {request.location}")
    
    try:
        from app.scrapers.stealth_browser import scrape_with_stealth, ProxyConfig
        
        # Configurer le proxy si fourni
        proxy = None
        if request.proxy_server:
            proxy = ProxyConfig(
                server=request.proxy_server,
                username=request.proxy_username,
                password=request.proxy_password,
            )
        
        results = await scrape_with_stealth(
            source=request.source,
            location=request.location,
            transaction_type=request.transaction_type or "rent",
            limit=request.limit or 50,
            proxy=proxy,
        )
        
        await emit_activity("success", f"Stealth Browser terminé: {len(results)} annonces trouvées")
        
        return ScrapingResponse(
            status="completed",
            count=len(results),
            results=[ScrapingResult(**r) for r in results]
        )
        
    except ImportError:
        # region agent log
        _agent_dbg(
            hypothesisId="H3",
            location="app/api/scraping.py:stealth_import_error",
            message="playwright import error",
            data={"proxy_enabled": bool(request.proxy_server)},
        )
        # endregion
        await emit_activity("error", "Playwright non installé. Exécutez: pip install playwright && playwright install chromium")
        raise HTTPException(
            status_code=501,
            detail="Playwright non installé. Exécutez: pip install playwright && playwright install chromium"
        )
    except Exception as e:
        # region agent log
        _agent_dbg(
            hypothesisId="H4",
            location="app/api/scraping.py:stealth_exception",
            message="stealth endpoint exception",
            data={"exc_type": type(e).__name__, "exc": str(e)[:200]},
        )
        # endregion
        scraping_logger.error(f"[StealthBrowser] Erreur: {e}")
        await emit_activity("error", f"Erreur Stealth Browser: {str(e)}")
        status = getattr(e, "status_code", None)
        if isinstance(status, int) and status in (403, 404, 429, 501):
            raise HTTPException(status_code=status, detail=str(e))
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# CADASTRES CANTONAUX (NE, FR, VS, BE)
# =============================================================================

class CadastreRequest(BaseModel):
    canton: str  # NE, FR, VS, BE
    commune: str
    limit: Optional[int] = 100


@router.post("/cadastre", response_model=ScrapingResponse)
async def scrape_cadastre_endpoint(request: CadastreRequest):
    """Scrape les cadastres cantonaux (NE, FR, VS, BE)."""
    await emit_activity("scraping", f"Démarrage Cadastre {request.canton} - {request.commune}")
    
    try:
        from app.scrapers.cadastre_ch import scrape_cadastre, CadastreError
        
        results = await scrape_cadastre(
            canton=request.canton,
            commune=request.commune,
            limit=request.limit or 100
        )
        
        await emit_activity("scraping", f"Cadastre {request.canton} terminé: {len(results)} parcelles")
        
        return ScrapingResponse(
            status="completed",
            count=len(results),
            results=[ScrapingResult(**r) for r in results]
        )
        
    except Exception as e:
        scraping_logger.error(f"[Cadastre] Erreur: {e}")
        status = getattr(e, "status_code", None) or 400
        raise HTTPException(status_code=status, detail=str(e))


@router.get("/cadastre/communes")
async def get_cadastre_communes(canton: str):
    """Liste des communes disponibles pour un canton."""
    from app.scrapers.cadastre_ch import get_communes_for_canton
    communes = get_communes_for_canton(canton)
    return {"canton": canton.upper(), "communes": communes}


# =============================================================================
# SCRAPING MASSIF - Rues GE/VD
# =============================================================================

class MassScrapingRequest(BaseModel):
    canton: str  # GE, VD
    commune: Optional[str] = None  # Commune spécifique ou None pour toutes
    source: Optional[str] = "searchch"  # searchch, localch
    delay_seconds: Optional[int] = 2
    save_to_prospects: Optional[bool] = True


class StreetScrapingRequest(BaseModel):
    street: str
    ville: str
    canton: Optional[str] = "GE"
    save: Optional[bool] = True


@router.get("/mass-scraper/streets")
async def get_mass_scraper_streets(canton: str, commune: Optional[str] = None):
    """Liste des rues disponibles pour le scraping massif."""
    from app.data.streets_ge_vd import get_streets, get_communes, get_stats
    
    streets = get_streets(canton, commune)
    communes_list = get_communes(canton)
    stats = get_stats()
    
    return {
        "canton": canton.upper(),
        "commune": commune,
        "streets_count": len(streets),
        "streets": streets[:100],  # Limiter pour l'API
        "communes": communes_list,
        "stats": stats.get(canton.upper(), {})
    }


@router.post("/mass-scraper/job")
async def create_mass_scraper_job(request: MassScrapingRequest, background_tasks: BackgroundTasks):
    """Crée et lance un job de scraping massif."""
    from app.services.mass_scraper import MassScraperService, MassScraperError
    
    service = MassScraperService()
    
    try:
        job_id = await service.create_job(
            canton=request.canton,
            commune=request.commune,
            source=request.source or "searchch",
        )
        
        # Lancer le job en arrière-plan
        async def run_job_bg():
            await service.run_job(
                job_id=job_id,
                delay_seconds=request.delay_seconds or 2,
                save_to_prospects=request.save_to_prospects,
            )
        
        background_tasks.add_task(run_job_bg)
        
        return {
            "job_id": job_id,
            "status": "started",
            "message": f"Job de scraping massif créé et lancé"
        }
        
    except MassScraperError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/mass-scraper/jobs")
async def list_mass_scraper_jobs(limit: int = 20):
    """Liste les jobs de scraping massif."""
    from app.services.mass_scraper import MassScraperService
    
    service = MassScraperService()
    jobs = await service.list_jobs(limit=limit)
    return {"jobs": jobs}


@router.get("/mass-scraper/job/{job_id}")
async def get_mass_scraper_job(job_id: int):
    """Récupère le statut d'un job."""
    from app.services.mass_scraper import MassScraperService
    
    service = MassScraperService()
    job = await service.get_job_status(job_id)
    
    if not job:
        raise HTTPException(status_code=404, detail="Job non trouvé")
    
    return job


@router.post("/mass-scraper/job/{job_id}/stop")
async def stop_mass_scraper_job(job_id: int):
    """Arrête un job de scraping massif."""
    from app.services.mass_scraper import MassScraperService
    from app.core.database import MassScrapingJob
    from sqlalchemy import update
    
    async with AsyncSessionLocal() as db:
        await db.execute(
            update(MassScrapingJob)
            .where(MassScrapingJob.id == job_id)
            .values(status="paused")
        )
        await db.commit()
    
    return {"message": "Job arrêté"}


@router.post("/mass-scraper/street")
async def scrape_single_street(request: StreetScrapingRequest):
    """Scrape une seule rue rapidement."""
    from app.services.mass_scraper import quick_scrape_street
    
    try:
        result = await quick_scrape_street(
            street=request.street,
            ville=request.ville,
            canton=request.canton or "GE",
            save=request.save or True,
        )
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/mass-scraper/coverage/{canton}")
async def get_mass_scraper_coverage(canton: str):
    """Récupère la couverture de scraping pour un canton."""
    from app.services.mass_scraper import get_scraping_coverage
    
    coverage = await get_scraping_coverage(canton)
    return coverage
