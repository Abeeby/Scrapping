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

from app.core.database import get_db, Prospect
from app.core.websocket import sio, emit_activity
from app.core.logger import scraping_logger

# Import des scrapers réels avec Playwright
from app.scrapers.searchch import SearchChScraper
from app.scrapers.localch import LocalChScraper
from app.scrapers.scanner import scrape_neighborhood, get_available_communes, get_rues_for_commune, COMMUNES_GE as SCANNER_COMMUNES_GE, COMMUNES_VD as SCANNER_COMMUNES_VD

router = APIRouter()

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
    source: str

class ScrapingResponse(BaseModel):
    status: str
    count: int
    results: List[ScrapingResult]

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
        params = {
            "geometry": f"{x},{y}",
            "geometryType": "esriGeometryPoint",
            "sr": "2056",
            "layers": "all:ch.bfs.gebaeude_wohnungs_register",
            "tolerance": 50,
            "returnGeometry": "false",
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

async def scrape_searchch(query: str, ville: str, limit: int) -> List[dict]:
    """Scrape les particuliers sur Search.ch via API"""
    results = []
    
    # Generer le lien de verification pour l'utilisateur
    import urllib.parse
    search_term = f"{query} {ville}".strip() if query else ville
    lien_verification = f"https://search.ch/tel/?was={urllib.parse.quote(search_term)}"
    
    print(f"[Search.ch] Demarrage scraping: {query} a {ville} (limit: {limit})")
    print(f"[Search.ch] Lien verification: {lien_verification}")
    
    try:
        async with SearchChScraper() as scraper:
            raw_results = await scraper.search(query, ville, limit)
            
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
                
        print(f"[Search.ch] Termine: {len(results)} resultats")
        
    except Exception as e:
        print(f"[Search.ch] Erreur: {e}")
    
    return results

# =============================================================================
# SCRAPER LOCAL.CH - REEL AVEC PLAYWRIGHT
# =============================================================================

async def scrape_localch(query: str, ville: str, limit: int) -> List[dict]:
    """Scrape les entreprises et particuliers sur Local.ch"""
    results = []
    
    import urllib.parse
    
    # Generer le lien de verification
    ville_slug = ville.lower().replace(' ', '-').replace('è', 'e').replace('é', 'e')
    query_slug = query.replace(' ', '-') if query else ''
    lien_verification = f"https://www.local.ch/fr/q/{ville_slug}/{query_slug}" if query else f"https://www.local.ch/fr/q/{ville_slug}"
    
    print(f"[Local.ch] Demarrage scraping: {query} a {ville} (limit: {limit})")
    print(f"[Local.ch] Lien verification: {lien_verification}")
    
    try:
        async with LocalChScraper() as scraper:
            raw_results = await scraper.search(query, ville, limit)
            
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
                
        print(f"[Local.ch] Termine: {len(results)} resultats")
        
    except Exception as e:
        print(f"[Local.ch] Erreur: {e}")
    
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
    """Liste des communes disponibles pour GE et VD"""
    return {
        "geneve": list(COMMUNES_GE.keys()),
        "vaud": SCANNER_COMMUNES_VD,
        "scanner_ge": get_available_communes("GE"),
        "scanner_vd": get_available_communes("VD")
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
    
    results = await scrape_neighborhood(commune, rue, request.limit, type_recherche=type_recherche)
    
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
    
    results = await scrape_searchch(
        request.query or "", 
        request.commune, 
        request.limit,
        type_recherche=request.type_recherche or "person"
    )
    
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
    
    results = await scrape_localch(
        request.query or "", 
        request.commune, 
        request.limit,
        type_recherche=request.type_recherche or "person"
    )
    
    await emit_activity("scraping", f"Local.ch terminé: {len(results)} {type_label} trouvés")
    
    return ScrapingResponse(
        status="completed",
        count=len(results),
        results=[ScrapingResult(**r) for r in results]
    )

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
    db: AsyncSession = Depends(get_db)
):
    """Ajoute les résultats de scraping aux prospects avec déduplication intelligente"""
    from sqlalchemy import select, and_, or_
    
    added = 0
    duplicates = 0
    
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
            source=result.source,
            notes=f"Parcelle: {result.parcelle}\nLien RF: {result.lien_rf}" if result.parcelle else ""
        )
        db.add(prospect)
        added += 1
    
    await db.commit()
    
    # Notifier
    if added > 0:
        await emit_activity("import", f"Import terminé : {added} ajoutés, {duplicates} doublons ignorés")
    else:
        await emit_activity("info", f"Aucun nouveau prospect ({duplicates} doublons)")

    return {"added": added, "duplicates": duplicates}

