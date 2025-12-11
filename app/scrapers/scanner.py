# =============================================================================
# SCANNER DE QUARTIER - Scraping par adresse
# =============================================================================

import asyncio
import uuid
import json
import os
from typing import List, Dict, Optional
from app.scrapers.searchch import SearchChScraper
from app.core.websocket import sio, emit_activity

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
    canton: Optional[str] = None
) -> List[Dict]:
    """
    Scanne une rue numero par numero pour trouver les residents.
    Supporte Geneve (GE) et Vaud (VD).
    """
    results = []
    
    # Determiner le canton si non specifie
    if canton is None:
        canton = get_canton(commune)
    
    print(f"[Scanner] Demarrage scanner: {rue}, {commune} ({canton})")
    
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
        
        if total == 0:
            print(f"[Scanner] Aucune rue trouvee pour {commune}")
            return results
        
        for i, adresse_base in enumerate(adresses_a_tester):
            adresse_complete = f"{adresse_base}, {commune}"
            
            # Utiliser le champ 'wo' pour l'adresse et laisser 'was' vide pour tout trouver
            # C'est la technique cle pour le reverse search
            try:
                scan_results = await scraper.search(query="", ville=adresse_complete, limit=5)
            except Exception as e:
                print(f"[Scanner] Erreur pour {adresse_complete}: {e}")
                continue
            
            for res in scan_results:
                # Le scraper searchch.py a deja un filtre anti-entreprise
                # On ajoute une couche supplementaire de validation
                if res and res.get('nom') and res.get('telephone'):
                    # Ajouter l'info "Source: Scanner"
                    res['source'] = f"Scanner {canton}"
                    res['notes'] = f"Trouve a l'adresse: {adresse_complete}"
                    res['canton'] = canton
                    
                    # Generer un ID unique si absent
                    if not res.get('id'):
                        res['id'] = str(uuid.uuid4())
                    
                    # Deduplication locale (nom + telephone)
                    dedup_key = f"{res.get('nom', '')}-{res.get('telephone', '')}"
                    existing_keys = [f"{r.get('nom', '')}-{r.get('telephone', '')}" for r in results]
                    if dedup_key not in existing_keys:
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
            await asyncio.sleep(0.3)
            
            # Si on a assez de resultats, on arrete
            if len(results) >= limit:
                break
                
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
