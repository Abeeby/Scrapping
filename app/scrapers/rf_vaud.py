# =============================================================================
# SCRAPER REGISTRE FONCIER VAUD - Extraction propriétaires
# =============================================================================
# Sources: 
#   - InterCapi VD: https://intercapi.vd.ch/
#   - Géoportail VD: https://geo.vd.ch/
#   - RF Vaud: https://www.vd.ch/registre-foncier
# =============================================================================

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from app.core.logger import scraping_logger

try:
    from playwright.async_api import async_playwright, Browser
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


class RFVaudError(Exception):
    """Erreur explicite RF Vaud."""
    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


@dataclass
class ProprietaireVD:
    """Propriétaire extrait du RF Vaud."""
    nom: str
    prenom: str = ""
    date_naissance: Optional[str] = None
    adresse: str = ""
    code_postal: str = ""
    ville: str = ""
    type_proprietaire: str = "prive"
    part_propriete: str = ""
    
    # Données parcelle
    commune: str = ""
    code_commune: int = 0
    numero_parcelle: str = ""
    egrid: str = ""
    surface_m2: float = 0
    zone: str = ""
    nature: str = ""
    
    # Métadonnées
    lien_rf: str = ""
    lien_intercapi: str = ""
    source: str = "RF Vaud"
    extracted_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "nom": self.nom,
            "prenom": self.prenom,
            "date_naissance": self.date_naissance,
            "adresse": self.adresse,
            "code_postal": self.code_postal,
            "ville": self.ville,
            "canton": "VD",
            "type_proprietaire": self.type_proprietaire,
            "part_propriete": self.part_propriete,
            "commune": self.commune,
            "numero_parcelle": self.numero_parcelle,
            "egrid": self.egrid,
            "surface_m2": self.surface_m2,
            "zone": self.zone,
            "lien_rf": self.lien_rf or self.lien_intercapi,
            "source": self.source,
        }

    def to_prospect_dict(self) -> Dict[str, Any]:
        """Format compatible avec le modèle Prospect."""
        return {
            "nom": self.nom,
            "prenom": self.prenom,
            "adresse": self.adresse,
            "code_postal": self.code_postal,
            "ville": self.ville or self.commune,
            "canton": "VD",
            "type_bien": self.nature or self.zone,
            "lien_rf": self.lien_rf or self.lien_intercapi,
            "source": f"RF Vaud - Parcelle {self.numero_parcelle}",
            "notes": f"EGRID: {self.egrid}\nSurface: {self.surface_m2}m²\nZone: {self.zone}",
        }


# =============================================================================
# COMMUNES VAUD (principales - il y en a 300+)
# =============================================================================

DISTRICTS_VD = {
    "Aigle": ["Aigle", "Bex", "Gryon", "Lavey-Morcles", "Leysin", "Noville", "Ollon", "Ormont-Dessous", "Ormont-Dessus", "Rennaz", "Roche", "Villeneuve", "Yvorne"],
    "Broye-Vully": ["Avenches", "Belmont-Broye", "Cudrefin", "Corcelles-près-Payerne", "Faoug", "Grandcour", "Henniez", "Montagny", "Payerne", "Trélex", "Vully-les-Lacs"],
    "Gros-de-Vaud": ["Assens", "Bercher", "Bottens", "Bretigny-sur-Morrens", "Echallens", "Goumoëns", "Jorat-Menthue", "Montilliez", "Oulens-sous-Echallens", "Penthaz", "Poliez-Pittet", "Saint-Barthélemy", "Sullens"],
    "Jura-Nord vaudois": ["Ballaigues", "Baulmes", "Chavornay", "Grandson", "L'Abergement", "La Praz", "Lignerolle", "Orbe", "Premier", "Sainte-Croix", "Vallorbe", "Vuiteboeuf", "Yverdon-les-Bains"],
    "Lausanne": ["Lausanne"],
    "Lavaux-Oron": ["Bourg-en-Lavaux", "Chexbres", "Forel", "Jorat-Mézières", "Lutry", "Oron", "Puidoux", "Rivaz", "Saint-Saphorin"],
    "Morges": ["Apples", "Aubonne", "Bière", "Buchillon", "Chigny", "Colombier", "Denges", "Echandens", "Etoy", "Lavigny", "Lonay", "Lully", "Morges", "Préverenges", "Reverolle", "Saint-Livres", "Saint-Prex", "Tolochenaz", "Vufflens-le-Château"],
    "Nyon": ["Arnex-sur-Nyon", "Arzier-Le Muids", "Bassins", "Begnins", "Bogis-Bossey", "Borex", "Bursinel", "Bursins", "Chavannes-de-Bogis", "Chavannes-des-Bois", "Chéserex", "Coinsins", "Commugny", "Coppet", "Crans-près-Céligny", "Crassier", "Duillier", "Eysins", "Founex", "Genolier", "Gilly", "Gingins", "Givrins", "Gland", "Grens", "Le Vaud", "Longirod", "Luins", "Marchissy", "Mies", "Mont-sur-Rolle", "Nyon", "Perroy", "Prangins", "Rolle", "Saint-Cergue", "Saint-George", "Signy-Avenex", "Tannay", "Tartegnin", "Trélex", "Vinzel", "Vich"],
    "Ouest lausannois": ["Bussigny", "Chavannes-près-Renens", "Crissier", "Ecublens", "Prilly", "Renens", "Saint-Sulpice", "Villars-Sainte-Croix"],
    "Riviera-Pays-d'Enhaut": ["Blonay-Saint-Légier", "Château-d'Œx", "Corsier-sur-Vevey", "Corseaux", "Chardonne", "Jongny", "La Tour-de-Peilz", "Montreux", "Rossinière", "Rougemont", "Vevey", "Veytaux"],
}

# Communes principales avec NPA
COMMUNES_VD = {
    "Lausanne": {"npa": "1000", "district": "Lausanne", "parcelles_estimate": 20000},
    "Morges": {"npa": "1110", "district": "Morges", "parcelles_estimate": 3000},
    "Nyon": {"npa": "1260", "district": "Nyon", "parcelles_estimate": 3500},
    "Vevey": {"npa": "1800", "district": "Riviera-Pays-d'Enhaut", "parcelles_estimate": 2500},
    "Montreux": {"npa": "1820", "district": "Riviera-Pays-d'Enhaut", "parcelles_estimate": 4000},
    "Yverdon-les-Bains": {"npa": "1400", "district": "Jura-Nord vaudois", "parcelles_estimate": 3000},
    "Renens": {"npa": "1020", "district": "Ouest lausannois", "parcelles_estimate": 2000},
    "Pully": {"npa": "1009", "district": "Lavaux-Oron", "parcelles_estimate": 2500},
    "Prilly": {"npa": "1008", "district": "Ouest lausannois", "parcelles_estimate": 1800},
    "Ecublens": {"npa": "1024", "district": "Ouest lausannois", "parcelles_estimate": 1500},
    "Gland": {"npa": "1196", "district": "Nyon", "parcelles_estimate": 2000},
    "Rolle": {"npa": "1180", "district": "Nyon", "parcelles_estimate": 1200},
    "La Tour-de-Peilz": {"npa": "1814", "district": "Riviera-Pays-d'Enhaut", "parcelles_estimate": 1500},
    "Lutry": {"npa": "1095", "district": "Lavaux-Oron", "parcelles_estimate": 1200},
    "Aigle": {"npa": "1860", "district": "Aigle", "parcelles_estimate": 1500},
    "Bex": {"npa": "1880", "district": "Aigle", "parcelles_estimate": 1000},
    "Villeneuve": {"npa": "1844", "district": "Aigle", "parcelles_estimate": 800},
    "Payerne": {"npa": "1530", "district": "Broye-Vully", "parcelles_estimate": 1200},
    "Echallens": {"npa": "1040", "district": "Gros-de-Vaud", "parcelles_estimate": 1000},
    "Crissier": {"npa": "1023", "district": "Ouest lausannois", "parcelles_estimate": 1200},
    "Bussigny": {"npa": "1030", "district": "Ouest lausannois", "parcelles_estimate": 1500},
    "Aubonne": {"npa": "1170", "district": "Morges", "parcelles_estimate": 800},
}


class RFVaudScraper:
    """
    Scraper pour le Registre Foncier du canton de Vaud.
    
    Sources utilisées:
    1. InterCapi (système officiel VD)
    2. Géoportail VD (API WFS)
    3. RF Vaud direct
    
    Usage:
        async with RFVaudScraper() as scraper:
            # Extraire via EGRID
            proprio = await scraper.get_by_egrid("CH123456789")
            
            # Scanner une commune
            proprios = await scraper.scan_commune("Lausanne", limit=100)
    """

    INTERCAPI_URL = "https://intercapi.vd.ch"
    GEOPORTAIL_URL = "https://geo.vd.ch"
    WFS_URL = "https://geo.vd.ch/geoserver/wfs"
    
    def __init__(self, timeout: int = 30, use_playwright: bool = True):
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: Optional[aiohttp.ClientSession] = None
        self._browser: Optional[Browser] = None
        self._playwright = None
        self.use_playwright = use_playwright and PLAYWRIGHT_AVAILABLE
        
    async def __aenter__(self):
        self._session = aiohttp.ClientSession(
            timeout=self.timeout,
            headers={
                "Accept": "application/json, text/html, application/xml",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept-Language": "fr-CH,fr;q=0.9",
            },
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    async def _init_browser(self):
        """Initialise Playwright si nécessaire."""
        if self._browser or not PLAYWRIGHT_AVAILABLE:
            return
        
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )

    async def search_parcelles_wfs(
        self,
        commune: str,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Recherche des parcelles via WFS (Géoportail VD).
        
        Retourne les parcelles avec leurs attributs (sans propriétaire).
        """
        # Requête WFS GetFeature
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeName": "cadastre:parcelles",
            "outputFormat": "application/json",
            "count": limit,
            "CQL_FILTER": f"commune='{commune}'",
        }
        
        try:
            async with self._session.get(self.WFS_URL, params=params) as response:
                if response.status != 200:
                    scraping_logger.warning(f"[RF VD] WFS erreur {response.status}")
                    return []
                
                data = await response.json()
                features = data.get("features", [])
                
                results = []
                for feat in features[:limit]:
                    props = feat.get("properties", {})
                    geom = feat.get("geometry", {})
                    
                    results.append({
                        "commune": commune,
                        "numero_parcelle": props.get("numero", ""),
                        "egrid": props.get("egrid", ""),
                        "surface_m2": props.get("surface", 0),
                        "zone": props.get("zone", ""),
                        "nature": props.get("nature", ""),
                        "coordinates": geom.get("coordinates"),
                    })
                
                return results
                
        except Exception as e:
            scraping_logger.error(f"[RF VD] Erreur WFS: {e}")
            return []

    async def get_by_egrid(self, egrid: str) -> Optional[ProprietaireVD]:
        """
        Recherche un propriétaire par EGRID (identifiant fédéral).
        
        Args:
            egrid: EGRID (ex: "CH123456789012")
            
        Returns:
            ProprietaireVD ou None
        """
        scraping_logger.info(f"[RF VD] Recherche EGRID: {egrid}")
        
        # URL InterCapi
        url = f"{self.INTERCAPI_URL}/recherche?egrid={egrid}"
        
        if self.use_playwright:
            return await self._extract_intercapi(url, egrid)
        else:
            # Fallback HTTP simple
            try:
                async with self._session.get(url) as response:
                    if response.status == 200:
                        html = await response.text()
                        return self._parse_intercapi_html(html, egrid)
            except Exception as e:
                scraping_logger.error(f"[RF VD] Erreur HTTP: {e}")
        
        return None

    async def _extract_intercapi(self, url: str, ref: str) -> Optional[ProprietaireVD]:
        """Extraction via Playwright sur InterCapi."""
        await self._init_browser()
        
        if not self._browser:
            return None
        
        context = await self._browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="fr-CH",
        )
        
        page = await context.new_page()
        
        try:
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(2)
            
            html = await page.content()
            return self._parse_intercapi_html(html, ref)
            
        except Exception as e:
            scraping_logger.error(f"[RF VD] Erreur Playwright: {e}")
            return None
            
        finally:
            await context.close()

    def _parse_intercapi_html(self, html: str, ref: str) -> Optional[ProprietaireVD]:
        """Parse le HTML InterCapi."""
        
        # Patterns d'extraction
        patterns = {
            "proprietaire": [
                r"Propriétaire[:\s]*</[^>]+>\s*([^<]+)",
                r"Titulaire[:\s]*</[^>]+>\s*([^<]+)",
                r"class=\"owner\"[^>]*>([^<]+)",
            ],
            "adresse": [
                r"Adresse[:\s]*</[^>]+>\s*([^<]+)",
            ],
            "surface": [
                r"Surface[:\s]*(\d+[\s']?\d*)\s*m",
            ],
            "commune": [
                r"Commune[:\s]*</[^>]+>\s*([^<]+)",
            ],
            "parcelle": [
                r"Parcelle[:\s]*</[^>]+>\s*([^<]+)",
                r"N°\s*(\d+)",
            ],
        }
        
        extracted = {}
        for field, field_patterns in patterns.items():
            for pattern in field_patterns:
                match = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
                if match:
                    extracted[field] = match.group(1).strip()
                    break
        
        if "proprietaire" not in extracted:
            return ProprietaireVD(
                nom="[À EXTRAIRE]",
                lien_intercapi=f"{self.INTERCAPI_URL}/recherche?egrid={ref}",
                source="RF Vaud (lien seul)",
            )
        
        # Parser le nom
        nom_complet = extracted.get("proprietaire", "")
        parts = nom_complet.split()
        nom = parts[0] if parts else ""
        prenom = " ".join(parts[1:]) if len(parts) > 1 else ""
        
        # Parser l'adresse
        adresse = extracted.get("adresse", "")
        npa = ""
        ville = ""
        npa_match = re.search(r"(\d{4})\s+(.+)$", adresse)
        if npa_match:
            npa = npa_match.group(1)
            ville = npa_match.group(2)
            adresse = adresse[:npa_match.start()].strip().rstrip(",")
        
        return ProprietaireVD(
            nom=nom,
            prenom=prenom,
            adresse=adresse,
            code_postal=npa,
            ville=ville,
            commune=extracted.get("commune", ""),
            numero_parcelle=extracted.get("parcelle", ""),
            egrid=ref if ref.startswith("CH") else "",
            lien_intercapi=f"{self.INTERCAPI_URL}/recherche?egrid={ref}",
        )

    async def scan_commune(
        self,
        commune: str,
        limit: int = 100,
        delay_ms: int = 500,
    ) -> List[ProprietaireVD]:
        """
        Scanne les parcelles d'une commune.
        
        1. Récupère la liste des parcelles via WFS
        2. Pour chaque parcelle, tente d'extraire le propriétaire
        """
        scraping_logger.info(f"[RF VD] Scan commune: {commune}")
        
        # Étape 1: Liste des parcelles
        parcelles = await self.search_parcelles_wfs(commune, limit)
        
        if not parcelles:
            scraping_logger.warning(f"[RF VD] Aucune parcelle trouvée pour {commune}")
            return []
        
        results = []
        
        for i, parcelle in enumerate(parcelles):
            egrid = parcelle.get("egrid", "")
            
            if egrid:
                try:
                    proprio = await self.get_by_egrid(egrid)
                    if proprio:
                        # Enrichir avec données WFS
                        proprio.surface_m2 = parcelle.get("surface_m2", 0)
                        proprio.zone = parcelle.get("zone", "")
                        proprio.nature = parcelle.get("nature", "")
                        proprio.commune = commune
                        proprio.numero_parcelle = parcelle.get("numero_parcelle", "")
                        results.append(proprio)
                        
                except Exception as e:
                    scraping_logger.warning(f"[RF VD] Erreur EGRID {egrid}: {e}")
            
            # Rate limiting
            if delay_ms > 0 and i < len(parcelles) - 1:
                await asyncio.sleep(delay_ms / 1000)
        
        scraping_logger.info(f"[RF VD] Scan terminé: {len(results)} propriétaires")
        return results

    async def generate_liens_batch(
        self,
        commune: str,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Génère des liens RF pour traitement batch.
        """
        parcelles = await self.search_parcelles_wfs(commune, limit)
        
        liens = []
        for p in parcelles:
            egrid = p.get("egrid", "")
            lien = f"{self.INTERCAPI_URL}/recherche?egrid={egrid}" if egrid else ""
            
            liens.append({
                "commune": commune,
                "canton": "VD",
                "numero_parcelle": p.get("numero_parcelle", ""),
                "egrid": egrid,
                "surface_m2": p.get("surface_m2", 0),
                "zone": p.get("zone", ""),
                "lien_rf": lien,
                "source": "RF Vaud (lien généré)",
            })
        
        return liens


# =============================================================================
# FONCTIONS UTILITAIRES
# =============================================================================

async def scrape_rf_vaud_commune(
    commune: str,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Helper pour scanner une commune VD."""
    async with RFVaudScraper() as scraper:
        proprios = await scraper.scan_commune(commune, limit)
        return [p.to_dict() for p in proprios]


async def generate_rf_liens_vaud(
    commune: str,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Génère des liens RF pour traitement batch."""
    async with RFVaudScraper() as scraper:
        return await scraper.generate_liens_batch(commune, limit)


def get_communes_vaud() -> Dict[str, Dict[str, Any]]:
    """Retourne le dictionnaire des communes vaudoises."""
    return COMMUNES_VD.copy()


def get_districts_vaud() -> Dict[str, List[str]]:
    """Retourne les districts avec leurs communes."""
    return DISTRICTS_VD.copy()


