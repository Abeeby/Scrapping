# =============================================================================
# SCRAPER REGISTRE FONCIER GENÈVE - Extraction propriétaires
# =============================================================================
# Source: https://ge.ch/terextraitfoncier/rapport.aspx
# Données: Nom propriétaire, date naissance, adresse, parcelle, EGRID
# =============================================================================

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import aiohttp

from app.core.logger import scraping_logger

try:
    from playwright.async_api import async_playwright, Page, Browser
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


class RFGeneveError(Exception):
    """Erreur explicite RF Genève (réseau, parsing, accès)."""
    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


@dataclass
class ProprietaireGE:
    """Propriétaire extrait du RF Genève."""
    nom: str
    prenom: str = ""
    date_naissance: Optional[str] = None
    adresse: str = ""
    code_postal: str = ""
    ville: str = ""
    type_proprietaire: str = "prive"  # prive, societe, copropriete, ppp
    part_propriete: str = ""  # ex: "1/2", "100%"
    
    # Données parcelle
    commune: str = ""
    code_commune: int = 0
    numero_parcelle: int = 0
    egrid: str = ""
    surface_m2: float = 0
    zone: str = ""
    nature: str = ""
    
    # Métadonnées
    lien_rf: str = ""
    source: str = "RF Genève"
    extracted_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "nom": self.nom,
            "prenom": self.prenom,
            "date_naissance": self.date_naissance,
            "adresse": self.adresse,
            "code_postal": self.code_postal,
            "ville": self.ville,
            "canton": "GE",
            "type_proprietaire": self.type_proprietaire,
            "part_propriete": self.part_propriete,
            "commune": self.commune,
            "numero_parcelle": self.numero_parcelle,
            "egrid": self.egrid,
            "surface_m2": self.surface_m2,
            "zone": self.zone,
            "lien_rf": self.lien_rf,
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
            "canton": "GE",
            "type_bien": self.nature or self.zone,
            "lien_rf": self.lien_rf,
            "source": f"RF Genève - Parcelle {self.numero_parcelle}",
            "notes": f"EGRID: {self.egrid}\nSurface: {self.surface_m2}m²\nZone: {self.zone}",
        }


# =============================================================================
# COMMUNES GENÈVE (codes officiels RF)
# =============================================================================

COMMUNES_GE = {
    1: "Aire-la-Ville",
    2: "Anières", 
    3: "Avully",
    4: "Avusy",
    5: "Bardonnex",
    6: "Bellevue",
    7: "Bernex",
    8: "Carouge",
    9: "Cartigny",
    10: "Céligny",
    11: "Chancy",
    12: "Chêne-Bougeries",
    13: "Chêne-Bourg",
    14: "Choulex",
    15: "Collex-Bossy",
    16: "Collonge-Bellerive",
    17: "Cologny",
    18: "Confignon",
    19: "Genève",
    20: "Genthod",
    21: "Grand-Saconnex",
    22: "Gy",
    23: "Hermance",
    24: "Jussy",
    25: "Laconnex",
    26: "Lancy",
    27: "Meinier",
    28: "Meyrin",
    29: "Onex",
    30: "Perly-Certoux",
    31: "Plan-les-Ouates",
    32: "Pregny-Chambésy",
    33: "Presinge",
    34: "Puplinge",
    35: "Russin",
    36: "Satigny",
    37: "Soral",
    38: "Thônex",
    39: "Troinex",
    40: "Vandœuvres",
    41: "Vernier",
    42: "Versoix",
    43: "Veyrier",
}

# Nombre approximatif de parcelles par commune
PARCELLES_PAR_COMMUNE = {
    19: 15000,  # Genève (ville)
    41: 4000,   # Vernier
    26: 3500,   # Lancy
    28: 3000,   # Meyrin
    8: 2500,    # Carouge
    29: 2000,   # Onex
    38: 2000,   # Thônex
    12: 1800,   # Chêne-Bougeries
    42: 1500,   # Versoix
    21: 1200,   # Grand-Saconnex
    # ... autres communes plus petites
}


class RFGeneveScraper:
    """
    Scraper pour le Registre Foncier de Genève.
    
    Méthodes d'extraction:
    1. Via URL directe (rapport.aspx?commune=X&parcelle=Y)
    2. Via API SITG (si disponible)
    3. Via scraping HTML avec Playwright
    
    Usage:
        async with RFGeneveScraper() as scraper:
            # Extraire une parcelle spécifique
            proprio = await scraper.get_proprietaire(commune=19, parcelle=1234)
            
            # Scanner toutes les parcelles d'une commune
            proprios = await scraper.scan_commune(commune=19, start=1, end=100)
    """

    RF_BASE_URL = "https://ge.ch/terextraitfoncier/rapport.aspx"
    SITG_WFS_URL = "https://ge.ch/sitgags1/rest/services/VECTOR/SITG_OPENDATA_02/MapServer"
    
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
                "Accept": "application/json, text/html",
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

    def get_rf_url(self, commune: int, parcelle: int) -> str:
        """Génère l'URL du rapport RF pour une parcelle."""
        return f"{self.RF_BASE_URL}?commune={commune}&parcelle={parcelle}"

    async def _init_browser(self):
        """Initialise Playwright si nécessaire."""
        if self._browser:
            return
        if not PLAYWRIGHT_AVAILABLE:
            raise RFGeneveError("Playwright non disponible. Installez-le: pip install playwright && playwright install chromium")
        
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        )

    async def get_proprietaire(
        self, 
        commune: int, 
        parcelle: int,
        use_cache: bool = True,
    ) -> Optional[ProprietaireGE]:
        """
        Extrait les informations du propriétaire d'une parcelle.
        
        Args:
            commune: Code commune (1-43)
            parcelle: Numéro de parcelle
            use_cache: Utiliser le cache si disponible
            
        Returns:
            ProprietaireGE ou None si non trouvé/accès refusé
        """
        url = self.get_rf_url(commune, parcelle)
        scraping_logger.info(f"[RF GE] Extraction: commune={commune} parcelle={parcelle}")
        
        if self.use_playwright:
            return await self._extract_with_playwright(url, commune, parcelle)
        else:
            return await self._extract_with_http(url, commune, parcelle)

    async def _extract_with_http(
        self, 
        url: str, 
        commune: int, 
        parcelle: int
    ) -> Optional[ProprietaireGE]:
        """Extraction via requête HTTP simple (limité - souvent bloqué)."""
        try:
            async with self._session.get(url) as response:
                if response.status != 200:
                    scraping_logger.warning(f"[RF GE] HTTP {response.status} pour {url}")
                    return None
                
                html = await response.text()
                return self._parse_rf_html(html, commune, parcelle, url)
                
        except Exception as e:
            scraping_logger.error(f"[RF GE] Erreur HTTP: {e}")
            return None

    async def _extract_with_playwright(
        self, 
        url: str, 
        commune: int, 
        parcelle: int
    ) -> Optional[ProprietaireGE]:
        """Extraction via navigateur Playwright (plus robuste)."""
        await self._init_browser()
        
        context = await self._browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="fr-CH",
        )
        
        page = await context.new_page()
        
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(2)  # Attendre le chargement JS
            
            # Vérifier si accès refusé
            content = await page.content()
            if "accès refusé" in content.lower() or "access denied" in content.lower():
                scraping_logger.warning(f"[RF GE] Accès refusé pour parcelle {parcelle}")
                return self._create_lien_only(commune, parcelle, url)
            
            # Extraire le contenu
            html = await page.content()
            return self._parse_rf_html(html, commune, parcelle, url)
            
        except Exception as e:
            scraping_logger.error(f"[RF GE] Erreur Playwright: {e}")
            return self._create_lien_only(commune, parcelle, url)
            
        finally:
            await context.close()

    def _parse_rf_html(
        self, 
        html: str, 
        commune: int, 
        parcelle: int, 
        url: str
    ) -> Optional[ProprietaireGE]:
        """Parse le HTML du rapport RF pour extraire les données."""
        
        # Patterns d'extraction (à adapter selon le format réel)
        patterns = {
            "proprietaire": [
                r"Propriétaire[:\s]*([^<\n]+)",
                r"Titulaire[:\s]*([^<\n]+)",
                r"class=\"proprietaire\"[^>]*>([^<]+)",
            ],
            "adresse": [
                r"Adresse[:\s]*([^<\n]+)",
                r"Domicile[:\s]*([^<\n]+)",
            ],
            "surface": [
                r"Surface[:\s]*(\d+[\s']?\d*)\s*m",
                r"(\d+[\s']?\d*)\s*m²",
            ],
            "egrid": [
                r"EGRID[:\s]*([A-Z0-9]+)",
                r"CH(\d+)",
            ],
            "zone": [
                r"Zone[:\s]*([^<\n]+)",
                r"Affectation[:\s]*([^<\n]+)",
            ],
        }
        
        extracted = {}
        for field, field_patterns in patterns.items():
            for pattern in field_patterns:
                match = re.search(pattern, html, re.IGNORECASE)
                if match:
                    extracted[field] = match.group(1).strip()
                    break
        
        # Si aucun propriétaire trouvé, retourner un lien seulement
        if "proprietaire" not in extracted:
            return self._create_lien_only(commune, parcelle, url)
        
        # Parser le nom du propriétaire
        nom_complet = extracted.get("proprietaire", "")
        nom, prenom = self._parse_nom(nom_complet)
        
        # Détecter le type de propriétaire
        type_proprio = self._detect_type_proprietaire(nom_complet)
        
        # Parser l'adresse
        adresse_parts = self._parse_adresse(extracted.get("adresse", ""))
        
        # Parser la surface
        surface = 0.0
        if "surface" in extracted:
            surface_str = extracted["surface"].replace("'", "").replace(" ", "")
            try:
                surface = float(surface_str)
            except:
                pass
        
        return ProprietaireGE(
            nom=nom,
            prenom=prenom,
            adresse=adresse_parts.get("rue", ""),
            code_postal=adresse_parts.get("npa", ""),
            ville=adresse_parts.get("ville", ""),
            type_proprietaire=type_proprio,
            commune=COMMUNES_GE.get(commune, str(commune)),
            code_commune=commune,
            numero_parcelle=parcelle,
            egrid=extracted.get("egrid", ""),
            surface_m2=surface,
            zone=extracted.get("zone", ""),
            lien_rf=url,
        )

    def _create_lien_only(
        self, 
        commune: int, 
        parcelle: int, 
        url: str
    ) -> ProprietaireGE:
        """Crée un enregistrement avec le lien RF seulement (extraction manuelle requise)."""
        return ProprietaireGE(
            nom="[À EXTRAIRE MANUELLEMENT]",
            commune=COMMUNES_GE.get(commune, str(commune)),
            code_commune=commune,
            numero_parcelle=parcelle,
            lien_rf=url,
            source="RF Genève (lien seul)",
        )

    def _parse_nom(self, nom_complet: str) -> Tuple[str, str]:
        """Parse un nom complet en nom et prénom."""
        nom_complet = nom_complet.strip()
        
        # Format "NOM Prénom"
        parts = nom_complet.split()
        if len(parts) >= 2:
            # Heuristique: partie en majuscules = nom de famille
            nom_parts = []
            prenom_parts = []
            for part in parts:
                if part.isupper() or (len(part) > 1 and part[0].isupper() and part[1:].islower()):
                    if not prenom_parts:
                        nom_parts.append(part)
                    else:
                        prenom_parts.append(part)
                else:
                    prenom_parts.append(part)
            
            if nom_parts and prenom_parts:
                return " ".join(nom_parts), " ".join(prenom_parts)
            
            # Sinon premier = nom, reste = prénom
            return parts[0], " ".join(parts[1:])
        
        return nom_complet, ""

    def _parse_adresse(self, adresse: str) -> Dict[str, str]:
        """Parse une adresse suisse."""
        result = {"rue": "", "npa": "", "ville": ""}
        
        if not adresse:
            return result
        
        # Pattern: "Rue 123, 1234 Ville" ou "1234 Ville"
        npa_match = re.search(r"(\d{4})\s+(.+)$", adresse)
        if npa_match:
            result["npa"] = npa_match.group(1)
            result["ville"] = npa_match.group(2).strip()
            result["rue"] = adresse[:npa_match.start()].strip().rstrip(",")
        else:
            result["rue"] = adresse
        
        return result

    def _detect_type_proprietaire(self, nom: str) -> str:
        """Détecte le type de propriétaire (privé, société, etc.)."""
        nom_lower = nom.lower()
        
        # Sociétés
        if any(kw in nom_lower for kw in [" sa", " s.a.", " ag", " sàrl", " sarl", " gmbh", " ltd"]):
            return "societe"
        
        # Copropriété
        if any(kw in nom_lower for kw in ["copropriété", "copropriétaires", "indivision"]):
            return "copropriete"
        
        # PPE
        if "ppe" in nom_lower or "propriété par étages" in nom_lower:
            return "ppe"
        
        # État / Commune
        if any(kw in nom_lower for kw in ["état de genève", "commune de", "ville de"]):
            return "public"
        
        return "prive"

    async def scan_commune(
        self,
        commune: int,
        start: int = 1,
        end: Optional[int] = None,
        delay_ms: int = 500,
        callback: Optional[callable] = None,
    ) -> List[ProprietaireGE]:
        """
        Scanne toutes les parcelles d'une commune.
        
        Args:
            commune: Code commune (1-43)
            start: Numéro de parcelle de départ
            end: Numéro de parcelle de fin (None = estimation auto)
            delay_ms: Délai entre requêtes (rate limiting)
            callback: Fonction appelée à chaque parcelle (progress)
            
        Returns:
            Liste de ProprietaireGE
        """
        if end is None:
            end = PARCELLES_PAR_COMMUNE.get(commune, 500)
        
        results = []
        errors = 0
        
        scraping_logger.info(f"[RF GE] Scan commune {COMMUNES_GE.get(commune, commune)}: parcelles {start}-{end}")
        
        for parcelle in range(start, end + 1):
            try:
                proprio = await self.get_proprietaire(commune, parcelle)
                if proprio:
                    results.append(proprio)
                    
                if callback:
                    callback({
                        "commune": commune,
                        "parcelle": parcelle,
                        "total": end - start + 1,
                        "processed": parcelle - start + 1,
                        "found": len(results),
                    })
                    
            except Exception as e:
                errors += 1
                scraping_logger.warning(f"[RF GE] Erreur parcelle {parcelle}: {e}")
                if errors > 10:
                    scraping_logger.error("[RF GE] Trop d'erreurs, arrêt du scan")
                    break
            
            # Rate limiting
            if delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000)
        
        scraping_logger.info(f"[RF GE] Scan terminé: {len(results)} propriétaires trouvés")
        return results

    async def generate_liens_batch(
        self,
        commune: int,
        start: int = 1,
        end: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Génère une liste de liens RF sans extraction (pour traitement manuel/batch).
        
        Utile pour créer des fichiers CSV à traiter manuellement.
        """
        liens = []
        commune_nom = COMMUNES_GE.get(commune, str(commune))
        
        for parcelle in range(start, end + 1):
            url = self.get_rf_url(commune, parcelle)
            liens.append({
                "commune": commune_nom,
                "code_commune": commune,
                "numero_parcelle": parcelle,
                "lien_rf": url,
                "source": "RF Genève (lien généré)",
            })
        
        return liens


# =============================================================================
# FONCTIONS UTILITAIRES
# =============================================================================

async def scrape_rf_geneve(
    commune: int,
    parcelle: int,
) -> Optional[Dict[str, Any]]:
    """
    Helper pour extraire une parcelle sans context manager.
    """
    async with RFGeneveScraper() as scraper:
        proprio = await scraper.get_proprietaire(commune, parcelle)
        return proprio.to_dict() if proprio else None


async def generate_rf_liens_geneve(
    commune: int,
    start: int = 1,
    end: int = 100,
) -> List[Dict[str, Any]]:
    """
    Génère des liens RF pour traitement batch.
    """
    async with RFGeneveScraper() as scraper:
        return await scraper.generate_liens_batch(commune, start, end)


def get_communes_geneve() -> Dict[int, str]:
    """Retourne le dictionnaire des communes genevoises."""
    return COMMUNES_GE.copy()


def get_parcelles_estimate(commune: int) -> int:
    """Retourne une estimation du nombre de parcelles pour une commune."""
    return PARCELLES_PAR_COMMUNE.get(commune, 500)

