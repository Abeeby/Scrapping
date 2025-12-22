# =============================================================================
# SCRAPER FAO - Permis de Construire (Genève & Vaud)
# =============================================================================
# Sources:
#   - GE: https://ge.ch/ods/autorisations-construire
#   - VD: FAO cantonale + communes
# Cible: Propriétaires qui rénovent/construisent = potentiels vendeurs
# =============================================================================

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import aiohttp

from app.core.logger import scraping_logger

try:
    from playwright.async_api import async_playwright, Browser
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


class FAOError(Exception):
    """Erreur FAO/Permis."""
    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


@dataclass
class PermisConstruire:
    """Permis de construire extrait des FAO."""
    # Identification
    numero_dossier: str
    type_permis: str  # construction, transformation, démolition, agrandissement
    date_depot: str
    date_decision: Optional[str] = None
    statut: str = "en_cours"  # en_cours, accordé, refusé
    
    # Propriétaire/Requérant
    nom_requerant: str = ""
    prenom_requerant: str = ""
    adresse_requerant: str = ""
    code_postal_requerant: str = ""
    ville_requerant: str = ""
    
    # Bien concerné
    adresse_bien: str = ""
    commune_bien: str = ""
    canton: str = ""
    numero_parcelle: str = ""
    egrid: str = ""
    
    # Projet
    description_projet: str = ""
    nature_travaux: str = ""
    surface_concernee: float = 0
    cout_estime: float = 0
    
    # Intervenants
    architecte: str = ""
    entreprise_generale: str = ""
    
    # Métadonnées
    lien_fao: str = ""
    source: str = "FAO"
    extracted_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    
    # Score (calculé)
    score_interet: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "numero_dossier": self.numero_dossier,
            "type_permis": self.type_permis,
            "date_depot": self.date_depot,
            "date_decision": self.date_decision,
            "statut": self.statut,
            "nom": self.nom_requerant,
            "prenom": self.prenom_requerant,
            "adresse": self.adresse_requerant,
            "code_postal": self.code_postal_requerant,
            "ville": self.ville_requerant,
            "adresse_bien": self.adresse_bien,
            "commune_bien": self.commune_bien,
            "canton": self.canton,
            "numero_parcelle": self.numero_parcelle,
            "description": self.description_projet,
            "cout_estime": self.cout_estime,
            "architecte": self.architecte,
            "lien_fao": self.lien_fao,
            "source": self.source,
            "score_interet": self.score_interet,
        }

    def to_prospect_dict(self) -> Dict[str, Any]:
        """Format compatible avec le modèle Prospect."""
        notes_parts = [
            f"Permis: {self.type_permis}",
            f"Dossier: {self.numero_dossier}",
            f"Date dépôt: {self.date_depot}",
        ]
        if self.description_projet:
            notes_parts.append(f"Travaux: {self.description_projet}")
        if self.cout_estime:
            notes_parts.append(f"Coût estimé: CHF {self.cout_estime:,.0f}")
        if self.architecte:
            notes_parts.append(f"Architecte: {self.architecte}")
        
        return {
            "nom": self.nom_requerant,
            "prenom": self.prenom_requerant,
            "adresse": self.adresse_requerant or self.adresse_bien,
            "code_postal": self.code_postal_requerant,
            "ville": self.ville_requerant or self.commune_bien,
            "canton": self.canton,
            "type_bien": self.nature_travaux or self.type_permis,
            "lien_rf": self.lien_fao,
            "source": f"FAO Permis - {self.type_permis}",
            "notes": "\n".join(notes_parts),
            "tags": ["FAO", "permis", self.type_permis],
        }

    def calculate_interest_score(self) -> int:
        """Calcule un score d'intérêt (vendeur potentiel)."""
        score = 0
        
        # Type de permis (certains indiquent une vente future)
        type_scores = {
            "transformation": 30,      # Rénovation majeure
            "agrandissement": 25,      # Extension
            "surélévation": 25,
            "construction": 20,        # Nouveau = investisseur
            "rénovation": 30,          # Prépare la vente
            "démolition": 35,          # Projet immobilier
            "changement_affectation": 25,
        }
        for t, s in type_scores.items():
            if t in self.type_permis.lower():
                score += s
                break
        
        # Coût des travaux (gros budget = investissement sérieux)
        if self.cout_estime > 0:
            if self.cout_estime >= 500000:
                score += 20
            elif self.cout_estime >= 200000:
                score += 15
            elif self.cout_estime >= 100000:
                score += 10
        
        # Description suggère vente
        vente_keywords = ["vente", "mise en vente", "division", "copropriété", "ppe", "lotissement"]
        for kw in vente_keywords:
            if kw in self.description_projet.lower():
                score += 15
                break
        
        # Propriétaire privé vs promoteur
        promoteur_keywords = ["sa", "sàrl", "ag", "immobilier", "promotion", "développement"]
        nom_lower = self.nom_requerant.lower()
        if not any(kw in nom_lower for kw in promoteur_keywords):
            score += 10  # Bonus propriétaire privé
        
        self.score_interet = score
        return score


class FAOScraper:
    """
    Scraper pour les FAO (Feuilles d'Avis Officielles) - Permis de construire.
    
    Usage:
        async with FAOScraper() as scraper:
            permis = await scraper.search_permis_ge(days_back=30)
    """

    # URLs FAO Genève
    FAO_GE_URL = "https://ge.ch/ods/autorisations-construire"
    FAO_GE_API = "https://ge.ch/sitgags1/rest/services/VECTOR/SITG_OPENDATA_04/MapServer/0/query"
    
    # URLs FAO Vaud
    FAO_VD_URL = "https://www.vd.ch/themes/territoire-et-construction/constructions/"
    
    def __init__(self, timeout: int = 30):
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: Optional[aiohttp.ClientSession] = None
        self._browser: Optional[Browser] = None
        self._playwright = None
        
    async def __aenter__(self):
        self._session = aiohttp.ClientSession(
            timeout=self.timeout,
            headers={
                "Accept": "application/json, text/html",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
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

    async def search_permis_ge(
        self,
        commune: Optional[str] = None,
        types: Optional[List[str]] = None,
        days_back: int = 90,
        limit: int = 200,
    ) -> List[PermisConstruire]:
        """
        Recherche les permis de construire à Genève.
        
        Args:
            commune: Filtrer par commune
            types: Types de permis (construction, transformation, etc.)
            days_back: Jours à remonter
            limit: Nombre max de résultats
            
        Returns:
            Liste de PermisConstruire
        """
        scraping_logger.info(f"[FAO GE] Recherche permis: commune={commune}, days={days_back}")
        
        # Tenter l'API ArcGIS du SITG
        try:
            return await self._search_ge_api(commune, types, days_back, limit)
        except Exception as e:
            scraping_logger.warning(f"[FAO GE] API failed: {e}, trying HTML")
            return await self._search_ge_html(commune, types, days_back, limit)

    async def _search_ge_api(
        self,
        commune: Optional[str],
        types: Optional[List[str]],
        days_back: int,
        limit: int,
    ) -> List[PermisConstruire]:
        """Recherche via API ArcGIS SITG."""
        
        date_from = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        
        # Construire la requête
        where_clauses = [f"DATE_DECISION >= '{date_from}'"]
        
        if commune:
            where_clauses.append(f"COMMUNE = '{commune}'")
        
        if types:
            type_clause = " OR ".join([f"TYPE_AUTORISATION LIKE '%{t}%'" for t in types])
            where_clauses.append(f"({type_clause})")
        
        params = {
            "where": " AND ".join(where_clauses),
            "outFields": "*",
            "returnGeometry": "false",
            "f": "json",
            "resultRecordCount": limit,
        }
        
        async with self._session.get(self.FAO_GE_API, params=params) as response:
            if response.status != 200:
                raise FAOError(f"API SITG erreur {response.status}")
            
            data = await response.json()
            features = data.get("features", [])
            
            results = []
            for feat in features:
                attrs = feat.get("attributes", {})
                permis = self._parse_ge_permis(attrs)
                if permis:
                    permis.calculate_interest_score()
                    results.append(permis)
            
            return results

    def _parse_ge_permis(self, attrs: Dict[str, Any]) -> Optional[PermisConstruire]:
        """Parse un permis depuis l'API SITG."""
        try:
            # Convertir timestamp en date
            date_depot = ""
            if attrs.get("DATE_DEPOT"):
                ts = attrs["DATE_DEPOT"] / 1000  # ms -> s
                date_depot = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
            
            date_decision = ""
            if attrs.get("DATE_DECISION"):
                ts = attrs["DATE_DECISION"] / 1000
                date_decision = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
            
            return PermisConstruire(
                numero_dossier=attrs.get("NUM_DOSSIER", ""),
                type_permis=attrs.get("TYPE_AUTORISATION", ""),
                date_depot=date_depot,
                date_decision=date_decision,
                statut=attrs.get("ETAT", ""),
                nom_requerant=attrs.get("REQUERANT", ""),
                adresse_bien=attrs.get("ADRESSE", ""),
                commune_bien=attrs.get("COMMUNE", ""),
                canton="GE",
                numero_parcelle=attrs.get("NO_PARCELLE", ""),
                description_projet=attrs.get("OBJET", ""),
                nature_travaux=attrs.get("NATURE_TRAVAUX", ""),
                architecte=attrs.get("ARCHITECTE", ""),
                lien_fao=f"{self.FAO_GE_URL}?dossier={attrs.get('NUM_DOSSIER', '')}",
                source="FAO Genève",
            )
        except Exception as e:
            scraping_logger.warning(f"[FAO GE] Parse error: {e}")
            return None

    async def _search_ge_html(
        self,
        commune: Optional[str],
        types: Optional[List[str]],
        days_back: int,
        limit: int,
    ) -> List[PermisConstruire]:
        """Fallback: scraping HTML."""
        if not PLAYWRIGHT_AVAILABLE:
            return []
        
        if not self._browser:
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(headless=True)
        
        context = await self._browser.new_context()
        page = await context.new_page()
        
        try:
            await page.goto(self.FAO_GE_URL, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(2)
            
            # Appliquer les filtres si disponibles
            if commune:
                try:
                    await page.fill('input[name="commune"]', commune)
                except:
                    pass
            
            # Extraire les résultats
            html = await page.content()
            return self._parse_ge_html(html, limit)
            
        finally:
            await context.close()

    def _parse_ge_html(self, html: str, limit: int) -> List[PermisConstruire]:
        """Parse le HTML de la page FAO GE."""
        results = []
        
        # Pattern pour extraire les permis
        pattern = r'<tr[^>]*class="[^"]*permis[^"]*"[^>]*>(.*?)</tr>'
        
        for match in re.finditer(pattern, html, re.DOTALL | re.IGNORECASE):
            row = match.group(1)
            
            # Extraire les cellules
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            
            if len(cells) >= 4:
                results.append(PermisConstruire(
                    numero_dossier=re.sub(r'<[^>]+>', '', cells[0]).strip(),
                    type_permis=re.sub(r'<[^>]+>', '', cells[1]).strip() if len(cells) > 1 else "",
                    date_depot=re.sub(r'<[^>]+>', '', cells[2]).strip() if len(cells) > 2 else "",
                    description_projet=re.sub(r'<[^>]+>', '', cells[3]).strip() if len(cells) > 3 else "",
                    canton="GE",
                    source="FAO Genève (HTML)",
                ))
            
            if len(results) >= limit:
                break
        
        return results

    async def search_permis_vd(
        self,
        commune: Optional[str] = None,
        days_back: int = 90,
        limit: int = 200,
    ) -> List[PermisConstruire]:
        """
        Recherche les permis de construire dans le canton de Vaud.
        
        Note: VD n'a pas d'API centralisée, scraping des FAO cantonales.
        """
        scraping_logger.info(f"[FAO VD] Recherche permis: commune={commune}")
        
        # Pour VD, utiliser les FAO en ligne ou les communes directement
        # Exemple: Lausanne a une API propre
        
        if commune and commune.lower() == "lausanne":
            return await self._search_vd_lausanne(days_back, limit)
        
        # Fallback: générer des liens vers les FAO
        return []

    async def _search_vd_lausanne(self, days_back: int, limit: int) -> List[PermisConstruire]:
        """Recherche spécifique pour Lausanne."""
        # Lausanne utilise un système séparé
        # https://www.lausanne.ch/officiel/permis-de-construire.html
        
        lausanne_url = "https://www.lausanne.ch/officiel/permis-de-construire.html"
        
        if not PLAYWRIGHT_AVAILABLE:
            return []
        
        if not self._browser:
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(headless=True)
        
        context = await self._browser.new_context()
        page = await context.new_page()
        
        try:
            await page.goto(lausanne_url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(2)
            
            # Extraire les permis de la page
            html = await page.content()
            
            results = []
            # Pattern pour les entrées de permis
            pattern = r'class="permis-item"[^>]*>(.*?)</div>'
            
            for match in re.finditer(pattern, html, re.DOTALL):
                block = match.group(1)
                
                # Extraire les infos basiques
                numero_match = re.search(r'(\d{4,})', block)
                adresse_match = re.search(r'Adresse[:\s]*([^<]+)', block, re.IGNORECASE)
                
                if numero_match:
                    results.append(PermisConstruire(
                        numero_dossier=numero_match.group(1),
                        type_permis="permis",
                        date_depot="",
                        adresse_bien=adresse_match.group(1).strip() if adresse_match else "",
                        commune_bien="Lausanne",
                        canton="VD",
                        lien_fao=lausanne_url,
                        source="Permis Lausanne",
                    ))
                
                if len(results) >= limit:
                    break
            
            return results
            
        finally:
            await context.close()

    async def get_permis_recent_ge_vd(
        self,
        days_back: int = 60,
        limit: int = 300,
    ) -> List[PermisConstruire]:
        """
        Récupère les permis récents pour GE et VD combinés.
        
        Utile pour le pipeline de prospection.
        """
        results = []
        
        # Genève
        try:
            ge_permis = await self.search_permis_ge(days_back=days_back, limit=limit // 2)
            results.extend(ge_permis)
        except Exception as e:
            scraping_logger.error(f"[FAO] Erreur GE: {e}")
        
        # Vaud - Principales communes
        vd_communes = ["Lausanne", "Nyon", "Morges", "Vevey", "Montreux"]
        for commune in vd_communes:
            try:
                vd_permis = await self.search_permis_vd(commune=commune, days_back=days_back, limit=limit // 10)
                results.extend(vd_permis)
            except Exception as e:
                scraping_logger.warning(f"[FAO] Erreur VD/{commune}: {e}")
        
        # Calculer les scores
        for p in results:
            p.calculate_interest_score()
        
        # Trier par score d'intérêt
        results.sort(key=lambda x: x.score_interet, reverse=True)
        
        return results[:limit]


# =============================================================================
# FONCTIONS UTILITAIRES
# =============================================================================

async def search_fao_permis(
    canton: str = "GE",
    commune: Optional[str] = None,
    days_back: int = 60,
) -> List[Dict[str, Any]]:
    """Helper pour rechercher les permis."""
    async with FAOScraper() as scraper:
        if canton.upper() == "GE":
            permis = await scraper.search_permis_ge(commune=commune, days_back=days_back)
        elif canton.upper() == "VD":
            permis = await scraper.search_permis_vd(commune=commune, days_back=days_back)
        else:
            permis = []
        
        return [p.to_dict() for p in permis]


async def fao_permis_to_prospects(
    days_back: int = 60,
    min_score: int = 20,
) -> List[Dict[str, Any]]:
    """
    Convertit les permis FAO en prospects pour import.
    
    Args:
        days_back: Jours à remonter
        min_score: Score minimum pour inclure
        
    Returns:
        Liste de prospects formatés
    """
    async with FAOScraper() as scraper:
        permis = await scraper.get_permis_recent_ge_vd(days_back=days_back)
        
        prospects = []
        for p in permis:
            if p.score_interet >= min_score:
                prospects.append(p.to_prospect_dict())
        
        return prospects


