# =============================================================================
# SCRAPER FOSC/SHAB - Faillites, Poursuites, Ventes Forcées
# =============================================================================
# Source: https://www.shab.ch/ (Feuille Officielle Suisse du Commerce)
# Données: Faillites, poursuites avec réalisation, ventes aux enchères
# Cible: Propriétaires en difficulté = ventes urgentes/forcées
# =============================================================================

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from enum import Enum

import aiohttp

from app.core.logger import scraping_logger

try:
    from playwright.async_api import async_playwright, Browser
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


class FOSCError(Exception):
    """Erreur explicite FOSC/SHAB."""
    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class TypePublication(Enum):
    """Types de publications FOSC pertinentes pour l'immobilier."""
    FAILLITE = "faillite"
    POURSUITE = "poursuite"
    VENTE_ENCHERES = "vente_encheres"
    REALISATION_FORCEE = "realisation_forcee"
    SUCCESSION = "succession"
    LIQUIDATION = "liquidation"


@dataclass
class PublicationFOSC:
    """Publication extraite du FOSC/SHAB."""
    # Identification
    id_publication: str
    type_publication: str
    date_publication: str
    
    # Débiteur/Propriétaire
    nom_debiteur: str
    prenom_debiteur: str = ""
    adresse_debiteur: str = ""
    code_postal: str = ""
    ville: str = ""
    canton: str = ""
    
    # Bien immobilier (si applicable)
    adresse_bien: str = ""
    commune_bien: str = ""
    numero_parcelle: str = ""
    egrid: str = ""
    description_bien: str = ""
    estimation_valeur: float = 0
    
    # Procédure
    office_poursuites: str = ""
    numero_dossier: str = ""
    date_vente: Optional[str] = None
    date_limite: Optional[str] = None
    
    # Métadonnées
    texte_complet: str = ""
    lien_fosc: str = ""
    source: str = "FOSC/SHAB"
    extracted_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    
    # Score priorité (calculé)
    score_urgence: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id_publication": self.id_publication,
            "type_publication": self.type_publication,
            "date_publication": self.date_publication,
            "nom": self.nom_debiteur,
            "prenom": self.prenom_debiteur,
            "adresse": self.adresse_debiteur,
            "code_postal": self.code_postal,
            "ville": self.ville,
            "canton": self.canton,
            "adresse_bien": self.adresse_bien,
            "commune_bien": self.commune_bien,
            "numero_parcelle": self.numero_parcelle,
            "estimation_valeur": self.estimation_valeur,
            "date_vente": self.date_vente,
            "office_poursuites": self.office_poursuites,
            "lien_fosc": self.lien_fosc,
            "source": self.source,
            "score_urgence": self.score_urgence,
        }

    def to_prospect_dict(self) -> Dict[str, Any]:
        """Format compatible avec le modèle Prospect."""
        notes_parts = [
            f"Type: {self.type_publication}",
            f"Date publication: {self.date_publication}",
        ]
        if self.date_vente:
            notes_parts.append(f"Date vente: {self.date_vente}")
        if self.estimation_valeur:
            notes_parts.append(f"Estimation: CHF {self.estimation_valeur:,.0f}")
        if self.numero_dossier:
            notes_parts.append(f"Dossier: {self.numero_dossier}")
        
        return {
            "nom": self.nom_debiteur,
            "prenom": self.prenom_debiteur,
            "adresse": self.adresse_debiteur or self.adresse_bien,
            "code_postal": self.code_postal,
            "ville": self.ville or self.commune_bien,
            "canton": self.canton,
            "type_bien": self.description_bien or self.type_publication,
            "lien_rf": self.lien_fosc,
            "source": f"FOSC - {self.type_publication}",
            "notes": "\n".join(notes_parts),
            "tags": ["FOSC", "urgent", self.type_publication],
        }

    def calculate_urgency_score(self) -> int:
        """Calcule un score d'urgence (plus haut = plus urgent à contacter)."""
        score = 0
        
        # Type de publication
        type_scores = {
            "vente_encheres": 50,     # Vente imminente
            "realisation_forcee": 45,
            "faillite": 40,
            "poursuite": 30,
            "succession": 20,
            "liquidation": 35,
        }
        score += type_scores.get(self.type_publication, 10)
        
        # Date de vente proche
        if self.date_vente:
            try:
                vente_date = datetime.strptime(self.date_vente, "%Y-%m-%d")
                days_until = (vente_date - datetime.utcnow()).days
                if days_until <= 7:
                    score += 30
                elif days_until <= 30:
                    score += 20
                elif days_until <= 60:
                    score += 10
            except:
                pass
        
        # Valeur du bien (plus de valeur = plus intéressant)
        if self.estimation_valeur > 0:
            if self.estimation_valeur >= 2000000:
                score += 15
            elif self.estimation_valeur >= 1000000:
                score += 10
            elif self.estimation_valeur >= 500000:
                score += 5
        
        # Canton prioritaire (VD/GE)
        if self.canton in ("GE", "VD"):
            score += 10
        
        self.score_urgence = score
        return score


class FOSCScraper:
    """
    Scraper pour le FOSC/SHAB (publications officielles).
    
    Recherche:
    - Faillites (personnes et entreprises)
    - Poursuites avec réalisation immobilière
    - Ventes aux enchères immobilières
    - Successions avec immeubles
    
    Usage:
        async with FOSCScraper() as scraper:
            # Rechercher les publications récentes pour VD/GE
            pubs = await scraper.search_publications(
                cantons=["VD", "GE"],
                types=["faillite", "vente_encheres"],
                days_back=30
            )
    """

    SHAB_URL = "https://www.shab.ch"
    SHAB_SEARCH_URL = "https://www.shab.ch/#!/search/publications"
    SHAB_API_URL = "https://www.shab.ch/api/v1"
    
    # Rubriques FOSC pertinentes
    RUBRIQUES_IMMOBILIER = [
        "HR01",  # Registre du commerce - faillites
        "HR03",  # Registre du commerce - liquidations
        "AB01",  # Avis officiels - ventes aux enchères
        "AB02",  # Avis officiels - poursuites
        "KK",    # Faillites et concordats
        "LP",    # Loi sur la poursuite
    ]
    
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

    async def search_publications(
        self,
        cantons: List[str] = None,
        types: List[str] = None,
        keywords: List[str] = None,
        days_back: int = 30,
        limit: int = 100,
    ) -> List[PublicationFOSC]:
        """
        Recherche des publications FOSC.
        
        Args:
            cantons: Liste de codes cantons (ex: ["VD", "GE"])
            types: Types de publications (faillite, poursuite, vente_encheres, etc.)
            keywords: Mots-clés de recherche (ex: ["immobilier", "parcelle"])
            days_back: Nombre de jours à remonter
            limit: Nombre max de résultats
            
        Returns:
            Liste de PublicationFOSC triée par urgence
        """
        cantons = cantons or ["VD", "GE"]
        types = types or ["faillite", "poursuite", "vente_encheres", "realisation_forcee"]
        keywords = keywords or ["immobilier", "parcelle", "bien-fonds", "immeuble"]
        
        scraping_logger.info(f"[FOSC] Recherche: cantons={cantons}, types={types}, days={days_back}")
        
        # Date de début
        date_from = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        date_to = datetime.utcnow().strftime("%Y-%m-%d")
        
        all_results = []
        
        # Recherche par canton et type
        for canton in cantons:
            for pub_type in types:
                try:
                    results = await self._search_by_type(
                        canton=canton,
                        pub_type=pub_type,
                        date_from=date_from,
                        date_to=date_to,
                        keywords=keywords,
                        limit=limit // (len(cantons) * len(types)),
                    )
                    all_results.extend(results)
                except Exception as e:
                    scraping_logger.warning(f"[FOSC] Erreur recherche {canton}/{pub_type}: {e}")
        
        # Calculer les scores d'urgence
        for pub in all_results:
            pub.calculate_urgency_score()
        
        # Trier par urgence (desc)
        all_results.sort(key=lambda x: x.score_urgence, reverse=True)
        
        scraping_logger.info(f"[FOSC] {len(all_results)} publications trouvées")
        return all_results[:limit]

    async def _search_by_type(
        self,
        canton: str,
        pub_type: str,
        date_from: str,
        date_to: str,
        keywords: List[str],
        limit: int,
    ) -> List[PublicationFOSC]:
        """Recherche par type de publication."""
        
        # Construction de la requête
        # Note: L'API SHAB peut nécessiter une adaptation
        query_parts = [f"canton:{canton}"]
        
        if pub_type == "faillite":
            query_parts.append("rubric:KK OR rubric:HR01")
        elif pub_type == "poursuite":
            query_parts.append("rubric:LP OR rubric:AB02")
        elif pub_type == "vente_encheres":
            query_parts.append("rubric:AB01")
        elif pub_type == "realisation_forcee":
            query_parts.append("rubric:LP")
        
        # Ajouter les mots-clés immobilier
        kw_query = " OR ".join(keywords)
        query_parts.append(f"({kw_query})")
        
        full_query = " AND ".join(query_parts)
        
        try:
            # Tenter l'API
            return await self._api_search(full_query, date_from, date_to, canton, pub_type, limit)
        except Exception as e:
            scraping_logger.warning(f"[FOSC] API failed: {e}, trying HTML scrape")
            # Fallback scraping HTML
            return await self._scrape_search(canton, pub_type, keywords, limit)

    async def _api_search(
        self,
        query: str,
        date_from: str,
        date_to: str,
        canton: str,
        pub_type: str,
        limit: int,
    ) -> List[PublicationFOSC]:
        """Recherche via API SHAB."""
        
        params = {
            "query": query,
            "from": date_from,
            "to": date_to,
            "size": limit,
        }
        
        async with self._session.get(
            f"{self.SHAB_API_URL}/publications",
            params=params
        ) as response:
            if response.status != 200:
                raise FOSCError(f"API SHAB erreur {response.status}")
            
            data = await response.json()
            publications = data.get("publications", [])
            
            results = []
            for pub in publications:
                parsed = self._parse_publication(pub, canton, pub_type)
                if parsed:
                    results.append(parsed)
            
            return results

    async def _scrape_search(
        self,
        canton: str,
        pub_type: str,
        keywords: List[str],
        limit: int,
    ) -> List[PublicationFOSC]:
        """Scraping HTML fallback."""
        
        if not PLAYWRIGHT_AVAILABLE:
            return []
        
        if not self._browser:
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(headless=True)
        
        context = await self._browser.new_context()
        page = await context.new_page()
        
        try:
            # Construire l'URL de recherche
            url = f"{self.SHAB_SEARCH_URL}?canton={canton}&keyword={'+'.join(keywords)}"
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(3)
            
            # Extraire les résultats
            html = await page.content()
            return self._parse_search_results_html(html, canton, pub_type)
            
        finally:
            await context.close()

    def _parse_publication(
        self,
        pub_data: Dict[str, Any],
        canton: str,
        pub_type: str,
    ) -> Optional[PublicationFOSC]:
        """Parse une publication de l'API."""
        
        try:
            content = pub_data.get("content", {})
            meta = pub_data.get("meta", {})
            
            # Extraire le débiteur
            debitor = content.get("debitor", {}) or content.get("person", {}) or {}
            nom = debitor.get("lastName", "") or debitor.get("name", "")
            prenom = debitor.get("firstName", "")
            
            if not nom:
                # Essayer d'extraire du texte
                text = content.get("text", "")
                nom_match = re.search(r"(?:concernant|gegen|betreffend)\s+([A-Za-zÀ-ÿ\-]+(?:\s+[A-Za-zÀ-ÿ\-]+)?)", text, re.IGNORECASE)
                if nom_match:
                    nom = nom_match.group(1)
            
            # Adresse
            address = debitor.get("address", {}) or {}
            adresse = address.get("street", "")
            code_postal = address.get("zip", "")
            ville = address.get("city", "")
            
            # Bien immobilier
            property_info = content.get("property", {}) or {}
            adresse_bien = property_info.get("address", "")
            commune_bien = property_info.get("municipality", "")
            numero_parcelle = property_info.get("parcelNumber", "")
            
            # Valeur estimée
            estimation = 0
            val_str = property_info.get("estimatedValue", "") or content.get("amount", "")
            if val_str:
                val_clean = re.sub(r"[^\d]", "", str(val_str))
                if val_clean:
                    estimation = float(val_clean)
            
            # Date de vente
            date_vente = content.get("auctionDate") or content.get("saleDate")
            
            return PublicationFOSC(
                id_publication=str(pub_data.get("id", "")),
                type_publication=pub_type,
                date_publication=meta.get("publicationDate", ""),
                nom_debiteur=nom,
                prenom_debiteur=prenom,
                adresse_debiteur=adresse,
                code_postal=code_postal,
                ville=ville,
                canton=canton,
                adresse_bien=adresse_bien,
                commune_bien=commune_bien,
                numero_parcelle=numero_parcelle,
                estimation_valeur=estimation,
                date_vente=date_vente,
                office_poursuites=content.get("office", ""),
                numero_dossier=content.get("caseNumber", ""),
                texte_complet=content.get("text", "")[:1000],
                lien_fosc=f"{self.SHAB_URL}/#!/publication/{pub_data.get('id', '')}",
            )
            
        except Exception as e:
            scraping_logger.warning(f"[FOSC] Erreur parsing: {e}")
            return None

    def _parse_search_results_html(
        self,
        html: str,
        canton: str,
        pub_type: str,
    ) -> List[PublicationFOSC]:
        """Parse les résultats HTML de recherche."""
        results = []
        
        # Patterns simplifiés pour extraction HTML
        # À adapter selon la structure réelle du site
        pub_pattern = r'class="publication-item"[^>]*>(.*?)</div>\s*</div>'
        
        for match in re.finditer(pub_pattern, html, re.DOTALL):
            block = match.group(1)
            
            # Extraire les données basiques
            title_match = re.search(r'class="title"[^>]*>([^<]+)', block)
            date_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', block)
            
            if title_match:
                results.append(PublicationFOSC(
                    id_publication=f"html-{len(results)}",
                    type_publication=pub_type,
                    date_publication=date_match.group(1) if date_match else "",
                    nom_debiteur=title_match.group(1).strip(),
                    canton=canton,
                    source="FOSC/SHAB (HTML)",
                ))
        
        return results

    async def get_ventes_encheres_immobilieres(
        self,
        cantons: List[str] = None,
        days_ahead: int = 60,
    ) -> List[PublicationFOSC]:
        """
        Récupère spécifiquement les ventes aux enchères immobilières à venir.
        
        Args:
            cantons: Cantons cibles
            days_ahead: Jours à l'avance pour les ventes
            
        Returns:
            Liste de ventes aux enchères triées par date
        """
        cantons = cantons or ["VD", "GE"]
        
        ventes = await self.search_publications(
            cantons=cantons,
            types=["vente_encheres", "realisation_forcee"],
            keywords=["vente aux enchères", "réalisation forcée", "immeuble", "parcelle"],
            days_back=90,  # Remonter plus pour avoir les annonces
            limit=200,
        )
        
        # Filtrer par date de vente future
        now = datetime.utcnow()
        future_limit = now + timedelta(days=days_ahead)
        
        future_ventes = []
        for v in ventes:
            if v.date_vente:
                try:
                    vente_date = datetime.strptime(v.date_vente, "%Y-%m-%d")
                    if now <= vente_date <= future_limit:
                        future_ventes.append(v)
                except:
                    pass
        
        # Trier par date de vente (plus proche = plus urgent)
        future_ventes.sort(key=lambda x: x.date_vente or "9999-99-99")
        
        return future_ventes


# =============================================================================
# FONCTIONS UTILITAIRES
# =============================================================================

async def search_fosc_immobilier(
    cantons: List[str] = None,
    days_back: int = 30,
) -> List[Dict[str, Any]]:
    """Helper pour rechercher les publications immobilières."""
    async with FOSCScraper() as scraper:
        pubs = await scraper.search_publications(
            cantons=cantons,
            days_back=days_back,
        )
        return [p.to_dict() for p in pubs]


async def get_ventes_encheres_vd_ge(days_ahead: int = 60) -> List[Dict[str, Any]]:
    """Récupère les ventes aux enchères VD/GE à venir."""
    async with FOSCScraper() as scraper:
        ventes = await scraper.get_ventes_encheres_immobilieres(
            cantons=["VD", "GE"],
            days_ahead=days_ahead,
        )
        return [v.to_dict() for v in ventes]


async def fosc_to_prospects(
    cantons: List[str] = None,
    days_back: int = 30,
) -> List[Dict[str, Any]]:
    """
    Convertit les publications FOSC en prospects pour import direct.
    """
    async with FOSCScraper() as scraper:
        pubs = await scraper.search_publications(
            cantons=cantons,
            days_back=days_back,
        )
        return [p.to_prospect_dict() for p in pubs]
