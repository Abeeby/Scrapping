# =============================================================================
# SCRAPER ANIBIS.CH - Petites annonces Suisse (prioritaire pour particuliers)
# =============================================================================
# anibis.ch est la plus grande plateforme de petites annonces suisse avec
# plus de 68'000 annonces immobilières dont beaucoup de particuliers
# Source: https://www.anibis.ch/fr/immobilier--ede
# =============================================================================

from __future__ import annotations

import asyncio
import re
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from datetime import datetime

import aiohttp
from bs4 import BeautifulSoup

from app.core.logger import scraping_logger
from app.scrapers.antibot import StealthSession, get_stealth_headers, random_delay


class AnibisError(Exception):
    """Erreur explicite Anibis (réseau, blocage, parsing)."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


@dataclass
class AnibisListing:
    """Représentation d'une annonce Anibis."""

    id: str
    title: str
    description: str = ""
    price: Optional[float] = None
    currency: str = "CHF"
    
    # Localisation
    address: str = ""
    zip_code: str = ""
    city: str = ""
    canton: str = ""
    
    # Détails bien
    property_type: str = ""
    rooms: Optional[float] = None
    surface: Optional[float] = None
    
    # Contact vendeur
    seller_name: str = ""
    seller_phone: str = ""
    seller_type: str = "private"  # private, professional
    
    # Métadonnées
    url: str = ""
    images: List[str] = field(default_factory=list)
    created_at: str = ""
    
    # Indicateurs particulier
    is_private: bool = True
    agency_indicators: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "description": self.description[:500] if self.description else "",
            "price": self.price,
            "currency": self.currency,
            "address": self.address,
            "zip_code": self.zip_code,
            "city": self.city,
            "canton": self.canton,
            "property_type": self.property_type,
            "rooms": self.rooms,
            "surface": self.surface,
            "seller_name": self.seller_name,
            "seller_phone": self.seller_phone,
            "seller_type": self.seller_type,
            "url": self.url,
            "images": self.images[:5],
            "created_at": self.created_at,
            "is_private": self.is_private,
            "agency_indicators": self.agency_indicators,
        }

    def to_prospect_format(self) -> Dict[str, Any]:
        """Convertit en format compatible avec le modèle Prospect."""
        return {
            "id": f"anibis-{self.id}",
            "nom": self.seller_name or self.title[:50],
            "prenom": "",
            "adresse": self.address,
            "code_postal": self.zip_code,
            "ville": self.city,
            "canton": self.canton,
            "telephone": self.seller_phone,
            "email": "",
            "type_bien": self.property_type,
            "surface": self.surface or 0,
            "prix": self.price or 0,
            "lien_rf": self.url,
            "url_annonce": self.url,
            "titre": self.title,
            "pieces": self.rooms,
            "source": "Anibis",
            "is_private": self.is_private,
            "seller_type": self.seller_type,
        }


# =============================================================================
# DÉTECTION PARTICULIER VS AGENCE
# =============================================================================

AGENCY_KEYWORDS = [
    # Mots-clés français
    "agence", "immobilier", "immobilière", "régie", "sàrl", "sarl", "sa",
    "gestion", "courtage", "courtier", "promotion", "promoteur",
    "fiduciaire", "conseil", "consulting", "services", "group", "groupe",
    "partners", "partenaires", "invest", "capital", "holding",
    # Mots-clés allemands
    "immobilien", "agentur", "makler", "verwaltung", "treuhand",
    "gmbh", "ag", "beratung",
    # Mots-clés italiens
    "agenzia", "immobiliare", "gestione",
    # Patterns génériques
    "@", ".ch", ".com", "www.", "http",
]

AGENCY_NAME_PATTERNS = [
    r"\b(sàrl|sarl|sa|gmbh|ag)\b",
    r"\b(immobili[eè]re?|immobilien|agenzia)\b",
    r"\b(r[eé]gie|courtage|verwaltung)\b",
    r"\b(group[e]?|partners?|consulting)\b",
]

PRIVATE_INDICATORS = [
    "particulier", "privé", "privat", "privato",
    "de particulier à particulier", "sans agence",
    "agences s'abstenir", "pas d'agences", "keine makler",
    "proprietaire", "propriétaire", "eigentümer",
]


def detect_seller_type(name: str, description: str = "") -> tuple[str, bool, List[str]]:
    """
    Détecte si le vendeur est un particulier ou une agence.
    
    Returns:
        (seller_type, is_private, agency_indicators)
    """
    text = f"{name} {description}".lower()
    indicators = []
    
    # Vérifier les indicateurs de particulier (prioritaire)
    for indicator in PRIVATE_INDICATORS:
        if indicator in text:
            return ("private", True, [f"Indicateur particulier: {indicator}"])
    
    # Vérifier les patterns d'agence
    for pattern in AGENCY_NAME_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            indicators.append(f"Pattern agence: {pattern}")
    
    # Vérifier les mots-clés d'agence
    for keyword in AGENCY_KEYWORDS:
        if keyword in text:
            indicators.append(f"Mot-clé agence: {keyword}")
    
    # Score final
    if len(indicators) >= 2:
        return ("professional", False, indicators)
    elif len(indicators) == 1:
        return ("likely_professional", False, indicators)
    else:
        return ("private", True, [])


# =============================================================================
# SCRAPER ANIBIS
# =============================================================================

class AnibisScraper:
    """
    Scraper pour anibis.ch - Petites annonces immobilières suisses.
    Utilise Playwright pour le rendu JavaScript.
    
    Usage:
        async with AnibisScraper() as scraper:
            listings = await scraper.search_real_estate(
                canton="geneve",
                transaction_type="vente",
                limit=50
            )
    """

    BASE_URL = "https://www.anibis.ch"
    
    TRANSACTION_TYPES = {
        "vente": "vendre--36",
        "location": "louer--37",
        "sell": "vendre--36",
        "rent": "louer--37",
        "buy": "vendre--36",
    }
    
    CANTONS = {
        "geneve": "geneve--10",
        "vaud": "vaud--22",
        "valais": "valais--21",
        "fribourg": "fribourg--8",
        "neuchatel": "neuchatel--14",
        "jura": "jura--11",
        "berne": "berne--4",
        "zurich": "zurich--26",
        "GE": "geneve--10",
        "VD": "vaud--22",
        "VS": "valais--21",
        "FR": "fribourg--8",
        "NE": "neuchatel--14",
        "JU": "jura--11",
        "BE": "berne--4",
        "ZH": "zurich--26",
    }

    def __init__(self, timeout: int = 60, language: str = "fr"):
        self.timeout = timeout
        self.language = language
        self._browser = None
        self._context = None
        self._page = None
        self._playwright = None

    async def __aenter__(self):
        try:
            from playwright.async_api import async_playwright
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(headless=True)
            self._context = await self._browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                locale="fr-CH",
            )
            self._page = await self._context.new_page()
        except ImportError:
            raise AnibisError("Playwright non installé. Exécutez: pip install playwright && playwright install chromium", status_code=501)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._page:
            await self._page.close()
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    async def _fetch_page(self, url: str) -> str:
        """Récupère le contenu HTML après exécution JavaScript."""
        if not self._page:
            raise AnibisError("Browser non initialisé. Utilisez 'async with'.")

        try:
            await self._page.goto(url, wait_until="networkidle", timeout=self.timeout * 1000)
            await self._page.wait_for_timeout(2000)  # Attendre le rendu
            return await self._page.content()
        except Exception as e:
            error_msg = str(e).lower()
            if "timeout" in error_msg:
                raise AnibisError("Timeout lors du chargement", status_code=504)
            raise AnibisError(f"Erreur navigation: {e}")

    def _parse_listing_card(self, card, transaction_type: str = "vente") -> Optional[AnibisListing]:
        """Parse une carte d'annonce depuis la page de résultats."""
        try:
            # ID et URL
            link = card.find("a", href=True)
            if not link:
                return None
            
            url = link.get("href", "")
            if not url.startswith("http"):
                url = f"{self.BASE_URL}{url}"
            
            # Extraire l'ID de l'URL
            id_match = re.search(r"/(\d+)/?", url)
            listing_id = id_match.group(1) if id_match else str(hash(url))[-8:]
            
            # Titre
            title_el = card.find(["h2", "h3", "span"], class_=re.compile(r"title|heading", re.I))
            if not title_el:
                title_el = card.find("a")
            title = title_el.get_text(strip=True) if title_el else ""
            
            # Prix
            price = None
            price_el = card.find(class_=re.compile(r"price", re.I))
            if price_el:
                price_text = price_el.get_text(strip=True)
                price_match = re.search(r"[\d']+", price_text.replace("'", ""))
                if price_match:
                    try:
                        price = float(price_match.group().replace("'", ""))
                    except:
                        pass
            
            # Localisation
            location_el = card.find(class_=re.compile(r"location|place|city", re.I))
            city = ""
            canton = ""
            zip_code = ""
            if location_el:
                loc_text = location_el.get_text(strip=True)
                # Pattern: "1200 Genève" ou "Genève, GE"
                npa_match = re.match(r"(\d{4})\s*(.+)", loc_text)
                if npa_match:
                    zip_code = npa_match.group(1)
                    city = npa_match.group(2).strip()
                else:
                    city = loc_text
                
                # Extraire canton si présent
                canton_match = re.search(r"\b([A-Z]{2})\b", loc_text)
                if canton_match:
                    canton = canton_match.group(1)
            
            # Image
            images = []
            img_el = card.find("img", src=True)
            if img_el:
                img_src = img_el.get("src", "") or img_el.get("data-src", "")
                if img_src:
                    images.append(img_src)
            
            # Date (si disponible)
            date_el = card.find(class_=re.compile(r"date|time|posted", re.I))
            created_at = date_el.get_text(strip=True) if date_el else ""
            
            # Type de bien (si disponible)
            property_type = ""
            type_el = card.find(class_=re.compile(r"type|category", re.I))
            if type_el:
                property_type = type_el.get_text(strip=True)
            
            # Surface et pièces
            surface = None
            rooms = None
            details_el = card.find(class_=re.compile(r"detail|info|feature", re.I))
            if details_el:
                details_text = details_el.get_text()
                # Surface: "120 m²"
                surface_match = re.search(r"(\d+)\s*m[²2]", details_text)
                if surface_match:
                    surface = float(surface_match.group(1))
                # Pièces: "4 pièces" ou "4.5 pièces"
                rooms_match = re.search(r"([\d.,]+)\s*pi[èe]ces?", details_text, re.I)
                if rooms_match:
                    rooms = float(rooms_match.group(1).replace(",", "."))
            
            # Détection particulier vs agence
            seller_type, is_private, agency_indicators = detect_seller_type(title, "")
            
            return AnibisListing(
                id=listing_id,
                title=title,
                price=price,
                address="",
                zip_code=zip_code,
                city=city,
                canton=canton,
                property_type=property_type,
                rooms=rooms,
                surface=surface,
                url=url,
                images=images,
                created_at=created_at,
                seller_type=seller_type,
                is_private=is_private,
                agency_indicators=agency_indicators,
            )
            
        except Exception as e:
            scraping_logger.debug(f"[Anibis] Erreur parsing carte: {e}")
            return None

    async def search_real_estate(
        self,
        canton: str = "",
        city: str = "",
        transaction_type: str = "vente",
        property_type: str = "",
        price_min: Optional[int] = None,
        price_max: Optional[int] = None,
        only_private: bool = True,
        limit: int = 100,
    ) -> List[AnibisListing]:
        """
        Recherche des annonces immobilières sur Anibis.
        
        Args:
            canton: Code canton (GE, VD, etc.) ou nom
            city: Ville spécifique
            transaction_type: "vente" ou "location"
            property_type: "appartement", "maison", "villa", etc.
            price_min: Prix minimum
            price_max: Prix maximum
            only_private: Ne garder que les particuliers
            limit: Nombre maximum d'annonces
            
        Returns:
            Liste de AnibisListing
        """
        # Construire l'URL
        trans = self.TRANSACTION_TYPES.get(transaction_type.lower(), "vendre--36")
        
        # URL de base
        url_parts = [self.BASE_URL, self.language]
        
        # Catégorie immobilier + type de transaction
        url_parts.append(self.CATEGORY_REAL_ESTATE)
        url_parts.append(trans)
        
        # Type de bien si spécifié
        if property_type:
            prop = self.PROPERTY_TYPES.get(property_type.lower())
            if prop:
                url_parts.append(prop)
        
        # Canton
        if canton:
            canton_slug = self.CANTONS.get(canton.lower()) or self.CANTONS.get(canton.upper())
            if canton_slug:
                url_parts.append(canton_slug)
        
        # Construire l'URL finale
        base_url = "/".join(url_parts)
        
        scraping_logger.info(f"[Anibis] Recherche: {canton or 'Suisse'} ({transaction_type}) limit={limit}")

        all_listings = []
        page = 1
        max_pages = (limit // 30) + 2  # ~30 annonces par page

        while len(all_listings) < limit and page <= max_pages:
            page_url = base_url if page == 1 else f"{base_url}?page={page}"
            
            try:
                html = await self._fetch_page(page_url)
                soup = BeautifulSoup(html, "html.parser")
                
                # Trouver les cartes d'annonces
                # Anibis utilise différentes classes selon les pages
                cards = soup.find_all("article") or \
                        soup.find_all(class_=re.compile(r"listing|item|card", re.I)) or \
                        soup.find_all("div", {"data-testid": re.compile(r"listing", re.I)})
                
                if not cards:
                    scraping_logger.debug(f"[Anibis] Pas de cartes trouvées page {page}")
                    break
                
                new_count = 0
                for card in cards:
                    listing = self._parse_listing_card(card, transaction_type)
                    if listing:
                        # Filtrer par particulier si demandé
                        if only_private and not listing.is_private:
                            continue
                        all_listings.append(listing)
                        new_count += 1
                
                scraping_logger.debug(f"[Anibis] Page {page}: {new_count} annonces")
                
                if new_count == 0:
                    break
                    
                page += 1
                
                # Délai anti-blocage
                await random_delay(0.5, 1.5)

            except AnibisError:
                raise
            except Exception as e:
                scraping_logger.error(f"[Anibis] Erreur page {page}: {e}")
                break

        # Filtrer par prix si spécifié
        if price_min or price_max:
            filtered = []
            for listing in all_listings:
                if listing.price:
                    if price_min and listing.price < price_min:
                        continue
                    if price_max and listing.price > price_max:
                        continue
                filtered.append(listing)
            all_listings = filtered

        scraping_logger.info(f"[Anibis] {len(all_listings)} annonces trouvées")
        return all_listings[:limit]

    async def get_listing_details(self, url: str) -> Optional[AnibisListing]:
        """
        Récupère les détails complets d'une annonce.
        
        Args:
            url: URL de l'annonce Anibis
            
        Returns:
            AnibisListing avec tous les détails
        """
        if not url or "anibis.ch" not in url:
            raise AnibisError("URL Anibis invalide.", status_code=400)

        try:
            html = await self._fetch_page(url)
            soup = BeautifulSoup(html, "html.parser")
            
            # Extraire l'ID
            id_match = re.search(r"/(\d+)/?", url)
            listing_id = id_match.group(1) if id_match else str(hash(url))[-8:]
            
            # Titre
            title_el = soup.find("h1")
            title = title_el.get_text(strip=True) if title_el else ""
            
            # Description
            desc_el = soup.find(class_=re.compile(r"description|content|text", re.I))
            description = desc_el.get_text(strip=True) if desc_el else ""
            
            # Prix
            price = None
            price_el = soup.find(class_=re.compile(r"price", re.I))
            if price_el:
                price_text = price_el.get_text(strip=True)
                price_match = re.search(r"[\d']+", price_text.replace("'", ""))
                if price_match:
                    try:
                        price = float(price_match.group().replace("'", ""))
                    except:
                        pass
            
            # Contact vendeur
            seller_name = ""
            seller_phone = ""
            contact_el = soup.find(class_=re.compile(r"seller|contact|user", re.I))
            if contact_el:
                name_el = contact_el.find(class_=re.compile(r"name", re.I))
                if name_el:
                    seller_name = name_el.get_text(strip=True)
                    
                phone_el = contact_el.find(class_=re.compile(r"phone|tel", re.I))
                if phone_el:
                    seller_phone = phone_el.get_text(strip=True)
                    # Nettoyer le numéro
                    seller_phone = re.sub(r"[^\d+]", "", seller_phone)
            
            # Recherche téléphone dans les scripts
            if not seller_phone:
                for script in soup.find_all("script"):
                    if script.string:
                        phone_match = re.search(r'"phone"\s*:\s*"([^"]+)"', script.string)
                        if phone_match:
                            seller_phone = re.sub(r"[^\d+]", "", phone_match.group(1))
                            break
            
            # Localisation
            location_el = soup.find(class_=re.compile(r"location|address|place", re.I))
            city = ""
            canton = ""
            zip_code = ""
            address = ""
            if location_el:
                loc_text = location_el.get_text(strip=True)
                address = loc_text
                npa_match = re.match(r"(\d{4})\s*(.+)", loc_text)
                if npa_match:
                    zip_code = npa_match.group(1)
                    city = npa_match.group(2).strip()
            
            # Images
            images = []
            for img in soup.find_all("img", src=True):
                src = img.get("src", "") or img.get("data-src", "")
                if src and ("anibis" in src or "cdn" in src):
                    if src not in images:
                        images.append(src)
            
            # Détection particulier vs agence
            seller_type, is_private, agency_indicators = detect_seller_type(
                seller_name, description
            )
            
            return AnibisListing(
                id=listing_id,
                title=title,
                description=description,
                price=price,
                address=address,
                zip_code=zip_code,
                city=city,
                canton=canton,
                seller_name=seller_name,
                seller_phone=seller_phone,
                seller_type=seller_type,
                url=url,
                images=images[:10],
                is_private=is_private,
                agency_indicators=agency_indicators,
            )
            
        except AnibisError:
            raise
        except Exception as e:
            scraping_logger.error(f"[Anibis] Erreur détails: {e}")
            return None


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

async def scrape_anibis(
    canton: str = "GE",
    transaction_type: str = "vente",
    only_private: bool = True,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """
    Fonction helper pour scraper Anibis.
    
    Args:
        canton: Code canton
        transaction_type: "vente" ou "location"
        only_private: Ne garder que les particuliers
        limit: Nombre max
        
    Returns:
        Liste de résultats au format Prospect
    """
    async with AnibisScraper() as scraper:
        listings = await scraper.search_real_estate(
            canton=canton,
            transaction_type=transaction_type,
            only_private=only_private,
            limit=limit,
        )
        return [l.to_prospect_format() for l in listings]

