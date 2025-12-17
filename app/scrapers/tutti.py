# =============================================================================
# SCRAPER TUTTI.CH - Petites annonces Suisse (plateforme majeure)
# =============================================================================
# tutti.ch est une des plus grandes plateformes de petites annonces suisses
# Beaucoup de particuliers y publient leurs biens immobiliers
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
from app.scrapers.anibis import detect_seller_type, AGENCY_KEYWORDS


class TuttiError(Exception):
    """Erreur explicite Tutti (réseau, blocage, parsing)."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


@dataclass
class TuttiListing:
    """Représentation d'une annonce Tutti."""

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
    region: str = ""
    
    # Détails bien
    property_type: str = ""
    rooms: Optional[float] = None
    surface: Optional[float] = None
    
    # Contact vendeur
    seller_name: str = ""
    seller_phone: str = ""
    seller_type: str = "private"
    
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
            "region": self.region,
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
            "id": f"tutti-{self.id}",
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
            "source": "Tutti",
            "is_private": self.is_private,
            "seller_type": self.seller_type,
        }


class TuttiScraper:
    """
    Scraper pour tutti.ch - Petites annonces immobilières suisses.
    Utilise Playwright pour le rendu JavaScript.
    
    Usage:
        async with TuttiScraper() as scraper:
            listings = await scraper.search_real_estate(
                canton="geneve",
                transaction_type="vente",
                limit=50
            )
    """

    BASE_URL = "https://www.tutti.ch"
    
    CATEGORIES = {
        "immobilier": "immobilien",
        "appartement_vente": "immobilien/wohnungen/zu-verkaufen",
        "appartement_location": "immobilien/wohnungen/zu-vermieten",
        "maison_vente": "immobilien/haeuser/zu-verkaufen",
        "maison_location": "immobilien/haeuser/zu-vermieten",
    }
    
    REGIONS = {
        "geneve": "genf", "vaud": "waadt", "valais": "wallis",
        "fribourg": "freiburg", "neuchatel": "neuenburg", "jura": "jura",
        "zurich": "zurich", "berne": "bern", "lucerne": "luzern",
        "GE": "genf", "VD": "waadt", "VS": "wallis", "FR": "freiburg",
        "NE": "neuenburg", "JU": "jura", "ZH": "zurich", "BE": "bern",
    }

    def __init__(self, timeout: int = 60, language: str = "de"):
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
                locale="de-CH",
            )
            self._page = await self._context.new_page()
        except ImportError:
            raise TuttiError("Playwright non installé.", status_code=501)
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
            raise TuttiError("Browser non initialisé.")

        try:
            await self._page.goto(url, wait_until="networkidle", timeout=self.timeout * 1000)
            await self._page.wait_for_timeout(2000)
            return await self._page.content()
        except Exception as e:
            raise TuttiError(f"Erreur navigation: {e}")

    def _extract_json_data(self, html: str) -> Optional[Dict]:
        """Extrait les données JSON embarquées dans la page."""
        # tutti.ch utilise Next.js, les données sont dans __NEXT_DATA__
        soup = BeautifulSoup(html, "html.parser")
        script = soup.find("script", {"id": "__NEXT_DATA__"})
        
        if script and script.string:
            try:
                return json.loads(script.string)
            except json.JSONDecodeError:
                pass
        
        # Fallback: chercher via regex
        pattern = r'<script id="__NEXT_DATA__"[^>]*>(.+?)</script>'
        match = re.search(pattern, html, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass
        
        return None

    def _parse_listing_from_json(self, item: Dict, region: str = "") -> Optional[TuttiListing]:
        """Parse une annonce depuis les données JSON."""
        try:
            listing_id = str(item.get("id", ""))
            if not listing_id:
                return None
            
            title = item.get("subject", "") or item.get("title", "")
            description = item.get("body", "") or item.get("description", "")
            
            # Prix
            price = None
            price_data = item.get("price", {})
            if isinstance(price_data, dict):
                price = price_data.get("value")
            elif isinstance(price_data, (int, float)):
                price = price_data
            
            # Localisation
            location = item.get("location", {}) or {}
            city = location.get("city", "") or location.get("name", "")
            zip_code = location.get("zip", "") or location.get("postalCode", "")
            canton = location.get("canton", "") or region.upper()[:2]
            
            # URL
            url = item.get("url", "") or item.get("link", "")
            if url and not url.startswith("http"):
                url = f"{self.BASE_URL}{url}"
            
            # Images
            images = []
            media = item.get("media", []) or item.get("images", [])
            for m in media[:10]:
                if isinstance(m, dict):
                    img_url = m.get("url", "") or m.get("src", "")
                elif isinstance(m, str):
                    img_url = m
                else:
                    continue
                if img_url:
                    images.append(img_url)
            
            # Attributs
            attributes = item.get("attributes", {}) or item.get("details", {})
            rooms = None
            surface = None
            property_type = ""
            
            if isinstance(attributes, dict):
                rooms_val = attributes.get("rooms") or attributes.get("zimmer")
                if rooms_val:
                    try:
                        rooms = float(str(rooms_val).replace(",", "."))
                    except:
                        pass
                
                surface_val = attributes.get("livingSpace") or attributes.get("surface")
                if surface_val:
                    try:
                        surface = float(re.sub(r"[^\d.]", "", str(surface_val)))
                    except:
                        pass
                        
                property_type = attributes.get("type", "") or attributes.get("objectType", "")
            elif isinstance(attributes, list):
                for attr in attributes:
                    if isinstance(attr, dict):
                        label = attr.get("label", "").lower()
                        value = attr.get("value", "")
                        if "zimmer" in label or "pièce" in label:
                            try:
                                rooms = float(str(value).replace(",", "."))
                            except:
                                pass
                        elif "fläche" in label or "surface" in label:
                            try:
                                surface = float(re.sub(r"[^\d.]", "", str(value)))
                            except:
                                pass
            
            # Vendeur
            seller = item.get("seller", {}) or item.get("user", {})
            seller_name = ""
            if isinstance(seller, dict):
                seller_name = seller.get("name", "") or seller.get("displayName", "")
            
            # Date
            created_at = item.get("createdAt", "") or item.get("publishedAt", "")
            
            # Détection particulier vs agence
            seller_type, is_private, agency_indicators = detect_seller_type(
                seller_name, description
            )
            
            return TuttiListing(
                id=listing_id,
                title=title,
                description=description,
                price=price,
                city=city,
                zip_code=zip_code,
                canton=canton,
                region=region,
                property_type=property_type,
                rooms=rooms,
                surface=surface,
                seller_name=seller_name,
                url=url,
                images=images,
                created_at=created_at,
                seller_type=seller_type,
                is_private=is_private,
                agency_indicators=agency_indicators,
            )
            
        except Exception as e:
            scraping_logger.debug(f"[Tutti] Erreur parsing JSON: {e}")
            return None

    def _parse_listing_card(self, card, region: str = "") -> Optional[TuttiListing]:
        """Parse une carte d'annonce depuis le HTML."""
        try:
            # ID et URL
            link = card.find("a", href=True)
            if not link:
                return None
            
            url = link.get("href", "")
            if not url.startswith("http"):
                url = f"{self.BASE_URL}{url}"
            
            id_match = re.search(r"/(\d+)(?:\?|$)", url)
            listing_id = id_match.group(1) if id_match else str(hash(url))[-8:]
            
            # Titre
            title_el = card.find(["h2", "h3", "span"], class_=re.compile(r"title|heading", re.I))
            title = title_el.get_text(strip=True) if title_el else ""
            
            # Prix
            price = None
            price_el = card.find(class_=re.compile(r"price", re.I))
            if price_el:
                price_text = price_el.get_text(strip=True)
                price_match = re.search(r"[\d']+", price_text.replace("'", "").replace(" ", ""))
                if price_match:
                    try:
                        price = float(price_match.group())
                    except:
                        pass
            
            # Localisation
            location_el = card.find(class_=re.compile(r"location|place", re.I))
            city = ""
            zip_code = ""
            if location_el:
                loc_text = location_el.get_text(strip=True)
                npa_match = re.match(r"(\d{4})\s*(.+)", loc_text)
                if npa_match:
                    zip_code = npa_match.group(1)
                    city = npa_match.group(2).strip()
                else:
                    city = loc_text
            
            # Images
            images = []
            img_el = card.find("img", src=True)
            if img_el:
                img_src = img_el.get("src", "") or img_el.get("data-src", "")
                if img_src:
                    images.append(img_src)
            
            # Détection particulier
            seller_type, is_private, agency_indicators = detect_seller_type(title, "")
            
            return TuttiListing(
                id=listing_id,
                title=title,
                price=price,
                city=city,
                zip_code=zip_code,
                canton=region.upper()[:2] if region else "",
                region=region,
                url=url,
                images=images,
                seller_type=seller_type,
                is_private=is_private,
                agency_indicators=agency_indicators,
            )
            
        except Exception as e:
            scraping_logger.debug(f"[Tutti] Erreur parsing carte: {e}")
            return None

    async def search_real_estate(
        self,
        canton: str = "",
        transaction_type: str = "vente",
        property_type: str = "appartement",
        price_min: Optional[int] = None,
        price_max: Optional[int] = None,
        only_private: bool = True,
        limit: int = 100,
    ) -> List[TuttiListing]:
        """
        Recherche des annonces immobilières sur Tutti.
        
        Args:
            canton: Code canton ou nom
            transaction_type: "vente" ou "location"
            property_type: "appartement", "maison", etc.
            price_min: Prix minimum
            price_max: Prix maximum
            only_private: Ne garder que les particuliers
            limit: Nombre maximum d'annonces
            
        Returns:
            Liste de TuttiListing
        """
        # Déterminer la catégorie
        cat_key = f"{property_type}_{transaction_type}" if property_type else "immobilier"
        category = self.CATEGORIES.get(cat_key, self.CATEGORIES["immobilier"])
        
        # Région
        region = self.REGIONS.get(canton.lower()) or self.REGIONS.get(canton.upper(), "")
        
        # URL de base
        url_parts = [self.BASE_URL, category]
        if region:
            url_parts.append(region)
        
        base_url = "/".join(url_parts)
        
        scraping_logger.info(f"[Tutti] Recherche: {canton or 'Suisse'} ({transaction_type}) limit={limit}")

        all_listings = []
        page = 1
        max_pages = (limit // 30) + 2

        while len(all_listings) < limit and page <= max_pages:
            page_url = base_url if page == 1 else f"{base_url}?page={page}"
            
            try:
                html = await self._fetch_page(page_url)
                
                # Essayer d'extraire les données JSON d'abord
                json_data = self._extract_json_data(html)
                
                if json_data:
                    # Naviguer dans la structure Next.js
                    props = json_data.get("props", {})
                    page_props = props.get("pageProps", {})
                    
                    # Les listings peuvent être à différents endroits
                    items = (
                        page_props.get("listings", []) or
                        page_props.get("items", []) or
                        page_props.get("results", []) or
                        page_props.get("ads", [])
                    )
                    
                    if not items:
                        # Essayer d'autres chemins
                        initial_data = page_props.get("initialData", {})
                        items = initial_data.get("listings", []) or initial_data.get("items", [])
                    
                    new_count = 0
                    for item in items:
                        listing = self._parse_listing_from_json(item, region)
                        if listing:
                            if only_private and not listing.is_private:
                                continue
                            all_listings.append(listing)
                            new_count += 1
                    
                    if new_count > 0:
                        scraping_logger.debug(f"[Tutti] Page {page}: {new_count} annonces (JSON)")
                    else:
                        # Fallback HTML
                        soup = BeautifulSoup(html, "html.parser")
                        cards = soup.find_all("article") or soup.find_all(class_=re.compile(r"listing|item", re.I))
                        
                        for card in cards:
                            listing = self._parse_listing_card(card, region)
                            if listing:
                                if only_private and not listing.is_private:
                                    continue
                                all_listings.append(listing)
                                new_count += 1
                        
                        scraping_logger.debug(f"[Tutti] Page {page}: {new_count} annonces (HTML)")
                else:
                    # Pas de JSON, parser le HTML
                    soup = BeautifulSoup(html, "html.parser")
                    cards = soup.find_all("article") or soup.find_all(class_=re.compile(r"listing|item", re.I))
                    
                    new_count = 0
                    for card in cards:
                        listing = self._parse_listing_card(card, region)
                        if listing:
                            if only_private and not listing.is_private:
                                continue
                            all_listings.append(listing)
                            new_count += 1
                    
                    scraping_logger.debug(f"[Tutti] Page {page}: {new_count} annonces")
                
                if new_count == 0:
                    break
                    
                page += 1
                await random_delay(0.5, 1.5)

            except TuttiError:
                raise
            except Exception as e:
                scraping_logger.error(f"[Tutti] Erreur page {page}: {e}")
                break

        # Filtrer par prix
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

        scraping_logger.info(f"[Tutti] {len(all_listings)} annonces trouvées")
        return all_listings[:limit]


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

async def scrape_tutti(
    canton: str = "GE",
    transaction_type: str = "vente",
    property_type: str = "appartement",
    only_private: bool = True,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """
    Fonction helper pour scraper Tutti.
    
    Args:
        canton: Code canton
        transaction_type: "vente" ou "location"
        property_type: "appartement", "maison", etc.
        only_private: Ne garder que les particuliers
        limit: Nombre max
        
    Returns:
        Liste de résultats au format Prospect
    """
    async with TuttiScraper() as scraper:
        listings = await scraper.search_real_estate(
            canton=canton,
            transaction_type=transaction_type,
            property_type=property_type,
            only_private=only_private,
            limit=limit,
        )
        return [l.to_prospect_format() for l in listings]
