# =============================================================================
# SCRAPER IMMOSCOUT24.CH - Annonces immobilières suisses
# =============================================================================
# Extraction des données JSON cachées dans les balises script (INITIAL_STATE)
# Similaire à l'approche Comparis mais sans besoin de Playwright
# =============================================================================

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import aiohttp
from bs4 import BeautifulSoup

from app.core.logger import scraping_logger


class Immoscout24Error(Exception):
    """Erreur explicite Immoscout24 (réseau, blocage, parsing)."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


@dataclass
class PropertyListing:
    """Représentation d'une annonce immobilière."""

    id: str
    title: str
    property_type: str  # apartment, house, etc.
    transaction_type: str  # rent, buy
    price: Optional[float] = None
    price_unit: str = "CHF"
    rooms: Optional[float] = None
    surface_living: Optional[float] = None  # m²
    surface_land: Optional[float] = None  # m²
    floor: Optional[int] = None
    address: str = ""
    zip_code: str = ""
    city: str = ""
    canton: str = ""
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    description: str = ""
    features: List[str] = field(default_factory=list)
    images: List[str] = field(default_factory=list)
    agency_name: str = ""
    agency_phone: str = ""
    agency_email: str = ""
    url: str = ""
    available_from: str = ""
    year_built: Optional[int] = None
    last_renovation: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "property_type": self.property_type,
            "transaction_type": self.transaction_type,
            "price": self.price,
            "price_unit": self.price_unit,
            "rooms": self.rooms,
            "surface_living": self.surface_living,
            "surface_land": self.surface_land,
            "floor": self.floor,
            "address": self.address,
            "zip_code": self.zip_code,
            "city": self.city,
            "canton": self.canton,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "description": self.description,
            "features": self.features,
            "images": self.images[:5],  # Limiter les images
            "agency_name": self.agency_name,
            "agency_phone": self.agency_phone,
            "agency_email": self.agency_email,
            "url": self.url,
            "available_from": self.available_from,
            "year_built": self.year_built,
            "last_renovation": self.last_renovation,
        }

    def to_prospect_format(self) -> Dict[str, Any]:
        """Convertit en format compatible avec le modèle Prospect."""
        return {
            "id": f"immo24-{self.id}",
            "nom": self.agency_name or self.title[:50],
            "prenom": "",
            "adresse": self.address,
            "code_postal": self.zip_code,
            "ville": self.city,
            "canton": self.canton,
            "telephone": self.agency_phone,
            "email": self.agency_email,
            "type_bien": self.property_type,
            "surface": self.surface_living or 0,
            "prix": self.price or 0,
            "lien_rf": self.url,
            "url_annonce": self.url,
            "titre": self.title,
            "pieces": self.rooms,
            "surface_habitable_m2": self.surface_living,
            "surface_terrain_m2": self.surface_land,
            "annee_construction": self.year_built,
            "annee_renovation": self.last_renovation,
            "disponibilite": self.available_from,
            "prix_vente_chf": int(self.price) if self.price and self.transaction_type == "buy" else None,
            "source": "Immoscout24",
        }


class Immoscout24Scraper:
    """
    Scraper pour Immoscout24.ch.
    
    Extrait les données JSON cachées dans les balises script (window.__INITIAL_STATE__).
    
    Usage:
        async with Immoscout24Scraper() as scraper:
            # Recherche d'annonces
            listings = await scraper.search("Genève", transaction_type="rent", limit=50)
            
            # Détails d'une annonce
            details = await scraper.get_listing_details("https://www.immoscout24.ch/...")
    """

    BASE_URL = "https://www.immoscout24.ch"
    
    # Mapping des types de transaction
    TRANSACTION_TYPES = {
        "rent": "rent",
        "buy": "buy",
        "location": "rent",
        "vente": "buy",
        "achat": "buy",
    }
    
    # Mapping des types de biens
    PROPERTY_TYPES = {
        "apartment": "apartment",
        "house": "house",
        "parking": "parking",
        "commercial": "commercial",
        "land": "land",
        "appartement": "apartment",
        "maison": "house",
        "terrain": "land",
    }

    def __init__(self, timeout: int = 30):
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self._session = aiohttp.ClientSession(
            timeout=self.timeout,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "fr-CH,fr;q=0.9,en;q=0.8",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                "Referer": "https://www.immoscout24.ch/",
            },
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()
            self._session = None

    async def _fetch_page(self, url: str) -> str:
        """Récupère le contenu HTML d'une page."""
        if not self._session:
            raise Immoscout24Error("Session non initialisée. Utilisez 'async with'.")

        try:
            async with self._session.get(url) as response:
                if response.status == 200:
                    return await response.text()
                elif response.status == 403:
                    raise Immoscout24Error("Accès bloqué par Immoscout24 (anti-bot).", status_code=403)
                elif response.status == 429:
                    raise Immoscout24Error("Rate limit Immoscout24 atteint.", status_code=429)
                else:
                    raise Immoscout24Error(f"Erreur HTTP {response.status}", status_code=response.status)
        except aiohttp.ClientError as e:
            raise Immoscout24Error(f"Erreur réseau: {e}") from e

    def _extract_initial_state(self, html: str) -> Optional[Dict]:
        """Extrait le JSON INITIAL_STATE depuis le HTML."""
        # Chercher le script contenant INITIAL_STATE
        pattern = r'window\.__INITIAL_STATE__\s*=\s*(.+?)(?:;\s*</script>|;\s*window\.)'
        match = re.search(pattern, html, re.DOTALL)
        
        if not match:
            # Essayer un autre pattern
            pattern2 = r'<script[^>]*>window\.__INITIAL_STATE__\s*=\s*(.+?)</script>'
            match = re.search(pattern2, html, re.DOTALL)

        if not match:
            return None

        json_str = match.group(1).strip()
        
        # Nettoyer le JSON
        json_str = json_str.rstrip(';')
        json_str = json_str.replace('undefined', 'null')
        
        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            scraping_logger.warning(f"[Immoscout24] Erreur parsing JSON: {e}")
            return None

    def _parse_listing(self, data: Dict, transaction_type: str = "rent") -> PropertyListing:
        """Parse les données d'une annonce."""
        # ID
        listing_id = str(data.get("id", ""))
        
        # Titre
        title = data.get("title", "") or data.get("localization", {}).get("de", {}).get("text", {}).get("title", "")
        
        # Type de bien
        prop_type_id = data.get("propertyTypeId", 0)
        prop_type_map = {1: "apartment", 2: "house", 3: "parking", 4: "commercial", 5: "land"}
        property_type = prop_type_map.get(prop_type_id, "apartment")
        
        # Prix
        price = data.get("price") or data.get("grossPrice") or data.get("netPrice")
        price_unit = "CHF"
        
        # Pièces et surface
        rooms = data.get("numberOfRooms")
        surface_living = data.get("surfaceLiving")
        surface_land = data.get("surfaceProperty") or data.get("surfaceLand")
        
        # Adresse
        address = data.get("street", "")
        zip_code = str(data.get("zip", "") or data.get("zipId", ""))
        city = data.get("cityName", "") or data.get("city", "")
        canton = data.get("stateShort", "") or data.get("state", "")[:2] if data.get("state") else ""
        
        # Coordonnées
        latitude = data.get("latitude")
        longitude = data.get("longitude")
        
        # Agence
        agency = data.get("agency", {})
        agency_name = agency.get("companyName1", "") or agency.get("companyName", "")
        agency_phone = agency.get("companyPhoneBusiness", "") or agency.get("phone", "")
        
        # URL
        url = data.get("propertyUrl", "") or data.get("propertyDetailUrl", "")
        if url and not url.startswith("http"):
            url = f"{self.BASE_URL}{url}"
        
        # Disponibilité
        available_from = data.get("availableFromFormatted", "") or data.get("availableFrom", "")
        
        # Images
        images = []
        for img in data.get("images", [])[:5]:
            img_url = img.get("url", "")
            if img_url:
                # Remplacer les placeholders
                img_url = img_url.replace("{width}", "800").replace("{height}", "600")
                img_url = img_url.replace("{resizemode}", "4").replace("{quality}", "80")
                images.append(img_url)

        return PropertyListing(
            id=listing_id,
            title=title,
            property_type=property_type,
            transaction_type=transaction_type,
            price=float(price) if price else None,
            price_unit=price_unit,
            rooms=float(rooms) if rooms else None,
            surface_living=float(surface_living) if surface_living else None,
            surface_land=float(surface_land) if surface_land else None,
            address=address,
            zip_code=zip_code,
            city=city,
            canton=canton,
            latitude=float(latitude) if latitude else None,
            longitude=float(longitude) if longitude else None,
            agency_name=agency_name,
            agency_phone=agency_phone,
            url=url,
            available_from=available_from,
            images=images,
        )

    async def search(
        self,
        location: str,
        transaction_type: str = "rent",
        property_type: str = "apartment",
        limit: int = 100,
        price_min: Optional[int] = None,
        price_max: Optional[int] = None,
        rooms_min: Optional[float] = None,
    ) -> List[PropertyListing]:
        """
        Recherche des annonces immobilières.
        
        Args:
            location: Ville ou région (ex: "Genève", "Lausanne")
            transaction_type: "rent" ou "buy"
            property_type: "apartment", "house", etc.
            limit: Nombre maximum d'annonces
            price_min: Prix minimum
            price_max: Prix maximum
            rooms_min: Nombre minimum de pièces
            
        Returns:
            Liste de PropertyListing
        """
        # Normaliser le type de transaction
        trans_type = self.TRANSACTION_TYPES.get(transaction_type.lower(), "rent")
        
        # Normaliser le type de bien
        prop_type = self.PROPERTY_TYPES.get(property_type.lower(), "apartment")
        
        # Construire l'URL de recherche
        location_slug = location.lower().replace(" ", "-").replace("è", "e").replace("é", "e")
        url = f"{self.BASE_URL}/en/real-estate/{trans_type}/city-{location_slug}"
        
        # Paramètres optionnels
        params = []
        if price_min:
            params.append(f"nf={price_min}")
        if price_max:
            params.append(f"nh={price_max}")
        if rooms_min:
            params.append(f"nrf={rooms_min}")
        
        if params:
            url += "?" + "&".join(params)

        scraping_logger.info(f"[Immoscout24] Recherche: {location} ({trans_type}) limit={limit}")

        all_listings = []
        page = 1
        max_pages = (limit // 20) + 1  # Environ 20 résultats par page

        while len(all_listings) < limit and page <= max_pages:
            page_url = url if page == 1 else f"{url}{'&' if '?' in url else '?'}pn={page}"
            
            try:
                html = await self._fetch_page(page_url)
                data = self._extract_initial_state(html)
                
                if not data:
                    scraping_logger.warning(f"[Immoscout24] Pas de données JSON page {page}")
                    break

                # Extraire les annonces
                result_data = data.get("resultList", {}).get("search", {}).get("fullSearch", {}).get("result", {})
                listings_data = result_data.get("listings", [])
                
                if not listings_data:
                    break

                for item in listings_data:
                    try:
                        listing = self._parse_listing(item, trans_type)
                        all_listings.append(listing)
                    except Exception as e:
                        scraping_logger.warning(f"[Immoscout24] Erreur parsing listing: {e}")
                        continue

                scraping_logger.debug(f"[Immoscout24] Page {page}: {len(listings_data)} annonces")
                page += 1

            except Immoscout24Error:
                raise
            except Exception as e:
                scraping_logger.error(f"[Immoscout24] Erreur page {page}: {e}")
                break

        scraping_logger.info(f"[Immoscout24] {len(all_listings)} annonces trouvées")
        return all_listings[:limit]

    async def get_listing_details(self, url: str) -> Optional[PropertyListing]:
        """
        Récupère les détails complets d'une annonce.
        
        Args:
            url: URL complète de l'annonce
            
        Returns:
            PropertyListing avec tous les détails
        """
        if not url or "immoscout24.ch" not in url:
            raise Immoscout24Error("URL Immoscout24 invalide.", status_code=400)

        scraping_logger.info(f"[Immoscout24] Détails annonce: {url}")

        try:
            html = await self._fetch_page(url)
            data = self._extract_initial_state(html)
            
            if not data:
                return None

            # Les détails sont dans listing.listing
            listing_data = data.get("listing", {}).get("listing", {})
            
            if not listing_data:
                return None

            # Déterminer le type de transaction depuis l'URL
            trans_type = "buy" if "/buy/" in url or "/kaufen/" in url else "rent"

            listing = self._parse_listing(listing_data, trans_type)
            
            # Enrichir avec les détails supplémentaires
            localization = listing_data.get("localization", {})
            lang_data = localization.get("fr", {}) or localization.get("de", {}) or {}
            text_data = lang_data.get("text", {})
            
            if text_data.get("description"):
                listing.description = text_data["description"][:500]

            # Caractéristiques
            characteristics = listing_data.get("characteristics", [])
            listing.features = [c.get("label", "") for c in characteristics if c.get("label")]

            listing.url = url

            return listing

        except Immoscout24Error:
            raise
        except Exception as e:
            scraping_logger.error(f"[Immoscout24] Erreur détails: {e}")
            raise Immoscout24Error(f"Erreur récupération détails: {e}")


# Fonction helper
async def search_immoscout24(
    location: str,
    transaction_type: str = "rent",
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """
    Helper pour rechercher sur Immoscout24 sans context manager.
    
    Returns:
        Liste de dictionnaires au format prospect-compatible
    """
    async with Immoscout24Scraper() as scraper:
        listings = await scraper.search(location, transaction_type=transaction_type, limit=limit)
        return [l.to_prospect_format() for l in listings]

