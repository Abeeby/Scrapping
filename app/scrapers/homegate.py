# =============================================================================
# SCRAPER HOMEGATE.CH - Annonces immobilières suisses
# =============================================================================
# Plus grand portail immobilier suisse
# Extraction des données JSON depuis les balises script __NEXT_DATA__
# =============================================================================

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import aiohttp
from bs4 import BeautifulSoup

from app.core.logger import scraping_logger


class HomegateError(Exception):
    """Erreur explicite Homegate (réseau, blocage, parsing)."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


@dataclass
class HomegateProperty:
    """Représentation d'une annonce Homegate."""

    id: str
    title: str
    property_type: str
    transaction_type: str  # rent, buy
    price: Optional[float] = None
    price_unit: str = "CHF"
    price_interval: str = ""  # month, year, one-time
    rooms: Optional[float] = None
    surface_living: Optional[float] = None
    surface_usable: Optional[float] = None
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
    advertiser_name: str = ""
    advertiser_phone: str = ""
    advertiser_type: str = ""  # agency, private
    url: str = ""
    available_from: str = ""
    year_built: Optional[int] = None
    listing_type: str = ""  # primary, secondary

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "property_type": self.property_type,
            "transaction_type": self.transaction_type,
            "price": self.price,
            "price_unit": self.price_unit,
            "price_interval": self.price_interval,
            "rooms": self.rooms,
            "surface_living": self.surface_living,
            "surface_usable": self.surface_usable,
            "floor": self.floor,
            "address": self.address,
            "zip_code": self.zip_code,
            "city": self.city,
            "canton": self.canton,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "description": self.description[:500] if self.description else "",
            "features": self.features[:10],
            "images": self.images[:5],
            "advertiser_name": self.advertiser_name,
            "advertiser_phone": self.advertiser_phone,
            "advertiser_type": self.advertiser_type,
            "url": self.url,
            "available_from": self.available_from,
            "year_built": self.year_built,
        }

    def to_prospect_format(self) -> Dict[str, Any]:
        """Convertit en format compatible avec le modèle Prospect."""
        return {
            "id": f"homegate-{self.id}",
            "nom": self.advertiser_name or self.title[:50],
            "prenom": "",
            "adresse": self.address,
            "code_postal": self.zip_code,
            "ville": self.city,
            "canton": self.canton,
            "telephone": self.advertiser_phone,
            "email": "",
            "type_bien": self.property_type,
            "surface": self.surface_living or 0,
            "prix": self.price or 0,
            "lien_rf": self.url,
            "url_annonce": self.url,
            "titre": self.title,
            "pieces": self.rooms,
            "surface_habitable_m2": self.surface_living,
            "surface_terrain_m2": self.surface_usable,
            "annee_construction": self.year_built,
            "disponibilite": self.available_from,
            "prix_vente_chf": int(self.price) if self.price and self.transaction_type == "buy" else None,
            "source": "Homegate",
        }


class HomegateScraper:
    """
    Scraper pour Homegate.ch.
    
    Extrait les données JSON depuis __NEXT_DATA__ (Next.js).
    
    Usage:
        async with HomegateScraper() as scraper:
            # Recherche d'annonces
            listings = await scraper.search("geneve", transaction_type="rent", limit=50)
            
            # Détails d'une annonce  
            details = await scraper.get_listing_details("https://www.homegate.ch/...")
    """

    BASE_URL = "https://www.homegate.ch"
    
    TRANSACTION_TYPES = {
        "rent": "rent",
        "buy": "buy",
        "location": "rent",
        "vente": "buy",
        "achat": "buy",
        "louer": "rent",
        "acheter": "buy",
    }
    
    PROPERTY_TYPES = {
        "apartment": "apartment",
        "house": "house",
        "parking": "parking-space",
        "commercial": "commercial",
        "land": "plot",
        "appartement": "apartment",
        "maison": "house",
        "terrain": "plot",
    }

    def __init__(self, timeout: int = 30, language: str = "fr"):
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.language = language
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self._session = aiohttp.ClientSession(
            timeout=self.timeout,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": f"{self.language}-CH,{self.language};q=0.9,en;q=0.8",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                "Referer": "https://www.homegate.ch/",
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
            raise HomegateError("Session non initialisée. Utilisez 'async with'.")

        try:
            async with self._session.get(url) as response:
                if response.status == 200:
                    return await response.text()
                elif response.status == 403:
                    raise HomegateError("Accès bloqué par Homegate (anti-bot).", status_code=403)
                elif response.status == 429:
                    raise HomegateError("Rate limit Homegate atteint.", status_code=429)
                else:
                    raise HomegateError(f"Erreur HTTP {response.status}", status_code=response.status)
        except aiohttp.ClientError as e:
            raise HomegateError(f"Erreur réseau: {e}") from e

    def _extract_next_data(self, html: str) -> Optional[Dict]:
        """Extrait le JSON __NEXT_DATA__ depuis le HTML (Next.js)."""
        soup = BeautifulSoup(html, "html.parser")
        
        # Chercher le script __NEXT_DATA__
        script = soup.find("script", {"id": "__NEXT_DATA__"})
        
        if not script or not script.string:
            # Fallback: chercher via regex
            pattern = r'<script id="__NEXT_DATA__"[^>]*>(.+?)</script>'
            match = re.search(pattern, html, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(1))
                except json.JSONDecodeError:
                    return None
            return None

        try:
            return json.loads(script.string)
        except json.JSONDecodeError as e:
            scraping_logger.warning(f"[Homegate] Erreur parsing JSON: {e}")
            return None

    def _parse_listing(self, data: Dict, transaction_type: str = "rent") -> HomegateProperty:
        """Parse les données d'une annonce Homegate."""
        # ID
        listing_id = str(data.get("id", "") or data.get("listingId", ""))
        
        # Titre
        title = data.get("title", "") or data.get("listingTitle", "")
        
        # Type de bien
        listing_type_data = data.get("listingType", {})
        if isinstance(listing_type_data, dict):
            property_type = listing_type_data.get("type", "apartment")
        else:
            property_type = "apartment"
        
        # Prix
        price_data = data.get("prices", {}) or data.get("price", {})
        if isinstance(price_data, dict):
            price = price_data.get("rent", {}).get("gross") or price_data.get("buy", {}).get("price")
            price_interval = price_data.get("rent", {}).get("interval", "")
        elif isinstance(price_data, (int, float)):
            price = price_data
            price_interval = ""
        else:
            price = None
            price_interval = ""
        
        # Pièces et surface
        characteristics = data.get("characteristics", {})
        if isinstance(characteristics, dict):
            rooms = characteristics.get("numberOfRooms")
            surface_living = characteristics.get("livingSpace")
            surface_usable = characteristics.get("usableFloorSpace") or characteristics.get("lotSize")
            floor = characteristics.get("floor")
        else:
            rooms = data.get("numberOfRooms")
            surface_living = data.get("surfaceLiving")
            surface_usable = data.get("surfaceUsable")
            floor = data.get("floor")
        
        # Adresse
        address_data = data.get("address", {})
        if isinstance(address_data, dict):
            street = address_data.get("street", "")
            house_number = address_data.get("houseNumber", "")
            address = f"{street} {house_number}".strip() if street else ""
            zip_code = str(address_data.get("postalCode", "") or address_data.get("zipCode", ""))
            city = address_data.get("locality", "") or address_data.get("city", "")
            canton = address_data.get("region", "") or address_data.get("canton", "")
        else:
            address = data.get("street", "")
            zip_code = str(data.get("zipCode", "") or data.get("postalCode", ""))
            city = data.get("city", "") or data.get("locality", "")
            canton = data.get("canton", "")
        
        # Coordonnées
        geo_data = data.get("geoLocation", {}) or data.get("coordinates", {})
        latitude = geo_data.get("latitude") or geo_data.get("lat")
        longitude = geo_data.get("longitude") or geo_data.get("lng")
        
        # Annonceur
        advertiser_data = data.get("lister", {}) or data.get("advertiser", {})
        if isinstance(advertiser_data, dict):
            advertiser_name = advertiser_data.get("company", "") or advertiser_data.get("name", "")
            advertiser_phone = advertiser_data.get("phone", "") or advertiser_data.get("phoneNumber", "")
            advertiser_type = advertiser_data.get("type", "")
        else:
            advertiser_name = ""
            advertiser_phone = ""
            advertiser_type = ""
        
        # URL
        url = data.get("url", "") or data.get("detailUrl", "")
        if url and not url.startswith("http"):
            url = f"{self.BASE_URL}{url}"
        
        # Images
        images = []
        images_data = data.get("images", []) or data.get("pictures", [])
        for img in images_data[:5]:
            if isinstance(img, dict):
                img_url = img.get("url", "") or img.get("src", "")
            elif isinstance(img, str):
                img_url = img
            else:
                continue
            if img_url:
                images.append(img_url)

        return HomegateProperty(
            id=listing_id,
            title=title,
            property_type=property_type,
            transaction_type=transaction_type,
            price=float(price) if price else None,
            price_interval=price_interval,
            rooms=float(rooms) if rooms else None,
            surface_living=float(surface_living) if surface_living else None,
            surface_usable=float(surface_usable) if surface_usable else None,
            floor=int(floor) if floor else None,
            address=address,
            zip_code=zip_code,
            city=city,
            canton=canton,
            latitude=float(latitude) if latitude else None,
            longitude=float(longitude) if longitude else None,
            advertiser_name=advertiser_name,
            advertiser_phone=advertiser_phone,
            advertiser_type=advertiser_type,
            url=url,
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
    ) -> List[HomegateProperty]:
        """
        Recherche des annonces immobilières sur Homegate.
        
        Args:
            location: Ville ou région (ex: "geneve", "lausanne")
            transaction_type: "rent" ou "buy"
            property_type: "apartment", "house", etc.
            limit: Nombre maximum d'annonces
            price_min: Prix minimum
            price_max: Prix maximum
            rooms_min: Nombre minimum de pièces
            
        Returns:
            Liste de HomegateProperty
        """
        # Normaliser le type de transaction
        trans_type = self.TRANSACTION_TYPES.get(transaction_type.lower(), "rent")
        
        # Normaliser le type de bien
        prop_type = self.PROPERTY_TYPES.get(property_type.lower(), "apartment")
        
        # Construire l'URL de recherche
        location_slug = location.lower().replace(" ", "-").replace("è", "e").replace("é", "e").replace("ü", "u")
        
        # URL format Homegate
        url = f"{self.BASE_URL}/{trans_type}/{prop_type}/city-{location_slug}/matching-list"
        
        # Paramètres optionnels
        params = []
        if price_max:
            if trans_type == "rent":
                params.append(f"ag={price_max}")
            else:
                params.append(f"ab={price_max}")
        if rooms_min:
            params.append(f"ac={rooms_min}")
        
        if params:
            url += "?" + "&".join(params)

        scraping_logger.info(f"[Homegate] Recherche: {location} ({trans_type}) limit={limit}")

        all_listings = []
        page = 1
        max_pages = (limit // 20) + 1

        while len(all_listings) < limit and page <= max_pages:
            page_url = url if page == 1 else f"{url}{'&' if '?' in url else '?'}ep={page}"
            
            try:
                html = await self._fetch_page(page_url)
                data = self._extract_next_data(html)
                
                if not data:
                    scraping_logger.warning(f"[Homegate] Pas de données JSON page {page}")
                    break

                # Naviguer dans la structure Next.js
                props = data.get("props", {})
                page_props = props.get("pageProps", {})
                
                # Les listings peuvent être à différents endroits
                listings_data = (
                    page_props.get("listings", []) or
                    page_props.get("resultList", {}).get("listings", []) or
                    page_props.get("initialData", {}).get("listings", [])
                )
                
                if not listings_data:
                    # Essayer une autre structure
                    search_result = page_props.get("searchResult", {})
                    listings_data = search_result.get("listings", [])
                
                if not listings_data:
                    scraping_logger.debug(f"[Homegate] Aucune annonce trouvée page {page}")
                    break

                for item in listings_data:
                    try:
                        listing = self._parse_listing(item, trans_type)
                        all_listings.append(listing)
                    except Exception as e:
                        scraping_logger.warning(f"[Homegate] Erreur parsing listing: {e}")
                        continue

                scraping_logger.debug(f"[Homegate] Page {page}: {len(listings_data)} annonces")
                page += 1

            except HomegateError:
                raise
            except Exception as e:
                scraping_logger.error(f"[Homegate] Erreur page {page}: {e}")
                break

        scraping_logger.info(f"[Homegate] {len(all_listings)} annonces trouvées")
        return all_listings[:limit]

    async def get_listing_details(self, url: str) -> Optional[HomegateProperty]:
        """
        Récupère les détails complets d'une annonce.
        
        Args:
            url: URL complète de l'annonce
            
        Returns:
            HomegateProperty avec tous les détails
        """
        if not url or "homegate.ch" not in url:
            raise HomegateError("URL Homegate invalide.", status_code=400)

        scraping_logger.info(f"[Homegate] Détails annonce: {url}")

        try:
            html = await self._fetch_page(url)
            data = self._extract_next_data(html)
            
            if not data:
                return None

            # Naviguer vers les données de l'annonce
            props = data.get("props", {})
            page_props = props.get("pageProps", {})
            
            listing_data = (
                page_props.get("listing", {}) or
                page_props.get("listingDetails", {}) or
                page_props.get("initialData", {}).get("listing", {})
            )
            
            if not listing_data:
                return None

            # Déterminer le type de transaction
            trans_type = "buy" if "/buy/" in url or "/acheter/" in url else "rent"

            listing = self._parse_listing(listing_data, trans_type)
            listing.url = url
            
            # Enrichir avec la description
            description = listing_data.get("description", "") or listing_data.get("text", "")
            if isinstance(description, dict):
                description = description.get("text", "") or description.get("description", "")
            listing.description = str(description)[:500] if description else ""
            
            # Caractéristiques
            features_data = listing_data.get("features", []) or listing_data.get("characteristics", {}).get("features", [])
            if isinstance(features_data, list):
                listing.features = [str(f) if not isinstance(f, dict) else f.get("label", "") for f in features_data[:10]]

            return listing

        except HomegateError:
            raise
        except Exception as e:
            scraping_logger.error(f"[Homegate] Erreur détails: {e}")
            raise HomegateError(f"Erreur récupération détails: {e}")


# Fonction helper
async def search_homegate(
    location: str,
    transaction_type: str = "rent",
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """
    Helper pour rechercher sur Homegate sans context manager.
    
    Returns:
        Liste de dictionnaires au format prospect-compatible
    """
    async with HomegateScraper() as scraper:
        listings = await scraper.search(location, transaction_type=transaction_type, limit=limit)
        return [l.to_prospect_format() for l in listings]

