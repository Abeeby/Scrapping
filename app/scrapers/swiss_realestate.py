# =============================================================================
# SWISS REAL ESTATE SCRAPER - Alternative via sources publiques
# =============================================================================
# Utilise des sources publiques suisses plus stables:
# - GeoAdmin (Swisstopo) pour les données géographiques
# - Registre du commerce (Zefix) pour les agences
# - API cantonales publiques
# =============================================================================

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from app.core.logger import scraping_logger
from app.scrapers.antibot import StealthSession, random_delay


@dataclass
class SwissProperty:
    """Représentation d'une propriété suisse."""
    
    id: str
    egrid: str = ""  # Numéro RF suisse
    address: str = ""
    zip_code: str = ""
    city: str = ""
    canton: str = ""
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    owner_name: str = ""
    owner_type: str = ""  # private, company
    property_type: str = ""
    surface_m2: Optional[float] = None
    year_built: Optional[int] = None
    source: str = ""
    
    def to_prospect_format(self) -> Dict[str, Any]:
        return {
            "id": f"swiss-{self.id}",
            "nom": self.owner_name,
            "prenom": "",
            "adresse": self.address,
            "code_postal": self.zip_code,
            "ville": self.city,
            "canton": self.canton,
            "telephone": "",
            "email": "",
            "type_bien": self.property_type,
            "surface": self.surface_m2 or 0,
            "egrid": self.egrid,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "source": self.source or "SwissRealestate",
        }


class SwissRealestateClient:
    """
    Client pour accéder aux données immobilières suisses via APIs publiques.
    
    Sources utilisées:
    - api3.geo.admin.ch (Swisstopo)
    - API cantonales (RF Genève, RF Vaud)
    """
    
    GEOADMIN_BASE = "https://api3.geo.admin.ch"
    
    def __init__(self, timeout: int = 30):
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()
            self._session = None
    
    async def search_by_location(
        self,
        city: str,
        canton: str = "",
        limit: int = 50,
    ) -> List[SwissProperty]:
        """
        Recherche des propriétés par localisation via GeoAdmin.
        
        Args:
            city: Ville ou commune
            canton: Code canton (GE, VD)
            limit: Nombre max de résultats
        
        Returns:
            Liste de propriétés
        """
        results = []
        
        try:
            # 1. Rechercher les adresses via GeoAdmin
            search_url = f"{self.GEOADMIN_BASE}/rest/services/api/SearchServer"
            params = {
                "searchText": city,
                "type": "locations",
                "limit": limit,
                "sr": "2056",  # Système de coordonnées suisse
            }
            
            async with self._session.get(search_url, params=params) as resp:
                if resp.status != 200:
                    scraping_logger.warning(f"[SwissRealestate] GeoAdmin search failed: {resp.status}")
                    return results
                
                data = await resp.json()
            
            locations = data.get("results", [])
            scraping_logger.info(f"[SwissRealestate] Found {len(locations)} locations for '{city}'")
            
            # 2. Pour chaque location, récupérer les détails
            for loc in locations[:limit]:
                try:
                    attrs = loc.get("attrs", {})
                    
                    # Extraire l'adresse
                    label = attrs.get("label", "")
                    detail = attrs.get("detail", "")
                    
                    # Parser l'adresse suisse
                    address_parts = self._parse_swiss_address(label)
                    
                    prop = SwissProperty(
                        id=str(attrs.get("featureId", "")),
                        address=address_parts.get("street", label),
                        zip_code=address_parts.get("zip_code", ""),
                        city=address_parts.get("city", city),
                        canton=canton or address_parts.get("canton", ""),
                        latitude=attrs.get("lat"),
                        longitude=attrs.get("lon"),
                        source="GeoAdmin",
                    )
                    
                    results.append(prop)
                    
                except Exception as e:
                    scraping_logger.debug(f"[SwissRealestate] Error parsing location: {e}")
                    continue
            
        except Exception as e:
            scraping_logger.error(f"[SwissRealestate] Search error: {e}")
        
        return results
    
    async def get_parcels_in_area(
        self,
        lat: float,
        lon: float,
        radius_m: int = 500,
        limit: int = 50,
    ) -> List[SwissProperty]:
        """
        Récupère les parcelles dans un rayon donné.
        
        Args:
            lat: Latitude
            lon: Longitude
            radius_m: Rayon en mètres
            limit: Nombre max de résultats
        
        Returns:
            Liste de propriétés
        """
        results = []
        
        try:
            # Convertir en coordonnées suisses LV95
            # (approximation simple pour Suisse romande)
            e = 2600000 + (lon - 6.0) * 111000
            n = 1200000 + (lat - 46.0) * 111000
            
            # Définir la zone de recherche
            tolerance = radius_m
            bbox = f"{e-tolerance},{n-tolerance},{e+tolerance},{n+tolerance}"
            
            # Requête GeoAdmin identify
            identify_url = f"{self.GEOADMIN_BASE}/rest/services/api/MapServer/identify"
            params = {
                "geometry": f"{e},{n}",
                "geometryType": "esriGeometryPoint",
                "layers": "all:ch.swisstopo.amtliches-strassenverzeichnis",
                "tolerance": tolerance,
                "mapExtent": bbox,
                "imageDisplay": "1000,1000,96",
                "returnGeometry": "true",
                "sr": "2056",
            }
            
            async with self._session.get(identify_url, params=params) as resp:
                if resp.status != 200:
                    return results
                
                data = await resp.json()
            
            for result in data.get("results", [])[:limit]:
                try:
                    attrs = result.get("attributes", {})
                    
                    prop = SwissProperty(
                        id=str(attrs.get("id", "")),
                        address=attrs.get("str_name", ""),
                        zip_code=str(attrs.get("plz", "")),
                        city=attrs.get("gdename", ""),
                        canton=attrs.get("kanton", ""),
                        latitude=lat,
                        longitude=lon,
                        source="GeoAdmin-Parcels",
                    )
                    
                    results.append(prop)
                    
                except Exception as e:
                    scraping_logger.debug(f"[SwissRealestate] Error parsing parcel: {e}")
                    continue
            
        except Exception as e:
            scraping_logger.error(f"[SwissRealestate] Parcel search error: {e}")
        
        return results
    
    async def search_addresses_in_commune(
        self,
        commune: str,
        canton: str,
        limit: int = 100,
    ) -> List[SwissProperty]:
        """
        Récupère toutes les adresses d'une commune via GeoAdmin.
        
        C'est une alternative au scraping des sites immobiliers.
        
        Args:
            commune: Nom de la commune
            canton: Code canton
            limit: Nombre max
            
        Returns:
            Liste de propriétés
        """
        results = []
        
        try:
            # Recherche via l'API des adresses fédérales
            url = f"{self.GEOADMIN_BASE}/rest/services/api/SearchServer"
            params = {
                "searchText": f"{commune} {canton}",
                "type": "locations",
                "origins": "address",
                "limit": limit,
            }
            
            async with self._session.get(url, params=params) as resp:
                if resp.status != 200:
                    return results
                
                data = await resp.json()
            
            for item in data.get("results", [])[:limit]:
                try:
                    attrs = item.get("attrs", {})
                    label = attrs.get("label", "")
                    
                    # Parser l'adresse
                    parts = self._parse_swiss_address(label)
                    
                    prop = SwissProperty(
                        id=str(attrs.get("featureId", "")),
                        address=parts.get("street", ""),
                        zip_code=parts.get("zip_code", ""),
                        city=parts.get("city", commune),
                        canton=canton,
                        latitude=attrs.get("lat"),
                        longitude=attrs.get("lon"),
                        source="GeoAdmin-Addresses",
                    )
                    
                    if prop.address:
                        results.append(prop)
                    
                except Exception as e:
                    continue
            
            scraping_logger.info(f"[SwissRealestate] Found {len(results)} addresses in {commune}")
            
        except Exception as e:
            scraping_logger.error(f"[SwissRealestate] Commune search error: {e}")
        
        return results
    
    def _parse_swiss_address(self, text: str) -> Dict[str, str]:
        """Parse une adresse suisse."""
        result = {
            "street": "",
            "zip_code": "",
            "city": "",
            "canton": "",
        }
        
        if not text:
            return result
        
        # Nettoyer HTML
        text = re.sub(r"<[^>]+>", " ", text).strip()
        
        # Pattern: "Rue 123, 1234 Ville"
        match = re.search(r"^(.+?),?\s*(\d{4})\s+(.+?)(?:\s+\(([A-Z]{2})\))?$", text)
        if match:
            result["street"] = match.group(1).strip()
            result["zip_code"] = match.group(2)
            result["city"] = match.group(3).strip()
            if match.group(4):
                result["canton"] = match.group(4)
            return result
        
        # Pattern simple: "1234 Ville"
        match = re.search(r"(\d{4})\s+(.+)", text)
        if match:
            result["zip_code"] = match.group(1)
            result["city"] = match.group(2).strip()
            return result
        
        result["street"] = text
        return result


# Helper function
async def search_swiss_properties(
    city: str,
    canton: str = "",
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """
    Recherche des propriétés suisses via APIs publiques.
    
    Alternative stable aux scrapers Immoscout24/Homegate.
    """
    async with SwissRealestateClient() as client:
        properties = await client.search_by_location(city, canton, limit)
        return [p.to_prospect_format() for p in properties]


async def search_commune_addresses(
    commune: str,
    canton: str,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """
    Récupère les adresses d'une commune.
    
    Alternative au mass scraper quand Search.ch est bloqué.
    """
    async with SwissRealestateClient() as client:
        properties = await client.search_addresses_in_commune(commune, canton, limit)
        return [p.to_prospect_format() for p in properties]
