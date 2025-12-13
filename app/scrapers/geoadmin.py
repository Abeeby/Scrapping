# =============================================================================
# CLIENT API GEOADMIN - Swisstopo (Données géographiques suisses)
# =============================================================================
# API officielle: https://api3.geo.admin.ch/
# Documentation: https://api3.geo.admin.ch/services/sdiservices.html
# Gratuit et sans authentification
# =============================================================================

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from app.core.logger import scraping_logger


class GeoAdminError(Exception):
    """Erreur explicite GeoAdmin (réseau, API, parsing)."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


@dataclass
class SwissAddress:
    """Adresse suisse normalisée."""

    street: str
    house_number: str
    zip_code: str
    city: str
    canton: str
    egid: Optional[str] = None  # Identifiant fédéral du bâtiment
    egaid: Optional[str] = None  # Identifiant fédéral de l'adresse
    coordinates: Optional[Tuple[float, float]] = None  # (lon, lat) WGS84
    confidence: float = 0.0  # Score de confiance 0-1

    @property
    def full_address(self) -> str:
        """Retourne l'adresse formatée complète."""
        parts = []
        if self.street:
            parts.append(self.street)
            if self.house_number:
                parts[-1] += f" {self.house_number}"
        if self.zip_code and self.city:
            parts.append(f"{self.zip_code} {self.city}")
        elif self.city:
            parts.append(self.city)
        return ", ".join(parts)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "street": self.street,
            "house_number": self.house_number,
            "zip_code": self.zip_code,
            "city": self.city,
            "canton": self.canton,
            "egid": self.egid,
            "egaid": self.egaid,
            "coordinates": list(self.coordinates) if self.coordinates else None,
            "confidence": self.confidence,
            "full_address": self.full_address,
        }


@dataclass
class GeoLocation:
    """Localisation géographique."""

    latitude: float
    longitude: float
    address: Optional[str] = None
    label: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "address": self.address,
            "label": self.label,
        }


class GeoAdminClient:
    """
    Client pour l'API GeoAdmin (Swisstopo).
    
    Fonctionnalités:
    - Géocodage d'adresses (adresse -> coordonnées)
    - Géocodage inverse (coordonnées -> adresse)
    - Recherche d'adresses
    - Validation/normalisation d'adresses
    - Informations sur les bâtiments (EGID)
    
    Usage:
        async with GeoAdminClient() as client:
            # Géocoder une adresse
            location = await client.geocode("Rue du Rhône 1, Genève")
            
            # Valider/normaliser une adresse
            normalized = await client.normalize_address("Rue du Rhône 1", "1204", "Genève")
    """

    BASE_URL = "https://api3.geo.admin.ch"
    SEARCH_URL = f"{BASE_URL}/rest/services/api/SearchServer"
    IDENTIFY_URL = f"{BASE_URL}/rest/services/api/MapServer/identify"
    
    def __init__(self, timeout: int = 15):
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self._session = aiohttp.ClientSession(
            timeout=self.timeout,
            headers={
                "Accept": "application/json",
                "User-Agent": "ProspectionPro/5.1 (scraping CRM)",
            },
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()
            self._session = None

    async def _get(self, url: str, params: Optional[Dict] = None) -> Any:
        """Effectue une requête GET."""
        if not self._session:
            raise GeoAdminError("Session non initialisée. Utilisez 'async with'.")

        try:
            async with self._session.get(url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 429:
                    raise GeoAdminError("Rate limit GeoAdmin atteint.", status_code=429)
                else:
                    text = await response.text()
                    raise GeoAdminError(f"GeoAdmin API erreur {response.status}: {text[:200]}", status_code=response.status)
        except aiohttp.ClientError as e:
            raise GeoAdminError(f"Erreur réseau GeoAdmin: {e}") from e
        except asyncio.TimeoutError:
            raise GeoAdminError("Timeout GeoAdmin API", status_code=504)

    async def search(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Recherche générale (adresses, lieux, parcelles).
        
        Args:
            query: Texte de recherche
            limit: Nombre max de résultats
            
        Returns:
            Liste de résultats bruts
        """
        params = {
            "searchText": query,
            "type": "locations",
            "limit": limit,
            "sr": "4326",  # WGS84
        }

        try:
            data = await self._get(self.SEARCH_URL, params)
        except GeoAdminError:
            raise
        except Exception as e:
            raise GeoAdminError(f"Erreur recherche GeoAdmin: {e}")

        return data.get("results", [])

    async def geocode(self, address: str) -> Optional[GeoLocation]:
        """
        Géocode une adresse (convertit en coordonnées).
        
        Args:
            address: Adresse à géocoder (ex: "Rue du Rhône 1, 1204 Genève")
            
        Returns:
            GeoLocation ou None si non trouvé
        """
        scraping_logger.debug(f"[GeoAdmin] Géocodage: {address}")

        results = await self.search(address, limit=1)
        
        if not results:
            return None

        best = results[0]
        attrs = best.get("attrs", {})
        
        # Extraire les coordonnées
        lat = attrs.get("lat") or attrs.get("y")
        lon = attrs.get("lon") or attrs.get("x")
        
        if lat is None or lon is None:
            # Essayer avec la géométrie bbox
            bbox = best.get("bbox")
            if bbox and len(bbox) >= 4:
                lon = (bbox[0] + bbox[2]) / 2
                lat = (bbox[1] + bbox[3]) / 2

        if lat is None or lon is None:
            return None

        return GeoLocation(
            latitude=float(lat),
            longitude=float(lon),
            address=attrs.get("detail") or attrs.get("label"),
            label=attrs.get("label"),
        )

    async def reverse_geocode(self, lat: float, lon: float) -> Optional[str]:
        """
        Géocodage inverse (coordonnées -> adresse).
        
        Args:
            lat: Latitude WGS84
            lon: Longitude WGS84
            
        Returns:
            Adresse ou None
        """
        # Convertir en Swiss coordinates (LV95) pour l'API identify
        # Approximation simple pour la Suisse
        # Pour plus de précision, utiliser une vraie transformation
        
        # NOTE: l'endpoint Identify requiert désormais imageDisplay + mapExtent.
        extent = self._make_map_extent(lon, lat, delta=0.01)
        params = {
            "geometry": f"{lon},{lat}",
            "geometryType": "esriGeometryPoint",
            "sr": "4326",
            "layers": "all:ch.bfs.gebaeude_wohnungs_register",
            "tolerance": 50,
            "returnGeometry": "false",
            "imageDisplay": "600,550,96",
            "mapExtent": extent,
            "f": "json",
        }

        try:
            data = await self._get(self.IDENTIFY_URL, params)
        except GeoAdminError:
            raise
        except Exception as e:
            raise GeoAdminError(f"Erreur reverse geocode: {e}")

        results = data.get("results", [])
        if not results:
            return None

        attrs = results[0].get("attributes", {})
        
        # Construire l'adresse
        street = attrs.get("strname_deinr", "") or attrs.get("strname", "")
        zip_code = attrs.get("dplz4", "") or attrs.get("plz4", "")
        city = attrs.get("dplzname", "") or attrs.get("ggdename", "")

        parts = []
        if street:
            parts.append(street)
        if zip_code or city:
            parts.append(f"{zip_code} {city}".strip())

        return ", ".join(parts) if parts else None

    def _make_map_extent(self, lon: float, lat: float, delta: float = 0.01) -> str:
        """Construit un mapExtent (minx,miny,maxx,maxy) autour d'un point (WGS84)."""
        return f"{lon - delta},{lat - delta},{lon + delta},{lat + delta}"

    async def identify(
        self,
        lon: float,
        lat: float,
        layers: str,
        sr: str = "4326",
        tolerance: int = 5,
        return_geometry: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Wrapper Identify ArcGIS (GeoAdmin) pour récupérer les entités à un point.
        """
        extent = self._make_map_extent(lon, lat, delta=0.01)
        params = {
            "geometry": f"{lon},{lat}",
            "geometryType": "esriGeometryPoint",
            "sr": sr,
            "layers": layers,
            "tolerance": int(tolerance),
            "returnGeometry": "true" if return_geometry else "false",
            "imageDisplay": "600,550,96",
            "mapExtent": extent,
            "f": "json",
        }
        data = await self._get(self.IDENTIFY_URL, params)
        return data.get("results", []) or []

    async def identify_parcel(self, lon: float, lat: float) -> Optional[Dict[str, Any]]:
        """
        Identifie la parcelle cadastrale à un point (retourne EGRID + numéro).

        Utilise la couche GeoAdmin:
        - ch.kantone.cadastralwebmap-farbe
        """
        results = await self.identify(
            lon=lon,
            lat=lat,
            layers="all:ch.kantone.cadastralwebmap-farbe",
            sr="4326",
            tolerance=5,
            return_geometry=False,
        )
        if not results:
            return None

        # Prendre le premier résultat qui contient un EGRID
        for r in results:
            attrs = r.get("attributes", {}) or {}
            egrid = attrs.get("egris_egrid") or attrs.get("egrid") or attrs.get("EGRID")
            if egrid:
                return {
                    "egrid": egrid,
                    "parcel_number": str(attrs.get("number") or "").strip(),
                    "ak": (attrs.get("ak") or "").strip(),  # canton (ex: GE, VD, BE)
                    "identnd": (attrs.get("identnd") or "").strip(),  # identifiant admin (si dispo)
                    "geoportal_url": (attrs.get("geoportal_url") or "").strip(),
                    "label": (attrs.get("label") or "").strip(),
                    "raw": attrs,
                }

        return None

    async def normalize_address(
        self,
        street: str,
        zip_code: str = "",
        city: str = "",
        canton: str = "",
    ) -> Optional[SwissAddress]:
        """
        Valide et normalise une adresse suisse.
        
        Args:
            street: Rue et numéro (ex: "Rue du Rhône 1")
            zip_code: Code postal (ex: "1204")
            city: Ville (ex: "Genève")
            canton: Code canton optionnel (ex: "GE")
            
        Returns:
            SwissAddress normalisée ou None si non trouvée
        """
        # Construire la requête
        query_parts = []
        if street:
            query_parts.append(street)
        if zip_code:
            query_parts.append(zip_code)
        if city:
            query_parts.append(city)
        
        query = " ".join(query_parts)
        
        if not query.strip():
            return None

        scraping_logger.debug(f"[GeoAdmin] Normalisation: {query}")

        results = await self.search(query, limit=5)
        
        if not results:
            return None

        # Trouver le meilleur match
        best_match = None
        best_score = 0.0

        for result in results:
            attrs = result.get("attrs", {})
            origin = attrs.get("origin", "")
            
            # Favoriser les adresses (pas les lieux génériques)
            if origin not in ("address", "parcel"):
                continue

            score = 0.0
            detail = attrs.get("detail", "") or attrs.get("label", "")
            
            # Score basé sur la correspondance
            if zip_code and zip_code in detail:
                score += 0.4
            if city and city.lower() in detail.lower():
                score += 0.3
            if street:
                street_clean = re.sub(r'\d+', '', street).strip().lower()
                if street_clean and street_clean in detail.lower():
                    score += 0.3

            if score > best_score:
                best_score = score
                best_match = result

        if not best_match or best_score < 0.3:
            # Fallback: prendre le premier résultat
            best_match = results[0]
            best_score = 0.5

        attrs = best_match.get("attrs", {})
        # GeoAdmin renvoie souvent un `label` plus propre (peut contenir du HTML),
        # et un `detail` plus "technique". On préfère le label.
        label = attrs.get("label", "") or ""
        detail = attrs.get("detail", "") or ""
        to_parse = label or detail

        # Parser l'adresse normalisée
        parsed = self._parse_swiss_address(to_parse)
        
        # Ajouter les coordonnées
        lat = attrs.get("lat") or attrs.get("y")
        lon = attrs.get("lon") or attrs.get("x")
        coords = (float(lon), float(lat)) if lon and lat else None

        return SwissAddress(
            street=parsed.get("street", street),
            house_number=parsed.get("house_number", ""),
            zip_code=parsed.get("zip_code", zip_code),
            city=parsed.get("city", city),
            canton=parsed.get("canton", canton),
            egid=attrs.get("featureId"),
            coordinates=coords,
            confidence=best_score,
        )

    def _parse_swiss_address(self, address_str: str) -> Dict[str, str]:
        """
        Parse une chaîne d'adresse suisse.
        
        Formats supportés:
        - "Rue du Rhône 1, 1204 Genève"
        - "1204 Genève, Rue du Rhône 1"
        - "Genève (GE), Rue du Rhône 1"
        """
        result = {
            "street": "",
            "house_number": "",
            "zip_code": "",
            "city": "",
            "canton": "",
        }

        if not address_str:
            return result

        # Nettoyer (supprimer HTML du label GeoAdmin)
        addr = re.sub(r"<[^>]+>", " ", (address_str or "")).strip()
        addr = " ".join(addr.split())

        # Extraire le canton (XX) à la fin
        canton_match = re.search(r'\(([A-Z]{2})\)\s*$', addr)
        if canton_match:
            result["canton"] = canton_match.group(1)
            addr = addr[:canton_match.start()].strip()

        # Chercher le code postal (4 chiffres suisses)
        zip_match = re.search(r'\b(\d{4})\b', addr)
        if zip_match:
            result["zip_code"] = zip_match.group(1)

        # Fallback robuste: format sans virgule "Rue 1 1200 Ville"
        if "," not in addr and result["zip_code"]:
            m = re.search(rf"^(?P<street>.+?)\s+{result['zip_code']}\s+(?P<city>.+)$", addr)
            if m:
                street_part = m.group("street").strip()
                city_part = m.group("city").strip()
                city_part = re.sub(r"\bCH\b\s*\b[A-Z]{2}\b\s*$", "", city_part).strip()

                num_match = re.search(r"\b(\d+[a-zA-Z]?)\s*$", street_part)
                if num_match:
                    result["house_number"] = num_match.group(1)
                    result["street"] = street_part[:num_match.start()].strip()
                else:
                    result["street"] = street_part

                result["city"] = city_part
                return result

        # Séparer par virgule si présent (format classique)
        if "," in addr:
            parts = [p.strip() for p in addr.split(",")]
        else:
            # Fallback: format sans virgule: "Rue 1 1200 Ville"
            parts = [addr]

        for part in parts:
            # Si contient le code postal, c'est ville + NPA
            if result["zip_code"] and result["zip_code"] in part:
                # Ex: "1200 Genève" ou "Rue 1 1200 Genève"
                # On ne garde que ce qui suit le NPA
                city_part = part.split(result["zip_code"], 1)[-1].strip()
                # Nettoyer les suffixes éventuels "CH XX"
                city_part = re.sub(r"\bCH\b\s*\b[A-Z]{2}\b\s*$", "", city_part).strip()
                if city_part:
                    result["city"] = city_part
            else:
                # C'est probablement la rue
                # Extraire le numéro de rue
                num_match = re.search(r'\b(\d+[a-zA-Z]?)\s*$', part)
                if num_match:
                    result["house_number"] = num_match.group(1)
                    result["street"] = part[:num_match.start()].strip()
                else:
                    if not result["street"]:
                        result["street"] = part

        return result

    async def get_building_info(self, egid: str) -> Optional[Dict[str, Any]]:
        """
        Récupère les informations sur un bâtiment par son EGID.
        
        Args:
            egid: Identifiant fédéral du bâtiment
            
        Returns:
            Dictionnaire avec les informations du bâtiment
        """
        params = {
            "searchText": egid,
            "type": "featuresearch",
            "features": "ch.bfs.gebaeude_wohnungs_register",
            "limit": 1,
        }

        try:
            data = await self._get(self.SEARCH_URL, params)
        except GeoAdminError:
            raise
        except Exception as e:
            raise GeoAdminError(f"Erreur recherche bâtiment: {e}")

        results = data.get("results", [])
        if not results:
            return None

        return results[0].get("attrs", {})

    async def validate_swiss_zip(self, zip_code: str, city: str = "") -> bool:
        """
        Valide un code postal suisse.
        
        Args:
            zip_code: Code postal à valider
            city: Ville optionnelle pour validation croisée
            
        Returns:
            True si valide
        """
        if not zip_code or not re.match(r'^\d{4}$', zip_code):
            return False

        query = f"{zip_code} {city}".strip()
        results = await self.search(query, limit=1)
        
        if not results:
            return False

        # Vérifier que le résultat contient le même code postal
        detail = results[0].get("attrs", {}).get("detail", "")
        return zip_code in detail


# Fonctions helpers pour usage simple
async def geocode_address(address: str) -> Optional[Dict[str, Any]]:
    """
    Helper pour géocoder une adresse sans context manager.
    """
    async with GeoAdminClient() as client:
        location = await client.geocode(address)
        return location.to_dict() if location else None


async def normalize_swiss_address(
    street: str,
    zip_code: str = "",
    city: str = "",
) -> Optional[Dict[str, Any]]:
    """
    Helper pour normaliser une adresse sans context manager.
    """
    async with GeoAdminClient() as client:
        normalized = await client.normalize_address(street, zip_code, city)
        return normalized.to_dict() if normalized else None

