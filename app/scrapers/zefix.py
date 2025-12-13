# =============================================================================
# CLIENT API ZEFIX - Registre du Commerce Suisse
# =============================================================================
# API REST officielle et gratuite: https://www.zefix.admin.ch/ZefixPublicREST/
# Documentation: https://www.zefix.admin.ch/ZefixREST/swagger-ui/index.html
# =============================================================================

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import aiohttp

from app.core.logger import scraping_logger


class ZefixError(Exception):
    """Erreur explicite Zefix (réseau, API, parsing)."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


# Mapping des cantons suisses
CANTONS = {
    "AG": "Aargau",
    "AI": "Appenzell Innerrhoden",
    "AR": "Appenzell Ausserrhoden",
    "BE": "Bern",
    "BL": "Basel-Landschaft",
    "BS": "Basel-Stadt",
    "FR": "Fribourg",
    "GE": "Genève",
    "GL": "Glarus",
    "GR": "Graubünden",
    "JU": "Jura",
    "LU": "Luzern",
    "NE": "Neuchâtel",
    "NW": "Nidwalden",
    "OW": "Obwalden",
    "SG": "St. Gallen",
    "SH": "Schaffhausen",
    "SO": "Solothurn",
    "SZ": "Schwyz",
    "TG": "Thurgau",
    "TI": "Ticino",
    "UR": "Uri",
    "VD": "Vaud",
    "VS": "Valais",
    "ZG": "Zug",
    "ZH": "Zürich",
}


@dataclass
class ZefixCompany:
    """Représentation d'une entreprise du registre du commerce."""

    uid: str  # CHE-xxx.xxx.xxx
    name: str
    legal_form: str
    status: str
    canton: str
    city: str
    address: Optional[str] = None
    zip_code: Optional[str] = None
    purpose: Optional[str] = None
    capital: Optional[float] = None
    persons: Optional[List[Dict[str, Any]]] = None
    registration_date: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "uid": self.uid,
            "name": self.name,
            "legal_form": self.legal_form,
            "status": self.status,
            "canton": self.canton,
            "city": self.city,
            "address": self.address,
            "zip_code": self.zip_code,
            "purpose": self.purpose,
            "capital": self.capital,
            "persons": self.persons or [],
            "registration_date": self.registration_date,
        }


class ZefixClient:
    """
    Client pour l'API REST Zefix (Registre du Commerce Suisse).
    
    Usage:
        async with ZefixClient() as client:
            results = await client.search("Dupont SA", canton="GE")
            for company in results:
                details = await client.get_details(company.uid)
    """

    BASE_URL = "https://www.zefix.admin.ch/ZefixREST/api/v1"
    
    def __init__(self, timeout: int = 30):
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

    async def _get(self, endpoint: str, params: Optional[Dict] = None) -> Any:
        """Effectue une requête GET vers l'API Zefix."""
        if not self._session:
            raise ZefixError("Session non initialisée. Utilisez 'async with'.")

        url = f"{self.BASE_URL}{endpoint}"
        
        try:
            async with self._session.get(url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 404:
                    return None
                elif response.status == 429:
                    raise ZefixError("Rate limit Zefix atteint. Réessayez plus tard.", status_code=429)
                else:
                    text = await response.text()
                    raise ZefixError(f"Zefix API erreur {response.status}: {text[:200]}", status_code=response.status)
        except aiohttp.ClientError as e:
            raise ZefixError(f"Erreur réseau Zefix: {e}") from e
        except asyncio.TimeoutError:
            raise ZefixError("Timeout Zefix API", status_code=504)

    async def _post(self, endpoint: str, data: Dict) -> Any:
        """Effectue une requête POST vers l'API Zefix."""
        if not self._session:
            raise ZefixError("Session non initialisée. Utilisez 'async with'.")

        url = f"{self.BASE_URL}{endpoint}"
        
        try:
            async with self._session.post(url, json=data) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 404:
                    return None
                elif response.status == 429:
                    raise ZefixError("Rate limit Zefix atteint.", status_code=429)
                else:
                    text = await response.text()
                    raise ZefixError(f"Zefix API erreur {response.status}: {text[:200]}", status_code=response.status)
        except aiohttp.ClientError as e:
            raise ZefixError(f"Erreur réseau Zefix: {e}") from e
        except asyncio.TimeoutError:
            raise ZefixError("Timeout Zefix API", status_code=504)

    def _parse_company(self, data: Dict) -> ZefixCompany:
        """Parse les données brutes en objet ZefixCompany."""
        # Extraction de l'adresse
        address_parts = []
        if data.get("address"):
            addr = data["address"]
            if addr.get("street"):
                address_parts.append(addr["street"])
            if addr.get("houseNumber"):
                if address_parts:
                    address_parts[-1] += f" {addr['houseNumber']}"
                else:
                    address_parts.append(addr["houseNumber"])

        address = ", ".join(address_parts) if address_parts else None
        zip_code = data.get("address", {}).get("swissZipCode") or data.get("address", {}).get("zipCode")
        city = data.get("address", {}).get("city", "")

        # Extraction des personnes (dirigeants, administrateurs)
        persons = []
        if data.get("shabPubPersons"):
            for p in data["shabPubPersons"]:
                person_info = {
                    "name": f"{p.get('firstName', '')} {p.get('lastName', '')}".strip(),
                    "function": p.get("function", ""),
                    "authorization": p.get("authorization", ""),
                }
                if person_info["name"]:
                    persons.append(person_info)

        return ZefixCompany(
            uid=data.get("uid", ""),
            name=data.get("name", ""),
            legal_form=data.get("legalForm", {}).get("name", {}).get("fr", "") or data.get("legalForm", {}).get("shortName", ""),
            status=data.get("status", ""),
            canton=data.get("cantonalExcerptWeb", {}).get("canton", "") or data.get("canton", ""),
            city=city,
            address=address,
            zip_code=str(zip_code) if zip_code else None,
            purpose=data.get("purpose"),
            capital=data.get("capital"),
            persons=persons if persons else None,
            registration_date=data.get("registrationDate"),
        )

    async def search(
        self,
        name: str,
        canton: Optional[str] = None,
        active_only: bool = True,
        limit: int = 100,
    ) -> List[ZefixCompany]:
        """
        Recherche des entreprises par nom.
        
        Args:
            name: Nom ou partie du nom de l'entreprise
            canton: Code canton (ex: "GE", "VD") - optionnel
            active_only: Ne retourner que les entreprises actives
            limit: Nombre maximum de résultats
            
        Returns:
            Liste d'objets ZefixCompany
        """
        scraping_logger.info(f"[Zefix] Recherche: '{name}' canton={canton} limit={limit}")

        # L'API Zefix utilise POST pour la recherche
        search_data = {
            "name": name,
            "maxEntries": min(limit, 500),  # Max 500 selon l'API
        }

        if canton:
            canton_upper = canton.upper()
            if canton_upper in CANTONS:
                search_data["registryOffices"] = [canton_upper]

        if active_only:
            search_data["activeOnly"] = True

        try:
            data = await self._post("/firm/search.json", search_data)
        except ZefixError:
            raise
        except Exception as e:
            scraping_logger.error(f"[Zefix] Erreur recherche: {e}")
            raise ZefixError(f"Erreur recherche Zefix: {e}")

        if not data or not isinstance(data, list):
            return []

        results = []
        for item in data[:limit]:
            try:
                company = self._parse_company(item)
                results.append(company)
            except Exception as e:
                scraping_logger.warning(f"[Zefix] Erreur parsing: {e}")
                continue

        scraping_logger.info(f"[Zefix] {len(results)} entreprises trouvées")
        return results

    async def get_by_uid(self, uid: str) -> Optional[ZefixCompany]:
        """
        Récupère les détails d'une entreprise par son UID (CHE-xxx.xxx.xxx).
        
        Args:
            uid: Numéro d'identification unique (format CHE-xxx.xxx.xxx ou xxx.xxx.xxx)
            
        Returns:
            ZefixCompany ou None si non trouvé
        """
        # Normaliser l'UID
        uid_clean = uid.replace("CHE-", "").replace(".", "")
        
        scraping_logger.info(f"[Zefix] Récupération UID: {uid}")

        try:
            data = await self._get(f"/firm/{uid_clean}.json")
        except ZefixError:
            raise
        except Exception as e:
            scraping_logger.error(f"[Zefix] Erreur get_by_uid: {e}")
            raise ZefixError(f"Erreur Zefix: {e}")

        if not data:
            return None

        return self._parse_company(data)

    async def search_by_address(
        self,
        city: str,
        street: Optional[str] = None,
        canton: Optional[str] = None,
        limit: int = 50,
    ) -> List[ZefixCompany]:
        """
        Recherche des entreprises par adresse.
        
        Args:
            city: Nom de la ville
            street: Nom de rue (optionnel)
            canton: Code canton (optionnel)
            limit: Nombre maximum de résultats
            
        Returns:
            Liste d'objets ZefixCompany
        """
        scraping_logger.info(f"[Zefix] Recherche par adresse: {city}, {street}")

        search_data = {
            "address": {
                "city": city,
            },
            "maxEntries": min(limit, 500),
            "activeOnly": True,
        }

        if street:
            search_data["address"]["street"] = street

        if canton:
            canton_upper = canton.upper()
            if canton_upper in CANTONS:
                search_data["registryOffices"] = [canton_upper]

        try:
            data = await self._post("/firm/search.json", search_data)
        except ZefixError:
            raise
        except Exception as e:
            scraping_logger.error(f"[Zefix] Erreur recherche adresse: {e}")
            raise ZefixError(f"Erreur recherche Zefix: {e}")

        if not data or not isinstance(data, list):
            return []

        results = []
        for item in data[:limit]:
            try:
                company = self._parse_company(item)
                results.append(company)
            except Exception as e:
                scraping_logger.warning(f"[Zefix] Erreur parsing: {e}")
                continue

        scraping_logger.info(f"[Zefix] {len(results)} entreprises trouvées par adresse")
        return results

    async def get_persons(self, uid: str) -> List[Dict[str, Any]]:
        """
        Récupère la liste des personnes associées à une entreprise.
        
        Args:
            uid: UID de l'entreprise
            
        Returns:
            Liste de dictionnaires avec nom, fonction, autorisation
        """
        company = await self.get_by_uid(uid)
        if company and company.persons:
            return company.persons
        return []


# Fonction helper pour usage simple
async def search_zefix(
    name: str,
    canton: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """
    Helper pour rechercher dans Zefix sans context manager.
    
    Returns:
        Liste de dictionnaires (format prospect-compatible)
    """
    async with ZefixClient() as client:
        companies = await client.search(name, canton=canton, limit=limit)
        return [c.to_dict() for c in companies]

