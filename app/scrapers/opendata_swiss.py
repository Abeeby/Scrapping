# =============================================================================
# OPENDATA.SWISS CLIENT - Catalogue CKAN (sources open data)
# =============================================================================
# Objectif:
# - Rechercher des datasets pertinents (permis, géodata, statistiques)
# - Récupérer les ressources (CSV/JSON) pour ingestion contrôlée
#
# API CKAN:
# - https://opendata.swiss/api/3/action/package_search
# - https://opendata.swiss/api/3/action/package_show
# =============================================================================

from __future__ import annotations

from typing import Any, Dict, Optional

import aiohttp

from app.core.logger import logger


class OpenDataSwissError(Exception):
    """Erreur explicite OpenData.swiss (réseau, HTTP, parsing)."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class OpenDataSwissClient:
    BASE_URL = "https://opendata.swiss/api/3/action"

    def __init__(self, timeout: int = 20):
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self):
        self._session = aiohttp.ClientSession(
            timeout=self.timeout,
            headers={
                "Accept": "application/json",
                "User-Agent": "ProspectionPro/5.1 (opendata client)",
            },
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()
            self._session = None

    async def _get(self, action: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if not self._session:
            raise OpenDataSwissError("Session non initialisée. Utilisez 'async with'.")

        url = f"{self.BASE_URL}/{action}"
        try:
            async with self._session.get(url, params=params) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise OpenDataSwissError(
                        f"OpenData.swiss HTTP {resp.status}: {text[:200]}",
                        status_code=resp.status,
                    )
                data = await resp.json()
        except aiohttp.ClientError as e:
            raise OpenDataSwissError(f"Erreur réseau OpenData.swiss: {e}") from e
        except aiohttp.ClientPayloadError as e:
            raise OpenDataSwissError(f"Erreur parsing OpenData.swiss: {e}") from e

        if not isinstance(data, dict) or data.get("success") is not True:
            raise OpenDataSwissError(f"Réponse CKAN invalide: {str(data)[:200]}")

        return data

    async def search_datasets(self, q: str, rows: int = 20, start: int = 0) -> Dict[str, Any]:
        """Recherche des datasets dans le catalogue."""
        rows = max(1, min(int(rows), 100))
        start = max(0, int(start))
        params = {"q": q, "rows": rows, "start": start}
        return await self._get("package_search", params=params)

    async def get_dataset(self, dataset_id: str) -> Dict[str, Any]:
        """Retourne le détail d'un dataset (resources incluses)."""
        return await self._get("package_show", params={"id": dataset_id})
