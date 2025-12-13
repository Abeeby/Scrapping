# =============================================================================
# SCRAPER COMPARIS.CH - Détails d'annonce (JS required)
# =============================================================================

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Optional

from app.core.logger import scraping_logger

try:
    from playwright.async_api import async_playwright

    PLAYWRIGHT_AVAILABLE = True
except Exception:
    PLAYWRIGHT_AVAILABLE = False


class ComparisScraperError(Exception):
    """Erreur explicite Comparis (réseau, blocage anti-bot, parsing)."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


def _parse_int(text: str) -> Optional[int]:
    if not text:
        return None
    digits = re.sub(r"[^0-9]", "", text)
    if not digits:
        return None
    try:
        return int(digits)
    except Exception:
        return None


def _parse_float(text: str) -> Optional[float]:
    if not text:
        return None
    s = text.strip().replace("'", "")
    # garder chiffres, '.' ','
    s = re.sub(r"[^0-9\.,]", "", s)
    if not s:
        return None
    # heuristique: si virgule utilisée, convertir
    if s.count(",") == 1 and s.count(".") == 0:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def _extract_listing_id(url: str) -> str:
    m = re.search(r"/(\d+)(?:\?.*)?$", url)
    return m.group(1) if m else re.sub(r"\W+", "_", url)[-32:]


class ComparisScraper:
    """Scraper Comparis via navigateur (Playwright)."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None

    async def extract_details(self, url: str) -> Dict[str, Any]:
        if not url or "comparis.ch" not in url:
            raise ComparisScraperError("URL Comparis invalide.", status_code=400)

        if not PLAYWRIGHT_AVAILABLE:
            raise ComparisScraperError(
                "Comparis nécessite un navigateur (JS). Installez Playwright (pip install playwright) puis installez Chromium (playwright install chromium).",
                status_code=501,
            )

        listing_id = _extract_listing_id(url)

        # Mapping labels -> champs
        label_map = {
            "Type de bien": "type_bien",
            "Pièce": "pieces",
            "Pièces": "pieces",
            "Nombre d'étages": "nombre_etages",
            "Surface habitable": "surface_habitable_m2",
            "Construction": "annee_construction",
            "Année de rénovation": "annee_renovation",
            "Disponibilité": "disponibilite",
            "Surface du terrain": "surface_terrain_m2",
            "Prix de vente": "prix_vente_chf",
        }

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                ],
            )
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
                locale="fr-CH",
            )

            # Stealth minimal
            await context.add_init_script(
                """
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                """
            )

            page = await context.new_page()

            try:
                scraping_logger.info("[Comparis] Ouverture annonce %s", url)
                await page.goto(url, wait_until="domcontentloaded", timeout=45000)

                # Si blocage anti-bot, la page contient souvent ce message.
                body_text = (await page.locator("body").inner_text())[:5000]
                if "Please enable JS" in body_text or "disable any ad blocker" in body_text:
                    raise ComparisScraperError(
                        "Comparis bloque l'accès automatisé (anti-bot). Cette annonce nécessite un navigateur 'vrai' / une exécution Playwright compatible.",
                        status_code=403,
                    )

                # Attendre l'apparition du titre/section (best effort)
                try:
                    await page.wait_for_selector("text=Caractéristiques principales", timeout=15000)
                except Exception:
                    # pas bloquant
                    pass

                title = ""
                try:
                    title = (await page.title()) or ""
                except Exception:
                    title = ""

                # Extraction textuelle robuste (label puis valeur ligne suivante)
                full_text = await page.locator("body").inner_text()
                lines = [l.strip() for l in full_text.splitlines() if l.strip()]

                extracted: Dict[str, Any] = {
                    "listing_id": listing_id,
                    "url_annonce": url,
                    "titre": title,
                }

                for i, line in enumerate(lines):
                    key = label_map.get(line)
                    if not key:
                        continue

                    # next non-empty line
                    val = ""
                    for j in range(i + 1, min(i + 8, len(lines))):
                        if lines[j].strip():
                            val = lines[j].strip()
                            break
                    if not val:
                        continue

                    extracted[key] = val

                # Parsing numérique
                if isinstance(extracted.get("pieces"), str):
                    extracted["pieces"] = _parse_float(extracted.get("pieces", ""))

                if isinstance(extracted.get("nombre_etages"), str):
                    extracted["nombre_etages"] = _parse_int(extracted.get("nombre_etages", ""))

                if isinstance(extracted.get("surface_habitable_m2"), str):
                    extracted["surface_habitable_m2"] = _parse_float(extracted.get("surface_habitable_m2", ""))

                if isinstance(extracted.get("surface_terrain_m2"), str):
                    extracted["surface_terrain_m2"] = _parse_float(extracted.get("surface_terrain_m2", ""))

                if isinstance(extracted.get("annee_construction"), str):
                    extracted["annee_construction"] = _parse_int(extracted.get("annee_construction", ""))

                if isinstance(extracted.get("annee_renovation"), str):
                    extracted["annee_renovation"] = _parse_int(extracted.get("annee_renovation", ""))

                if isinstance(extracted.get("prix_vente_chf"), str):
                    extracted["prix_vente_chf"] = _parse_int(extracted.get("prix_vente_chf", ""))

                return extracted

            finally:
                try:
                    await context.close()
                except Exception:
                    pass
                try:
                    await browser.close()
                except Exception:
                    pass

