# =============================================================================
# STEALTH BROWSER - Scraper avec Playwright et techniques anti-détection
# =============================================================================
# Techniques légales pour éviter la détection:
# - Fingerprint randomization
# - Patterns de navigation humains
# - Gestion de sessions persistantes
# - Support pour proxies résidentiels
# =============================================================================

from __future__ import annotations

import asyncio
import random
import json
from typing import Any, Dict, List, Optional
from dataclasses import dataclass

from app.core.logger import scraping_logger

# Vérifier si Playwright est disponible
try:
    from playwright.async_api import async_playwright, Browser, BrowserContext, Page
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    scraping_logger.warning("[StealthBrowser] Playwright non installé. pip install playwright && playwright install chromium")


# =============================================================================
# FINGERPRINTS - Configurations de navigateur réalistes
# =============================================================================

BROWSER_FINGERPRINTS = [
    {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "viewport": {"width": 1920, "height": 1080},
        "locale": "fr-CH",
        "timezone": "Europe/Zurich",
        "platform": "Win32",
        "webgl_vendor": "Google Inc. (NVIDIA)",
        "webgl_renderer": "ANGLE (NVIDIA, NVIDIA GeForce RTX 3060 Direct3D11 vs_5_0 ps_5_0)",
    },
    {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "viewport": {"width": 1536, "height": 864},
        "locale": "fr-FR",
        "timezone": "Europe/Paris",
        "platform": "Win32",
        "webgl_vendor": "Google Inc. (Intel)",
        "webgl_renderer": "ANGLE (Intel, Intel(R) UHD Graphics 630 Direct3D11 vs_5_0 ps_5_0)",
    },
    {
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "viewport": {"width": 1440, "height": 900},
        "locale": "fr-CH",
        "timezone": "Europe/Zurich",
        "platform": "MacIntel",
        "webgl_vendor": "Apple Inc.",
        "webgl_renderer": "Apple M1 Pro",
    },
    {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
        "viewport": {"width": 1920, "height": 1080},
        "locale": "de-CH",
        "timezone": "Europe/Zurich",
        "platform": "Win32",
        "webgl_vendor": "Mozilla",
        "webgl_renderer": "Mozilla",
    },
    {
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "viewport": {"width": 1680, "height": 1050},
        "locale": "fr-CH",
        "timezone": "Europe/Zurich",
        "platform": "MacIntel",
        "webgl_vendor": "Apple Inc.",
        "webgl_renderer": "Apple GPU",
    },
]


# =============================================================================
# STEALTH SCRIPTS - JavaScript pour cacher l'automatisation
# =============================================================================

STEALTH_SCRIPTS = """
// Masquer webdriver
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

// Masquer l'automatisation Chrome
window.chrome = { runtime: {} };

// Masquer les plugins vides
Object.defineProperty(navigator, 'plugins', {
    get: () => [
        { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer' },
        { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
        { name: 'Native Client', filename: 'internal-nacl-plugin' },
    ],
});

// Masquer les langues
Object.defineProperty(navigator, 'languages', {
    get: () => ['fr-CH', 'fr', 'de-CH', 'de', 'en-US', 'en'],
});

// Masquer la détection de headless
Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });

// Permissions réalistes
const originalQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (parameters) => (
    parameters.name === 'notifications' ?
        Promise.resolve({ state: Notification.permission }) :
        originalQuery(parameters)
);

// Console propre (pas de warnings Playwright)
const originalConsoleDebug = console.debug;
console.debug = (...args) => {
    if (args[0]?.includes?.('Playwright')) return;
    originalConsoleDebug.apply(console, args);
};
"""


# =============================================================================
# HUMAN BEHAVIOR - Simulation de comportement humain
# =============================================================================

async def human_delay(min_ms: int = 500, max_ms: int = 2000):
    """Délai aléatoire simulant un humain."""
    delay = random.randint(min_ms, max_ms) / 1000
    await asyncio.sleep(delay)


async def human_scroll(page: "Page", scroll_count: int = 3):
    """Scroll progressif comme un humain."""
    for _ in range(scroll_count):
        # Scroll aléatoire
        scroll_amount = random.randint(200, 500)
        await page.mouse.wheel(0, scroll_amount)
        await human_delay(300, 800)


async def human_mouse_movement(page: "Page"):
    """Mouvements de souris aléatoires."""
    viewport = page.viewport_size
    if not viewport:
        return
    
    # Quelques mouvements aléatoires
    for _ in range(random.randint(2, 5)):
        x = random.randint(100, viewport["width"] - 100)
        y = random.randint(100, viewport["height"] - 100)
        await page.mouse.move(x, y)
        await human_delay(100, 300)


async def human_typing(page: "Page", selector: str, text: str):
    """Frappe de texte avec vitesse humaine."""
    await page.click(selector)
    await human_delay(200, 400)
    
    for char in text:
        await page.keyboard.type(char, delay=random.randint(50, 150))
        
        # Parfois une pause plus longue
        if random.random() < 0.1:
            await human_delay(200, 500)


# =============================================================================
# STEALTH BROWSER CLASS
# =============================================================================

@dataclass
class ProxyConfig:
    """Configuration d'un proxy."""
    server: str  # host:port
    username: Optional[str] = None
    password: Optional[str] = None


class StealthBrowser:
    """
    Navigateur avec techniques anti-détection.
    
    Usage:
        async with StealthBrowser() as browser:
            page = await browser.new_page()
            await page.goto("https://example.com")
            content = await page.content()
    
    Avec proxy résidentiel:
        proxy = ProxyConfig(
            server="proxy.example.com:8080",
            username="user",
            password="pass"
        )
        async with StealthBrowser(proxy=proxy) as browser:
            ...
    """
    
    def __init__(
        self,
        proxy: Optional[ProxyConfig] = None,
        headless: bool = True,
        slow_mo: int = 50,
    ):
        self.proxy = proxy
        self.headless = headless
        self.slow_mo = slow_mo
        self._playwright = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._fingerprint: Dict[str, Any] = {}
    
    async def __aenter__(self):
        if not PLAYWRIGHT_AVAILABLE:
            raise RuntimeError(
                "Playwright non installé. Exécutez: pip install playwright && playwright install chromium"
            )
        
        # Choisir un fingerprint aléatoire
        self._fingerprint = random.choice(BROWSER_FINGERPRINTS)
        
        # Lancer Playwright
        self._playwright = await async_playwright().start()
        
        # Options de lancement
        launch_options = {
            "headless": self.headless,
            "slow_mo": self.slow_mo,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-infobars",
                "--window-position=0,0",
                "--ignore-certifcate-errors",
                "--ignore-certifcate-errors-spki-list",
                f"--window-size={self._fingerprint['viewport']['width']},{self._fingerprint['viewport']['height']}",
            ],
        }
        
        # Ajouter proxy si configuré
        if self.proxy:
            launch_options["proxy"] = {
                "server": f"http://{self.proxy.server}",
            }
            if self.proxy.username and self.proxy.password:
                launch_options["proxy"]["username"] = self.proxy.username
                launch_options["proxy"]["password"] = self.proxy.password
        
        # Lancer le navigateur
        self._browser = await self._playwright.chromium.launch(**launch_options)
        
        # Créer le contexte avec fingerprint
        context_options = {
            "user_agent": self._fingerprint["user_agent"],
            "viewport": self._fingerprint["viewport"],
            "locale": self._fingerprint["locale"],
            "timezone_id": self._fingerprint["timezone"],
            "permissions": ["geolocation"],
            "geolocation": {"latitude": 46.2044, "longitude": 6.1432},  # Genève
            "color_scheme": "light",
            "java_script_enabled": True,
            "bypass_csp": True,
            "ignore_https_errors": True,
        }
        
        self._context = await self._browser.new_context(**context_options)
        
        # Injecter les scripts stealth
        await self._context.add_init_script(STEALTH_SCRIPTS)
        
        # Ajouter des cookies réalistes (simulation de visite précédente)
        await self._add_realistic_cookies()
        
        scraping_logger.info(f"[StealthBrowser] Lancé avec fingerprint: {self._fingerprint['platform']}")
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
    
    async def _add_realistic_cookies(self):
        """Ajoute des cookies pour simuler un utilisateur normal."""
        # Cookies génériques de consentement (GDPR)
        cookies = [
            {
                "name": "cookie_consent",
                "value": "accepted",
                "domain": ".homegate.ch",
                "path": "/",
            },
            {
                "name": "cookie_consent",
                "value": "accepted",
                "domain": ".immoscout24.ch",
                "path": "/",
            },
            {
                "name": "cookieConsent",
                "value": "true",
                "domain": ".comparis.ch",
                "path": "/",
            },
        ]
        
        await self._context.add_cookies(cookies)
    
    async def new_page(self) -> "Page":
        """Crée une nouvelle page avec comportement humain."""
        page = await self._context.new_page()
        
        # Intercepter les requêtes pour ajouter des headers réalistes
        await page.route("**/*", self._handle_route)
        
        return page
    
    async def _handle_route(self, route):
        """Modifie les headers des requêtes."""
        headers = {
            **route.request.headers,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": self._fingerprint["locale"] + ",en;q=0.5",
            "Cache-Control": "no-cache",
            "DNT": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        }
        
        await route.continue_(headers=headers)
    
    async def goto_with_human_behavior(
        self,
        page: "Page",
        url: str,
        wait_for_selector: Optional[str] = None,
    ) -> str:
        """
        Navigue vers une URL avec comportement humain.
        
        Returns:
            Contenu HTML de la page
        """
        scraping_logger.info(f"[StealthBrowser] Navigation: {url}")
        
        # Navigation
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        
        # Attendre un peu (lecture de la page)
        await human_delay(1000, 2000)
        
        # Mouvements de souris
        await human_mouse_movement(page)
        
        # Scroll comme un humain
        await human_scroll(page, scroll_count=random.randint(2, 4))
        
        # Attendre un sélecteur si spécifié
        if wait_for_selector:
            try:
                await page.wait_for_selector(wait_for_selector, timeout=10000)
            except Exception:
                pass
        
        # Petit délai final
        await human_delay(500, 1000)
        
        return await page.content()


# =============================================================================
# SCRAPER IMMOBILIER AVEC STEALTH BROWSER
# =============================================================================

class StealthPropertyScraper:
    """
    Scraper immobilier utilisant le navigateur stealth.
    
    Supporte:
    - Immoscout24
    - Homegate
    - Comparis
    """
    
    def __init__(self, proxy: Optional[ProxyConfig] = None):
        self.proxy = proxy
    
    async def scrape_immoscout24(
        self,
        location: str,
        transaction_type: str = "rent",
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Scrape Immoscout24 avec le navigateur stealth."""
        results = []
        
        async with StealthBrowser(proxy=self.proxy) as browser:
            page = await browser.new_page()
            
            # Construire l'URL
            location_slug = location.lower().replace(" ", "-").replace("è", "e").replace("é", "e")
            url = f"https://www.immoscout24.ch/fr/immobilier/{transaction_type}/lieu-{location_slug}"
            
            try:
                # Navigation avec comportement humain
                html = await browser.goto_with_human_behavior(
                    page,
                    url,
                    wait_for_selector="article"
                )
                
                # Extraire les annonces
                listings = await page.query_selector_all("article[data-test='result-list-item']")
                
                for listing in listings[:limit]:
                    try:
                        result = await self._parse_immoscout_listing(listing)
                        if result:
                            results.append(result)
                    except Exception as e:
                        scraping_logger.debug(f"[StealthScraper] Erreur parsing: {e}")
                        continue
                
                scraping_logger.info(f"[StealthScraper] Immoscout24: {len(results)} annonces")
                
            except Exception as e:
                scraping_logger.error(f"[StealthScraper] Erreur Immoscout24: {e}")
        
        return results
    
    async def _parse_immoscout_listing(self, element) -> Optional[Dict[str, Any]]:
        """Parse une annonce Immoscout24."""
        try:
            # Titre
            title_el = await element.query_selector("h3")
            title = await title_el.inner_text() if title_el else ""
            
            # Prix
            price_el = await element.query_selector("[data-test='price']")
            price_text = await price_el.inner_text() if price_el else ""
            price = self._extract_price(price_text)
            
            # Adresse
            address_el = await element.query_selector("[data-test='address']")
            address = await address_el.inner_text() if address_el else ""
            
            # Lien
            link_el = await element.query_selector("a")
            link = await link_el.get_attribute("href") if link_el else ""
            if link and not link.startswith("http"):
                link = f"https://www.immoscout24.ch{link}"
            
            return {
                "id": f"immo24-stealth-{hash(link) % 100000}",
                "titre": title,
                "prix": price,
                "adresse": address,
                "url_annonce": link,
                "source": "Immoscout24-Stealth",
            }
            
        except Exception:
            return None
    
    async def scrape_homegate(
        self,
        location: str,
        transaction_type: str = "rent",
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Scrape Homegate avec le navigateur stealth."""
        results = []
        
        async with StealthBrowser(proxy=self.proxy) as browser:
            page = await browser.new_page()
            
            # Construire l'URL
            location_slug = location.lower().replace(" ", "-").replace("è", "e").replace("é", "e")
            trans = "louer" if transaction_type == "rent" else "acheter"
            url = f"https://www.homegate.ch/{trans}/appartement/ville-{location_slug}/liste-annonces"
            
            try:
                # Navigation avec comportement humain
                html = await browser.goto_with_human_behavior(
                    page,
                    url,
                    wait_for_selector="[data-test='result-list']"
                )
                
                # Extraire les annonces
                listings = await page.query_selector_all("[data-test='result-list-item']")
                
                for listing in listings[:limit]:
                    try:
                        result = await self._parse_homegate_listing(listing)
                        if result:
                            results.append(result)
                    except Exception as e:
                        scraping_logger.debug(f"[StealthScraper] Erreur parsing: {e}")
                        continue
                
                scraping_logger.info(f"[StealthScraper] Homegate: {len(results)} annonces")
                
            except Exception as e:
                scraping_logger.error(f"[StealthScraper] Erreur Homegate: {e}")
        
        return results
    
    async def _parse_homegate_listing(self, element) -> Optional[Dict[str, Any]]:
        """Parse une annonce Homegate."""
        try:
            # Titre
            title_el = await element.query_selector("h3, [data-test='title']")
            title = await title_el.inner_text() if title_el else ""
            
            # Prix
            price_el = await element.query_selector("[data-test='price']")
            price_text = await price_el.inner_text() if price_el else ""
            price = self._extract_price(price_text)
            
            # Adresse
            address_el = await element.query_selector("[data-test='address']")
            address = await address_el.inner_text() if address_el else ""
            
            # Lien
            link_el = await element.query_selector("a")
            link = await link_el.get_attribute("href") if link_el else ""
            if link and not link.startswith("http"):
                link = f"https://www.homegate.ch{link}"
            
            return {
                "id": f"homegate-stealth-{hash(link) % 100000}",
                "titre": title,
                "prix": price,
                "adresse": address,
                "url_annonce": link,
                "source": "Homegate-Stealth",
            }
            
        except Exception:
            return None
    
    def _extract_price(self, text: str) -> Optional[float]:
        """Extrait le prix d'un texte."""
        import re
        if not text:
            return None
        # Enlever tout sauf chiffres
        digits = re.sub(r"[^\d]", "", text)
        if digits:
            return float(digits)
        return None


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

async def scrape_with_stealth(
    source: str,
    location: str,
    transaction_type: str = "rent",
    limit: int = 50,
    proxy: Optional[ProxyConfig] = None,
) -> List[Dict[str, Any]]:
    """
    Scrape avec le navigateur stealth.
    
    Args:
        source: "immoscout24" ou "homegate"
        location: Ville
        transaction_type: "rent" ou "buy"
        limit: Nombre max
        proxy: Configuration proxy optionnelle
    
    Returns:
        Liste de résultats
    """
    scraper = StealthPropertyScraper(proxy=proxy)
    
    if source.lower() == "immoscout24":
        return await scraper.scrape_immoscout24(location, transaction_type, limit)
    elif source.lower() == "homegate":
        return await scraper.scrape_homegate(location, transaction_type, limit)
    else:
        raise ValueError(f"Source non supportée: {source}")
