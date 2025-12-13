# =============================================================================
# LINKEDIN SCRAPER - Extraction de profils LinkedIn
# =============================================================================
# Scrape LinkedIn pour trouver des informations de contact:
#   - Recherche par nom + entreprise
#   - Extraction de numéros de téléphone (si publics)
#   - Extraction d'emails professionnels
#   - Identification des profils immobiliers
# =============================================================================

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus, urljoin

import aiohttp
from playwright.async_api import async_playwright, Browser, Page, TimeoutError as PlaywrightTimeout

from app.core.logger import logger


# =============================================================================
# CONFIGURATION
# =============================================================================

LINKEDIN_BASE_URL = "https://www.linkedin.com"
LINKEDIN_SEARCH_URL = "https://www.linkedin.com/search/results/people/"

# User agents rotatifs
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

# Délais pour éviter la détection
MIN_DELAY = 2.0
MAX_DELAY = 5.0

# Mots-clés pour détecter les profils immobiliers
REAL_ESTATE_KEYWORDS = [
    "immobilier", "real estate", "courtier", "broker", "agent immobilier",
    "property", "propriétaire", "investisseur", "investissement",
    "biens", "transactions", "achat", "vente",
]


@dataclass
class LinkedInProfile:
    """Profil LinkedIn extrait."""
    # Identité
    profile_url: str
    full_name: str = ""
    first_name: str = ""
    last_name: str = ""
    headline: str = ""
    
    # Localisation
    location: str = ""
    city: str = ""
    country: str = ""
    
    # Contact
    phone: str = ""
    mobile: str = ""
    email: str = ""
    
    # Professionnel
    current_company: str = ""
    current_title: str = ""
    industry: str = ""
    
    # Immobilier
    is_real_estate_related: bool = False
    real_estate_score: float = 0.0
    
    # Métadonnées
    profile_image: str = ""
    connections_count: int = 0
    extracted_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    source: str = "LinkedIn"
    confidence: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "profile_url": self.profile_url,
            "full_name": self.full_name,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "headline": self.headline,
            "location": self.location,
            "city": self.city,
            "phone": self.phone,
            "mobile": self.mobile,
            "email": self.email,
            "current_company": self.current_company,
            "current_title": self.current_title,
            "is_real_estate_related": self.is_real_estate_related,
            "real_estate_score": self.real_estate_score,
            "connections_count": self.connections_count,
            "source": self.source,
            "confidence": self.confidence,
            "extracted_at": self.extracted_at,
        }


class LinkedInScraper:
    """
    Scraper LinkedIn pour extraire des informations de contact.
    
    ATTENTION: Le scraping LinkedIn peut violer leurs conditions d'utilisation.
    Utiliser avec précaution et uniquement pour des données publiques.
    
    Fonctionnalités:
    - Recherche par nom + localisation
    - Recherche par entreprise
    - Extraction d'informations publiques
    - Détection de profils immobiliers
    
    Usage:
        scraper = LinkedInScraper()
        
        # Rechercher une personne
        profiles = await scraper.search_person(
            name="Jean Dupont",
            location="Genève"
        )
        
        # Extraire les détails d'un profil
        profile = await scraper.get_profile_details(profile_url)
    """

    def __init__(
        self,
        linkedin_email: Optional[str] = None,
        linkedin_password: Optional[str] = None,
        use_stealth: bool = True,
    ):
        self.linkedin_email = linkedin_email
        self.linkedin_password = linkedin_password
        self.use_stealth = use_stealth
        
        self._browser: Optional[Browser] = None
        self._page: Optional[Page] = None
        self._logged_in: bool = False
        self._ua_index: int = 0

    async def _init_browser(self):
        """Initialise le navigateur Playwright."""
        if self._browser:
            return
        
        playwright = await async_playwright().start()
        
        launch_args = [
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--no-sandbox",
        ]
        
        self._browser = await playwright.chromium.launch(
            headless=True,
            args=launch_args,
        )
        
        # Créer un contexte avec des paramètres réalistes
        self._context = await self._browser.new_context(
            user_agent=USER_AGENTS[self._ua_index],
            viewport={"width": 1920, "height": 1080},
            locale="fr-CH",
            timezone_id="Europe/Zurich",
        )
        
        # Appliquer des scripts anti-détection
        if self.use_stealth:
            await self._apply_stealth_scripts()
        
        self._page = await self._context.new_page()

    async def _apply_stealth_scripts(self):
        """Applique des scripts pour éviter la détection."""
        if not self._context:
            return
        
        # Scripts anti-détection
        await self._context.add_init_script("""
            // Masquer webdriver
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            
            // Masquer les plugins Chromium
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            
            // Modifier les langues
            Object.defineProperty(navigator, 'languages', {
                get: () => ['fr-CH', 'fr', 'en-US', 'en']
            });
        """)

    async def _random_delay(self, min_sec: float = MIN_DELAY, max_sec: float = MAX_DELAY):
        """Délai aléatoire pour simuler un comportement humain."""
        import random
        delay = random.uniform(min_sec, max_sec)
        await asyncio.sleep(delay)

    async def close(self):
        """Ferme le navigateur."""
        if self._browser:
            await self._browser.close()
            self._browser = None
            self._page = None
            self._logged_in = False

    async def _login(self):
        """Se connecte à LinkedIn (si credentials fournis)."""
        if self._logged_in or not self.linkedin_email:
            return
        
        await self._init_browser()
        
        try:
            # Aller sur la page de login
            await self._page.goto(f"{LINKEDIN_BASE_URL}/login", wait_until="networkidle")
            await self._random_delay(1, 2)
            
            # Remplir le formulaire
            await self._page.fill('input[name="session_key"]', self.linkedin_email)
            await self._random_delay(0.5, 1)
            
            await self._page.fill('input[name="session_password"]', self.linkedin_password)
            await self._random_delay(0.5, 1)
            
            # Soumettre
            await self._page.click('button[type="submit"]')
            await self._page.wait_for_load_state("networkidle")
            
            # Vérifier la connexion
            if "/feed" in self._page.url or "/in/" in self._page.url:
                self._logged_in = True
                logger.info("[LinkedIn] Connexion réussie")
            else:
                logger.warning("[LinkedIn] Connexion échouée ou 2FA requis")
                
        except Exception as e:
            logger.error(f"[LinkedIn] Erreur connexion: {e}")

    async def search_person(
        self,
        name: str,
        location: str = "",
        company: str = "",
        max_results: int = 5,
    ) -> List[LinkedInProfile]:
        """
        Recherche une personne sur LinkedIn.
        
        Args:
            name: Nom de la personne
            location: Localisation (ville ou pays)
            company: Entreprise actuelle (optionnel)
            max_results: Nombre max de résultats
            
        Returns:
            Liste de profils trouvés
        """
        profiles = []
        
        await self._init_browser()
        
        # Construire l'URL de recherche
        search_params = [f"keywords={quote_plus(name)}"]
        
        if location:
            # Utiliser les geo IDs LinkedIn pour la Suisse
            geo_ids = {
                "genève": "106693272",
                "geneva": "106693272",
                "vaud": "90009616",
                "lausanne": "102996959",
                "zurich": "100083833",
                "suisse": "106693272",
                "switzerland": "106693272",
            }
            
            location_lower = location.lower()
            geo_id = geo_ids.get(location_lower)
            if geo_id:
                search_params.append(f"geoUrn=%5B%22{geo_id}%22%5D")
        
        search_url = f"{LINKEDIN_SEARCH_URL}?{'&'.join(search_params)}"
        
        try:
            await self._page.goto(search_url, wait_until="networkidle")
            await self._random_delay()
            
            # LinkedIn peut demander de se connecter
            if "/login" in self._page.url or "authwall" in self._page.url:
                if self.linkedin_email:
                    await self._login()
                    await self._page.goto(search_url, wait_until="networkidle")
                    await self._random_delay()
                else:
                    # Essayer la recherche publique via Google
                    profiles = await self._search_via_google(name, location)
                    return profiles
            
            # Parser les résultats
            profiles = await self._parse_search_results(max_results)
            
        except PlaywrightTimeout:
            logger.warning("[LinkedIn] Timeout recherche")
        except Exception as e:
            logger.error(f"[LinkedIn] Erreur recherche: {e}")
        
        return profiles

    async def _search_via_google(
        self,
        name: str,
        location: str,
    ) -> List[LinkedInProfile]:
        """
        Recherche LinkedIn via Google (contourne l'authwall).
        """
        profiles = []
        
        try:
            # Recherche Google site:linkedin.com
            query = f"site:linkedin.com/in {name} {location}".strip()
            google_url = f"https://www.google.com/search?q={quote_plus(query)}"
            
            await self._page.goto(google_url, wait_until="networkidle")
            await self._random_delay()
            
            # Extraire les URLs LinkedIn des résultats Google
            links = await self._page.query_selector_all('a[href*="linkedin.com/in/"]')
            
            for link in links[:5]:
                href = await link.get_attribute("href")
                if href and "/in/" in href:
                    # Nettoyer l'URL
                    if "url?q=" in href:
                        href = href.split("url?q=")[1].split("&")[0]
                    
                    profile = LinkedInProfile(
                        profile_url=href,
                        confidence=0.4,
                    )
                    
                    # Essayer d'extraire le nom depuis l'URL
                    profile_id = href.split("/in/")[-1].rstrip("/").split("?")[0]
                    profile.full_name = profile_id.replace("-", " ").title()
                    
                    profiles.append(profile)
                    
        except Exception as e:
            logger.warning(f"[LinkedIn] Erreur recherche Google: {e}")
        
        return profiles

    async def _parse_search_results(self, max_results: int) -> List[LinkedInProfile]:
        """Parse les résultats de recherche LinkedIn."""
        profiles = []
        
        try:
            # Attendre les résultats
            await self._page.wait_for_selector(
                'div[class*="search-results"]',
                timeout=10000,
            )
            
            # Trouver les cartes de profil
            cards = await self._page.query_selector_all(
                'li[class*="reusable-search__result-container"]'
            )
            
            for card in cards[:max_results]:
                try:
                    profile = await self._parse_profile_card(card)
                    if profile:
                        profiles.append(profile)
                except Exception as e:
                    logger.debug(f"[LinkedIn] Erreur parsing carte: {e}")
                    
        except Exception as e:
            logger.warning(f"[LinkedIn] Erreur parsing résultats: {e}")
        
        return profiles

    async def _parse_profile_card(self, card) -> Optional[LinkedInProfile]:
        """Parse une carte de profil depuis les résultats de recherche."""
        profile = LinkedInProfile(profile_url="")
        
        try:
            # URL du profil
            link = await card.query_selector('a[href*="/in/"]')
            if link:
                profile.profile_url = await link.get_attribute("href") or ""
            
            if not profile.profile_url:
                return None
            
            # Nom
            name_el = await card.query_selector('span[class*="entity-result__title"] span[aria-hidden="true"]')
            if name_el:
                profile.full_name = (await name_el.text_content() or "").strip()
                parts = profile.full_name.split()
                if len(parts) >= 2:
                    profile.first_name = parts[0]
                    profile.last_name = " ".join(parts[1:])
                elif parts:
                    profile.last_name = parts[0]
            
            # Headline
            headline_el = await card.query_selector('div[class*="entity-result__primary-subtitle"]')
            if headline_el:
                profile.headline = (await headline_el.text_content() or "").strip()
            
            # Location
            location_el = await card.query_selector('div[class*="entity-result__secondary-subtitle"]')
            if location_el:
                profile.location = (await location_el.text_content() or "").strip()
                # Extraire la ville
                if "," in profile.location:
                    profile.city = profile.location.split(",")[0].strip()
                else:
                    profile.city = profile.location
            
            # Score immobilier
            profile.is_real_estate_related, profile.real_estate_score = self._calculate_real_estate_score(
                profile.headline, profile.current_company
            )
            
            # Confiance basée sur les informations disponibles
            profile.confidence = 0.5
            if profile.full_name:
                profile.confidence += 0.2
            if profile.location:
                profile.confidence += 0.1
            if profile.headline:
                profile.confidence += 0.1
                
        except Exception as e:
            logger.debug(f"[LinkedIn] Erreur parsing profile card: {e}")
            return None
        
        return profile

    async def get_profile_details(self, profile_url: str) -> Optional[LinkedInProfile]:
        """
        Extrait les détails d'un profil LinkedIn.
        
        Args:
            profile_url: URL du profil LinkedIn
            
        Returns:
            Profil avec détails complets
        """
        await self._init_browser()
        
        profile = LinkedInProfile(profile_url=profile_url)
        
        try:
            await self._page.goto(profile_url, wait_until="networkidle")
            await self._random_delay()
            
            # Vérifier l'authwall
            if "/login" in self._page.url or "authwall" in self._page.url:
                if self.linkedin_email:
                    await self._login()
                    await self._page.goto(profile_url, wait_until="networkidle")
                    await self._random_delay()
                else:
                    return await self._get_public_profile_details(profile_url)
            
            # Extraire le nom
            name_el = await self._page.query_selector('h1[class*="text-heading-xlarge"]')
            if name_el:
                profile.full_name = (await name_el.text_content() or "").strip()
                parts = profile.full_name.split()
                if len(parts) >= 2:
                    profile.first_name = parts[0]
                    profile.last_name = " ".join(parts[1:])
            
            # Headline
            headline_el = await self._page.query_selector('div[class*="text-body-medium"]')
            if headline_el:
                profile.headline = (await headline_el.text_content() or "").strip()
            
            # Location
            location_el = await self._page.query_selector('span[class*="text-body-small inline t-black--light break-words"]')
            if location_el:
                profile.location = (await location_el.text_content() or "").strip()
            
            # Contact info (si visible)
            await self._extract_contact_info(profile)
            
            # Score immobilier
            profile.is_real_estate_related, profile.real_estate_score = self._calculate_real_estate_score(
                profile.headline, profile.current_company
            )
            
            profile.confidence = 0.8
            
        except Exception as e:
            logger.error(f"[LinkedIn] Erreur extraction profil: {e}")
        
        return profile

    async def _get_public_profile_details(self, profile_url: str) -> Optional[LinkedInProfile]:
        """Extrait les infos publiques sans connexion."""
        profile = LinkedInProfile(profile_url=profile_url)
        
        try:
            # Extraire l'ID du profil
            profile_id = profile_url.split("/in/")[-1].rstrip("/").split("?")[0]
            profile.full_name = profile_id.replace("-", " ").title()
            
            parts = profile.full_name.split()
            if len(parts) >= 2:
                profile.first_name = parts[0]
                profile.last_name = " ".join(parts[1:])
            
            profile.confidence = 0.3
            
        except Exception:
            pass
        
        return profile

    async def _extract_contact_info(self, profile: LinkedInProfile):
        """Extrait les informations de contact (section Contact Info)."""
        try:
            # Cliquer sur "Contact info" si disponible
            contact_btn = await self._page.query_selector('a[href*="/overlay/contact-info"]')
            
            if contact_btn:
                await contact_btn.click()
                await self._page.wait_for_selector('div[class*="pv-contact-info"]', timeout=5000)
                await self._random_delay(0.5, 1)
                
                # Extraire le téléphone
                phone_el = await self._page.query_selector('section[class*="phone"] a[href^="tel:"]')
                if phone_el:
                    phone = await phone_el.text_content()
                    if phone:
                        normalized = self._normalize_phone(phone)
                        if self._is_swiss_mobile(normalized):
                            profile.mobile = normalized
                        else:
                            profile.phone = normalized
                
                # Extraire l'email
                email_el = await self._page.query_selector('section[class*="email"] a[href^="mailto:"]')
                if email_el:
                    profile.email = (await email_el.text_content() or "").strip()
                
                # Fermer le modal
                close_btn = await self._page.query_selector('button[aria-label*="Dismiss"]')
                if close_btn:
                    await close_btn.click()
                    await self._random_delay(0.3, 0.5)
                    
        except Exception as e:
            logger.debug(f"[LinkedIn] Erreur extraction contact: {e}")

    def _calculate_real_estate_score(
        self,
        headline: str,
        company: str,
    ) -> tuple[bool, float]:
        """Calcule un score d'affinité avec l'immobilier."""
        text = f"{headline or ''} {company or ''}".lower()
        
        if not text.strip():
            return False, 0.0
        
        score = 0.0
        matches = 0
        
        for keyword in REAL_ESTATE_KEYWORDS:
            if keyword.lower() in text:
                matches += 1
                score += 0.15
        
        score = min(score, 1.0)
        is_related = matches >= 1
        
        return is_related, score

    def _normalize_phone(self, phone: str) -> str:
        """Normalise un numéro de téléphone."""
        if not phone:
            return ""
        cleaned = re.sub(r'[^\d+]', '', phone)
        if cleaned.startswith('00'):
            cleaned = '+' + cleaned[2:]
        elif cleaned.startswith('0') and len(cleaned) == 10:
            cleaned = '+41' + cleaned[1:]
        return cleaned

    def _is_swiss_mobile(self, phone: str) -> bool:
        """Vérifie si c'est un mobile suisse."""
        return bool(re.match(r'\+417[4-9]\d{7}$', phone))


# =============================================================================
# FONCTIONS UTILITAIRES
# =============================================================================

async def search_linkedin_profile(
    name: str,
    city: str = "",
    company: str = "",
) -> Optional[LinkedInProfile]:
    """
    Helper pour rechercher un profil LinkedIn.
    
    Returns:
        Premier profil correspondant ou None
    """
    scraper = LinkedInScraper()
    
    try:
        profiles = await scraper.search_person(
            name=name,
            location=city,
            company=company,
            max_results=3,
        )
        
        if profiles:
            # Retourner le profil avec le meilleur score
            return max(profiles, key=lambda p: p.confidence)
            
    except Exception as e:
        logger.error(f"[LinkedIn] Erreur recherche: {e}")
    finally:
        await scraper.close()
    
    return None


async def extract_linkedin_contact(profile_url: str) -> Dict[str, str]:
    """
    Extrait les infos de contact d'un profil LinkedIn.
    
    Returns:
        Dict avec phone, mobile, email
    """
    scraper = LinkedInScraper()
    result = {
        "phone": "",
        "mobile": "",
        "email": "",
    }
    
    try:
        profile = await scraper.get_profile_details(profile_url)
        
        if profile:
            result["phone"] = profile.phone
            result["mobile"] = profile.mobile
            result["email"] = profile.email
            
    except Exception as e:
        logger.error(f"[LinkedIn] Erreur extraction: {e}")
    finally:
        await scraper.close()
    
    return result
