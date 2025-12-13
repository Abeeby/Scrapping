# =============================================================================
# SOCIAL SCRAPER - Scraping Facebook et Instagram
# =============================================================================
# Recherche d'informations de contact sur les réseaux sociaux:
#   - Facebook (recherche publique)
#   - Instagram (via Instaloader ou API)
#   - Extraction de numéros dans bio/about
# =============================================================================

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

import aiohttp
from playwright.async_api import async_playwright, Browser, Page, TimeoutError as PlaywrightTimeout

from app.core.logger import logger


# =============================================================================
# CONFIGURATION
# =============================================================================

FACEBOOK_BASE_URL = "https://www.facebook.com"
INSTAGRAM_BASE_URL = "https://www.instagram.com"

# Patterns pour extraire les numéros de téléphone suisses
SWISS_PHONE_PATTERNS = [
    r'(?:\+41|0041|0)[\s\.]?7[4-9][\s\.]?\d{3}[\s\.]?\d{2}[\s\.]?\d{2}',  # Mobile
    r'(?:\+41|0041|0)[\s\.]?[2-9]\d[\s\.]?\d{3}[\s\.]?\d{2}[\s\.]?\d{2}',  # Fixe
]

# Patterns pour détecter les liens de contact
CONTACT_PATTERNS = {
    "whatsapp": r'wa\.me/(\d+)',
    "telegram": r't\.me/(\+?\d+)',
    "phone_link": r'tel:(\+?\d+)',
}


@dataclass
class SocialProfile:
    """Profil réseau social extrait."""
    # Plateforme
    platform: str  # facebook, instagram
    profile_url: str
    profile_id: str = ""
    
    # Identité
    display_name: str = ""
    username: str = ""
    
    # Bio
    bio: str = ""
    
    # Contact
    phone: str = ""
    mobile: str = ""
    email: str = ""
    website: str = ""
    
    # Localisation
    location: str = ""
    city: str = ""
    
    # Métadonnées
    profile_image: str = ""
    followers_count: int = 0
    is_verified: bool = False
    is_business: bool = False
    
    # Extraction
    extracted_phones: List[str] = field(default_factory=list)
    confidence: float = 0.0
    extracted_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "platform": self.platform,
            "profile_url": self.profile_url,
            "profile_id": self.profile_id,
            "display_name": self.display_name,
            "username": self.username,
            "phone": self.phone,
            "mobile": self.mobile,
            "email": self.email,
            "location": self.location,
            "city": self.city,
            "followers_count": self.followers_count,
            "is_business": self.is_business,
            "extracted_phones": self.extracted_phones,
            "confidence": self.confidence,
        }


class SocialScraper:
    """
    Scraper pour Facebook et Instagram.
    
    ATTENTION: Le scraping des réseaux sociaux peut violer leurs conditions.
    Utiliser uniquement pour des données publiques.
    
    Fonctionnalités:
    - Recherche par nom sur Facebook
    - Recherche par nom sur Instagram
    - Extraction de numéros dans les bios
    - Détection de profils business
    
    Usage:
        scraper = SocialScraper()
        
        # Rechercher sur Facebook
        profiles = await scraper.search_facebook(name="Jean Dupont", city="Genève")
        
        # Rechercher sur Instagram
        profiles = await scraper.search_instagram(name="Jean Dupont")
        
        # Recherche multi-plateforme
        profiles = await scraper.search_all(name="Jean Dupont", city="Genève")
    """

    def __init__(self):
        self._browser: Optional[Browser] = None
        self._page: Optional[Page] = None
        self._session: Optional[aiohttp.ClientSession] = None

    async def _init_browser(self):
        """Initialise le navigateur."""
        if self._browser:
            return
        
        playwright = await async_playwright().start()
        
        self._browser = await playwright.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ],
        )
        
        self._context = await self._browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="fr-CH",
        )
        
        # Anti-détection
        await self._context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        """)
        
        self._page = await self._context.new_page()

    async def _get_session(self) -> aiohttp.ClientSession:
        """Retourne la session HTTP."""
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        """Ferme les ressources."""
        if self._browser:
            await self._browser.close()
            self._browser = None
            self._page = None
        
        if self._session and not self._session.closed:
            await self._session.close()

    async def _random_delay(self, min_s: float = 1.0, max_s: float = 3.0):
        """Délai aléatoire."""
        import random
        await asyncio.sleep(random.uniform(min_s, max_s))

    # =========================================================================
    # FACEBOOK
    # =========================================================================

    async def search_facebook(
        self,
        name: str,
        city: str = "",
        max_results: int = 5,
    ) -> List[SocialProfile]:
        """
        Recherche une personne sur Facebook.
        
        Note: Facebook limite fortement les recherches non-authentifiées.
        """
        profiles = []
        
        await self._init_browser()
        
        # Construire la requête de recherche
        query = f"{name} {city}".strip()
        search_url = f"{FACEBOOK_BASE_URL}/search/people?q={quote_plus(query)}"
        
        try:
            await self._page.goto(search_url, wait_until="networkidle")
            await self._random_delay()
            
            # Facebook redirige souvent vers login
            if "/login" in self._page.url:
                logger.info("[Facebook] Authentification requise, essai recherche alternative")
                return await self._search_facebook_public(name, city, max_results)
            
            # Parser les résultats si on arrive à la page de recherche
            profiles = await self._parse_facebook_results(max_results)
            
        except PlaywrightTimeout:
            logger.warning("[Facebook] Timeout recherche")
        except Exception as e:
            logger.error(f"[Facebook] Erreur recherche: {e}")
        
        return profiles

    async def _search_facebook_public(
        self,
        name: str,
        city: str,
        max_results: int,
    ) -> List[SocialProfile]:
        """
        Recherche Facebook via recherche Google (contourne l'auth).
        """
        profiles = []
        
        try:
            query = f"site:facebook.com {name} {city}".strip()
            google_url = f"https://www.google.com/search?q={quote_plus(query)}"
            
            await self._page.goto(google_url, wait_until="networkidle")
            await self._random_delay()
            
            # Extraire les URLs Facebook
            links = await self._page.query_selector_all('a[href*="facebook.com"]')
            
            seen_urls = set()
            for link in links[:max_results * 2]:
                href = await link.get_attribute("href")
                if not href:
                    continue
                
                # Nettoyer l'URL Google
                if "url?q=" in href:
                    href = href.split("url?q=")[1].split("&")[0]
                
                # Filtrer les profils
                if "/people/" in href or ".facebook.com/" in href:
                    if href not in seen_urls:
                        seen_urls.add(href)
                        
                        profile = SocialProfile(
                            platform="facebook",
                            profile_url=href,
                            confidence=0.3,
                        )
                        
                        # Extraire le nom depuis l'URL ou le titre
                        try:
                            title_el = await link.query_selector("h3")
                            if title_el:
                                title = await title_el.text_content()
                                if title:
                                    profile.display_name = title.split(" - ")[0].strip()
                        except:
                            pass
                        
                        profiles.append(profile)
                        
                        if len(profiles) >= max_results:
                            break
                            
        except Exception as e:
            logger.warning(f"[Facebook] Erreur recherche Google: {e}")
        
        return profiles

    async def _parse_facebook_results(self, max_results: int) -> List[SocialProfile]:
        """Parse les résultats de recherche Facebook."""
        profiles = []
        
        try:
            # Attendre les résultats
            await self._page.wait_for_selector('[role="main"]', timeout=10000)
            
            # Trouver les cartes de profil
            cards = await self._page.query_selector_all('[data-visualcompletion="ignore-dynamic"]')
            
            for card in cards[:max_results]:
                try:
                    profile = await self._parse_facebook_card(card)
                    if profile:
                        profiles.append(profile)
                except:
                    continue
                    
        except Exception as e:
            logger.warning(f"[Facebook] Erreur parsing: {e}")
        
        return profiles

    async def _parse_facebook_card(self, card) -> Optional[SocialProfile]:
        """Parse une carte de profil Facebook."""
        try:
            link = await card.query_selector('a[role="link"]')
            if not link:
                return None
            
            url = await link.get_attribute("href")
            if not url or "facebook.com" not in url:
                return None
            
            profile = SocialProfile(
                platform="facebook",
                profile_url=url,
            )
            
            # Nom
            name_el = await card.query_selector('span[dir="auto"]')
            if name_el:
                profile.display_name = (await name_el.text_content() or "").strip()
            
            # Username depuis l'URL
            if "/profile.php?" in url:
                profile.profile_id = url.split("id=")[1].split("&")[0] if "id=" in url else ""
            else:
                profile.username = url.split("facebook.com/")[1].split("/")[0].split("?")[0]
            
            profile.confidence = 0.5
            
            return profile
            
        except:
            return None

    async def get_facebook_profile_details(
        self,
        profile_url: str,
    ) -> Optional[SocialProfile]:
        """
        Extrait les détails d'un profil Facebook.
        """
        await self._init_browser()
        
        profile = SocialProfile(
            platform="facebook",
            profile_url=profile_url,
        )
        
        try:
            # Aller sur le profil + section About
            about_url = f"{profile_url}/about" if not profile_url.endswith("/about") else profile_url
            
            await self._page.goto(about_url, wait_until="networkidle")
            await self._random_delay()
            
            # Vérifier si login requis
            if "/login" in self._page.url:
                return profile  # Retourner profil basique
            
            # Extraire le nom
            name_el = await self._page.query_selector('h1')
            if name_el:
                profile.display_name = (await name_el.text_content() or "").strip()
            
            # Extraire le contenu About
            content = await self._page.content()
            
            # Chercher des numéros de téléphone
            profile.extracted_phones = self._extract_phones(content)
            if profile.extracted_phones:
                for phone in profile.extracted_phones:
                    if self._is_swiss_mobile(phone):
                        profile.mobile = phone
                        break
                    else:
                        profile.phone = phone
            
            # Chercher email
            email_match = re.search(
                r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
                content
            )
            if email_match:
                profile.email = email_match.group()
            
            # Ville/Location
            location_patterns = [
                r'Lives in ([^<]+)',
                r'From ([^<]+)',
                r'Habite à ([^<]+)',
            ]
            for pattern in location_patterns:
                match = re.search(pattern, content)
                if match:
                    profile.location = match.group(1).strip()
                    profile.city = profile.location.split(",")[0].strip()
                    break
            
            profile.confidence = 0.6
            
        except Exception as e:
            logger.error(f"[Facebook] Erreur détails profil: {e}")
        
        return profile

    # =========================================================================
    # INSTAGRAM
    # =========================================================================

    async def search_instagram(
        self,
        name: str,
        max_results: int = 5,
    ) -> List[SocialProfile]:
        """
        Recherche une personne sur Instagram.
        """
        profiles = []
        
        # Instagram nécessite généralement une authentification
        # On utilise la recherche Google comme alternative
        
        await self._init_browser()
        
        try:
            query = f'site:instagram.com "{name}"'
            google_url = f"https://www.google.com/search?q={quote_plus(query)}"
            
            await self._page.goto(google_url, wait_until="networkidle")
            await self._random_delay()
            
            # Extraire les URLs Instagram
            links = await self._page.query_selector_all('a[href*="instagram.com"]')
            
            seen_urls = set()
            for link in links[:max_results * 2]:
                href = await link.get_attribute("href")
                if not href:
                    continue
                
                # Nettoyer l'URL
                if "url?q=" in href:
                    href = href.split("url?q=")[1].split("&")[0]
                
                # Filtrer les profils (pas les posts)
                if "instagram.com/" in href and "/p/" not in href and "/reel/" not in href:
                    clean_url = href.split("?")[0].rstrip("/")
                    
                    if clean_url not in seen_urls:
                        seen_urls.add(clean_url)
                        
                        # Extraire le username
                        parts = clean_url.split("instagram.com/")
                        username = parts[1].split("/")[0] if len(parts) > 1 else ""
                        
                        if username and username not in ["explore", "accounts", "stories"]:
                            profile = SocialProfile(
                                platform="instagram",
                                profile_url=clean_url,
                                username=username,
                                confidence=0.4,
                            )
                            
                            profiles.append(profile)
                            
                            if len(profiles) >= max_results:
                                break
                                
        except Exception as e:
            logger.error(f"[Instagram] Erreur recherche: {e}")
        
        return profiles

    async def get_instagram_profile_details(
        self,
        profile_url: str,
    ) -> Optional[SocialProfile]:
        """
        Extrait les détails d'un profil Instagram.
        """
        await self._init_browser()
        
        profile = SocialProfile(
            platform="instagram",
            profile_url=profile_url,
        )
        
        try:
            await self._page.goto(profile_url, wait_until="networkidle")
            await self._random_delay()
            
            # Vérifier login
            if "/accounts/login" in self._page.url:
                return profile
            
            # Extraire le username
            parts = profile_url.split("instagram.com/")
            if len(parts) > 1:
                profile.username = parts[1].split("/")[0].split("?")[0]
            
            # Extraire le nom d'affichage
            header = await self._page.query_selector('header section')
            if header:
                # Nom complet (généralement dans un h2 ou span)
                name_el = await header.query_selector('span[class*="x1lliihq"]')
                if name_el:
                    profile.display_name = (await name_el.text_content() or "").strip()
            
            # Extraire la bio
            bio_el = await self._page.query_selector('header section span[class*="x1e0frkt"]')
            if not bio_el:
                bio_el = await self._page.query_selector('header section div[class*="_aa_c"]')
            
            if bio_el:
                profile.bio = (await bio_el.text_content() or "").strip()
                
                # Chercher des numéros dans la bio
                profile.extracted_phones = self._extract_phones(profile.bio)
                if profile.extracted_phones:
                    for phone in profile.extracted_phones:
                        if self._is_swiss_mobile(phone):
                            profile.mobile = phone
                            break
                        else:
                            profile.phone = phone
            
            # Nombre de followers
            followers_el = await self._page.query_selector('a[href*="/followers/"] span span')
            if followers_el:
                followers_text = await followers_el.text_content()
                if followers_text:
                    profile.followers_count = self._parse_count(followers_text)
            
            # Vérifier si compte business
            contact_btn = await self._page.query_selector('a[href*="tel:"], a[href*="mailto:"], button[class*="contact"]')
            profile.is_business = contact_btn is not None
            
            # Email link
            email_link = await self._page.query_selector('a[href^="mailto:"]')
            if email_link:
                href = await email_link.get_attribute("href")
                if href:
                    profile.email = href.replace("mailto:", "").split("?")[0]
            
            profile.confidence = 0.6
            
        except Exception as e:
            logger.error(f"[Instagram] Erreur détails profil: {e}")
        
        return profile

    # =========================================================================
    # RECHERCHE MULTI-PLATEFORME
    # =========================================================================

    async def search_all(
        self,
        name: str,
        city: str = "",
        max_per_platform: int = 3,
    ) -> List[SocialProfile]:
        """
        Recherche sur toutes les plateformes.
        """
        all_profiles = []
        
        # Rechercher en parallèle
        tasks = [
            self.search_facebook(name, city, max_per_platform),
            self.search_instagram(name, max_per_platform),
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, list):
                all_profiles.extend(result)
            elif isinstance(result, Exception):
                logger.warning(f"[Social] Erreur recherche: {result}")
        
        return all_profiles

    async def extract_phones_from_profiles(
        self,
        profiles: List[SocialProfile],
    ) -> List[SocialProfile]:
        """
        Enrichit les profils en extrayant les téléphones des pages détaillées.
        """
        enriched = []
        
        for profile in profiles:
            try:
                if profile.platform == "facebook":
                    detailed = await self.get_facebook_profile_details(profile.profile_url)
                elif profile.platform == "instagram":
                    detailed = await self.get_instagram_profile_details(profile.profile_url)
                else:
                    detailed = profile
                
                if detailed:
                    enriched.append(detailed)
                    
                await self._random_delay(1, 2)
                
            except Exception as e:
                logger.warning(f"[Social] Erreur enrichissement: {e}")
                enriched.append(profile)
        
        return enriched

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _extract_phones(self, text: str) -> List[str]:
        """Extrait tous les numéros de téléphone suisses d'un texte."""
        phones = []
        
        for pattern in SWISS_PHONE_PATTERNS:
            matches = re.findall(pattern, text)
            for match in matches:
                normalized = self._normalize_phone(match)
                if normalized and normalized not in phones:
                    phones.append(normalized)
        
        # Chercher aussi les liens tel: et WhatsApp
        for name, pattern in CONTACT_PATTERNS.items():
            matches = re.findall(pattern, text)
            for match in matches:
                normalized = self._normalize_phone(match)
                if normalized and normalized not in phones:
                    phones.append(normalized)
        
        return phones

    def _normalize_phone(self, phone: str) -> str:
        """Normalise un numéro suisse."""
        if not phone:
            return ""
        
        cleaned = re.sub(r'[^\d+]', '', phone)
        
        if cleaned.startswith('00'):
            cleaned = '+' + cleaned[2:]
        elif cleaned.startswith('0') and len(cleaned) == 10:
            cleaned = '+41' + cleaned[1:]
        elif not cleaned.startswith('+') and len(cleaned) == 9:
            cleaned = '+41' + cleaned
        
        return cleaned

    def _is_swiss_mobile(self, phone: str) -> bool:
        """Vérifie si c'est un mobile suisse."""
        normalized = self._normalize_phone(phone)
        return bool(re.match(r'\+417[4-9]\d{7}$', normalized))

    def _parse_count(self, text: str) -> int:
        """Parse un nombre avec suffixe (K, M)."""
        text = text.strip().lower()
        
        multipliers = {
            'k': 1000,
            'm': 1000000,
            'b': 1000000000,
        }
        
        for suffix, mult in multipliers.items():
            if suffix in text:
                try:
                    num = float(text.replace(suffix, '').replace(',', '.').strip())
                    return int(num * mult)
                except:
                    pass
        
        try:
            return int(text.replace(',', '').replace(' ', ''))
        except:
            return 0


# =============================================================================
# FONCTIONS UTILITAIRES
# =============================================================================

async def search_social_profiles(
    name: str,
    city: str = "",
) -> List[Dict[str, Any]]:
    """
    Helper pour rechercher sur les réseaux sociaux.
    """
    scraper = SocialScraper()
    
    try:
        profiles = await scraper.search_all(name=name, city=city)
        return [p.to_dict() for p in profiles]
    finally:
        await scraper.close()


async def extract_mobile_from_social(
    name: str,
    city: str = "",
) -> Optional[str]:
    """
    Recherche un mobile sur les réseaux sociaux.
    
    Returns:
        Premier mobile suisse trouvé ou None
    """
    scraper = SocialScraper()
    
    try:
        profiles = await scraper.search_all(name=name, city=city, max_per_platform=2)
        enriched = await scraper.extract_phones_from_profiles(profiles)
        
        for profile in enriched:
            if profile.mobile:
                return profile.mobile
            if profile.extracted_phones:
                for phone in profile.extracted_phones:
                    if scraper._is_swiss_mobile(phone):
                        return phone
                        
    except Exception as e:
        logger.error(f"[Social] Erreur: {e}")
    finally:
        await scraper.close()
    
    return None

