# =============================================================================
# ANTI-BOT UTILITIES - Techniques pour éviter la détection
# =============================================================================

import random
import asyncio
from typing import Dict, List, Optional
import aiohttp

# Liste de User-Agents réalistes (navigateurs actuels)
USER_AGENTS = [
    # Chrome Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    # Chrome Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    # Firefox Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
    # Firefox Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:133.0) Gecko/20100101 Firefox/133.0",
    # Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    # Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
]

# Accept-Language variations
ACCEPT_LANGUAGES = [
    "fr-CH,fr;q=0.9,en;q=0.8,de;q=0.7",
    "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "de-CH,de;q=0.9,fr;q=0.8,en;q=0.7",
    "en-US,en;q=0.9,fr;q=0.8",
    "fr;q=0.9,en;q=0.8",
]

def get_random_user_agent() -> str:
    """Retourne un User-Agent aléatoire."""
    return random.choice(USER_AGENTS)

def get_random_accept_language() -> str:
    """Retourne un Accept-Language aléatoire."""
    return random.choice(ACCEPT_LANGUAGES)

def get_stealth_headers(referer: str = "") -> Dict[str, str]:
    """
    Génère des headers réalistes pour éviter la détection.
    """
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": get_random_accept_language(),
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "DNT": "1",
        "Sec-CH-UA": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none" if not referer else "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": get_random_user_agent(),
    }
    
    if referer:
        headers["Referer"] = referer
    
    return headers

async def random_delay(min_seconds: float = 0.5, max_seconds: float = 2.0):
    """Ajoute un délai aléatoire entre les requêtes."""
    delay = random.uniform(min_seconds, max_seconds)
    await asyncio.sleep(delay)

class StealthSession:
    """
    Session aiohttp avec protection anti-bot.
    Rotation automatique des User-Agents et gestion des cookies.
    """
    
    def __init__(
        self, 
        timeout: int = 30,
        max_retries: int = 3,
        base_delay: float = 1.0,
    ):
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.max_retries = max_retries
        self.base_delay = base_delay
        self._session: Optional[aiohttp.ClientSession] = None
        self._request_count = 0
        self._cookies: Dict[str, str] = {}
    
    async def __aenter__(self):
        # Créer une session avec cookie jar
        cookie_jar = aiohttp.CookieJar()
        self._session = aiohttp.ClientSession(
            timeout=self.timeout,
            cookie_jar=cookie_jar,
            headers=get_stealth_headers(),
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()
            self._session = None
    
    async def get(
        self, 
        url: str, 
        referer: str = "",
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Effectue une requête GET avec protection anti-bot.
        Inclut retry automatique et délais.
        """
        if not self._session:
            raise RuntimeError("Session non initialisée. Utilisez 'async with'.")
        
        # Rotation des headers à chaque requête
        headers = get_stealth_headers(referer)
        if extra_headers:
            headers.update(extra_headers)
        
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                # Délai entre requêtes (sauf première)
                if self._request_count > 0:
                    await random_delay(self.base_delay, self.base_delay * 2)
                
                self._request_count += 1
                
                async with self._session.get(url, headers=headers) as response:
                    if response.status == 200:
                        return await response.text()
                    
                    elif response.status == 403:
                        # Anti-bot détecté - attendre plus longtemps
                        await asyncio.sleep(5 + attempt * 5)
                        # Changer le User-Agent pour le prochain essai
                        headers["User-Agent"] = get_random_user_agent()
                        last_error = f"HTTP 403 - Accès bloqué (tentative {attempt + 1})"
                        continue
                    
                    elif response.status == 429:
                        # Rate limit - attendre encore plus
                        await asyncio.sleep(10 + attempt * 10)
                        last_error = f"HTTP 429 - Rate limit (tentative {attempt + 1})"
                        continue
                    
                    elif response.status >= 500:
                        # Erreur serveur - retry
                        await asyncio.sleep(2 + attempt * 2)
                        last_error = f"HTTP {response.status} - Erreur serveur"
                        continue
                    
                    else:
                        return await response.text()
            
            except aiohttp.ClientError as e:
                last_error = str(e)
                await asyncio.sleep(2 + attempt * 2)
                continue
        
        raise Exception(f"Échec après {self.max_retries} tentatives: {last_error}")

    async def get_with_cookies(
        self, 
        url: str,
        cookie_url: str = "",
    ) -> str:
        """
        Récupère d'abord les cookies depuis la page principale,
        puis effectue la requête cible.
        """
        # D'abord, visiter la page principale pour obtenir les cookies
        if cookie_url:
            try:
                await self.get(cookie_url)
                await random_delay(1.0, 2.0)
            except Exception:
                pass  # Continuer même si ça échoue
        
        # Ensuite, faire la vraie requête
        return await self.get(url, referer=cookie_url or url)

