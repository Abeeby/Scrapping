# =============================================================================
# MOBILE LOOKUP - Recherche de numéros mobiles via APIs tierces
# =============================================================================
# Utilise plusieurs sources pour trouver les numéros mobiles:
#   - Truecaller API (recherche inversée)
#   - Sync.me API (alternative)
#   - NumVerify API (validation)
#   - Recherche inverse sur annuaires
# =============================================================================

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

import aiohttp

from app.core.logger import logger


# =============================================================================
# CONFIGURATION
# =============================================================================

# API Endpoints
TRUECALLER_API = "https://search5-noneu.truecaller.com/v2/search"
SYNCME_API = "https://sync.me/api/search"
NUMVERIFY_API = "http://apilayer.net/api/validate"

# Headers pour Truecaller (simuler l'app mobile)
TRUECALLER_HEADERS = {
    "User-Agent": "Truecaller/11.75.6 (Android; 11)",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
}

# Configuration rate limiting
RATE_LIMIT_DELAY = 2.0  # secondes entre requêtes
MAX_RETRIES = 3


@dataclass
class MobileLookupResult:
    """Résultat d'une recherche de numéro mobile."""
    # Requête
    query_name: str
    query_city: str = ""
    query_country: str = "CH"
    
    # Résultats
    mobile_found: str = ""
    phone_type: str = ""  # mobile, landline, voip
    carrier: str = ""
    
    # Métadonnées
    source: str = ""
    confidence: float = 0.0
    raw_response: Dict[str, Any] = field(default_factory=dict)
    
    # Validation
    is_valid: bool = False
    is_swiss_mobile: bool = False
    formatted_number: str = ""
    
    # Timestamps
    queried_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "query_name": self.query_name,
            "query_city": self.query_city,
            "mobile_found": self.mobile_found,
            "formatted_number": self.formatted_number,
            "phone_type": self.phone_type,
            "carrier": self.carrier,
            "source": self.source,
            "confidence": self.confidence,
            "is_valid": self.is_valid,
            "is_swiss_mobile": self.is_swiss_mobile,
            "queried_at": self.queried_at,
        }


class MobileLookupScraper:
    """
    Scraper pour rechercher des numéros mobiles.
    
    Sources supportées:
    - Truecaller (API non-officielle)
    - Sync.me (API non-officielle)  
    - Recherche inversée web
    
    Usage:
        scraper = MobileLookupScraper()
        
        # Rechercher par nom
        result = await scraper.search_by_name("Jean Dupont", city="Genève")
        
        # Recherche inversée
        result = await scraper.reverse_lookup("+41791234567")
        
        # Valider un numéro
        is_valid = await scraper.validate_number("+41791234567")
    """

    def __init__(
        self,
        truecaller_token: Optional[str] = None,
        syncme_token: Optional[str] = None,
        numverify_key: Optional[str] = None,
    ):
        self.truecaller_token = truecaller_token
        self.syncme_token = syncme_token
        self.numverify_key = numverify_key
        self._session: Optional[aiohttp.ClientSession] = None
        self._last_request_time = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        """Retourne la session HTTP."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        """Ferme la session."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def _rate_limit(self):
        """Applique le rate limiting."""
        elapsed = time.time() - self._last_request_time
        if elapsed < RATE_LIMIT_DELAY:
            await asyncio.sleep(RATE_LIMIT_DELAY - elapsed)
        self._last_request_time = time.time()

    # =========================================================================
    # RECHERCHE PAR NOM
    # =========================================================================

    async def search_by_name(
        self,
        name: str,
        city: str = "",
        canton: str = "",
        use_all_sources: bool = True,
    ) -> List[MobileLookupResult]:
        """
        Recherche un numéro mobile par nom.
        
        Args:
            name: Nom complet de la personne
            city: Ville (optionnel, améliore les résultats)
            canton: Canton CH (GE, VD, etc.)
            use_all_sources: Utiliser toutes les sources disponibles
            
        Returns:
            Liste de résultats triés par confiance
        """
        results = []
        
        # Nettoyer le nom
        clean_name = self._clean_name(name)
        if not clean_name:
            return results
        
        # Construire la requête avec localisation
        location = city or canton or "Suisse"
        
        # Source 1: Truecaller (si token disponible)
        if use_all_sources or self.truecaller_token:
            try:
                tc_result = await self._search_truecaller(clean_name, location)
                if tc_result:
                    results.extend(tc_result)
            except Exception as e:
                logger.warning(f"[MobileLookup] Erreur Truecaller: {e}")
        
        # Source 2: Sync.me
        if use_all_sources or self.syncme_token:
            try:
                sm_result = await self._search_syncme(clean_name, location)
                if sm_result:
                    results.extend(sm_result)
            except Exception as e:
                logger.warning(f"[MobileLookup] Erreur Sync.me: {e}")
        
        # Source 3: Recherche web alternative
        try:
            web_results = await self._search_web_directories(clean_name, city)
            if web_results:
                results.extend(web_results)
        except Exception as e:
            logger.warning(f"[MobileLookup] Erreur recherche web: {e}")
        
        # Valider et déduper les résultats
        validated_results = await self._validate_and_dedup(results)
        
        # Trier par confiance
        validated_results.sort(key=lambda x: x.confidence, reverse=True)
        
        return validated_results

    async def _search_truecaller(
        self,
        name: str,
        location: str,
    ) -> List[MobileLookupResult]:
        """Recherche via Truecaller API."""
        results = []
        
        # Truecaller nécessite un token d'authentification
        if not self.truecaller_token:
            # Essayer la recherche web fallback
            return await self._search_truecaller_web(name, location)
        
        await self._rate_limit()
        session = await self._get_session()
        
        params = {
            "q": f"{name} {location}",
            "countryCode": "CH",
            "type": "4",  # Recherche par nom
            "locAddr": location,
            "placement": "SEARCHRESULTS,HISTORY,DETAILS",
            "encoding": "json",
        }
        
        headers = {
            **TRUECALLER_HEADERS,
            "Authorization": f"Bearer {self.truecaller_token}",
        }
        
        try:
            async with session.get(
                TRUECALLER_API,
                params=params,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    for entry in data.get("data", []):
                        phones = entry.get("phones", [])
                        for phone in phones:
                            number = phone.get("e164Format", "")
                            if number and self._is_swiss_mobile(number):
                                result = MobileLookupResult(
                                    query_name=name,
                                    query_city=location,
                                    mobile_found=number,
                                    phone_type=phone.get("type", "mobile"),
                                    carrier=phone.get("carrier", ""),
                                    source="Truecaller",
                                    confidence=0.8,
                                    raw_response=entry,
                                    is_swiss_mobile=True,
                                    formatted_number=self._format_swiss_number(number),
                                )
                                results.append(result)
                                
        except Exception as e:
            logger.error(f"[MobileLookup] Truecaller API error: {e}")
        
        return results

    async def _search_truecaller_web(
        self,
        name: str,
        location: str,
    ) -> List[MobileLookupResult]:
        """Recherche Truecaller via scraping web (fallback)."""
        # Truecaller web requiert JavaScript, on utilise un fallback
        return []

    async def _search_syncme(
        self,
        name: str,
        location: str,
    ) -> List[MobileLookupResult]:
        """Recherche via Sync.me API."""
        results = []
        
        await self._rate_limit()
        session = await self._get_session()
        
        # Sync.me utilise une API similaire
        # Note: Cette API peut nécessiter une authentification
        query = f"{name} {location} Switzerland"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        }
        
        if self.syncme_token:
            headers["Authorization"] = f"Bearer {self.syncme_token}"
        
        try:
            # Essayer l'API directe
            params = {"q": query, "country": "CH"}
            
            async with session.get(
                SYNCME_API,
                params=params,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    for entry in data.get("results", []):
                        number = entry.get("phone", "")
                        if number and self._is_swiss_mobile(number):
                            result = MobileLookupResult(
                                query_name=name,
                                query_city=location,
                                mobile_found=number,
                                phone_type="mobile",
                                source="Sync.me",
                                confidence=0.7,
                                raw_response=entry,
                                is_swiss_mobile=True,
                                formatted_number=self._format_swiss_number(number),
                            )
                            results.append(result)
                            
        except Exception as e:
            logger.debug(f"[MobileLookup] Sync.me error: {e}")
        
        return results

    async def _search_web_directories(
        self,
        name: str,
        city: str,
    ) -> List[MobileLookupResult]:
        """
        Recherche dans les annuaires web suisses.
        Cherche spécifiquement les numéros mobiles (07x).
        """
        results = []
        
        # Cette fonction complémente Search.ch/Local.ch
        # en cherchant des sources alternatives
        
        # Sources alternatives suisses pour mobiles
        alternative_sources = [
            ("tel.search.ch", f"https://tel.search.ch/result.html?q={quote_plus(name)}&w={quote_plus(city)}"),
            ("directories.ch", f"https://www.directories.ch/fr/recherche/{quote_plus(name)}/{quote_plus(city)}/"),
        ]
        
        session = await self._get_session()
        
        for source_name, url in alternative_sources:
            try:
                await self._rate_limit()
                
                async with session.get(
                    url,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    },
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as response:
                    if response.status == 200:
                        html = await response.text()
                        
                        # Chercher les numéros mobiles suisses dans le HTML
                        mobile_pattern = r'(?:\+41|0041|0)7[4-9]\s?\d{3}\s?\d{2}\s?\d{2}'
                        matches = re.findall(mobile_pattern, html)
                        
                        for match in matches[:3]:  # Limiter
                            number = self._normalize_phone(match)
                            if self._is_swiss_mobile(number):
                                result = MobileLookupResult(
                                    query_name=name,
                                    query_city=city,
                                    mobile_found=number,
                                    phone_type="mobile",
                                    source=source_name,
                                    confidence=0.5,
                                    is_swiss_mobile=True,
                                    formatted_number=self._format_swiss_number(number),
                                )
                                results.append(result)
                                
            except Exception as e:
                logger.debug(f"[MobileLookup] {source_name} error: {e}")
        
        return results

    # =========================================================================
    # RECHERCHE INVERSÉE
    # =========================================================================

    async def reverse_lookup(self, phone_number: str) -> Optional[MobileLookupResult]:
        """
        Recherche inversée: trouve le propriétaire d'un numéro.
        
        Args:
            phone_number: Numéro de téléphone à rechercher
            
        Returns:
            Informations sur le propriétaire si trouvé
        """
        # Normaliser le numéro
        normalized = self._normalize_phone(phone_number)
        
        if not normalized:
            return None
        
        # Essayer Truecaller d'abord
        if self.truecaller_token:
            try:
                result = await self._reverse_truecaller(normalized)
                if result:
                    return result
            except Exception as e:
                logger.warning(f"[MobileLookup] Reverse Truecaller error: {e}")
        
        # Puis Sync.me
        try:
            result = await self._reverse_syncme(normalized)
            if result:
                return result
        except Exception as e:
            logger.warning(f"[MobileLookup] Reverse Sync.me error: {e}")
        
        return None

    async def _reverse_truecaller(
        self,
        phone_number: str,
    ) -> Optional[MobileLookupResult]:
        """Recherche inversée via Truecaller."""
        await self._rate_limit()
        session = await self._get_session()
        
        params = {
            "q": phone_number,
            "countryCode": "CH",
            "type": "1",  # Recherche par numéro
        }
        
        headers = {
            **TRUECALLER_HEADERS,
            "Authorization": f"Bearer {self.truecaller_token}",
        }
        
        try:
            async with session.get(
                TRUECALLER_API,
                params=params,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    entries = data.get("data", [])
                    
                    if entries:
                        entry = entries[0]
                        name = entry.get("name", "")
                        
                        return MobileLookupResult(
                            query_name=name,
                            mobile_found=phone_number,
                            phone_type="mobile" if self._is_swiss_mobile(phone_number) else "landline",
                            carrier=entry.get("carrier", ""),
                            source="Truecaller",
                            confidence=0.85,
                            raw_response=entry,
                            is_swiss_mobile=self._is_swiss_mobile(phone_number),
                            formatted_number=self._format_swiss_number(phone_number),
                        )
                        
        except Exception as e:
            logger.error(f"[MobileLookup] Truecaller reverse error: {e}")
        
        return None

    async def _reverse_syncme(
        self,
        phone_number: str,
    ) -> Optional[MobileLookupResult]:
        """Recherche inversée via Sync.me."""
        # Sync.me reverse lookup
        return None

    # =========================================================================
    # VALIDATION
    # =========================================================================

    async def validate_number(
        self,
        phone_number: str,
    ) -> Dict[str, Any]:
        """
        Valide un numéro de téléphone.
        
        Args:
            phone_number: Numéro à valider
            
        Returns:
            Dict avec informations de validation
        """
        normalized = self._normalize_phone(phone_number)
        
        validation = {
            "original": phone_number,
            "normalized": normalized,
            "is_valid": False,
            "is_swiss": False,
            "is_mobile": False,
            "carrier": None,
            "line_type": None,
        }
        
        if not normalized:
            return validation
        
        # Validation locale (regex suisse)
        validation["is_swiss"] = normalized.startswith("+41")
        validation["is_mobile"] = self._is_swiss_mobile(normalized)
        validation["is_valid"] = validation["is_swiss"] and len(normalized) == 12
        
        # Validation via NumVerify API (si configuré)
        if self.numverify_key:
            try:
                api_result = await self._validate_numverify(normalized)
                if api_result:
                    validation.update(api_result)
            except Exception as e:
                logger.debug(f"[MobileLookup] NumVerify error: {e}")
        
        return validation

    async def _validate_numverify(
        self,
        phone_number: str,
    ) -> Optional[Dict[str, Any]]:
        """Valide via NumVerify API."""
        session = await self._get_session()
        
        params = {
            "access_key": self.numverify_key,
            "number": phone_number.lstrip("+"),
            "country_code": "CH",
            "format": 1,
        }
        
        try:
            async with session.get(
                NUMVERIFY_API,
                params=params,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    return {
                        "is_valid": data.get("valid", False),
                        "carrier": data.get("carrier"),
                        "line_type": data.get("line_type"),
                    }
                    
        except Exception as e:
            logger.error(f"[MobileLookup] NumVerify error: {e}")
        
        return None

    # =========================================================================
    # BATCH PROCESSING
    # =========================================================================

    async def batch_lookup(
        self,
        queries: List[Dict[str, str]],
        max_concurrent: int = 5,
    ) -> List[MobileLookupResult]:
        """
        Recherche batch de numéros mobiles.
        
        Args:
            queries: Liste de {"name": "...", "city": "..."}
            max_concurrent: Max requêtes simultanées
            
        Returns:
            Liste de résultats
        """
        results = []
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def process_query(query: Dict[str, str]):
            async with semaphore:
                try:
                    query_results = await self.search_by_name(
                        name=query.get("name", ""),
                        city=query.get("city", ""),
                        canton=query.get("canton", ""),
                    )
                    return query_results
                except Exception as e:
                    logger.warning(f"[MobileLookup] Batch error: {e}")
                    return []
        
        tasks = [process_query(q) for q in queries]
        batch_results = await asyncio.gather(*tasks)
        
        for query_results in batch_results:
            results.extend(query_results)
        
        return results

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _clean_name(self, name: str) -> str:
        """Nettoie un nom pour la recherche."""
        if not name:
            return ""
        # Supprimer les titres
        cleaned = re.sub(r'\b(Mr|Mrs|Ms|Dr|Prof|Mme|M\.)\b\.?', '', name, flags=re.IGNORECASE)
        # Supprimer les caractères spéciaux
        cleaned = re.sub(r'[^\w\s\-]', '', cleaned)
        return ' '.join(cleaned.split())

    def _normalize_phone(self, phone: str) -> str:
        """Normalise un numéro de téléphone suisse."""
        if not phone:
            return ""
        
        # Garder uniquement les chiffres et +
        cleaned = re.sub(r'[^\d+]', '', phone)
        
        # Conversions suisses
        if cleaned.startswith('00'):
            cleaned = '+' + cleaned[2:]
        elif cleaned.startswith('0') and len(cleaned) == 10:
            cleaned = '+41' + cleaned[1:]
        elif cleaned.startswith('41') and len(cleaned) == 11:
            cleaned = '+41' + cleaned[2:]
        elif not cleaned.startswith('+') and len(cleaned) == 9:
            cleaned = '+41' + cleaned
        
        return cleaned

    def _is_swiss_mobile(self, phone: str) -> bool:
        """Vérifie si c'est un numéro mobile suisse."""
        normalized = self._normalize_phone(phone)
        # Mobiles suisses: +417x (74-79)
        return bool(re.match(r'\+417[4-9]\d{7}$', normalized))

    def _format_swiss_number(self, phone: str) -> str:
        """Formate un numéro suisse pour affichage."""
        normalized = self._normalize_phone(phone)
        
        if len(normalized) == 12 and normalized.startswith('+41'):
            # Format: +41 79 123 45 67
            return f"{normalized[:3]} {normalized[3:5]} {normalized[5:8]} {normalized[8:10]} {normalized[10:]}"
        
        return normalized

    async def _validate_and_dedup(
        self,
        results: List[MobileLookupResult],
    ) -> List[MobileLookupResult]:
        """Valide et dédupe les résultats."""
        seen_numbers = set()
        unique_results = []
        
        for result in results:
            normalized = self._normalize_phone(result.mobile_found)
            
            if normalized and normalized not in seen_numbers:
                seen_numbers.add(normalized)
                
                # Validation additionnelle
                result.is_valid = len(normalized) == 12 and normalized.startswith('+41')
                result.is_swiss_mobile = self._is_swiss_mobile(normalized)
                result.formatted_number = self._format_swiss_number(normalized)
                
                unique_results.append(result)
        
        return unique_results


# =============================================================================
# FONCTIONS UTILITAIRES
# =============================================================================

async def lookup_mobile_for_prospect(
    name: str,
    city: str = "",
    canton: str = "",
) -> Optional[str]:
    """
    Helper pour rechercher un mobile pour un prospect.
    
    Returns:
        Numéro mobile formaté ou None
    """
    scraper = MobileLookupScraper()
    
    try:
        results = await scraper.search_by_name(
            name=name,
            city=city,
            canton=canton,
        )
        
        if results:
            # Prendre le résultat avec la meilleure confiance
            best = max(results, key=lambda x: x.confidence)
            if best.confidence >= 0.5 and best.is_swiss_mobile:
                return best.formatted_number
                
    except Exception as e:
        logger.error(f"[MobileLookup] Error: {e}")
    finally:
        await scraper.close()
    
    return None


async def batch_mobile_lookup(
    prospects: List[Dict[str, str]],
) -> Dict[str, str]:
    """
    Recherche batch de mobiles pour plusieurs prospects.
    
    Args:
        prospects: Liste de {"name": "...", "city": "...", "id": "..."}
        
    Returns:
        Dict {prospect_id: mobile_number}
    """
    scraper = MobileLookupScraper()
    results = {}
    
    try:
        lookup_results = await scraper.batch_lookup(prospects)
        
        # Mapper les résultats aux IDs
        for prospect in prospects:
            prospect_id = prospect.get("id", "")
            name = prospect.get("name", "")
            
            # Trouver le meilleur résultat pour ce prospect
            matching = [
                r for r in lookup_results
                if r.query_name.lower() == name.lower()
            ]
            
            if matching:
                best = max(matching, key=lambda x: x.confidence)
                if best.confidence >= 0.5:
                    results[prospect_id] = best.formatted_number
                    
    finally:
        await scraper.close()
    
    return results
