# =============================================================================
# SERVICE D'ENRICHISSEMENT MOBILE - Orchestration multi-sources
# =============================================================================
# Orchestre toutes les sources de numéros mobiles:
#   - Truecaller / Sync.me (mobile_lookup.py)
#   - LinkedIn (linkedin_scraper.py)
#   - Facebook / Instagram (social_scraper.py)
#   - Search.ch / Local.ch (annuaires existants)
# =============================================================================

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal, Prospect
from app.core.logger import logger
from app.core.websocket import emit_activity

# Import des scrapers
from app.scrapers.mobile_lookup import MobileLookupScraper, MobileLookupResult
from app.scrapers.linkedin_scraper import LinkedInScraper, LinkedInProfile
from app.scrapers.social_scraper import SocialScraper, SocialProfile
from app.scrapers.searchch import SearchChScraper


# =============================================================================
# CONFIGURATION
# =============================================================================

# Priorité des sources (plus haut = plus fiable)
SOURCE_PRIORITY = {
    "search.ch": 90,
    "local.ch": 85,
    "truecaller": 75,
    "sync.me": 70,
    "linkedin": 65,
    "facebook": 55,
    "instagram": 50,
    "manual": 100,
}

# Seuils de confiance
MIN_CONFIDENCE_THRESHOLD = 0.4
AUTO_UPDATE_THRESHOLD = 0.6


@dataclass
class MobileSearchResult:
    """Résultat d'une recherche mobile multi-sources."""
    # Requête
    prospect_id: str
    name: str
    city: str = ""
    canton: str = ""
    
    # Résultat
    mobile_found: str = ""
    formatted_mobile: str = ""
    
    # Métadonnées
    source: str = ""
    confidence: float = 0.0
    all_results: List[Dict[str, Any]] = field(default_factory=list)
    
    # Stats
    sources_checked: List[str] = field(default_factory=list)
    search_duration_ms: float = 0.0
    searched_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "prospect_id": self.prospect_id,
            "name": self.name,
            "city": self.city,
            "mobile_found": self.mobile_found,
            "formatted_mobile": self.formatted_mobile,
            "source": self.source,
            "confidence": self.confidence,
            "sources_checked": self.sources_checked,
            "results_count": len(self.all_results),
            "search_duration_ms": self.search_duration_ms,
        }


@dataclass
class BatchEnrichResult:
    """Résultat d'un enrichissement batch."""
    total_prospects: int
    mobiles_found: int
    already_had_mobile: int
    updated: int
    errors: int
    results: List[MobileSearchResult]
    duration_seconds: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_prospects": self.total_prospects,
            "mobiles_found": self.mobiles_found,
            "already_had_mobile": self.already_had_mobile,
            "updated": self.updated,
            "errors": self.errors,
            "success_rate": self.mobiles_found / self.total_prospects * 100 if self.total_prospects > 0 else 0,
            "duration_seconds": self.duration_seconds,
        }


class MobileEnrichService:
    """
    Service d'enrichissement de numéros mobiles.
    
    Orchestre plusieurs sources pour maximiser les chances de trouver
    un numéro mobile pour un prospect.
    
    Fonctionnalités:
    - Recherche multi-sources parallèle
    - Priorisation et déduplication des résultats
    - Validation des numéros suisses
    - Mise à jour automatique des prospects
    - Batch processing
    
    Usage:
        service = MobileEnrichService()
        
        # Enrichir un prospect
        result = await service.enrich_prospect(prospect_id="xxx")
        
        # Batch enrichissement
        result = await service.batch_enrich(canton="GE", limit=100)
        
        # Recherche directe
        result = await service.search_mobile(
            name="Jean Dupont",
            city="Genève"
        )
    """

    def __init__(
        self,
        # Par défaut: sources publiques/annuaires uniquement (conformité).
        # Les autres sources doivent rester désactivées sauf cadre légal/licence explicite.
        use_truecaller: bool = False,
        use_linkedin: bool = False,
        use_social: bool = False,
        use_directories: bool = True,
        truecaller_token: Optional[str] = None,
        linkedin_email: Optional[str] = None,
        linkedin_password: Optional[str] = None,
    ):
        self.use_truecaller = use_truecaller
        self.use_linkedin = use_linkedin
        self.use_social = use_social
        self.use_directories = use_directories
        
        # Tokens/credentials
        self.truecaller_token = truecaller_token
        self.linkedin_email = linkedin_email
        self.linkedin_password = linkedin_password
        
        # Scrapers (initialisés à la demande)
        self._mobile_scraper: Optional[MobileLookupScraper] = None
        self._linkedin_scraper: Optional[LinkedInScraper] = None
        self._social_scraper: Optional[SocialScraper] = None
        self._searchch_scraper: Optional[SearchChScraper] = None

    async def close(self):
        """Ferme tous les scrapers."""
        if self._mobile_scraper:
            await self._mobile_scraper.close()
        if self._linkedin_scraper:
            await self._linkedin_scraper.close()
        if self._social_scraper:
            await self._social_scraper.close()

    # =========================================================================
    # RECHERCHE MOBILE
    # =========================================================================

    async def search_mobile(
        self,
        name: str,
        city: str = "",
        canton: str = "",
        existing_phone: str = "",
        prospect_id: str = "",
    ) -> MobileSearchResult:
        """
        Recherche un numéro mobile via toutes les sources disponibles.
        
        Args:
            name: Nom complet de la personne
            city: Ville
            canton: Canton (GE, VD, etc.)
            existing_phone: Téléphone existant (pour éviter doublons)
            prospect_id: ID du prospect (optionnel)
            
        Returns:
            MobileSearchResult avec le meilleur résultat
        """
        start_time = datetime.utcnow()
        
        result = MobileSearchResult(
            prospect_id=prospect_id,
            name=name,
            city=city,
            canton=canton,
        )
        
        all_mobiles = []
        
        # Lancer les recherches en parallèle
        tasks = []
        
        if self.use_directories:
            tasks.append(self._search_directories(name, city, canton))
            result.sources_checked.append("directories")
        
        if self.use_truecaller:
            tasks.append(self._search_truecaller(name, city))
            result.sources_checked.append("truecaller")
        
        if self.use_linkedin:
            tasks.append(self._search_linkedin(name, city))
            result.sources_checked.append("linkedin")
        
        if self.use_social:
            tasks.append(self._search_social(name, city))
            result.sources_checked.append("social")
        
        # Exécuter en parallèle
        if tasks:
            search_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for search_result in search_results:
                if isinstance(search_result, list):
                    all_mobiles.extend(search_result)
                elif isinstance(search_result, Exception):
                    logger.warning(f"[MobileEnrich] Erreur source: {search_result}")
        
        # Filtrer et dédupliquer
        valid_mobiles = self._filter_and_rank(all_mobiles, existing_phone)
        
        result.all_results = [m for m in valid_mobiles]
        
        # Prendre le meilleur
        if valid_mobiles:
            best = valid_mobiles[0]
            result.mobile_found = best.get("mobile", "")
            result.formatted_mobile = best.get("formatted", "")
            result.source = best.get("source", "")
            result.confidence = best.get("confidence", 0.0)
        
        # Calculer la durée
        end_time = datetime.utcnow()
        result.search_duration_ms = (end_time - start_time).total_seconds() * 1000
        
        return result

    async def _search_directories(
        self,
        name: str,
        city: str,
        canton: str,
    ) -> List[Dict[str, Any]]:
        """Recherche dans Search.ch / Local.ch."""
        results = []
        
        try:
            if not self._searchch_scraper:
                self._searchch_scraper = SearchChScraper()
            
            # Rechercher sur Search.ch
            search_results = await self._searchch_scraper.search(
                query=name,
                ville=city or canton,
                limit=20,
                type_recherche="person",
            )
            
            for entry in search_results:
                phone = entry.get("telephone", "")
                if phone and self._is_swiss_mobile(phone):
                    results.append({
                        "mobile": self._normalize_phone(phone),
                        "formatted": self._format_phone(phone),
                        "source": "search.ch",
                        "confidence": 0.85,
                        "raw": entry,
                    })
                    
        except Exception as e:
            logger.warning(f"[MobileEnrich] Erreur directories: {e}")
        
        return results

    async def _search_truecaller(
        self,
        name: str,
        city: str,
    ) -> List[Dict[str, Any]]:
        """Recherche via Truecaller/Sync.me."""
        results = []
        
        try:
            if not self._mobile_scraper:
                self._mobile_scraper = MobileLookupScraper(
                    truecaller_token=self.truecaller_token,
                )
            
            lookup_results = await self._mobile_scraper.search_by_name(
                name=name,
                city=city,
            )
            
            for lr in lookup_results:
                if lr.is_swiss_mobile and lr.mobile_found:
                    results.append({
                        "mobile": lr.mobile_found,
                        "formatted": lr.formatted_number,
                        "source": lr.source.lower(),
                        "confidence": lr.confidence,
                        "raw": lr.raw_response,
                    })
                    
        except Exception as e:
            logger.warning(f"[MobileEnrich] Erreur Truecaller: {e}")
        
        return results

    async def _search_linkedin(
        self,
        name: str,
        city: str,
    ) -> List[Dict[str, Any]]:
        """Recherche via LinkedIn."""
        results = []
        
        try:
            if not self._linkedin_scraper:
                self._linkedin_scraper = LinkedInScraper(
                    linkedin_email=self.linkedin_email,
                    linkedin_password=self.linkedin_password,
                )
            
            profiles = await self._linkedin_scraper.search_person(
                name=name,
                location=city,
                max_results=3,
            )
            
            for profile in profiles:
                # Essayer d'obtenir les détails si on a trouvé des profils
                if profile.mobile:
                    results.append({
                        "mobile": self._normalize_phone(profile.mobile),
                        "formatted": self._format_phone(profile.mobile),
                        "source": "linkedin",
                        "confidence": 0.65,
                        "raw": profile.to_dict(),
                    })
                elif profile.phone and self._is_swiss_mobile(profile.phone):
                    results.append({
                        "mobile": self._normalize_phone(profile.phone),
                        "formatted": self._format_phone(profile.phone),
                        "source": "linkedin",
                        "confidence": 0.6,
                        "raw": profile.to_dict(),
                    })
                    
        except Exception as e:
            logger.warning(f"[MobileEnrich] Erreur LinkedIn: {e}")
        
        return results

    async def _search_social(
        self,
        name: str,
        city: str,
    ) -> List[Dict[str, Any]]:
        """Recherche sur Facebook/Instagram."""
        results = []
        
        try:
            if not self._social_scraper:
                self._social_scraper = SocialScraper()
            
            # Rechercher sur les deux plateformes
            profiles = await self._social_scraper.search_all(
                name=name,
                city=city,
                max_per_platform=2,
            )
            
            # Enrichir pour obtenir les téléphones
            enriched = await self._social_scraper.extract_phones_from_profiles(profiles)
            
            for profile in enriched:
                if profile.mobile:
                    results.append({
                        "mobile": profile.mobile,
                        "formatted": self._format_phone(profile.mobile),
                        "source": profile.platform,
                        "confidence": 0.55,
                        "raw": profile.to_dict(),
                    })
                elif profile.extracted_phones:
                    for phone in profile.extracted_phones:
                        if self._is_swiss_mobile(phone):
                            results.append({
                                "mobile": phone,
                                "formatted": self._format_phone(phone),
                                "source": profile.platform,
                                "confidence": 0.5,
                                "raw": profile.to_dict(),
                            })
                            break
                            
        except Exception as e:
            logger.warning(f"[MobileEnrich] Erreur social: {e}")
        
        return results

    def _filter_and_rank(
        self,
        mobiles: List[Dict[str, Any]],
        existing_phone: str = "",
    ) -> List[Dict[str, Any]]:
        """
        Filtre, déduplique et classe les résultats.
        """
        # Normaliser l'existant pour comparaison
        existing_normalized = self._normalize_phone(existing_phone) if existing_phone else ""
        
        # Filtrer et dédupliquer
        seen = set()
        valid = []
        
        for m in mobiles:
            mobile = m.get("mobile", "")
            normalized = self._normalize_phone(mobile)
            
            # Vérifications
            if not normalized:
                continue
            if not self._is_swiss_mobile(normalized):
                continue
            if normalized == existing_normalized:
                continue
            if normalized in seen:
                continue
            
            seen.add(normalized)
            
            # Calculer le score final
            source = m.get("source", "").lower()
            source_priority = SOURCE_PRIORITY.get(source, 50)
            base_confidence = m.get("confidence", 0.5)
            
            # Score final = priorité source normalisée + confiance
            final_score = (source_priority / 100 * 0.5) + (base_confidence * 0.5)
            
            valid.append({
                **m,
                "mobile": normalized,
                "formatted": self._format_phone(normalized),
                "final_score": final_score,
            })
        
        # Trier par score
        valid.sort(key=lambda x: x.get("final_score", 0), reverse=True)
        
        return valid

    # =========================================================================
    # ENRICHISSEMENT DE PROSPECTS
    # =========================================================================

    async def enrich_prospect(
        self,
        prospect_id: str,
        auto_update: bool = True,
    ) -> MobileSearchResult:
        """
        Enrichit un prospect existant avec un numéro mobile.
        
        Args:
            prospect_id: ID du prospect
            auto_update: Mettre à jour automatiquement si confiance suffisante
            
        Returns:
            MobileSearchResult
        """
        async with AsyncSessionLocal() as db:
            # Récupérer le prospect
            result = await db.execute(
                select(Prospect).where(Prospect.id == prospect_id)
            )
            prospect = result.scalar_one_or_none()
            
            if not prospect:
                return MobileSearchResult(
                    prospect_id=prospect_id,
                    name="",
                    confidence=0.0,
                )
            
            # Construire le nom complet
            full_name = f"{prospect.prenom or ''} {prospect.nom or ''}".strip()
            
            # Vérifier si on a déjà un mobile
            existing_phone = prospect.telephone or ""
            existing_mobile = ""
            
            # Chercher un mobile existant dans les notes
            if prospect.notes and "Mobile:" in prospect.notes:
                import re
                mobile_match = re.search(r'Mobile:\s*(\+?\d+)', prospect.notes)
                if mobile_match:
                    existing_mobile = mobile_match.group(1)
            
            if existing_mobile and self._is_swiss_mobile(existing_mobile):
                # On a déjà un mobile
                return MobileSearchResult(
                    prospect_id=prospect_id,
                    name=full_name,
                    city=prospect.ville or "",
                    canton=prospect.canton or "",
                    mobile_found=existing_mobile,
                    formatted_mobile=self._format_phone(existing_mobile),
                    source="existing",
                    confidence=1.0,
                )
            
            # Rechercher un mobile
            search_result = await self.search_mobile(
                name=full_name,
                city=prospect.ville or "",
                canton=prospect.canton or "",
                existing_phone=existing_phone,
                prospect_id=prospect_id,
            )
            
            # Auto-update si confiance suffisante
            if auto_update and search_result.mobile_found and search_result.confidence >= AUTO_UPDATE_THRESHOLD:
                await self._update_prospect_mobile(
                    db, prospect, search_result.mobile_found, search_result.source
                )
            
            return search_result

    async def batch_enrich(
        self,
        canton: str = "",
        city: str = "",
        limit: int = 100,
        skip_with_mobile: bool = True,
        auto_update: bool = True,
    ) -> BatchEnrichResult:
        """
        Enrichit un batch de prospects.
        
        Args:
            canton: Filtrer par canton
            city: Filtrer par ville
            limit: Nombre max de prospects
            skip_with_mobile: Ignorer ceux qui ont déjà un mobile
            auto_update: Mettre à jour automatiquement
            
        Returns:
            BatchEnrichResult
        """
        start_time = datetime.utcnow()
        
        results = []
        stats = {
            "total": 0,
            "found": 0,
            "had_mobile": 0,
            "updated": 0,
            "errors": 0,
        }
        
        async with AsyncSessionLocal() as db:
            # Construire la requête
            query = select(Prospect).where(Prospect.merged_into_id.is_(None))
            
            if canton:
                query = query.where(Prospect.canton == canton)
            if city:
                query = query.where(Prospect.ville.ilike(f"%{city}%"))
            
            # Filtrer ceux sans mobile si demandé
            if skip_with_mobile:
                # Les prospects sans mobile dans les notes
                query = query.where(
                    ~Prospect.notes.contains("Mobile:")
                )
            
            query = query.limit(limit)
            
            result = await db.execute(query)
            prospects = result.scalars().all()
            stats["total"] = len(prospects)
        
        # Traiter chaque prospect
        for prospect in prospects:
            try:
                # Vérifier si mobile déjà présent
                if skip_with_mobile and prospect.notes and "Mobile:" in prospect.notes:
                    stats["had_mobile"] += 1
                    continue
                
                search_result = await self.enrich_prospect(
                    prospect_id=prospect.id,
                    auto_update=auto_update,
                )
                
                results.append(search_result)
                
                if search_result.mobile_found:
                    stats["found"] += 1
                    if search_result.confidence >= AUTO_UPDATE_THRESHOLD:
                        stats["updated"] += 1
                        
            except Exception as e:
                logger.error(f"[MobileEnrich] Erreur prospect {prospect.id}: {e}")
                stats["errors"] += 1
            
            # Petit délai pour éviter le rate limiting
            await asyncio.sleep(0.5)
        
        # Calculer la durée
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        
        await emit_activity(
            "mobile_enrich",
            f"Batch terminé: {stats['found']}/{stats['total']} mobiles trouvés"
        )
        
        return BatchEnrichResult(
            total_prospects=stats["total"],
            mobiles_found=stats["found"],
            already_had_mobile=stats["had_mobile"],
            updated=stats["updated"],
            errors=stats["errors"],
            results=results,
            duration_seconds=duration,
        )

    async def _update_prospect_mobile(
        self,
        db: AsyncSession,
        prospect: Prospect,
        mobile: str,
        source: str,
    ):
        """Met à jour un prospect avec le mobile trouvé."""
        mobile_note = f"Mobile: {mobile} (source: {source})"
        
        if prospect.notes:
            if "Mobile:" not in prospect.notes:
                prospect.notes += f"\n{mobile_note}"
        else:
            prospect.notes = mobile_note
        
        prospect.updated_at = datetime.utcnow()
        await db.commit()
        
        logger.info(f"[MobileEnrich] Prospect {prospect.id} mis à jour avec mobile {mobile}")

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _normalize_phone(self, phone: str) -> str:
        """Normalise un numéro suisse."""
        if not phone:
            return ""
        
        import re
        cleaned = re.sub(r'[^\d+]', '', phone)
        
        if cleaned.startswith('00'):
            cleaned = '+' + cleaned[2:]
        elif cleaned.startswith('0') and len(cleaned) == 10:
            cleaned = '+41' + cleaned[1:]
        elif not cleaned.startswith('+') and len(cleaned) == 9:
            cleaned = '+41' + cleaned
        
        return cleaned

    def _format_phone(self, phone: str) -> str:
        """Formate un numéro suisse."""
        normalized = self._normalize_phone(phone)
        
        if len(normalized) == 12 and normalized.startswith('+41'):
            return f"{normalized[:3]} {normalized[3:5]} {normalized[5:8]} {normalized[8:10]} {normalized[10:]}"
        
        return normalized

    def _is_swiss_mobile(self, phone: str) -> bool:
        """Vérifie si c'est un mobile suisse."""
        import re
        normalized = self._normalize_phone(phone)
        return bool(re.match(r'\+417[4-9]\d{7}$', normalized))


# =============================================================================
# FONCTIONS UTILITAIRES
# =============================================================================

async def enrich_mobile_for_prospect(prospect_id: str) -> Dict[str, Any]:
    """
    Helper pour enrichir un prospect.
    """
    service = MobileEnrichService()
    
    try:
        result = await service.enrich_prospect(prospect_id)
        return result.to_dict()
    finally:
        await service.close()


async def batch_enrich_mobiles(
    canton: str = "",
    limit: int = 50,
) -> Dict[str, Any]:
    """
    Helper pour enrichissement batch.
    """
    service = MobileEnrichService()
    
    try:
        result = await service.batch_enrich(canton=canton, limit=limit)
        return result.to_dict()
    finally:
        await service.close()
