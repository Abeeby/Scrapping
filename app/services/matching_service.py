# =============================================================================
# SERVICE DE MATCHING ULTIMATE - Annonce → Propriétaire → Contact
# =============================================================================
# Combine toutes les sources pour identifier et contacter les propriétaires:
#   - Cadastre VD/GE → Parcelle → EGRID
#   - Registre Foncier → Propriétaire
#   - Annuaires (Search.ch/Local.ch) → Téléphone/Email
#   - FAO/FOSC → Signaux vendeur
# =============================================================================

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal, Prospect, ScrapedListing
from app.core.logger import logger
from app.core.websocket import emit_activity

# Import des scrapers
from app.scrapers.geoadmin import GeoAdminClient, GeoAdminError
from app.scrapers.searchch import SearchChScraper, SearchChScraperError

# Import conditionnel des nouveaux scrapers
try:
    from app.scrapers.rf_geneve import RFGeneveScraper, get_communes_geneve
    RF_GE_AVAILABLE = True
except ImportError:
    RF_GE_AVAILABLE = False

try:
    from app.scrapers.rf_vaud import RFVaudScraper, get_communes_vaud
    RF_VD_AVAILABLE = True
except ImportError:
    RF_VD_AVAILABLE = False


class MatchingError(Exception):
    """Erreur du service de matching."""
    pass


@dataclass
class MatchResult:
    """Résultat d'un matching annonce → propriétaire."""
    # Statut
    status: str = "pending"  # pending, matched, partial, no_match, error
    confidence: float = 0.0  # 0-1
    
    # Données propriétaire
    nom: str = ""
    prenom: str = ""
    telephone: str = ""
    email: str = ""
    adresse: str = ""
    code_postal: str = ""
    ville: str = ""
    canton: str = ""
    
    # Données cadastrales
    egrid: str = ""
    numero_parcelle: str = ""
    commune: str = ""
    surface_m2: float = 0
    
    # Sources utilisées
    sources: List[str] = field(default_factory=list)
    rf_link: str = ""
    
    # Métadonnées
    matched_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    processing_time_ms: int = 0
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "confidence": self.confidence,
            "proprietaire": {
                "nom": self.nom,
                "prenom": self.prenom,
                "telephone": self.telephone,
                "email": self.email,
                "adresse": self.adresse,
                "code_postal": self.code_postal,
                "ville": self.ville,
                "canton": self.canton,
            },
            "cadastre": {
                "egrid": self.egrid,
                "numero_parcelle": self.numero_parcelle,
                "commune": self.commune,
                "surface_m2": self.surface_m2,
            },
            "sources": self.sources,
            "rf_link": self.rf_link,
            "matched_at": self.matched_at,
            "processing_time_ms": self.processing_time_ms,
            "errors": self.errors,
        }

    def to_prospect_dict(self) -> Dict[str, Any]:
        """Format compatible avec le modèle Prospect."""
        return {
            "nom": self.nom,
            "prenom": self.prenom,
            "telephone": self.telephone,
            "email": self.email,
            "adresse": self.adresse,
            "code_postal": self.code_postal,
            "ville": self.ville,
            "canton": self.canton,
            "lien_rf": self.rf_link,
            "source": f"Matching ({', '.join(self.sources)})",
            "notes": f"EGRID: {self.egrid}\nParcelle: {self.numero_parcelle}\nSurface: {self.surface_m2}m²",
        }


class MatchingService:
    """
    Service de matching principal.
    
    Pipeline:
    1. Normaliser l'adresse (GeoAdmin)
    2. Rechercher la parcelle (Cadastre VD/GE)
    3. Extraire le propriétaire (Registre Foncier)
    4. Enrichir les contacts (Annuaires)
    5. Scorer le match
    
    Usage:
        service = MatchingService()
        result = await service.match_listing(listing)
    """

    def __init__(self):
        self._geoadmin: Optional[GeoAdminClient] = None
        self._searchch: Optional[SearchChScraper] = None
        self._rf_ge: Optional[RFGeneveScraper] = None
        self._rf_vd: Optional[RFVaudScraper] = None

    async def __aenter__(self):
        self._geoadmin = GeoAdminClient()
        await self._geoadmin.__aenter__()
        
        self._searchch = SearchChScraper()
        await self._searchch.__aenter__()
        
        if RF_GE_AVAILABLE:
            self._rf_ge = RFGeneveScraper()
            await self._rf_ge.__aenter__()
        
        if RF_VD_AVAILABLE:
            self._rf_vd = RFVaudScraper()
            await self._rf_vd.__aenter__()
        
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._geoadmin:
            await self._geoadmin.__aexit__(exc_type, exc_val, exc_tb)
        if self._searchch:
            await self._searchch.__aexit__(exc_type, exc_val, exc_tb)
        if self._rf_ge:
            await self._rf_ge.__aexit__(exc_type, exc_val, exc_tb)
        if self._rf_vd:
            await self._rf_vd.__aexit__(exc_type, exc_val, exc_tb)

    async def match_from_address(
        self,
        adresse: str,
        code_postal: str = "",
        ville: str = "",
        canton: str = "",
    ) -> MatchResult:
        """
        Matching complet à partir d'une adresse.
        
        Args:
            adresse: Rue et numéro
            code_postal: NPA
            ville: Nom de la ville
            canton: Code canton (GE, VD)
            
        Returns:
            MatchResult avec les données du propriétaire
        """
        start_time = datetime.utcnow()
        result = MatchResult()
        
        logger.info(f"[Matching] Début: {adresse}, {code_postal} {ville} ({canton})")
        
        try:
            # ÉTAPE 1: Normaliser l'adresse
            normalized = await self._normalize_address(adresse, code_postal, ville)
            coords = None
            if normalized:
                street = (normalized.get("street") or "").strip()
                house = (normalized.get("house_number") or "").strip()
                if street and house:
                    result.adresse = f"{street} {house}".strip()
                else:
                    result.adresse = street or adresse
                result.code_postal = normalized.get("zip_code", code_postal)
                result.ville = normalized.get("city", ville)
                result.canton = normalized.get("canton", canton) or canton
                # NOTE: GeoAdmin retourne un EGID bâtiment. Le champ `egrid` du MatchResult
                # est utilisé comme identifiant parcellaire (EGRID). On stocke donc l'EGID
                # seulement comme méta dans match_meta via les sources (ou écrasé plus tard).
                coords = normalized.get("coordinates")
                result.sources.append("GeoAdmin")
                
                if normalized.get("confidence", 0) >= 0.7:
                    result.confidence += 0.2
            
            # Déterminer le canton
            canton_detected = result.canton or self._detect_canton(result.code_postal, result.ville)
            result.canton = canton_detected
            
            # ÉTAPE 2: Rechercher dans le Registre Foncier
            rf_result = await self._search_rf(
                adresse=result.adresse or adresse,
                code_postal=result.code_postal or code_postal,
                ville=result.ville or ville,
                canton=canton_detected,
                coordinates=coords,
            )
            
            if rf_result:
                result.nom = rf_result.get("nom", "") or ""
                result.prenom = rf_result.get("prenom", "") or ""
                result.egrid = rf_result.get("egrid", "") or result.egrid
                result.numero_parcelle = rf_result.get("numero_parcelle", "")
                result.commune = rf_result.get("commune", "")
                result.surface_m2 = rf_result.get("surface_m2", 0)
                result.rf_link = rf_result.get("lien_rf", "")
                result.sources.append(f"RF {canton_detected}")
                
                if result.nom:
                    result.confidence += 0.4
                    result.status = "partial"
                else:
                    # On a au moins un lien/identifiant cadastral (utile pour traitement manuel)
                    if result.rf_link or result.numero_parcelle or result.egrid:
                        result.confidence += 0.1
            
            # ÉTAPE 3: Enrichir les contacts via annuaires
            if result.nom:
                contacts = await self._enrich_contacts(
                    nom=result.nom,
                    prenom=result.prenom,
                    ville=result.ville or result.commune,
                    adresse=result.adresse,
                )
                
                if contacts:
                    if contacts.get("telephone"):
                        result.telephone = contacts["telephone"]
                        result.confidence += 0.25
                    if contacts.get("email"):
                        result.email = contacts["email"]
                        result.confidence += 0.15
                    result.sources.append("Search.ch")
            
            # ÉTAPE 4: Déterminer le statut final
            if result.nom and (result.telephone or result.email):
                result.status = "matched"
                result.confidence = min(result.confidence, 1.0)
            elif result.nom:
                result.status = "partial"
            elif result.rf_link:
                result.status = "rf_link_only"
            else:
                result.status = "no_match"
            
        except Exception as e:
            logger.error(f"[Matching] Erreur: {e}")
            result.status = "error"
            result.errors.append(str(e))
        
        # Temps de traitement
        end_time = datetime.utcnow()
        result.processing_time_ms = int((end_time - start_time).total_seconds() * 1000)
        
        logger.info(f"[Matching] Terminé: status={result.status}, confidence={result.confidence:.2f}, time={result.processing_time_ms}ms")
        
        return result

    async def _normalize_address(
        self,
        adresse: str,
        code_postal: str,
        ville: str,
    ) -> Optional[Dict[str, Any]]:
        """Normalise l'adresse via GeoAdmin."""
        if not self._geoadmin:
            return None
        
        try:
            normalized = await self._geoadmin.normalize_address(
                street=adresse,
                zip_code=code_postal,
                city=ville,
            )
            
            if normalized:
                return normalized.to_dict()
                
        except GeoAdminError as e:
            logger.warning(f"[Matching] GeoAdmin error: {e}")
        
        return None

    async def _search_rf(
        self,
        adresse: str,
        code_postal: str,
        ville: str,
        canton: str,
        coordinates: Optional[List[float]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Recherche dans le Registre Foncier."""
        
        canton_upper = canton.upper()
        coords = coordinates or []
        lon = coords[0] if isinstance(coords, list) and len(coords) >= 2 else None
        lat = coords[1] if isinstance(coords, list) and len(coords) >= 2 else None

        # 0) Essayer d'identifier la parcelle via GeoAdmin (EGRID + numéro)
        parcel_info: Optional[Dict[str, Any]] = None
        try:
            if lon is not None and lat is not None and self._geoadmin:
                parcel_info = await self._geoadmin.identify_parcel(lon=float(lon), lat=float(lat))
        except Exception as e:
            logger.debug(f"[Matching] GeoAdmin identify parcel failed: {e}")
        
        # Genève
        if canton_upper == "GE" and self._rf_ge and RF_GE_AVAILABLE:
            try:
                from app.scrapers.rf_geneve import COMMUNES_GE
                
                # Trouver le code commune
                code_commune = None
                ville_lower = ville.lower()
                for code, nom in COMMUNES_GE.items():
                    if nom.lower() == ville_lower or nom.lower() in ville_lower:
                        code_commune = code
                        break
                
                if code_commune:
                    parcelle = None
                    if parcel_info and str(parcel_info.get("parcel_number") or "").isdigit():
                        parcelle = int(str(parcel_info.get("parcel_number")))

                    # Si on a le numéro de parcelle, tenter une extraction complète
                    if parcelle:
                        proprio = await self._rf_ge.get_proprietaire(commune=code_commune, parcelle=parcelle)
                        if proprio:
                            d = proprio.to_dict()
                            nom = (d.get("nom") or "").strip()
                            # Si le scraper n'a pas pu extraire le nom (placeholder), on ne considère pas ça comme un match.
                            if nom.startswith("[") or "extraire" in nom.lower():
                                return {
                                    "egrid": (parcel_info.get("egrid") if parcel_info else "") or d.get("egrid", ""),
                                    "numero_parcelle": str(parcelle),
                                    "commune": ville,
                                    "canton": "GE",
                                    "lien_rf": d.get("lien_rf", "") or self._rf_ge.get_rf_url(code_commune, parcelle),
                                }
                            return {
                                "nom": nom,
                                "prenom": d.get("prenom", ""),
                                "egrid": d.get("egrid", "") or (parcel_info.get("egrid") if parcel_info else ""),
                                "numero_parcelle": str(d.get("numero_parcelle", "") or parcelle),
                                "commune": d.get("commune", "") or ville,
                                "surface_m2": d.get("surface_m2", 0) or 0,
                                "lien_rf": d.get("lien_rf", "") or self._rf_ge.get_rf_url(code_commune, parcelle),
                            }

                    # Fallback: lien RF seulement (commune)
                    return {
                        "egrid": parcel_info.get("egrid") if parcel_info else "",
                        "numero_parcelle": str(parcel_info.get("parcel_number") or "") if parcel_info else "",
                        "commune": ville,
                        "canton": "GE",
                        "lien_rf": f"https://ge.ch/terextraitfoncier/rapport.aspx?commune={code_commune}",
                    }
                    
            except Exception as e:
                logger.warning(f"[Matching] RF GE error: {e}")
        
        # Vaud
        elif canton_upper == "VD" and self._rf_vd and RF_VD_AVAILABLE:
            try:
                egrid = (parcel_info.get("egrid") if parcel_info else "") or ""
                # Si on a l'EGRID, tenter l'extraction InterCapi
                if egrid:
                    proprio = await self._rf_vd.get_by_egrid(egrid)
                    if proprio:
                        d = proprio.to_dict()
                        nom = (d.get("nom") or "").strip()
                        if nom.startswith("[") or "extraire" in nom.lower():
                            return {
                                "egrid": egrid,
                                "numero_parcelle": str(parcel_info.get("parcel_number") or "") if parcel_info else "",
                                "commune": ville,
                                "canton": "VD",
                                "lien_rf": d.get("lien_rf", "") or d.get("lien_intercapi", "") or f"https://intercapi.vd.ch/recherche?egrid={egrid}",
                            }
                        return {
                            "nom": nom,
                            "prenom": d.get("prenom", ""),
                            "egrid": d.get("egrid", "") or egrid,
                            "numero_parcelle": str(d.get("numero_parcelle", "") or (parcel_info.get("parcel_number") if parcel_info else "")),
                            "commune": d.get("commune", "") or ville,
                            "surface_m2": d.get("surface_m2", 0) or 0,
                            "lien_rf": d.get("lien_rf", "") or d.get("lien_intercapi", "") or f"https://intercapi.vd.ch/recherche?egrid={egrid}",
                        }

                # Fallback: lien RF seulement
                return {
                    "egrid": egrid,
                    "numero_parcelle": str(parcel_info.get("parcel_number") or "") if parcel_info else "",
                    "commune": ville,
                    "canton": "VD",
                    "lien_rf": f"https://intercapi.vd.ch/recherche?ville={ville}",
                }
            except Exception as e:
                logger.warning(f"[Matching] RF VD error: {e}")
        
        return None

    async def _enrich_contacts(
        self,
        nom: str,
        prenom: str,
        ville: str,
        adresse: str = "",
    ) -> Optional[Dict[str, str]]:
        """Enrichit les contacts via Search.ch."""
        if not self._searchch:
            return None
        
        try:
            query = f"{prenom} {nom}".strip() if prenom else nom
            
            results = await self._searchch.search(
                query=query,
                ville=ville,
                limit=10,
                type_recherche="person",
            )
            
            if not results:
                return None
            
            # Trouver le meilleur match
            nom_lower = nom.lower()
            best = None
            best_score = 0
            
            for r in results:
                score = 0
                r_nom = (r.get("nom") or "").lower()
                
                if nom_lower in r_nom or r_nom in nom_lower:
                    score += 2
                
                if prenom:
                    r_prenom = (r.get("prenom") or "").lower()
                    if prenom.lower() in r_prenom:
                        score += 1
                
                if adresse:
                    r_adresse = (r.get("adresse") or "").lower()
                    if adresse.lower() in r_adresse or r_adresse in adresse.lower():
                        score += 1
                
                if r.get("telephone"):
                    score += 1
                
                if score > best_score:
                    best_score = score
                    best = r
            
            if best and best_score >= 2:
                return {
                    "telephone": best.get("telephone", ""),
                    "email": best.get("email", ""),
                    "adresse": best.get("adresse", ""),
                }
                
        except SearchChScraperError as e:
            logger.warning(f"[Matching] Search.ch error: {e}")
        
        return None

    def _detect_canton(self, code_postal: str, ville: str) -> str:
        """Détecte le canton à partir du NPA ou de la ville."""
        
        # NPA Genève: 1200-1299
        if code_postal.startswith("12"):
            return "GE"
        
        # NPA Vaud: 1000-1199, 1300-1499, 1800-1899
        if code_postal.startswith("10") or code_postal.startswith("11"):
            return "VD"
        if code_postal.startswith("13") or code_postal.startswith("14"):
            return "VD"
        if code_postal.startswith("18"):
            return "VD"
        
        # Détection par ville
        villes_ge = ["genève", "carouge", "lancy", "vernier", "meyrin", "thônex"]
        villes_vd = ["lausanne", "nyon", "morges", "vevey", "montreux", "renens"]
        
        ville_lower = ville.lower()
        if any(v in ville_lower for v in villes_ge):
            return "GE"
        if any(v in ville_lower for v in villes_vd):
            return "VD"
        
        return ""

    async def match_listing(self, listing: ScrapedListing) -> MatchResult:
        """
        Match une annonce scrapée vers un propriétaire.
        
        Args:
            listing: Annonce depuis la DB
            
        Returns:
            MatchResult
        """
        # Extraire les infos de l'annonce
        details = listing.details or {}
        
        adresse = listing.address or details.get("adresse", "")
        code_postal = listing.npa or details.get("code_postal", "")
        ville = listing.city or details.get("ville", "")
        canton = listing.canton or details.get("canton", "")
        
        return await self.match_from_address(
            adresse=adresse,
            code_postal=code_postal,
            ville=ville,
            canton=canton,
        )

    async def batch_match_listings(
        self,
        listing_ids: List[str],
        delay_ms: int = 500,
        callback: Optional[callable] = None,
        job_id: Optional[int] = None,
    ) -> Dict[str, MatchResult]:
        """
        Match plusieurs annonces en batch.
        
        Args:
            listing_ids: IDs des annonces à matcher
            delay_ms: Délai entre chaque match (rate limiting)
            callback: Callback de progression
            
        Returns:
            Dict {listing_id: MatchResult}
        """
        results: Dict[str, MatchResult] = {}

        async with AsyncSessionLocal() as db:
            job = None
            if job_id:
                try:
                    from app.core.database import BackgroundJob

                    job = await db.get(BackgroundJob, job_id)
                    if job:
                        job.status = "running"
                        job.started_at = datetime.utcnow()
                        job.total = len(listing_ids)
                        job.processed = 0
                        job.updated_at = datetime.utcnow()
                        await db.commit()
                except Exception:
                    job = None

            for i, lid in enumerate(listing_ids):
                lid_key = str(lid)
                try:
                    # Cast (certains appels passent des strings)
                    lid_int = int(lid) if isinstance(lid, str) else lid

                    # Récupérer l'annonce
                    stmt = select(ScrapedListing).where(ScrapedListing.id == lid_int)
                    res = await db.execute(stmt)
                    listing = res.scalar_one_or_none()

                    if listing:
                        result = await self.match_listing(listing)
                        results[lid_key] = result

                        # Mettre à jour l'annonce
                        listing.match_status = result.status
                        listing.match_score = result.confidence
                        listing.matched_at = datetime.utcnow()
                        listing.match_meta = result.to_dict()

                        # Progress job
                        if job:
                            job.processed = i + 1
                            job.updated_at = datetime.utcnow()

                        await db.commit()

                        if callback:
                            callback({
                                "listing_id": lid_key,
                                "index": i + 1,
                                "total": len(listing_ids),
                                "status": result.status,
                                "confidence": result.confidence,
                            })
                    else:
                        results[lid_key] = MatchResult(status="error", errors=["Listing introuvable"])

                except Exception as e:
                    logger.error(f"[Matching] Erreur listing {lid}: {e}")
                    results[lid_key] = MatchResult(status="error", errors=[str(e)])
                    if job:
                        job.processed = i + 1
                        job.updated_at = datetime.utcnow()
                        await db.commit()

                # Rate limiting
                if delay_ms > 0 and i < len(listing_ids) - 1:
                    await asyncio.sleep(delay_ms / 1000)

            # Finaliser job
            if job:
                try:
                    job.status = "completed"
                    job.completed_at = datetime.utcnow()
                    job.result = {
                        "total": len(listing_ids),
                        "processed": job.processed,
                        "matched": sum(1 for r in results.values() if r.status in ("matched", "partial")),
                        "rf_link_only": sum(1 for r in results.values() if r.status == "rf_link_only"),
                        "no_match": sum(1 for r in results.values() if r.status == "no_match"),
                        "errors": sum(1 for r in results.values() if r.status == "error"),
                    }
                    job.updated_at = datetime.utcnow()
                    await db.commit()
                except Exception:
                    pass

        return results


# =============================================================================
# FONCTIONS UTILITAIRES
# =============================================================================

async def match_address_to_owner(
    adresse: str,
    code_postal: str = "",
    ville: str = "",
    canton: str = "",
) -> Dict[str, Any]:
    """
    Helper pour matcher une adresse vers un propriétaire.
    """
    async with MatchingService() as service:
        result = await service.match_from_address(
            adresse=adresse,
            code_postal=code_postal,
            ville=ville,
            canton=canton,
        )
        return result.to_dict()


async def create_prospect_from_match(
    adresse: str,
    code_postal: str = "",
    ville: str = "",
    canton: str = "",
) -> Optional[Dict[str, Any]]:
    """
    Matche une adresse et retourne les données formatées pour créer un prospect.
    """
    async with MatchingService() as service:
        result = await service.match_from_address(
            adresse=adresse,
            code_postal=code_postal,
            ville=ville,
            canton=canton,
        )
        
        if result.status in ("matched", "partial"):
            return result.to_prospect_dict()
        
        return None


async def get_matching_stats() -> Dict[str, Any]:
    """
    Retourne les statistiques de matching.
    """
    async with AsyncSessionLocal() as db:
        # Compter par statut
        from sqlalchemy import func
        
        stats_query = select(
            ScrapedListing.match_status,
            func.count(ScrapedListing.id)
        ).group_by(ScrapedListing.match_status)
        
        res = await db.execute(stats_query)
        stats_by_status = {row[0] or "pending": row[1] for row in res.fetchall()}
        
        # Score moyen
        avg_query = select(func.avg(ScrapedListing.match_score)).where(
            ScrapedListing.match_score.isnot(None)
        )
        avg_res = await db.execute(avg_query)
        avg_score = avg_res.scalar() or 0
        
        total = sum(stats_by_status.values())
        matched = stats_by_status.get("matched", 0) + stats_by_status.get("partial", 0)
        
        return {
            "total_listings": total,
            "by_status": stats_by_status,
            "matched_count": matched,
            "match_rate": (matched / total * 100) if total > 0 else 0,
            "average_score": round(avg_score, 2),
        }

