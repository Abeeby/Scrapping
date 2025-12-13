# =============================================================================
# API PROSPECTION - Endpoints unifiés pour la collecte de données
# =============================================================================
# Combine tous les scrapers et services pour un pipeline de prospection complet
# =============================================================================

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
import asyncio

from fastapi import APIRouter, HTTPException, BackgroundTasks, Query
from pydantic import BaseModel, Field

from app.core.logger import logger
from app.core.websocket import emit_activity
from app.core.database import AsyncSessionLocal, BackgroundJob, ScrapedListing

# Import des services
from app.services.matching_service import (
    MatchingService,
    match_address_to_owner,
    create_prospect_from_match,
    get_matching_stats,
)

# Import conditionnel - fusion profils / enrichissement mobile (sources publiques)
try:
    from app.services.profile_merge_service import merge_profile_into_db, batch_merge_profiles
    PROFILE_MERGE_AVAILABLE = True
except Exception:
    PROFILE_MERGE_AVAILABLE = False

try:
    from app.services.mobile_enrich_service import MobileEnrichService
    MOBILE_ENRICH_AVAILABLE = True
except Exception:
    MOBILE_ENRICH_AVAILABLE = False

# Import conditionnel des scrapers
try:
    from app.scrapers.rf_geneve import (
        RFGeneveScraper,
        get_communes_geneve,
        generate_rf_liens_geneve,
    )
    RF_GE_AVAILABLE = True
except ImportError:
    RF_GE_AVAILABLE = False

try:
    from app.scrapers.rf_vaud import (
        RFVaudScraper,
        get_communes_vaud,
        generate_rf_liens_vaud,
    )
    RF_VD_AVAILABLE = True
except ImportError:
    RF_VD_AVAILABLE = False

try:
    from app.scrapers.fosc_shab import (
        FOSCScraper,
        search_fosc_immobilier,
        fosc_to_prospects,
    )
    FOSC_AVAILABLE = True
except ImportError:
    FOSC_AVAILABLE = False

try:
    from app.scrapers.fao_permis import (
        FAOScraper,
        search_fao_permis,
        fao_permis_to_prospects,
    )
    FAO_AVAILABLE = True
except ImportError:
    FAO_AVAILABLE = False

try:
    from app.data.streets_extended import (
        get_streets_extended,
        get_communes_extended,
        get_stats_extended,
        get_streets_for_mass_scraping,
    )
    STREETS_EXTENDED_AVAILABLE = True
except ImportError:
    STREETS_EXTENDED_AVAILABLE = False
    from app.data.streets_ge_vd import get_streets, get_communes, get_stats


router = APIRouter(prefix="/api/prospection", tags=["Prospection"])


# =============================================================================
# MODÈLES
# =============================================================================

class MatchRequest(BaseModel):
    """Requête de matching adresse → propriétaire."""
    adresse: str
    code_postal: str = ""
    ville: str = ""
    canton: str = ""


class BatchMatchRequest(BaseModel):
    """Requête de matching batch."""
    listing_ids: List[str]
    delay_ms: int = Field(default=500, ge=100, le=5000)


class RFScanRequest(BaseModel):
    """Requête de scan RF."""
    canton: str = Field(..., pattern="^(GE|VD)$")
    commune: Optional[str] = None
    start: int = Field(default=1, ge=1)
    end: int = Field(default=100, ge=1, le=10000)


class FOSCSearchRequest(BaseModel):
    """Requête de recherche FOSC."""
    cantons: List[str] = Field(default=["GE", "VD"])
    types: List[str] = Field(default=["faillite", "poursuite", "vente_encheres"])
    days_back: int = Field(default=30, ge=1, le=365)
    limit: int = Field(default=100, ge=1, le=500)


class FAOSearchRequest(BaseModel):
    """Requête de recherche FAO."""
    canton: str = Field(default="GE", pattern="^(GE|VD)$")
    commune: Optional[str] = None
    days_back: int = Field(default=60, ge=1, le=365)
    limit: int = Field(default=100, ge=1, le=500)


class MassScrapingRequest(BaseModel):
    """Requête de scraping massif."""
    canton: str = Field(..., pattern="^(GE|VD)$")
    commune: Optional[str] = None
    source: str = Field(default="searchch")
    delay_seconds: int = Field(default=2, ge=1, le=10)


class ProfileMergeRequest(BaseModel):
    """Requête de fusion/merge d'un profil dans la DB (prospects)."""
    nom: str = ""
    prenom: str = ""
    telephone: str = ""
    telephone_mobile: str = ""
    email: str = ""
    adresse: str = ""
    code_postal: str = ""
    ville: str = ""
    canton: str = ""
    egrid: str = ""
    numero_parcelle: str = ""
    lien_rf: str = ""
    type_bien: str = ""
    surface: float = 0
    source: str = "Manual"


class BatchProfileMergeRequest(BaseModel):
    """Batch merge profils."""
    profiles: List[ProfileMergeRequest]


class MobileLookupRequest(BaseModel):
    """Recherche mobile (sources publiques/annuaires)."""
    name: str
    city: str = ""
    canton: str = ""


class MobileBatchEnrichRequest(BaseModel):
    """Batch enrichissement mobiles pour prospects existants."""
    canton: str = ""
    city: str = ""
    limit: int = Field(default=50, ge=1, le=500)
    auto_update: bool = True


class OpenDataSearchRequest(BaseModel):
    q: str
    rows: int = Field(default=20, ge=1, le=100)
    start: int = Field(default=0, ge=0)


class OpenDataIngestRequest(BaseModel):
    """
    Ingestion contrôlée d'une ressource open data (CSV/JSON) vers `ScrapedListing`
    (portal='opendata'), pour réutiliser le pipeline matching propriétaire.
    """
    resource_url: str
    format: str = Field(default="csv", pattern="^(csv|json)$")
    canton: str = Field(default="GE", pattern="^(GE|VD|BE|NE|FR|VS|ZH|LU|SG|TI|AG|SO|BL|BS|AR|AI|GR|GL|JU|OW|NW|SH|SZ|TG|UR)$")
    address_field: str = "address"
    city_field: str = "city"
    zip_field: str = "zip"
    title_field: Optional[str] = None
    limit: int = Field(default=200, ge=1, le=5000)


# =============================================================================
# ENDPOINTS - MATCHING
# =============================================================================

@router.post("/match")
async def match_address(request: MatchRequest) -> Dict[str, Any]:
    """
    Matche une adresse vers un propriétaire.
    
    Pipeline:
    1. Normalise l'adresse (GeoAdmin)
    2. Recherche parcelle (Cadastre)
    3. Extrait propriétaire (RF)
    4. Enrichit contacts (Annuaires)
    """
    try:
        result = await match_address_to_owner(
            adresse=request.adresse,
            code_postal=request.code_postal,
            ville=request.ville,
            canton=request.canton,
        )
        return result
    except Exception as e:
        logger.error(f"[API] Match error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/match/batch")
async def batch_match(
    request: BatchMatchRequest,
    background_tasks: BackgroundTasks
) -> Dict[str, Any]:
    """
    Lance un matching batch en arrière-plan.
    """
    # Job persistant (suivi/progress)
    async with AsyncSessionLocal() as db:
        job = BackgroundJob(
            job_type="prospection_batch_match",
            status="pending",
            total=len(request.listing_ids),
            processed=0,
            meta={
                "listing_ids": request.listing_ids,
                "delay_ms": request.delay_ms,
            },
        )
        db.add(job)
        await db.commit()
        await db.refresh(job)

    async def run_batch(job_id: int):
        try:
            async with MatchingService() as service:
                await service.batch_match_listings(
                    listing_ids=request.listing_ids,
                    delay_ms=request.delay_ms,
                    job_id=job_id,
                )
        except Exception as e:
            async with AsyncSessionLocal() as db:
                job_db = await db.get(BackgroundJob, job_id)
                if job_db:
                    job_db.status = "error"
                    job_db.error_message = str(e)
                    job_db.completed_at = datetime.utcnow()
                    job_db.updated_at = datetime.utcnow()
                    await db.commit()
            await emit_activity("error", f"Batch match erreur: {e}")
    
    background_tasks.add_task(run_batch, job.id)
    
    return {
        "status": "started",
        "count": len(request.listing_ids),
        "message": f"Matching de {len(request.listing_ids)} annonces lancé en arrière-plan",
        "job_id": job.id,
    }


@router.get("/match/stats")
async def matching_stats() -> Dict[str, Any]:
    """Retourne les statistiques de matching."""
    return await get_matching_stats()


# =============================================================================
# ENDPOINTS - REGISTRE FONCIER
# =============================================================================

@router.get("/rf/communes/{canton}")
async def get_rf_communes(canton: str) -> Dict[str, Any]:
    """Liste les communes disponibles pour un canton."""
    canton = canton.upper()
    
    if canton == "GE" and RF_GE_AVAILABLE:
        communes = get_communes_geneve()
        return {"canton": "GE", "communes": communes}
    
    elif canton == "VD" and RF_VD_AVAILABLE:
        communes = get_communes_vaud()
        return {"canton": "VD", "communes": communes}
    
    raise HTTPException(status_code=404, detail=f"Canton {canton} non supporté")


@router.post("/rf/generate-liens")
async def generate_rf_liens(request: RFScanRequest) -> Dict[str, Any]:
    """
    Génère des liens RF pour traitement batch.
    
    Utile pour créer un fichier de liens à traiter manuellement.
    """
    canton = request.canton.upper()
    
    try:
        if canton == "GE" and RF_GE_AVAILABLE:
            # Trouver le code commune
            communes = get_communes_geneve()
            code_commune = None
            if request.commune:
                for code, nom in communes.items():
                    if nom.lower() == request.commune.lower():
                        code_commune = code
                        break
            
            if not code_commune:
                code_commune = 19  # Genève par défaut
            
            liens = await generate_rf_liens_geneve(
                commune=code_commune,
                start=request.start,
                end=request.end,
            )
            
        elif canton == "VD" and RF_VD_AVAILABLE:
            commune = request.commune or "Lausanne"
            liens = await generate_rf_liens_vaud(
                commune=commune,
                limit=request.end - request.start + 1,
            )
            
        else:
            raise HTTPException(status_code=404, detail=f"Canton {canton} non supporté")
        
        return {
            "canton": canton,
            "commune": request.commune,
            "count": len(liens),
            "liens": liens[:100],  # Limiter la réponse
            "total_available": len(liens),
        }
        
    except Exception as e:
        logger.error(f"[API] RF liens error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# ENDPOINTS - FOSC (FAILLITES/POURSUITES)
# =============================================================================

@router.post("/fosc/search")
async def search_fosc(request: FOSCSearchRequest) -> Dict[str, Any]:
    """
    Recherche les publications FOSC (faillites, poursuites, ventes aux enchères).
    
    Source de prospects haute priorité (vendeurs forcés).
    """
    if not FOSC_AVAILABLE:
        raise HTTPException(status_code=501, detail="Module FOSC non disponible")
    
    try:
        results = await search_fosc_immobilier(
            cantons=request.cantons,
            days_back=request.days_back,
        )
        
        return {
            "count": len(results),
            "publications": results[:request.limit],
            "filters": {
                "cantons": request.cantons,
                "types": request.types,
                "days_back": request.days_back,
            },
        }
        
    except Exception as e:
        logger.error(f"[API] FOSC error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/fosc/to-prospects")
async def fosc_to_prospects_endpoint(request: FOSCSearchRequest) -> Dict[str, Any]:
    """
    Convertit les publications FOSC en prospects importables.
    """
    if not FOSC_AVAILABLE:
        raise HTTPException(status_code=501, detail="Module FOSC non disponible")
    
    try:
        prospects = await fosc_to_prospects(
            cantons=request.cantons,
            days_back=request.days_back,
        )
        
        return {
            "count": len(prospects),
            "prospects": prospects[:request.limit],
        }
        
    except Exception as e:
        logger.error(f"[API] FOSC prospects error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# ENDPOINTS - FAO (PERMIS DE CONSTRUIRE)
# =============================================================================

@router.post("/fao/search")
async def search_fao(request: FAOSearchRequest) -> Dict[str, Any]:
    """
    Recherche les permis de construire.
    
    Cible: Propriétaires qui rénovent = vendeurs potentiels.
    """
    if not FAO_AVAILABLE:
        raise HTTPException(status_code=501, detail="Module FAO non disponible")
    
    try:
        results = await search_fao_permis(
            canton=request.canton,
            commune=request.commune,
            days_back=request.days_back,
        )
        
        return {
            "count": len(results),
            "permis": results[:request.limit],
            "filters": {
                "canton": request.canton,
                "commune": request.commune,
                "days_back": request.days_back,
            },
        }
        
    except Exception as e:
        logger.error(f"[API] FAO error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/fao/to-prospects")
async def fao_to_prospects_endpoint(request: FAOSearchRequest) -> Dict[str, Any]:
    """
    Convertit les permis FAO en prospects importables.
    """
    if not FAO_AVAILABLE:
        raise HTTPException(status_code=501, detail="Module FAO non disponible")
    
    try:
        prospects = await fao_permis_to_prospects(
            days_back=request.days_back,
            min_score=20,
        )
        
        return {
            "count": len(prospects),
            "prospects": prospects[:request.limit],
        }
        
    except Exception as e:
        logger.error(f"[API] FAO prospects error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# ENDPOINTS - STREETS DATA
# =============================================================================

@router.get("/streets/stats")
async def streets_stats() -> Dict[str, Any]:
    """Retourne les statistiques des rues disponibles."""
    if STREETS_EXTENDED_AVAILABLE:
        return get_stats_extended()
    else:
        return get_stats()


@router.get("/streets/{canton}")
async def get_streets_by_canton(
    canton: str,
    commune: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """Liste les rues pour un canton/commune."""
    canton = canton.upper()
    
    if STREETS_EXTENDED_AVAILABLE:
        streets = get_streets_extended(canton, commune)
        communes = get_communes_extended(canton)
    else:
        from app.data.streets_ge_vd import get_streets, get_communes
        streets = get_streets(canton, commune)
        communes = get_communes(canton)
    
    return {
        "canton": canton,
        "commune": commune,
        "count": len(streets),
        "streets": streets,
        "available_communes": communes,
    }


@router.get("/streets/{canton}/for-scraping")
async def get_streets_for_scraping(
    canton: str,
    communes: Optional[str] = Query(None, description="Communes séparées par des virgules"),
) -> Dict[str, Any]:
    """
    Retourne les rues formatées pour le mass scraping.
    
    Format: [(rue, ville), ...]
    """
    canton = canton.upper()
    
    commune_list = None
    if communes:
        commune_list = [c.strip() for c in communes.split(",")]
    
    if STREETS_EXTENDED_AVAILABLE:
        streets = get_streets_for_mass_scraping(canton, commune_list)
    else:
        # Fallback basique
        from app.data.streets_ge_vd import get_streets, get_communes
        base_streets = get_streets(canton)
        default_city = "Genève" if canton == "GE" else "Lausanne"
        streets = [(s, default_city) for s in base_streets]
    
    return {
        "canton": canton,
        "communes": commune_list,
        "count": len(streets),
        "streets": streets[:500],  # Limiter la réponse
        "total_available": len(streets),
    }


# =============================================================================
# ENDPOINTS - SOURCES OVERVIEW
# =============================================================================

@router.get("/sources")
async def get_available_sources() -> Dict[str, Any]:
    """
    Liste toutes les sources de données disponibles et leur statut.
    """
    return {
        "sources": {
            "registre_foncier_ge": {
                "available": RF_GE_AVAILABLE,
                "description": "Registre Foncier Genève - Propriétaires et parcelles",
                "estimated_records": "~100'000 parcelles",
            },
            "registre_foncier_vd": {
                "available": RF_VD_AVAILABLE,
                "description": "Registre Foncier Vaud (InterCapi) - Propriétaires et parcelles",
                "estimated_records": "~150'000 parcelles",
            },
            "fosc_shab": {
                "available": FOSC_AVAILABLE,
                "description": "FOSC/SHAB - Faillites, poursuites, ventes forcées",
                "estimated_records": "~500/an (VD/GE)",
            },
            "fao_permis": {
                "available": FAO_AVAILABLE,
                "description": "FAO - Permis de construire",
                "estimated_records": "~5'000/an (VD/GE)",
            },
            "annuaires": {
                "available": True,
                "description": "Search.ch / Local.ch - Coordonnées de contact",
                "estimated_records": "~80% couverture téléphones fixes",
            },
            "geoadmin": {
                "available": True,
                "description": "GeoAdmin (Swisstopo) - Normalisation adresses",
                "estimated_records": "Toutes adresses suisses",
            },
            "streets_extended": {
                "available": STREETS_EXTENDED_AVAILABLE,
                "description": "Base de rues étendue VD/GE",
                "estimated_records": "~8'000 rues",
            },
        },
        "cantons_supported": ["GE", "VD"],
        "features": {
            "matching": "Annonce → Propriétaire → Contact",
            "mass_scraping": "Scraping par rue/commune",
            "rf_scan": "Scan registre foncier par parcelle",
            "fosc_alerts": "Alertes ventes forcées",
            "fao_monitoring": "Suivi permis construire",
        },
    }


@router.get("/legal-sources")
async def legal_sources() -> Dict[str, Any]:
    """
    Sources légales/officialisées (open data, registres publics, APIs documentées).
    Objectif: construire une DB de qualité sans contournement.
    """
    return {
        "note": "Utiliser uniquement des sources publiques/autorisé(es) et respecter la LPD/FADP, RGPD (si applicable) et les CGU/robots.txt.",
        "sources": [
            {
                "name": "opendata.swiss (catalogue national CKAN)",
                "type": "open_data_catalog",
                "url": "https://opendata.swiss/",
                "api": "https://opendata.swiss/api/3/action/package_search",
                "use_cases": ["datasets officiels (cantons/communes)", "permis, géodata, statistiques"],
            },
            {
                "name": "GeoAdmin / Swisstopo",
                "type": "geocoding_geodata",
                "url": "https://api.geo.admin.ch/",
                "use_cases": ["normalisation adresses", "géocodage", "couches géographiques"],
            },
            {
                "name": "FOSC / SHAB",
                "type": "official_gazette",
                "url": "https://www.shab.ch/",
                "use_cases": ["faillites", "ventes aux enchères", "signaux vendeurs"],
            },
            {
                "name": "ZEFIX (registre du commerce)",
                "type": "official_register",
                "url": "https://www.zefix.ch/",
                "use_cases": ["sociétés", "gérances", "associations", "signataires"],
            },
            {
                "name": "UID Register (registre fédéral des entreprises)",
                "type": "official_register",
                "url": "https://www.uid.admin.ch/",
                "use_cases": ["identifiants entreprise", "réconciliation B2B"],
            },
            {
                "name": "Search.ch Tel API (API officielle)",
                "type": "directory_api",
                "url": "https://search.ch/tel/api/help.en.html",
                "use_cases": ["coordonnées publiques (selon opt-out)", "validation/format numéros"],
            },
            {
                "name": "Open Data Genève",
                "type": "cantonal_open_data",
                "url": "https://www.ge.ch/donnees-ouvertes",
                "use_cases": ["permis de construire", "données communales", "géodata"],
            },
            {
                "name": "Open Data Vaud",
                "type": "cantonal_open_data",
                "url": "https://opendata.vd.ch/",
                "use_cases": ["géodata", "statistiques", "datasets cantonaux"],
            },
        ],
        "best_practice": [
            "Traçabilité: stocker source + timestamp + URL/dataset",
            "Minimisation: collecter uniquement ce qui est nécessaire",
            "Opt-out: do_not_contact + suppression/rectification",
            "Rate limit + cache + respect robots/CGU",
        ],
    }


# =============================================================================
# ENDPOINT - PIPELINE COMPLET
# =============================================================================

@router.post("/pipeline/full")
async def run_full_pipeline(
    background_tasks: BackgroundTasks,
    canton: str = Query("GE", pattern="^(GE|VD)$"),
    commune: Optional[str] = Query(None),
    include_fosc: bool = Query(True),
    include_fao: bool = Query(True),
    days_back: int = Query(30, ge=1, le=180),
) -> Dict[str, Any]:
    """
    Lance le pipeline de prospection complet en arrière-plan.
    
    1. Collecte FOSC (faillites, poursuites)
    2. Collecte FAO (permis construire)
    3. Import automatique en prospects
    4. Enrichissement contacts
    """
    async def run_pipeline():
        try:
            await emit_activity("pipeline", f"🚀 Pipeline démarré: {canton}/{commune or 'tous'}")
            
            total_prospects = 0
            
            # FOSC
            if include_fosc and FOSC_AVAILABLE:
                await emit_activity("pipeline", "📋 Collecte FOSC...")
                prospects = await fosc_to_prospects(
                    cantons=[canton],
                    days_back=days_back,
                )
                total_prospects += len(prospects)
                await emit_activity("success", f"FOSC: {len(prospects)} prospects")
            
            # FAO
            if include_fao and FAO_AVAILABLE:
                await emit_activity("pipeline", "🔧 Collecte FAO (permis)...")
                prospects = await fao_permis_to_prospects(
                    days_back=days_back,
                )
                # Filtrer par canton
                prospects = [p for p in prospects if p.get("canton", "").upper() == canton]
                total_prospects += len(prospects)
                await emit_activity("success", f"FAO: {len(prospects)} prospects")
            
            await emit_activity("success", f"✅ Pipeline terminé: {total_prospects} prospects collectés")
            
        except Exception as e:
            await emit_activity("error", f"❌ Pipeline erreur: {e}")
            logger.error(f"[Pipeline] Error: {e}")
    
    background_tasks.add_task(run_pipeline)
    
    return {
        "status": "started",
        "canton": canton,
        "commune": commune,
        "config": {
            "include_fosc": include_fosc,
            "include_fao": include_fao,
            "days_back": days_back,
        },
        "message": "Pipeline de prospection lancé en arrière-plan",
    }


# =============================================================================
# ENDPOINTS - PROFILE MERGE / MOBILE (Conformité: sources publiques)
# =============================================================================

@router.post("/profile/merge")
async def profile_merge(request: ProfileMergeRequest) -> Dict[str, Any]:
    """Fusionne un profil (multi-sources) dans la DB prospects."""
    if not PROFILE_MERGE_AVAILABLE:
        raise HTTPException(status_code=501, detail="Module ProfileMerge non disponible")
    try:
        return await merge_profile_into_db(request.model_dump())
    except Exception as e:
        logger.error(f"[API] Profile merge error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/profile/merge/batch")
async def profile_merge_batch(request: BatchProfileMergeRequest) -> Dict[str, Any]:
    """Batch merge de profils."""
    if not PROFILE_MERGE_AVAILABLE:
        raise HTTPException(status_code=501, detail="Module ProfileMerge non disponible")
    try:
        payload = [p.model_dump() for p in request.profiles]
        return await batch_merge_profiles(payload)
    except Exception as e:
        logger.error(f"[API] Profile merge batch error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/mobile/lookup")
async def mobile_lookup(request: MobileLookupRequest) -> Dict[str, Any]:
    """
    Recherche un numéro mobile via sources publiques (annuaires).
    """
    if not MOBILE_ENRICH_AVAILABLE:
        raise HTTPException(status_code=501, detail="Module MobileEnrich non disponible")
    service = MobileEnrichService(
        use_directories=True,
        use_truecaller=False,
        use_linkedin=False,
        use_social=False,
    )
    try:
        res = await service.search_mobile(
            name=request.name,
            city=request.city,
            canton=request.canton,
        )
        return res.to_dict()
    except Exception as e:
        logger.error(f"[API] Mobile lookup error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await service.close()


@router.get("/profile/duplicates/{prospect_id}")
async def profile_duplicates(prospect_id: str) -> Dict[str, Any]:
    """Retourne des doublons potentiels pour un prospect (scoring flou)."""
    if not PROFILE_MERGE_AVAILABLE:
        raise HTTPException(status_code=501, detail="Module ProfileMerge non disponible")
    try:
        from app.services.profile_merge_service import ProfileMergeService, ProfileData, MATCH_THRESHOLD_LOW
        from app.core.database import AsyncSessionLocal, Prospect
        from sqlalchemy import select

        async with AsyncSessionLocal() as db:
            res = await db.execute(select(Prospect).where(Prospect.id == prospect_id))
            prospect = res.scalar_one_or_none()
            if not prospect:
                raise HTTPException(status_code=404, detail="Prospect introuvable")

        service = ProfileMergeService()
        profile = ProfileData(
            nom=prospect.nom or "",
            prenom=prospect.prenom or "",
            telephone=prospect.telephone or "",
            email=prospect.email or "",
            adresse=prospect.adresse or "",
            code_postal=prospect.code_postal or "",
            ville=prospect.ville or "",
            canton=prospect.canton or "",
            lien_rf=prospect.lien_rf or "",
            source="Database",
        )

        matches = await service.find_matching_prospects(profile, threshold=MATCH_THRESHOLD_LOW, limit=10)
        # Exclure soi-même
        matches = [(p, s) for (p, s) in matches if p.id != prospect_id]

        return {
            "prospect_id": prospect_id,
            "count": len(matches),
            "duplicates": [
                {
                    "id": p.id,
                    "nom": p.nom,
                    "prenom": p.prenom,
                    "ville": p.ville,
                    "telephone": p.telephone,
                    "email": p.email,
                    "score": float(score),
                }
                for (p, score) in matches
            ],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[API] Profile duplicates error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/mobile/batch-enrich")
async def mobile_batch_enrich(request: MobileBatchEnrichRequest) -> Dict[str, Any]:
    """
    Batch enrich mobiles (prospects) via sources publiques (annuaires).
    """
    if not MOBILE_ENRICH_AVAILABLE:
        raise HTTPException(status_code=501, detail="Module MobileEnrich non disponible")
    service = MobileEnrichService(
        use_directories=True,
        use_truecaller=False,
        use_linkedin=False,
        use_social=False,
    )
    try:
        res = await service.batch_enrich(
            canton=request.canton,
            city=request.city,
            limit=request.limit,
            auto_update=request.auto_update,
        )
        return res.to_dict()
    except Exception as e:
        logger.error(f"[API] Mobile batch enrich error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await service.close()


# =============================================================================
# ENDPOINTS - OPENDATA.SWISS (catalogue + ingestion contrôlée)
# =============================================================================

@router.get("/opendata/search")
async def opendata_search(
    q: str = Query(..., min_length=2),
    rows: int = Query(20, ge=1, le=100),
    start: int = Query(0, ge=0),
) -> Dict[str, Any]:
    """
    Recherche de datasets sur opendata.swiss (CKAN).
    """
    from app.scrapers.opendata_swiss import OpenDataSwissClient

    def _pick_lang(value: Any) -> str:
        if isinstance(value, dict):
            return (
                value.get("fr")
                or value.get("de")
                or value.get("en")
                or value.get("it")
                or next(iter(value.values()), "")
                or ""
            )
        return str(value) if value is not None else ""

    async with OpenDataSwissClient() as client:
        data = await client.search_datasets(q=q, rows=rows, start=start)
        result = data.get("result", {}) or {}
        # Réduire la taille (retourner le strict utile)
        out = []
        for d in result.get("results", []) or []:
            out.append(
                {
                    "id": d.get("id"),
                    "name": d.get("name"),
                    "title": _pick_lang(d.get("title")),
                    "organization": (d.get("organization") or {}).get("title"),
                    "notes": _pick_lang(d.get("notes"))[:300],
                    "tags": [t.get("name") for t in (d.get("tags") or [])][:10],
                    "num_resources": len(d.get("resources") or []),
                    "url": d.get("url") or d.get("ckan_url") or "",
                }
            )
        return {
            "count": len(out),
            "total": result.get("count", 0),
            "datasets": out,
        }


@router.get("/opendata/dataset/{dataset_id}")
async def opendata_dataset(dataset_id: str) -> Dict[str, Any]:
    """
    Détail d'un dataset opendata.swiss (resources inclues).
    """
    from app.scrapers.opendata_swiss import OpenDataSwissClient

    def _pick_lang(value: Any) -> str:
        if isinstance(value, dict):
            return (
                value.get("fr")
                or value.get("de")
                or value.get("en")
                or value.get("it")
                or next(iter(value.values()), "")
                or ""
            )
        return str(value) if value is not None else ""

    async with OpenDataSwissClient() as client:
        data = await client.get_dataset(dataset_id)
        d = data.get("result", {}) or {}
        resources = []
        for r in (d.get("resources") or []):
            resources.append(
                {
                    "id": r.get("id"),
                    "name": r.get("name") or r.get("description"),
                    "format": (r.get("format") or "").lower(),
                    "url": r.get("url"),
                    "mimetype": r.get("mimetype"),
                    "created": r.get("created"),
                    "last_modified": r.get("last_modified"),
                }
            )
        return {
            "id": d.get("id"),
            "name": d.get("name"),
            "title": _pick_lang(d.get("title")),
            "notes": _pick_lang(d.get("notes")),
            "organization": (d.get("organization") or {}).get("title"),
            "tags": [t.get("name") for t in (d.get("tags") or [])],
            "resources": resources,
        }


@router.post("/opendata/ingest")
async def opendata_ingest(request: OpenDataIngestRequest) -> Dict[str, Any]:
    """
    Ingestion (CSV/JSON) -> `scraped_listings` (portal='opendata').

    Le but n'est PAS de collecter des données privées, mais de créer des \"leads adresses\"
    à matcher ensuite via le pipeline propriétaire (GeoAdmin + RF + annuaires autorisés).
    """
    import csv
    import hashlib
    import io
    import json

    import httpx
    from sqlalchemy import select

    # Télécharger la ressource (simple, limité par `limit`)
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(request.resource_url)
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Fetch resource HTTP {r.status_code}")
        content = r.text

    # Extraire des enregistrements (dict)
    records = []
    if request.format == "csv":
        reader = csv.DictReader(io.StringIO(content))
        for row in reader:
            records.append(row)
            if len(records) >= request.limit:
                break
    else:
        data = json.loads(content)
        if isinstance(data, list):
            records = data[: request.limit]
        elif isinstance(data, dict) and isinstance(data.get("features"), list):
            # GeoJSON-like
            for feat in data.get("features", [])[: request.limit]:
                props = feat.get("properties") or {}
                records.append(props)
        else:
            raise HTTPException(status_code=400, detail="JSON non supporté (attendu: list ou {'features':[...]})")

    if not records:
        return {"status": "ok", "added": 0, "duplicates": 0, "errors": 0}

    resource_hash = hashlib.sha1(request.resource_url.encode("utf-8")).hexdigest()[:12]

    added = 0
    duplicates = 0
    errors = 0

    async with AsyncSessionLocal() as db:
        for idx, row in enumerate(records):
            try:
                addr = (row.get(request.address_field) or "").strip()
                city = (row.get(request.city_field) or "").strip()
                npa = (row.get(request.zip_field) or "").strip()
                title = (row.get(request.title_field) or "").strip() if request.title_field else ""

                if not addr and not city:
                    errors += 1
                    continue

                pseudo_url = f"opendata://{resource_hash}/{idx}"

                exists = await db.execute(select(ScrapedListing.id).where(ScrapedListing.url == pseudo_url))
                if exists.scalar_one_or_none():
                    duplicates += 1
                    continue

                listing = ScrapedListing(
                    portal="opendata",
                    listing_id=f"od-{resource_hash}-{idx}",
                    url=pseudo_url,
                    title=title or f"OpenData lead #{idx}",
                    address=addr or None,
                    city=city or None,
                    npa=npa or None,
                    canton=request.canton,
                    transaction_type="signal",
                    property_type="opendata",
                    details={
                        "source": "opendata",
                        "resource_url": request.resource_url,
                        "row_index": idx,
                        "raw": row,
                    },
                    match_status="pending",
                )
                db.add(listing)
                added += 1

            except Exception:
                errors += 1

        await db.commit()

    return {
        "status": "ok",
        "added": added,
        "duplicates": duplicates,
        "errors": errors,
        "resource_hash": resource_hash,
    }


# =============================================================================
# ENDPOINTS - JOBS (suivi des tâches de fond)
# =============================================================================

@router.get("/jobs")
async def list_jobs(
    job_type: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
) -> Dict[str, Any]:
    """Liste les jobs récents (batch/pipelines)."""
    from sqlalchemy import select, desc

    async with AsyncSessionLocal() as db:
        q = select(BackgroundJob).order_by(desc(BackgroundJob.created_at)).limit(limit)
        if job_type:
            q = q.where(BackgroundJob.job_type == job_type)
        if status:
            q = q.where(BackgroundJob.status == status)
        res = await db.execute(q)
        jobs = res.scalars().all()

        return {
            "count": len(jobs),
            "jobs": [
                {
                    "id": j.id,
                    "job_type": j.job_type,
                    "status": j.status,
                    "total": j.total,
                    "processed": j.processed,
                    "meta": j.meta,
                    "error_message": j.error_message,
                    "created_at": j.created_at.isoformat() if j.created_at else None,
                    "started_at": j.started_at.isoformat() if j.started_at else None,
                    "completed_at": j.completed_at.isoformat() if j.completed_at else None,
                }
                for j in jobs
            ],
        }


@router.get("/jobs/{job_id}")
async def get_job(job_id: int) -> Dict[str, Any]:
    """Détail d'un job (inclut result si terminé)."""
    async with AsyncSessionLocal() as db:
        job = await db.get(BackgroundJob, job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job introuvable")
        return {
            "id": job.id,
            "job_type": job.job_type,
            "status": job.status,
            "total": job.total,
            "processed": job.processed,
            "meta": job.meta,
            "result": job.result,
            "error_message": job.error_message,
            "created_at": job.created_at.isoformat() if job.created_at else None,
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        }
