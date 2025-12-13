# =============================================================================
# API BIENS EN VENTE - Gestion des annonces et matching propriétaires
# =============================================================================
# Endpoints pour:
#   - Liste des biens en vente avec informations propriétaires
#   - Matching biens <-> propriétaires
#   - Pipeline de "doublage" (contacter les propriétaires directement)
# =============================================================================

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel
from sqlalchemy import select, func, and_, or_, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal, ScrapedListing, Prospect, BrochureRequest, BackgroundJob
from app.core.logger import logger
from app.core.websocket import emit_activity


# =============================================================================
# ROUTER
# =============================================================================

router = APIRouter(prefix="/api/biens", tags=["Biens en Vente"])


# =============================================================================
# SCHEMAS
# =============================================================================

class BienSummary(BaseModel):
    """Résumé d'un bien pour la liste."""
    id: int
    portal: str
    listing_id: Optional[str]
    url: str
    title: Optional[str]
    address: Optional[str]
    city: Optional[str]
    npa: Optional[str]
    canton: Optional[str]
    price: Optional[float]
    rooms: Optional[float]
    surface: Optional[float]
    property_type: Optional[str]
    transaction_type: Optional[str]
    agency_name: Optional[str]
    
    # Matching
    match_status: str
    match_score: Optional[float]
    matched_prospect_id: Optional[str]
    
    # Propriétaire
    owner_name: Optional[str]
    owner_phone: Optional[str]
    owner_mobile: Optional[str]
    
    # Pipeline brochure
    brochure_requested: bool
    extracted_address: Optional[str]
    doubling_status: Optional[str]
    
    scraped_at: Optional[datetime]

    class Config:
        from_attributes = True


class BienDetail(BienSummary):
    """Détails complets d'un bien."""
    details: Optional[Dict[str, Any]]
    match_meta: Optional[Dict[str, Any]]
    brochure_request_id: Optional[int]
    response_email_id: Optional[int]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    
    # Prospect associé si matché
    prospect: Optional[Dict[str, Any]] = None


class MatchRequest(BaseModel):
    """Requête pour matcher un bien manuellement."""
    prospect_id: str


class BatchMatchRequest(BaseModel):
    """Requête pour matcher plusieurs biens."""
    listing_ids: List[int]
    auto_match: bool = True  # True = matching automatique


class DoublingRequest(BaseModel):
    """Requête pour changer le statut de doublage."""
    status: str  # pending, contacted, success, failed
    notes: Optional[str] = None


class BiensStatsResponse(BaseModel):
    """Statistiques des biens."""
    total: int
    by_match_status: Dict[str, int]
    by_portal: Dict[str, int]
    by_canton: Dict[str, int]
    by_doubling_status: Dict[str, int]
    with_owner_info: int
    with_mobile: int
    brochures_sent: int
    brochures_responded: int


# =============================================================================
# ENDPOINTS - LISTE ET DÉTAILS
# =============================================================================

@router.get("/", response_model=Dict[str, Any])
async def list_biens(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    portal: Optional[str] = None,
    canton: Optional[str] = None,
    city: Optional[str] = None,
    match_status: Optional[str] = None,
    doubling_status: Optional[str] = None,
    has_owner: Optional[bool] = None,
    has_mobile: Optional[bool] = None,
    transaction_type: Optional[str] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    search: Optional[str] = None,
    sort_by: str = Query("scraped_at", regex="^(scraped_at|price|match_score|created_at)$"),
    sort_order: str = Query("desc", regex="^(asc|desc)$"),
):
    """
    Liste les biens en vente avec filtres et pagination.
    """
    async with AsyncSessionLocal() as db:
        # Construire la requête
        query = select(ScrapedListing)
        count_query = select(func.count(ScrapedListing.id))
        
        # Filtres
        filters = []
        
        if portal:
            filters.append(ScrapedListing.portal == portal)
        if canton:
            filters.append(ScrapedListing.canton == canton)
        if city:
            filters.append(ScrapedListing.city.ilike(f"%{city}%"))
        if match_status:
            filters.append(ScrapedListing.match_status == match_status)
        if doubling_status:
            filters.append(ScrapedListing.doubling_status == doubling_status)
        if has_owner is True:
            filters.append(ScrapedListing.owner_name.isnot(None))
        if has_owner is False:
            filters.append(ScrapedListing.owner_name.is_(None))
        if has_mobile is True:
            filters.append(ScrapedListing.owner_mobile.isnot(None))
        if has_mobile is False:
            filters.append(ScrapedListing.owner_mobile.is_(None))
        if transaction_type:
            filters.append(ScrapedListing.transaction_type == transaction_type)
        if min_price:
            filters.append(ScrapedListing.price >= min_price)
        if max_price:
            filters.append(ScrapedListing.price <= max_price)
        if search:
            search_filter = or_(
                ScrapedListing.title.ilike(f"%{search}%"),
                ScrapedListing.address.ilike(f"%{search}%"),
                ScrapedListing.owner_name.ilike(f"%{search}%"),
            )
            filters.append(search_filter)
        
        if filters:
            query = query.where(and_(*filters))
            count_query = count_query.where(and_(*filters))
        
        # Tri
        sort_column = getattr(ScrapedListing, sort_by)
        if sort_order == "desc":
            query = query.order_by(desc(sort_column))
        else:
            query = query.order_by(sort_column)
        
        # Count total
        total_result = await db.execute(count_query)
        total = total_result.scalar() or 0
        
        # Pagination
        offset = (page - 1) * per_page
        query = query.offset(offset).limit(per_page)
        
        result = await db.execute(query)
        listings = result.scalars().all()
        
        # Convertir en dict
        items = []
        for listing in listings:
            items.append({
                "id": listing.id,
                "portal": listing.portal,
                "listing_id": listing.listing_id,
                "url": listing.url,
                "title": listing.title,
                "address": listing.address,
                "city": listing.city,
                "npa": listing.npa,
                "canton": listing.canton,
                "price": listing.price,
                "rooms": listing.rooms,
                "surface": listing.surface,
                "property_type": listing.property_type,
                "transaction_type": listing.transaction_type,
                "agency_name": listing.agency_name,
                "match_status": listing.match_status or "pending",
                "match_score": listing.match_score,
                "matched_prospect_id": listing.matched_prospect_id,
                "owner_name": listing.owner_name,
                "owner_phone": listing.owner_phone,
                "owner_mobile": listing.owner_mobile,
                "brochure_requested": listing.brochure_requested,
                "extracted_address": listing.extracted_address,
                "doubling_status": listing.doubling_status,
                "scraped_at": listing.scraped_at.isoformat() if listing.scraped_at else None,
            })
        
        return {
            "items": items,
            "total": total,
            "page": page,
            "per_page": per_page,
            "pages": (total + per_page - 1) // per_page,
        }


@router.get("/stats", response_model=BiensStatsResponse)
async def get_biens_stats():
    """
    Retourne les statistiques des biens.
    """
    async with AsyncSessionLocal() as db:
        # Total
        total_result = await db.execute(select(func.count(ScrapedListing.id)))
        total = total_result.scalar() or 0
        
        # Par statut de matching
        match_status_result = await db.execute(
            select(
                ScrapedListing.match_status,
                func.count(ScrapedListing.id)
            ).group_by(ScrapedListing.match_status)
        )
        by_match_status = {row[0] or "pending": row[1] for row in match_status_result.fetchall()}
        
        # Par portail
        portal_result = await db.execute(
            select(
                ScrapedListing.portal,
                func.count(ScrapedListing.id)
            ).group_by(ScrapedListing.portal)
        )
        by_portal = {row[0]: row[1] for row in portal_result.fetchall()}
        
        # Par canton
        canton_result = await db.execute(
            select(
                ScrapedListing.canton,
                func.count(ScrapedListing.id)
            ).group_by(ScrapedListing.canton)
        )
        by_canton = {row[0] or "N/A": row[1] for row in canton_result.fetchall()}
        
        # Par statut doublage
        doubling_result = await db.execute(
            select(
                ScrapedListing.doubling_status,
                func.count(ScrapedListing.id)
            ).where(ScrapedListing.doubling_status.isnot(None))
            .group_by(ScrapedListing.doubling_status)
        )
        by_doubling_status = {row[0]: row[1] for row in doubling_result.fetchall()}
        
        # Avec infos propriétaire
        owner_result = await db.execute(
            select(func.count(ScrapedListing.id))
            .where(ScrapedListing.owner_name.isnot(None))
        )
        with_owner_info = owner_result.scalar() or 0
        
        # Avec mobile
        mobile_result = await db.execute(
            select(func.count(ScrapedListing.id))
            .where(ScrapedListing.owner_mobile.isnot(None))
        )
        with_mobile = mobile_result.scalar() or 0
        
        # Brochures envoyées
        brochures_sent_result = await db.execute(
            select(func.count(ScrapedListing.id))
            .where(ScrapedListing.brochure_requested == True)
        )
        brochures_sent = brochures_sent_result.scalar() or 0
        
        # Brochures avec réponse
        brochures_responded_result = await db.execute(
            select(func.count(ScrapedListing.id))
            .where(ScrapedListing.extracted_address.isnot(None))
        )
        brochures_responded = brochures_responded_result.scalar() or 0
        
        return BiensStatsResponse(
            total=total,
            by_match_status=by_match_status,
            by_portal=by_portal,
            by_canton=by_canton,
            by_doubling_status=by_doubling_status,
            with_owner_info=with_owner_info,
            with_mobile=with_mobile,
            brochures_sent=brochures_sent,
            brochures_responded=brochures_responded,
        )


@router.get("/{bien_id}", response_model=BienDetail)
async def get_bien_detail(bien_id: int):
    """
    Récupère les détails d'un bien avec le prospect associé.
    """
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(ScrapedListing).where(ScrapedListing.id == bien_id)
        )
        listing = result.scalar_one_or_none()
        
        if not listing:
            raise HTTPException(status_code=404, detail="Bien non trouvé")
        
        # Récupérer le prospect associé si matché
        prospect_data = None
        if listing.matched_prospect_id:
            prospect_result = await db.execute(
                select(Prospect).where(Prospect.id == listing.matched_prospect_id)
            )
            prospect = prospect_result.scalar_one_or_none()
            if prospect:
                prospect_data = {
                    "id": prospect.id,
                    "nom": prospect.nom,
                    "prenom": prospect.prenom,
                    "telephone": prospect.telephone,
                    "email": prospect.email,
                    "adresse": prospect.adresse,
                    "ville": prospect.ville,
                    "canton": prospect.canton,
                    "lien_rf": prospect.lien_rf,
                    "notes": prospect.notes,
                }
        
        return BienDetail(
            id=listing.id,
            portal=listing.portal,
            listing_id=listing.listing_id,
            url=listing.url,
            title=listing.title,
            address=listing.address,
            city=listing.city,
            npa=listing.npa,
            canton=listing.canton,
            price=listing.price,
            rooms=listing.rooms,
            surface=listing.surface,
            property_type=listing.property_type,
            transaction_type=listing.transaction_type,
            agency_name=listing.agency_name,
            details=listing.details,
            match_status=listing.match_status or "pending",
            match_score=listing.match_score,
            matched_prospect_id=listing.matched_prospect_id,
            match_meta=listing.match_meta,
            owner_name=listing.owner_name,
            owner_phone=listing.owner_phone,
            owner_mobile=listing.owner_mobile,
            brochure_requested=listing.brochure_requested,
            brochure_request_id=listing.brochure_request_id,
            response_email_id=listing.response_email_id,
            extracted_address=listing.extracted_address,
            doubling_status=listing.doubling_status,
            scraped_at=listing.scraped_at,
            created_at=listing.created_at,
            updated_at=listing.updated_at,
            prospect=prospect_data,
        )


# =============================================================================
# ENDPOINTS - MATCHING
# =============================================================================

@router.post("/{bien_id}/match")
async def match_bien_to_prospect(
    bien_id: int,
    request: MatchRequest,
):
    """
    Matche manuellement un bien avec un prospect.
    """
    async with AsyncSessionLocal() as db:
        # Vérifier le bien
        listing_result = await db.execute(
            select(ScrapedListing).where(ScrapedListing.id == bien_id)
        )
        listing = listing_result.scalar_one_or_none()
        
        if not listing:
            raise HTTPException(status_code=404, detail="Bien non trouvé")
        
        # Vérifier le prospect
        prospect_result = await db.execute(
            select(Prospect).where(Prospect.id == request.prospect_id)
        )
        prospect = prospect_result.scalar_one_or_none()
        
        if not prospect:
            raise HTTPException(status_code=404, detail="Prospect non trouvé")
        
        # Mettre à jour le matching
        listing.match_status = "manual"
        listing.match_score = 1.0
        listing.matched_prospect_id = prospect.id
        listing.matched_at = datetime.utcnow()
        listing.match_meta = {"type": "manual", "by": "api"}
        
        # Copier les infos propriétaire
        listing.owner_name = f"{prospect.prenom or ''} {prospect.nom}".strip()
        listing.owner_phone = prospect.telephone
        
        # Chercher le mobile dans les notes
        if prospect.notes and "Mobile:" in prospect.notes:
            import re
            mobile_match = re.search(r'Mobile:\s*(\+?\d+)', prospect.notes)
            if mobile_match:
                listing.owner_mobile = mobile_match.group(1)
        
        listing.updated_at = datetime.utcnow()
        
        await db.commit()
        
        await emit_activity(
            "match",
            f"Bien {listing.title or listing.id} matché avec {listing.owner_name}"
        )
        
        return {"status": "ok", "matched_prospect_id": prospect.id}


@router.post("/batch-match")
async def batch_match_biens(
    request: BatchMatchRequest,
    background_tasks: BackgroundTasks,
):
    """
    Lance le matching automatique pour plusieurs biens.
    """
    from app.services.matching_service import MatchingService

    # Créer un job persistant (progress)
    async with AsyncSessionLocal() as db:
        job = BackgroundJob(
            job_type="biens_batch_match",
            status="pending",
            total=len(request.listing_ids),
            processed=0,
            meta={
                "listing_ids": request.listing_ids,
                "auto_match": request.auto_match,
            },
        )
        db.add(job)
        await db.commit()
        await db.refresh(job)
    
    async def run_batch_match(job_id: int):
        results = {"matched": 0, "no_match": 0, "errors": 0}
        started_at = datetime.utcnow()

        try:
            async with MatchingService() as service:
                async with AsyncSessionLocal() as db:
                    job_db = await db.get(BackgroundJob, job_id)
                    if job_db:
                        job_db.status = "running"
                        job_db.started_at = started_at
                        job_db.processed = 0
                        await db.commit()

                    for idx, listing_id in enumerate(request.listing_ids, start=1):
                        try:
                            result = await db.execute(
                                select(ScrapedListing).where(ScrapedListing.id == listing_id)
                            )
                            listing = result.scalar_one_or_none()
                            
                            if not listing:
                                results["errors"] += 1
                                continue
                            
                            address = listing.extracted_address or listing.address
                            if not address:
                                results["errors"] += 1
                                continue
                            
                            # Matching complet (adresse + contexte)
                            match_result = await service.match_from_address(
                                adresse=address,
                                code_postal=listing.npa or "",
                                ville=listing.city or "",
                                canton=listing.canton or "",
                            )
                            
                            if match_result and match_result.confidence >= 0.5 and match_result.status in ("matched", "partial"):
                                listing.match_status = match_result.status
                                listing.match_score = match_result.confidence
                                listing.owner_name = f"{match_result.prenom or ''} {match_result.nom or ''}".strip() or None
                                listing.owner_phone = match_result.telephone or None
                                listing.match_meta = match_result.to_dict()
                                listing.matched_at = datetime.utcnow()
                                listing.doubling_status = listing.doubling_status or "pending"
                                results["matched"] += 1
                            else:
                                listing.match_status = "no_match"
                                results["no_match"] += 1
                            
                            listing.updated_at = datetime.utcnow()

                        except Exception as e:
                            logger.error(f"[BatchMatch] Erreur listing {listing_id}: {e}")
                            results["errors"] += 1
                        finally:
                            # Progress job
                            if job_db:
                                job_db.processed = idx
                                job_db.updated_at = datetime.utcnow()
                            # Commit périodique (évite tout perdre + donne du progress)
                            if idx % 10 == 0:
                                await db.commit()

                    # Commit final (listings + job)
                    if job_db:
                        job_db.status = "completed"
                        job_db.completed_at = datetime.utcnow()
                        job_db.result = results
                        job_db.updated_at = datetime.utcnow()
                    await db.commit()

            await emit_activity(
                "batch_match",
                f"Batch terminé: {results['matched']} matchés, {results['no_match']} sans match"
            )
        except Exception as e:
            logger.error(f"[BatchMatch] Job error: {e}")
            async with AsyncSessionLocal() as db:
                job_db = await db.get(BackgroundJob, job_id)
                if job_db:
                    job_db.status = "error"
                    job_db.error_message = str(e)
                    job_db.completed_at = datetime.utcnow()
                    job_db.updated_at = datetime.utcnow()
                    await db.commit()
            await emit_activity("error", f"Batch match erreur: {e}")
    
    background_tasks.add_task(run_batch_match, job.id)
    
    return {
        "status": "started",
        "listings_count": len(request.listing_ids),
        "job_id": job.id,
    }


# =============================================================================
# ENDPOINTS - DOUBLAGE
# =============================================================================

@router.post("/{bien_id}/doubling")
async def update_doubling_status(
    bien_id: int,
    request: DoublingRequest,
):
    """
    Met à jour le statut de doublage d'un bien.
    """
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(ScrapedListing).where(ScrapedListing.id == bien_id)
        )
        listing = result.scalar_one_or_none()
        
        if not listing:
            raise HTTPException(status_code=404, detail="Bien non trouvé")
        
        listing.doubling_status = request.status
        listing.updated_at = datetime.utcnow()
        
        # Si marqué comme contacté avec succès et qu'on a un prospect matché
        if request.status == "contacted" and listing.matched_prospect_id:
            # Ajouter une note sur le prospect
            from app.core.database import InteractionLog
            
            interaction = InteractionLog(
                prospect_id=listing.matched_prospect_id,
                type="appel",
                notes=f"Contact suite au doublage - Bien: {listing.title or listing.url}",
            )
            db.add(interaction)
        
        await db.commit()
        
        return {"status": "ok", "doubling_status": request.status}


@router.get("/{bien_id}/owner")
async def get_bien_owner(bien_id: int):
    """
    Récupère les informations du propriétaire d'un bien.
    """
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(ScrapedListing).where(ScrapedListing.id == bien_id)
        )
        listing = result.scalar_one_or_none()
        
        if not listing:
            raise HTTPException(status_code=404, detail="Bien non trouvé")
        
        owner_info = {
            "bien_id": bien_id,
            "owner_name": listing.owner_name,
            "owner_phone": listing.owner_phone,
            "owner_mobile": listing.owner_mobile,
            "match_status": listing.match_status,
            "match_score": listing.match_score,
            "matched_prospect_id": listing.matched_prospect_id,
        }
        
        # Si on a un prospect matché, enrichir
        if listing.matched_prospect_id:
            prospect_result = await db.execute(
                select(Prospect).where(Prospect.id == listing.matched_prospect_id)
            )
            prospect = prospect_result.scalar_one_or_none()
            
            if prospect:
                owner_info["prospect"] = {
                    "id": prospect.id,
                    "nom": prospect.nom,
                    "prenom": prospect.prenom,
                    "telephone": prospect.telephone,
                    "email": prospect.email,
                    "adresse": prospect.adresse,
                    "ville": prospect.ville,
                    "lien_rf": prospect.lien_rf,
                }
        
        return owner_info


# =============================================================================
# ENDPOINTS - EXPORT
# =============================================================================

@router.get("/export/csv")
async def export_biens_csv(
    canton: Optional[str] = None,
    match_status: Optional[str] = None,
    has_owner: Optional[bool] = None,
    limit: int = Query(1000, le=5000),
):
    """
    Exporte les biens en CSV avec les informations propriétaires.
    """
    from fastapi.responses import StreamingResponse
    import csv
    import io
    
    async with AsyncSessionLocal() as db:
        query = select(ScrapedListing)
        
        filters = []
        if canton:
            filters.append(ScrapedListing.canton == canton)
        if match_status:
            filters.append(ScrapedListing.match_status == match_status)
        if has_owner is True:
            filters.append(ScrapedListing.owner_name.isnot(None))
        
        if filters:
            query = query.where(and_(*filters))
        
        query = query.limit(limit)
        
        result = await db.execute(query)
        listings = result.scalars().all()
    
    # Créer le CSV
    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")
    
    # En-têtes
    writer.writerow([
        "ID", "Portail", "URL", "Titre", "Adresse", "NPA", "Ville", "Canton",
        "Prix", "Pièces", "Surface", "Type", "Transaction",
        "Agence", "Match Status", "Score",
        "Propriétaire", "Téléphone", "Mobile",
        "Doublage", "Date Scraping"
    ])
    
    # Données
    for listing in listings:
        writer.writerow([
            listing.id,
            listing.portal,
            listing.url,
            listing.title,
            listing.address,
            listing.npa,
            listing.city,
            listing.canton,
            listing.price,
            listing.rooms,
            listing.surface,
            listing.property_type,
            listing.transaction_type,
            listing.agency_name,
            listing.match_status,
            listing.match_score,
            listing.owner_name,
            listing.owner_phone,
            listing.owner_mobile,
            listing.doubling_status,
            listing.scraped_at.isoformat() if listing.scraped_at else "",
        ])
    
    output.seek(0)
    
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=biens_export_{datetime.utcnow().strftime('%Y%m%d')}.csv"
        }
    )
