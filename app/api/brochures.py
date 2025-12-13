# =============================================================================
# API BROCHURES - Gestion des demandes de brochure automatiques
# =============================================================================

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, update, delete
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime

from app.core.database import (
    get_db,
    AsyncSessionLocal,
    BrochureRequest,
    BrochureSchedule,
    ScrapedListing,
    EmailAccount,
    BackgroundJob,
)
from app.core.websocket import emit_activity
from app.services.brochure_service import (
    BrochureService,
    get_queue_stats,
    get_brochure_history,
    process_brochure_responses,
    run_full_brochure_pipeline,
    BrochureServiceError,
    EmailRotationError,
)
from app.services.email_parser_service import (
    parse_emails_for_addresses,
    get_parsed_emails_stats,
)

router = APIRouter()


# =============================================================================
# SCHEMAS
# =============================================================================

class BrochureRequestCreate(BaseModel):
    listing_url: str
    portal: str  # comparis, immoscout24, homegate
    prospect_id: Optional[str] = None
    custom_message: Optional[str] = None
    requester_name: Optional[str] = None
    requester_phone: Optional[str] = None


class BrochureBatchRequest(BaseModel):
    listings: List[dict]  # [{"url": "...", "portal": "...", "prospect_id": "..."}]
    custom_message: Optional[str] = None


class BrochureScheduleCreate(BaseModel):
    name: str
    cron_expression: str = "0 9 * * *"  # Par défaut 9h tous les jours
    portal_filter: List[str] = ["comparis", "immoscout24", "homegate"]
    canton_filter: List[str] = ["GE", "VD"]
    max_requests_per_run: int = 10
    delay_between_requests: int = 30
    is_active: bool = True


class BrochureScheduleUpdate(BaseModel):
    name: Optional[str] = None
    cron_expression: Optional[str] = None
    portal_filter: Optional[List[str]] = None
    canton_filter: Optional[List[str]] = None
    max_requests_per_run: Optional[int] = None
    delay_between_requests: Optional[int] = None
    is_active: Optional[bool] = None


class BrochureRequestResponse(BaseModel):
    id: int
    prospect_id: Optional[str]
    portal: str
    listing_url: str
    listing_title: Optional[str]
    requester_name: Optional[str]
    requester_email: Optional[str]
    status: str
    sent_at: Optional[datetime]
    response_received: bool
    error_message: Optional[str]
    created_at: datetime

    class Config:
        from_attributes = True


class BrochureScheduleResponse(BaseModel):
    id: int
    name: str
    cron_expression: str
    portal_filter: List[str]
    canton_filter: List[str]
    max_requests_per_run: int
    delay_between_requests: int
    is_active: bool
    last_run: Optional[datetime]
    last_run_count: int
    total_sent: int
    total_responses: int
    created_at: datetime

    class Config:
        from_attributes = True


class QueueStatsResponse(BaseModel):
    pending: int
    total_sent: int
    total_errors: int
    sent_today: int
    by_status: dict
    by_portal: dict


class BrochurePipelineRunRequest(BaseModel):
    """Paramètres d'exécution du pipeline brochure."""
    days_back: int = 7
    auto_match: bool = True
    enrich_mobiles: bool = True
    canton: Optional[str] = None


# =============================================================================
# ROUTES - DEMANDES
# =============================================================================

@router.post("/request")
async def create_brochure_request(
    request: BrochureRequestCreate,
    background_tasks: BackgroundTasks,
):
    """Crée une nouvelle demande de brochure."""
    service = BrochureService()
    
    try:
        request_id = await service.submit_request(
            listing_url=request.listing_url,
            portal=request.portal,
            prospect_id=request.prospect_id,
            custom_message=request.custom_message,
            requester_name=request.requester_name,
            requester_phone=request.requester_phone,
        )
        
        return {"id": request_id, "status": "pending", "message": "Demande créée avec succès"}
    
    except EmailRotationError as e:
        raise HTTPException(status_code=429, detail=str(e))
    except BrochureServiceError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/batch")
async def create_batch_requests(
    request: BrochureBatchRequest,
    background_tasks: BackgroundTasks,
):
    """Crée un lot de demandes de brochure."""
    service = BrochureService()
    
    try:
        stats = await service.submit_batch(
            listings=request.listings,
            custom_message=request.custom_message,
        )
        
        return {
            "submitted": stats["submitted"],
            "skipped": stats["skipped"],
            "errors": stats["errors"],
            "message": f"{stats['submitted']} demandes créées"
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/process")
async def process_queue(
    max_requests: int = 10,
    background_tasks: BackgroundTasks = None,
):
    """Traite la file d'attente des demandes (envoi effectif)."""
    service = BrochureService()
    
    try:
        stats = await service.process_queue(max_requests=max_requests)
        await service.close()
        
        return {
            "processed": stats["processed"],
            "success": stats["success"],
            "errors": stats["errors"],
            "message": f"{stats['success']} demandes envoyées"
        }
    
    except Exception as e:
        await service.close()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/queue", response_model=QueueStatsResponse)
async def get_queue_status():
    """Récupère les statistiques de la file d'attente."""
    stats = await get_queue_stats()
    return QueueStatsResponse(**stats)


@router.get("/history", response_model=List[BrochureRequestResponse])
async def get_history(
    limit: int = 50,
    portal: Optional[str] = None,
    status: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """Récupère l'historique des demandes de brochure."""
    query = select(BrochureRequest).order_by(BrochureRequest.created_at.desc())
    
    if portal:
        query = query.where(BrochureRequest.portal == portal)
    if status:
        query = query.where(BrochureRequest.status == status)
    
    query = query.limit(limit)
    result = await db.execute(query)
    requests = result.scalars().all()
    
    return requests


@router.get("/request/{request_id}", response_model=BrochureRequestResponse)
async def get_request(request_id: int, db: AsyncSession = Depends(get_db)):
    """Récupère les détails d'une demande."""
    result = await db.execute(
        select(BrochureRequest).where(BrochureRequest.id == request_id)
    )
    request = result.scalar_one_or_none()
    
    if not request:
        raise HTTPException(status_code=404, detail="Demande non trouvée")
    
    return request


@router.delete("/request/{request_id}")
async def cancel_request(request_id: int, db: AsyncSession = Depends(get_db)):
    """Annule une demande en attente."""
    result = await db.execute(
        select(BrochureRequest).where(BrochureRequest.id == request_id)
    )
    request = result.scalar_one_or_none()
    
    if not request:
        raise HTTPException(status_code=404, detail="Demande non trouvée")
    
    if request.status != "pending":
        raise HTTPException(status_code=400, detail="Seules les demandes en attente peuvent être annulées")
    
    await db.delete(request)
    await db.commit()
    
    return {"message": "Demande annulée"}


@router.post("/retry/{request_id}")
async def retry_request(request_id: int, db: AsyncSession = Depends(get_db)):
    """Relance une demande en erreur."""
    result = await db.execute(
        select(BrochureRequest).where(BrochureRequest.id == request_id)
    )
    request = result.scalar_one_or_none()
    
    if not request:
        raise HTTPException(status_code=404, detail="Demande non trouvée")
    
    if request.status not in ["error"]:
        raise HTTPException(status_code=400, detail="Seules les demandes en erreur peuvent être relancées")
    
    request.status = "pending"
    request.error_message = None
    await db.commit()
    
    return {"message": "Demande remise en file d'attente"}


# =============================================================================
# ROUTES - PLANIFICATIONS
# =============================================================================

@router.get("/schedules", response_model=List[BrochureScheduleResponse])
async def list_schedules(db: AsyncSession = Depends(get_db)):
    """Liste toutes les planifications."""
    result = await db.execute(
        select(BrochureSchedule).order_by(BrochureSchedule.created_at.desc())
    )
    return result.scalars().all()


@router.post("/schedules", response_model=BrochureScheduleResponse)
async def create_schedule(
    schedule: BrochureScheduleCreate,
    db: AsyncSession = Depends(get_db),
):
    """Crée une nouvelle planification."""
    new_schedule = BrochureSchedule(
        name=schedule.name,
        cron_expression=schedule.cron_expression,
        portal_filter=schedule.portal_filter,
        canton_filter=schedule.canton_filter,
        max_requests_per_run=schedule.max_requests_per_run,
        delay_between_requests=schedule.delay_between_requests,
        is_active=schedule.is_active,
    )
    
    db.add(new_schedule)
    await db.commit()
    await db.refresh(new_schedule)
    
    await emit_activity("brochure", f"Planification créée: {schedule.name}")
    
    return new_schedule


@router.put("/schedules/{schedule_id}", response_model=BrochureScheduleResponse)
async def update_schedule(
    schedule_id: int,
    schedule: BrochureScheduleUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Met à jour une planification."""
    result = await db.execute(
        select(BrochureSchedule).where(BrochureSchedule.id == schedule_id)
    )
    existing = result.scalar_one_or_none()
    
    if not existing:
        raise HTTPException(status_code=404, detail="Planification non trouvée")
    
    # Mettre à jour les champs fournis
    if schedule.name is not None:
        existing.name = schedule.name
    if schedule.cron_expression is not None:
        existing.cron_expression = schedule.cron_expression
    if schedule.portal_filter is not None:
        existing.portal_filter = schedule.portal_filter
    if schedule.canton_filter is not None:
        existing.canton_filter = schedule.canton_filter
    if schedule.max_requests_per_run is not None:
        existing.max_requests_per_run = schedule.max_requests_per_run
    if schedule.delay_between_requests is not None:
        existing.delay_between_requests = schedule.delay_between_requests
    if schedule.is_active is not None:
        existing.is_active = schedule.is_active
    
    await db.commit()
    await db.refresh(existing)
    
    return existing


@router.delete("/schedules/{schedule_id}")
async def delete_schedule(schedule_id: int, db: AsyncSession = Depends(get_db)):
    """Supprime une planification."""
    result = await db.execute(
        select(BrochureSchedule).where(BrochureSchedule.id == schedule_id)
    )
    schedule = result.scalar_one_or_none()
    
    if not schedule:
        raise HTTPException(status_code=404, detail="Planification non trouvée")
    
    await db.delete(schedule)
    await db.commit()
    
    return {"message": "Planification supprimée"}


@router.post("/schedules/{schedule_id}/run")
async def run_schedule_now(
    schedule_id: int,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    """Exécute une planification immédiatement."""
    result = await db.execute(
        select(BrochureSchedule).where(BrochureSchedule.id == schedule_id)
    )
    schedule = result.scalar_one_or_none()
    
    if not schedule:
        raise HTTPException(status_code=404, detail="Planification non trouvée")
    
    # Lancer le traitement en arrière-plan
    async def run_scheduled_brochures():
        service = BrochureService()
        try:
            stats = await service.process_queue(max_requests=schedule.max_requests_per_run)
            
            # Mettre à jour les stats de la planification
            async with AsyncSession(bind=db.get_bind()) as session:
                schedule.last_run = datetime.utcnow()
                schedule.last_run_count = stats["success"]
                schedule.total_sent += stats["success"]
                await session.commit()
        finally:
            await service.close()
    
    background_tasks.add_task(run_scheduled_brochures)
    
    return {"message": f"Planification {schedule.name} lancée"}


# =============================================================================
# ROUTES - STATISTIQUES
# =============================================================================

@router.get("/stats")
async def get_stats(db: AsyncSession = Depends(get_db)):
    """Récupère les statistiques globales des brochures."""
    # Comptes par statut
    status_query = (
        select(BrochureRequest.status, func.count(BrochureRequest.id))
        .group_by(BrochureRequest.status)
    )
    status_result = await db.execute(status_query)
    by_status = dict(status_result.fetchall())
    
    # Comptes par portail
    portal_query = (
        select(BrochureRequest.portal, func.count(BrochureRequest.id))
        .group_by(BrochureRequest.portal)
    )
    portal_result = await db.execute(portal_query)
    by_portal = dict(portal_result.fetchall())
    
    # Aujourd'hui
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    today_query = (
        select(func.count(BrochureRequest.id))
        .where(BrochureRequest.sent_at >= today)
    )
    today_result = await db.execute(today_query)
    sent_today = today_result.scalar() or 0
    
    # Emails disponibles
    email_query = (
        select(func.count(EmailAccount.id))
        .where(EmailAccount.is_active == True)
        .where(EmailAccount.sent_today < EmailAccount.quota_daily)
    )
    email_result = await db.execute(email_query)
    available_emails = email_result.scalar() or 0
    
    # Réponses reçues
    responses_query = (
        select(func.count(BrochureRequest.id))
        .where(BrochureRequest.response_received == True)
    )
    responses_result = await db.execute(responses_query)
    total_responses = responses_result.scalar() or 0
    
    total_sent = by_status.get("sent", 0)
    response_rate = (total_responses / total_sent * 100) if total_sent > 0 else 0
    
    return {
        "total_requests": sum(by_status.values()),
        "pending": by_status.get("pending", 0),
        "sent": total_sent,
        "errors": by_status.get("error", 0),
        "responses": total_responses,
        "response_rate": round(response_rate, 1),
        "sent_today": sent_today,
        "available_emails": available_emails,
        "by_status": by_status,
        "by_portal": by_portal,
    }


# =============================================================================
# PIPELINE - EMAIL PARSER + MATCHING + ENRICH
# =============================================================================

@router.post("/parse-emails")
async def parse_emails(days_back: int = 7):
    """Parse les emails (tous comptes actifs) et extrait les adresses."""
    try:
        stats = await parse_emails_for_addresses(days_back=days_back)
        return {"status": "ok", "stats": stats}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pipeline/stats")
async def brochure_pipeline_stats(db: AsyncSession = Depends(get_db)):
    """Stats pipeline: réponses, adresses extraites, propriétaires, mobiles."""
    try:
        email_stats = await get_parsed_emails_stats()

        # Stats sur les listings
        extracted_q = await db.execute(
            select(func.count(ScrapedListing.id)).where(ScrapedListing.extracted_address.isnot(None))
        )
        owners_q = await db.execute(
            select(func.count(ScrapedListing.id)).where(ScrapedListing.owner_name.isnot(None))
        )
        mobiles_q = await db.execute(
            select(func.count(ScrapedListing.id)).where(ScrapedListing.owner_mobile.isnot(None))
        )

        return {
            "email": email_stats,
            "listings": {
                "addresses_extracted": extracted_q.scalar() or 0,
                "owners_matched": owners_q.scalar() or 0,
                "mobiles_found": mobiles_q.scalar() or 0,
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/pipeline/full")
async def run_brochure_pipeline(
    request: BrochurePipelineRunRequest,
    background_tasks: BackgroundTasks,
):
    """Lance le pipeline complet en arrière-plan."""

    # Créer un job persistant
    steps = ["email_parsing"]
    if request.auto_match:
        steps.append("auto_matching")
    if request.enrich_mobiles:
        steps.append("mobile_enrichment")

    async with AsyncSessionLocal() as db:
        job = BackgroundJob(
            job_type="brochure_pipeline",
            status="pending",
            total=len(steps),
            processed=0,
            meta={
                "days_back": request.days_back,
                "auto_match": request.auto_match,
                "enrich_mobiles": request.enrich_mobiles,
                "canton": request.canton,
                "steps": steps,
            },
        )
        db.add(job)
        await db.commit()
        await db.refresh(job)

    async def _run(job_id: int):
        start = datetime.utcnow()
        try:
            async with AsyncSessionLocal() as db:
                job_db = await db.get(BackgroundJob, job_id)
                if job_db:
                    job_db.status = "running"
                    job_db.started_at = start
                    job_db.processed = 0
                    await db.commit()

            await emit_activity(
                "pipeline",
                f"Pipeline brochure démarré (days_back={request.days_back}, canton={request.canton or 'ALL'})",
            )

            # Exécution step-by-step pour pouvoir tracer la progression
            from app.services.brochure_service import (
                process_brochure_responses,
                auto_match_listings_without_owner,
                batch_enrich_mobiles,
            )

            result: dict = {"steps": {}, "total_duration_seconds": 0}

            # Step 1
            result["steps"]["email_parsing"] = await process_brochure_responses(days_back=request.days_back)
            async with AsyncSessionLocal() as db:
                job_db = await db.get(BackgroundJob, job_id)
                if job_db:
                    job_db.processed = min(job_db.total, (job_db.processed or 0) + 1)
                    job_db.updated_at = datetime.utcnow()
                    await db.commit()

            # Step 2
            if request.auto_match:
                result["steps"]["auto_matching"] = await auto_match_listings_without_owner(
                    canton=request.canton,
                    limit=100,
                )
                async with AsyncSessionLocal() as db:
                    job_db = await db.get(BackgroundJob, job_id)
                    if job_db:
                        job_db.processed = min(job_db.total, (job_db.processed or 0) + 1)
                        job_db.updated_at = datetime.utcnow()
                        await db.commit()

            # Step 3
            if request.enrich_mobiles:
                result["steps"]["mobile_enrichment"] = await batch_enrich_mobiles(
                    canton=request.canton,
                    limit=30,
                )
                async with AsyncSessionLocal() as db:
                    job_db = await db.get(BackgroundJob, job_id)
                    if job_db:
                        job_db.processed = min(job_db.total, (job_db.processed or 0) + 1)
                        job_db.updated_at = datetime.utcnow()
                        await db.commit()

            end = datetime.utcnow()
            result["total_duration_seconds"] = (end - start).total_seconds()

            async with AsyncSessionLocal() as db:
                job_db = await db.get(BackgroundJob, job_id)
                if job_db:
                    job_db.status = "completed"
                    job_db.completed_at = end
                    job_db.result = result
                    job_db.updated_at = datetime.utcnow()
                    await db.commit()

            await emit_activity("success", f"Pipeline brochure terminé ({result.get('total_duration_seconds', 0):.1f}s)")
        except Exception as e:
            async with AsyncSessionLocal() as db:
                job_db = await db.get(BackgroundJob, job_id)
                if job_db:
                    job_db.status = "error"
                    job_db.error_message = str(e)
                    job_db.completed_at = datetime.utcnow()
                    job_db.updated_at = datetime.utcnow()
                    await db.commit()
            await emit_activity("error", f"Pipeline brochure erreur: {e}")

    background_tasks.add_task(_run, job.id)
    return {"status": "started", "job_id": job.id}


# =============================================================================
# ROUTES - ANNONCES SCRAPÉES
# =============================================================================

@router.get("/listings")
async def list_scraped_listings(
    limit: int = 50,
    portal: Optional[str] = None,
    canton: Optional[str] = None,
    brochure_requested: Optional[bool] = None,
    db: AsyncSession = Depends(get_db),
):
    """Liste les annonces scrapées."""
    query = select(ScrapedListing).order_by(ScrapedListing.scraped_at.desc())
    
    if portal:
        query = query.where(ScrapedListing.portal == portal)
    if canton:
        query = query.where(ScrapedListing.canton == canton)
    if brochure_requested is not None:
        query = query.where(ScrapedListing.brochure_requested == brochure_requested)
    
    query = query.limit(limit)
    result = await db.execute(query)
    listings = result.scalars().all()
    
    return [
        {
            "id": l.id,
            "portal": l.portal,
            "url": l.url,
            "title": l.title,
            "address": l.address,
            "city": l.city,
            "canton": l.canton,
            "price": l.price,
            "rooms": l.rooms,
            "surface": l.surface,
            "brochure_requested": l.brochure_requested,
            "scraped_at": l.scraped_at.isoformat() if l.scraped_at else None,
        }
        for l in listings
    ]


@router.post("/listings/request-all")
async def request_brochures_for_all(
    portal: Optional[str] = None,
    canton: Optional[str] = None,
    limit: int = 50,
    db: AsyncSession = Depends(get_db),
):
    """Crée des demandes de brochure pour toutes les annonces non traitées."""
    query = (
        select(ScrapedListing)
        .where(ScrapedListing.brochure_requested == False)
    )
    
    if portal:
        query = query.where(ScrapedListing.portal == portal)
    if canton:
        query = query.where(ScrapedListing.canton == canton)
    
    query = query.limit(limit)
    result = await db.execute(query)
    listings = result.scalars().all()
    
    if not listings:
        return {"message": "Aucune annonce à traiter", "count": 0}
    
    service = BrochureService()
    stats = await service.submit_batch([
        {"url": l.url, "portal": l.portal}
        for l in listings
    ])
    
    return {
        "message": f"{stats['submitted']} demandes créées",
        "submitted": stats["submitted"],
        "skipped": stats["skipped"],
        "errors": stats["errors"],
    }

