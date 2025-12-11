# =============================================================================
# API CAMPAIGNS - Gestion des campagnes automatisées
# =============================================================================

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime

from app.core.database import get_db, Campaign
from app.core.websocket import emit_campaign_progress, emit_activity

router = APIRouter()

# =============================================================================
# SCHEMAS
# =============================================================================

class CampaignCreate(BaseModel):
    name: str
    type: str = "brochure"
    target_portal: str = "comparis"
    target_city: str = "Genève"
    target_radius: int = 10
    total_targets: int = 50
    config: Optional[dict] = {}

class CampaignResponse(BaseModel):
    id: int
    name: str
    type: str
    target_portal: str
    target_city: str
    target_radius: int
    status: str
    total_targets: int
    sent_count: int
    response_count: int
    error_count: int
    config: dict
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    created_at: datetime
    
    # Calculés
    progress_percent: float = 0
    response_rate: float = 0

    class Config:
        from_attributes = True

# =============================================================================
# ROUTES
# =============================================================================

@router.get("/", response_model=List[CampaignResponse])
async def list_campaigns(
    status: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Liste toutes les campagnes"""
    query = select(Campaign).order_by(Campaign.created_at.desc())
    
    if status:
        query = query.where(Campaign.status == status)
    
    result = await db.execute(query)
    campaigns = result.scalars().all()
    
    # Enrichir avec les calculs
    response = []
    for c in campaigns:
        data = CampaignResponse.model_validate(c)
        data.progress_percent = round(c.sent_count / max(c.total_targets, 1) * 100, 1)
        data.response_rate = round(c.response_count / max(c.sent_count, 1) * 100, 1)
        response.append(data)
    
    return response

@router.get("/stats")
async def get_campaign_stats(db: AsyncSession = Depends(get_db)):
    """Statistiques globales des campagnes"""
    result = await db.execute(select(Campaign))
    campaigns = result.scalars().all()
    
    return {
        "total": len(campaigns),
        "running": len([c for c in campaigns if c.status == "running"]),
        "completed": len([c for c in campaigns if c.status == "completed"]),
        "total_sent": sum(c.sent_count for c in campaigns),
        "total_responses": sum(c.response_count for c in campaigns),
        "avg_response_rate": round(
            sum(c.response_count for c in campaigns) / 
            max(sum(c.sent_count for c in campaigns), 1) * 100, 1
        )
    }

@router.get("/{campaign_id}", response_model=CampaignResponse)
async def get_campaign(campaign_id: int, db: AsyncSession = Depends(get_db)):
    """Récupère une campagne"""
    result = await db.execute(select(Campaign).where(Campaign.id == campaign_id))
    campaign = result.scalar_one_or_none()
    
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    
    return campaign

@router.post("/", response_model=CampaignResponse)
async def create_campaign(data: CampaignCreate, db: AsyncSession = Depends(get_db)):
    """Crée une nouvelle campagne"""
    campaign = Campaign(**data.model_dump())
    db.add(campaign)
    await db.commit()
    await db.refresh(campaign)
    
    await emit_activity("campaign_created", f"Campagne '{campaign.name}' créée")
    return campaign

@router.post("/{campaign_id}/start")
async def start_campaign(
    campaign_id: int,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """Démarre une campagne"""
    result = await db.execute(select(Campaign).where(Campaign.id == campaign_id))
    campaign = result.scalar_one_or_none()
    
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    
    if campaign.status == "running":
        raise HTTPException(status_code=400, detail="Campaign already running")
    
    campaign.status = "running"
    campaign.started_at = datetime.utcnow()
    await db.commit()
    
    await emit_activity("campaign_started", f"Campagne '{campaign.name}' démarrée")
    
    # TODO: Lancer la campagne en arrière-plan
    # background_tasks.add_task(run_campaign, campaign_id)
    
    return {"status": "started"}

@router.post("/{campaign_id}/pause")
async def pause_campaign(campaign_id: int, db: AsyncSession = Depends(get_db)):
    """Met une campagne en pause"""
    await db.execute(
        update(Campaign)
        .where(Campaign.id == campaign_id)
        .values(status="paused")
    )
    await db.commit()
    return {"status": "paused"}

@router.post("/{campaign_id}/resume")
async def resume_campaign(campaign_id: int, db: AsyncSession = Depends(get_db)):
    """Reprend une campagne"""
    await db.execute(
        update(Campaign)
        .where(Campaign.id == campaign_id)
        .values(status="running")
    )
    await db.commit()
    return {"status": "running"}

@router.post("/{campaign_id}/stop")
async def stop_campaign(campaign_id: int, db: AsyncSession = Depends(get_db)):
    """Arrête une campagne"""
    result = await db.execute(select(Campaign).where(Campaign.id == campaign_id))
    campaign = result.scalar_one_or_none()
    
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    
    campaign.status = "completed"
    campaign.completed_at = datetime.utcnow()
    await db.commit()
    
    await emit_activity("campaign_completed", f"Campagne '{campaign.name}' terminée")
    return {"status": "completed"}

@router.delete("/{campaign_id}")
async def delete_campaign(campaign_id: int, db: AsyncSession = Depends(get_db)):
    """Supprime une campagne"""
    result = await db.execute(select(Campaign).where(Campaign.id == campaign_id))
    campaign = result.scalar_one_or_none()
    
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    
    await db.delete(campaign)
    await db.commit()
    return {"deleted": True}

