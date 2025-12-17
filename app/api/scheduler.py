# =============================================================================
# API SCHEDULER - Gestion des planifications de scraping
# =============================================================================

from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime
import json as _json_dbg
import time as _time_dbg

from app.core.database import get_db
from app.core.websocket import emit_activity
from app.services.scheduler_service import scheduler, ScrapingSchedule, ScheduleFrequency, ScheduleStatus

# region agent log
_DBG_LOG = r"c:\Users\admin10\Desktop\Scrapping data\.cursor\debug.log"
def _dbg(hid, loc, msg, data=None):
    try:
        with open(_DBG_LOG, "a", encoding="utf-8") as f:
            f.write(_json_dbg.dumps({"hypothesisId": hid, "location": loc, "message": msg, "data": data or {}, "timestamp": int(_time_dbg.time()*1000), "sessionId": "debug-session"}) + "\n")
    except: pass
_dbg("H1", "scheduler.py:module_load", "scheduler API module loaded", {})
# endregion

router = APIRouter()


# =============================================================================
# SCHEMAS
# =============================================================================

class ScheduleCreate(BaseModel):
    name: str
    source: str  # anibis, tutti, homegate, immoscout24, searchch, scanner
    parameters: Optional[dict] = {}
    frequency: Optional[str] = "daily"  # hourly, daily, weekly
    hour: Optional[int] = 6
    minute: Optional[int] = 0
    days_of_week: Optional[List[int]] = None  # [0,1,2,3,4,5,6] = Lun-Dim


class ScheduleUpdate(BaseModel):
    name: Optional[str] = None
    parameters: Optional[dict] = None
    frequency: Optional[str] = None
    hour: Optional[int] = None
    minute: Optional[int] = None
    days_of_week: Optional[List[int]] = None
    status: Optional[str] = None  # active, paused, disabled


class ScheduleResponse(BaseModel):
    id: str
    name: str
    source: str
    parameters: dict
    frequency: str
    hour: int
    minute: int
    days_of_week: List[int]
    status: str
    last_run: Optional[datetime]
    next_run: Optional[datetime]
    last_result: Optional[dict]
    total_runs: int
    total_leads_found: int
    success_rate: int
    created_at: datetime
    
    class Config:
        from_attributes = True


# =============================================================================
# ENDPOINTS
# =============================================================================

@router.get("/", response_model=List[ScheduleResponse])
async def list_schedules(active_only: bool = False):
    """Liste toutes les planifications de scraping."""
    # region agent log
    _dbg("H4", "scheduler.py:list_schedules", "endpoint called", {"active_only": active_only})
    # endregion
    try:
        schedules = await scheduler.get_schedules(active_only=active_only)
        # region agent log
        _dbg("H4", "scheduler.py:list_schedules", "success", {"count": len(schedules) if schedules else 0})
        # endregion
        return schedules
    except Exception as e:
        # region agent log
        _dbg("H5", "scheduler.py:list_schedules", "error", {"error": str(e), "type": type(e).__name__})
        # endregion
        raise


@router.post("/", response_model=ScheduleResponse)
async def create_schedule(data: ScheduleCreate):
    """
    Crée une nouvelle planification de scraping automatique.
    
    Exemple: Scraper Anibis tous les jours à 6h du matin
    ```json
    {
        "name": "Anibis Genève Quotidien",
        "source": "anibis",
        "parameters": {"canton": "GE", "transaction_type": "vente", "only_private": true},
        "frequency": "daily",
        "hour": 6
    }
    ```
    """
    try:
        schedule = await scheduler.create_schedule(
            name=data.name,
            source=data.source,
            parameters=data.parameters or {},
            frequency=data.frequency or "daily",
            hour=data.hour or 6,
            minute=data.minute or 0,
            days_of_week=data.days_of_week,
        )
        
        await emit_activity("scheduler", f"Planification créée: {data.name}")
        return schedule
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{schedule_id}", response_model=ScheduleResponse)
async def get_schedule(schedule_id: str):
    """Récupère une planification par ID."""
    schedule = await scheduler.get_schedule(schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail="Planification non trouvée")
    return schedule


@router.put("/{schedule_id}", response_model=ScheduleResponse)
async def update_schedule(schedule_id: str, data: ScheduleUpdate):
    """Met à jour une planification."""
    updates = {k: v for k, v in data.dict().items() if v is not None}
    
    schedule = await scheduler.update_schedule(schedule_id, **updates)
    if not schedule:
        raise HTTPException(status_code=404, detail="Planification non trouvée")
    
    await emit_activity("scheduler", f"Planification mise à jour: {schedule.name}")
    return schedule


@router.delete("/{schedule_id}")
async def delete_schedule(schedule_id: str):
    """Supprime une planification."""
    success = await scheduler.delete_schedule(schedule_id)
    if not success:
        raise HTTPException(status_code=404, detail="Planification non trouvée")
    
    return {"status": "deleted", "id": schedule_id}


@router.post("/{schedule_id}/run")
async def run_schedule_now(schedule_id: str):
    """Exécute immédiatement une planification."""
    try:
        result = await scheduler.run_now(schedule_id)
        return {"status": "executed", "result": result}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{schedule_id}/pause")
async def pause_schedule(schedule_id: str):
    """Met en pause une planification."""
    schedule = await scheduler.update_schedule(schedule_id, status=ScheduleStatus.PAUSED)
    if not schedule:
        raise HTTPException(status_code=404, detail="Planification non trouvée")
    
    await emit_activity("scheduler", f"Planification mise en pause: {schedule.name}")
    return {"status": "paused", "id": schedule_id}


@router.post("/{schedule_id}/resume")
async def resume_schedule(schedule_id: str):
    """Reprend une planification mise en pause."""
    schedule = await scheduler.update_schedule(schedule_id, status=ScheduleStatus.ACTIVE)
    if not schedule:
        raise HTTPException(status_code=404, detail="Planification non trouvée")
    
    await emit_activity("scheduler", f"Planification reprise: {schedule.name}")
    return {"status": "active", "id": schedule_id}


@router.get("/sources/available")
async def get_available_sources():
    """Liste les sources de scraping disponibles pour la planification."""
    return {
        "sources": [
            {
                "id": "anibis",
                "name": "Anibis.ch",
                "description": "Petites annonces suisses (68'000+ annonces immo)",
                "parameters": ["canton", "transaction_type", "property_type", "only_private", "limit"],
                "recommended": True,
            },
            {
                "id": "tutti",
                "name": "Tutti.ch",
                "description": "Petites annonces suisses (populaire en Suisse alémanique)",
                "parameters": ["canton", "transaction_type", "property_type", "only_private", "limit"],
                "recommended": True,
            },
            {
                "id": "homegate",
                "name": "Homegate.ch",
                "description": "Plus grand portail immobilier suisse",
                "parameters": ["location", "transaction_type", "limit"],
                "recommended": False,
            },
            {
                "id": "immoscout24",
                "name": "ImmoScout24.ch",
                "description": "Portail immobilier majeur",
                "parameters": ["location", "transaction_type", "limit"],
                "recommended": False,
            },
            {
                "id": "searchch",
                "name": "Search.ch",
                "description": "Annuaire téléphonique suisse",
                "parameters": ["query", "ville", "type_recherche", "limit"],
                "recommended": False,
            },
            {
                "id": "scanner",
                "name": "Scanner de quartier",
                "description": "Scan adresse par adresse via Search.ch",
                "parameters": ["commune", "rue", "type_recherche", "limit"],
                "recommended": False,
            },
            {
                "id": "swiss_addresses",
                "name": "Swiss Addresses (GeoAdmin)",
                "description": "Adresses suisses via API officielle",
                "parameters": ["location", "limit"],
                "recommended": False,
            },
        ],
        "frequencies": [
            {"id": "hourly", "name": "Toutes les heures"},
            {"id": "daily", "name": "Quotidien"},
            {"id": "weekly", "name": "Hebdomadaire"},
        ],
    }
