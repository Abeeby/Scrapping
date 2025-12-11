# =============================================================================
# API BOTS - Gestion des bots de scraping
# =============================================================================

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime
import asyncio

from app.core.database import get_db, Bot, Proxy
from app.core.websocket import emit_bot_status, emit_bot_log, emit_activity
from app.bots.bot_manager import BotManager

router = APIRouter()

# Instance globale du manager
bot_manager = BotManager()

# =============================================================================
# SCHEMAS
# =============================================================================

class BotCreate(BaseModel):
    name: str
    type: str  # comparis, immoscout, homegate
    proxy_id: Optional[int] = None
    email_id: Optional[int] = None
    config: Optional[dict] = {}

class BotResponse(BaseModel):
    id: int
    name: str
    type: str
    status: str
    proxy_id: Optional[int]
    email_id: Optional[int]
    requests_count: int
    success_count: int
    error_count: int
    last_run: Optional[datetime]
    config: dict
    created_at: datetime
    
    # Champs calculés
    current_ip: Optional[str] = None
    success_rate: Optional[float] = None

    class Config:
        from_attributes = True

class BotRunConfig(BaseModel):
    city: str = "Genève"
    radius_km: int = 10
    property_type: Optional[str] = None
    max_results: int = 50
    delay_seconds: int = 30

# =============================================================================
# ROUTES
# =============================================================================

@router.get("/", response_model=List[BotResponse])
async def list_bots(db: AsyncSession = Depends(get_db)):
    """Liste tous les bots"""
    result = await db.execute(select(Bot).order_by(Bot.created_at.desc()))
    bots = result.scalars().all()
    
    # Enrichir avec les IPs des proxies
    response = []
    for bot in bots:
        bot_data = BotResponse.model_validate(bot)
        
        # Récupérer l'IP du proxy
        if bot.proxy_id:
            proxy_result = await db.execute(
                select(Proxy).where(Proxy.id == bot.proxy_id)
            )
            proxy = proxy_result.scalar_one_or_none()
            if proxy:
                bot_data.current_ip = proxy.host
        
        # Calculer le taux de succès
        total = bot.success_count + bot.error_count
        if total > 0:
            bot_data.success_rate = round(bot.success_count / total * 100, 1)
        
        response.append(bot_data)
    
    return response

@router.get("/stats")
async def get_bots_stats(db: AsyncSession = Depends(get_db)):
    """Statistiques globales des bots"""
    result = await db.execute(select(Bot))
    bots = result.scalars().all()
    
    return {
        "total": len(bots),
        "running": len([b for b in bots if b.status == "running"]),
        "idle": len([b for b in bots if b.status == "idle"]),
        "error": len([b for b in bots if b.status == "error"]),
        "total_requests": sum(b.requests_count for b in bots),
        "total_success": sum(b.success_count for b in bots),
        "total_errors": sum(b.error_count for b in bots)
    }

@router.get("/{bot_id}", response_model=BotResponse)
async def get_bot(bot_id: int, db: AsyncSession = Depends(get_db)):
    """Récupère un bot par ID"""
    result = await db.execute(select(Bot).where(Bot.id == bot_id))
    bot = result.scalar_one_or_none()
    
    if not bot:
        raise HTTPException(status_code=404, detail="Bot not found")
    
    return bot

@router.post("/", response_model=BotResponse)
async def create_bot(data: BotCreate, db: AsyncSession = Depends(get_db)):
    """Crée un nouveau bot"""
    bot = Bot(**data.model_dump())
    db.add(bot)
    await db.commit()
    await db.refresh(bot)
    
    await emit_activity("bot_created", f"Bot {bot.name} créé")
    return bot

@router.post("/{bot_id}/start")
async def start_bot(
    bot_id: int,
    config: BotRunConfig,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """Démarre un bot"""
    result = await db.execute(select(Bot).where(Bot.id == bot_id))
    bot = result.scalar_one_or_none()
    
    if not bot:
        raise HTTPException(status_code=404, detail="Bot not found")
    
    if bot.status == "running":
        raise HTTPException(status_code=400, detail="Bot already running")
    
    # Mettre à jour le statut
    bot.status = "running"
    bot.last_run = datetime.utcnow()
    await db.commit()
    
    # Émettre le statut via WebSocket
    await emit_bot_status(bot_id, "running", {"config": config.model_dump()})
    await emit_bot_log(bot_id, f"🚀 Bot démarré - Cible: {config.city}")
    
    # Lancer en arrière-plan
    background_tasks.add_task(
        bot_manager.run_bot,
        bot_id,
        bot.type,
        config.model_dump()
    )
    
    return {"status": "started", "bot_id": bot_id}

@router.post("/{bot_id}/stop")
async def stop_bot(bot_id: int, db: AsyncSession = Depends(get_db)):
    """Arrête un bot"""
    result = await db.execute(select(Bot).where(Bot.id == bot_id))
    bot = result.scalar_one_or_none()
    
    if not bot:
        raise HTTPException(status_code=404, detail="Bot not found")
    
    # Signaler l'arrêt au manager
    bot_manager.stop_bot(bot_id)
    
    # Mettre à jour le statut
    bot.status = "idle"
    await db.commit()
    
    await emit_bot_status(bot_id, "idle")
    await emit_bot_log(bot_id, "⏹️ Bot arrêté")
    
    return {"status": "stopped", "bot_id": bot_id}

@router.post("/{bot_id}/pause")
async def pause_bot(bot_id: int, db: AsyncSession = Depends(get_db)):
    """Met un bot en pause"""
    result = await db.execute(select(Bot).where(Bot.id == bot_id))
    bot = result.scalar_one_or_none()
    
    if not bot:
        raise HTTPException(status_code=404, detail="Bot not found")
    
    bot_manager.pause_bot(bot_id)
    bot.status = "paused"
    await db.commit()
    
    await emit_bot_status(bot_id, "paused")
    await emit_bot_log(bot_id, "⏸️ Bot en pause")
    
    return {"status": "paused", "bot_id": bot_id}

@router.post("/{bot_id}/resume")
async def resume_bot(bot_id: int, db: AsyncSession = Depends(get_db)):
    """Reprend un bot en pause"""
    result = await db.execute(select(Bot).where(Bot.id == bot_id))
    bot = result.scalar_one_or_none()
    
    if not bot:
        raise HTTPException(status_code=404, detail="Bot not found")
    
    bot_manager.resume_bot(bot_id)
    bot.status = "running"
    await db.commit()
    
    await emit_bot_status(bot_id, "running")
    await emit_bot_log(bot_id, "▶️ Bot repris")
    
    return {"status": "running", "bot_id": bot_id}

@router.put("/{bot_id}/proxy")
async def change_proxy(
    bot_id: int,
    proxy_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Change le proxy d'un bot"""
    # Vérifier le bot
    bot_result = await db.execute(select(Bot).where(Bot.id == bot_id))
    bot = bot_result.scalar_one_or_none()
    if not bot:
        raise HTTPException(status_code=404, detail="Bot not found")
    
    # Vérifier le proxy
    proxy_result = await db.execute(select(Proxy).where(Proxy.id == proxy_id))
    proxy = proxy_result.scalar_one_or_none()
    if not proxy:
        raise HTTPException(status_code=404, detail="Proxy not found")
    
    bot.proxy_id = proxy_id
    await db.commit()
    
    await emit_bot_log(bot_id, f"🔄 Proxy changé: {proxy.host}")
    
    return {"success": True, "new_proxy": proxy.host}

@router.delete("/{bot_id}")
async def delete_bot(bot_id: int, db: AsyncSession = Depends(get_db)):
    """Supprime un bot"""
    result = await db.execute(select(Bot).where(Bot.id == bot_id))
    bot = result.scalar_one_or_none()
    
    if not bot:
        raise HTTPException(status_code=404, detail="Bot not found")
    
    # Arrêter si en cours
    if bot.status == "running":
        bot_manager.stop_bot(bot_id)
    
    await db.delete(bot)
    await db.commit()
    
    return {"deleted": True}

