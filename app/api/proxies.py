# =============================================================================
# API PROXIES - Gestion du pool de proxies
# =============================================================================

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update, delete
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime
import aiohttp
import asyncio
import csv
import io

from app.core.database import get_db, Proxy

router = APIRouter()

# =============================================================================
# SCHEMAS
# =============================================================================

class ProxyCreate(BaseModel):
    host: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None
    protocol: Optional[str] = "http"
    country: Optional[str] = "CH"

class ProxyResponse(BaseModel):
    id: int
    host: str
    port: int
    username: Optional[str]
    protocol: str
    country: str
    is_active: bool
    is_valid: bool
    latency_ms: Optional[int]
    success_rate: float
    last_checked: Optional[datetime]
    created_at: datetime

    class Config:
        from_attributes = True

class ProxyStats(BaseModel):
    total: int
    active: int
    valid: int
    swiss: int
    avg_latency: Optional[float]

# =============================================================================
# ROUTES
# =============================================================================

@router.get("/", response_model=List[ProxyResponse])
async def list_proxies(
    active_only: bool = False,
    valid_only: bool = False,
    country: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Liste tous les proxies"""
    query = select(Proxy)
    
    if active_only:
        query = query.where(Proxy.is_active == True)
    if valid_only:
        query = query.where(Proxy.is_valid == True)
    if country:
        query = query.where(Proxy.country == country)
    
    query = query.order_by(Proxy.latency_ms.asc().nullslast())
    
    result = await db.execute(query)
    return result.scalars().all()

@router.get("/stats", response_model=ProxyStats)
async def get_proxy_stats(db: AsyncSession = Depends(get_db)):
    """Statistiques des proxies"""
    result = await db.execute(select(Proxy))
    proxies = result.scalars().all()
    
    latencies = [p.latency_ms for p in proxies if p.latency_ms]
    
    return ProxyStats(
        total=len(proxies),
        active=len([p for p in proxies if p.is_active]),
        valid=len([p for p in proxies if p.is_valid]),
        swiss=len([p for p in proxies if p.country == "CH"]),
        avg_latency=sum(latencies) / len(latencies) if latencies else None
    )

@router.get("/available")
async def get_available_proxy(
    country: Optional[str] = "CH",
    db: AsyncSession = Depends(get_db)
):
    """Retourne un proxy disponible (meilleur latence)"""
    query = (
        select(Proxy)
        .where(Proxy.is_active == True)
        .where(Proxy.is_valid == True)
    )
    
    if country:
        query = query.where(Proxy.country == country)
    
    query = query.order_by(Proxy.latency_ms.asc().nullslast()).limit(1)
    
    result = await db.execute(query)
    proxy = result.scalar_one_or_none()
    
    if not proxy:
        raise HTTPException(status_code=404, detail="No available proxy")
    
    return {
        "id": proxy.id,
        "url": f"{proxy.protocol}://{proxy.host}:{proxy.port}",
        "auth": f"{proxy.username}:{proxy.password}" if proxy.username else None
    }

@router.post("/", response_model=ProxyResponse)
async def create_proxy(data: ProxyCreate, db: AsyncSession = Depends(get_db)):
    """Ajoute un proxy"""
    proxy = Proxy(**data.model_dump())
    db.add(proxy)
    await db.commit()
    await db.refresh(proxy)
    return proxy

@router.post("/import")
async def import_proxies(file: UploadFile = File(...), db: AsyncSession = Depends(get_db)):
    """Importe des proxies depuis un fichier (format: host:port:user:pass)"""
    content = await file.read()
    text = content.decode('utf-8')
    
    imported = 0
    errors = []
    
    for line in text.strip().split('\n'):
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        
        parts = line.split(':')
        if len(parts) < 2:
            continue
        
        try:
            proxy = Proxy(
                host=parts[0],
                port=int(parts[1]),
                username=parts[2] if len(parts) > 2 else None,
                password=parts[3] if len(parts) > 3 else None
            )
            db.add(proxy)
            imported += 1
        except Exception as e:
            errors.append(f"{line}: {str(e)}")
    
    await db.commit()
    
    return {"imported": imported, "errors": errors}

@router.post("/{proxy_id}/test")
async def test_proxy(proxy_id: int, db: AsyncSession = Depends(get_db)):
    """Teste un proxy"""
    result = await db.execute(select(Proxy).where(Proxy.id == proxy_id))
    proxy = result.scalar_one_or_none()
    
    if not proxy:
        raise HTTPException(status_code=404, detail="Proxy not found")
    
    # Construire l'URL du proxy
    proxy_url = f"{proxy.protocol}://"
    if proxy.username and proxy.password:
        proxy_url += f"{proxy.username}:{proxy.password}@"
    proxy_url += f"{proxy.host}:{proxy.port}"
    
    # Tester le proxy
    try:
        start_time = datetime.utcnow()
        
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://api.ipify.org?format=json",
                proxy=proxy_url,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    latency = (datetime.utcnow() - start_time).total_seconds() * 1000
                    
                    proxy.is_valid = True
                    proxy.latency_ms = int(latency)
                    proxy.last_checked = datetime.utcnow()
                    await db.commit()
                    
                    return {
                        "success": True,
                        "ip": data.get("ip"),
                        "latency_ms": int(latency)
                    }
    except Exception as e:
        proxy.is_valid = False
        proxy.last_checked = datetime.utcnow()
        await db.commit()
        
        return {
            "success": False,
            "error": str(e)
        }

@router.post("/test-all")
async def test_all_proxies(db: AsyncSession = Depends(get_db)):
    """Teste tous les proxies actifs"""
    result = await db.execute(
        select(Proxy).where(Proxy.is_active == True)
    )
    proxies = result.scalars().all()
    
    results = {"tested": 0, "valid": 0, "invalid": 0}
    
    for proxy in proxies:
        # Test simplifié
        proxy_url = f"{proxy.protocol}://{proxy.host}:{proxy.port}"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://api.ipify.org",
                    proxy=proxy_url,
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as response:
                    proxy.is_valid = response.status == 200
        except:
            proxy.is_valid = False
        
        proxy.last_checked = datetime.utcnow()
        results["tested"] += 1
        results["valid" if proxy.is_valid else "invalid"] += 1
    
    await db.commit()
    return results

@router.put("/{proxy_id}/toggle")
async def toggle_proxy(proxy_id: int, db: AsyncSession = Depends(get_db)):
    """Active/désactive un proxy"""
    result = await db.execute(select(Proxy).where(Proxy.id == proxy_id))
    proxy = result.scalar_one_or_none()
    
    if not proxy:
        raise HTTPException(status_code=404, detail="Proxy not found")
    
    proxy.is_active = not proxy.is_active
    await db.commit()
    
    return {"is_active": proxy.is_active}

@router.delete("/{proxy_id}")
async def delete_proxy(proxy_id: int, db: AsyncSession = Depends(get_db)):
    """Supprime un proxy"""
    result = await db.execute(delete(Proxy).where(Proxy.id == proxy_id))
    await db.commit()
    
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Proxy not found")
    
    return {"deleted": True}

@router.delete("/invalid")
async def delete_invalid_proxies(db: AsyncSession = Depends(get_db)):
    """Supprime tous les proxies invalides"""
    result = await db.execute(delete(Proxy).where(Proxy.is_valid == False))
    await db.commit()
    
    return {"deleted": result.rowcount}

