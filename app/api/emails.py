# =============================================================================
# API EMAILS - Gestion du pool d'emails
# =============================================================================

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime
import csv
import io

from app.core.database import get_db, EmailAccount

router = APIRouter()

# =============================================================================
# SCHEMAS
# =============================================================================

class EmailAccountCreate(BaseModel):
    email: str
    password: str
    imap_server: Optional[str] = ""
    smtp_server: Optional[str] = ""
    quota_daily: Optional[int] = 50

class EmailAccountResponse(BaseModel):
    id: int
    email: str
    imap_server: Optional[str]
    smtp_server: Optional[str]
    quota_daily: int
    sent_today: int
    is_active: bool
    last_used: Optional[datetime]
    error_count: int
    created_at: datetime

    class Config:
        from_attributes = True

class EmailStats(BaseModel):
    total_accounts: int
    active_accounts: int
    total_sent_today: int
    total_quota: int
    available_quota: int

# =============================================================================
# ROUTES
# =============================================================================

@router.get("/", response_model=List[EmailAccountResponse])
async def list_emails(
    active_only: bool = False,
    db: AsyncSession = Depends(get_db)
):
    """Liste tous les comptes email"""
    query = select(EmailAccount)
    if active_only:
        query = query.where(EmailAccount.is_active == True)
    query = query.order_by(EmailAccount.sent_today.asc())
    
    result = await db.execute(query)
    return result.scalars().all()

@router.get("/stats", response_model=EmailStats)
async def get_email_stats(db: AsyncSession = Depends(get_db)):
    """Statistiques du pool email"""
    # Total comptes
    total = await db.execute(select(func.count(EmailAccount.id)))
    total_accounts = total.scalar()
    
    # Comptes actifs
    active = await db.execute(
        select(func.count(EmailAccount.id))
        .where(EmailAccount.is_active == True)
    )
    active_accounts = active.scalar()
    
    # Envois aujourd'hui
    sent = await db.execute(select(func.sum(EmailAccount.sent_today)))
    total_sent = sent.scalar() or 0
    
    # Quota total
    quota = await db.execute(
        select(func.sum(EmailAccount.quota_daily))
        .where(EmailAccount.is_active == True)
    )
    total_quota = quota.scalar() or 0
    
    return EmailStats(
        total_accounts=total_accounts,
        active_accounts=active_accounts,
        total_sent_today=total_sent,
        total_quota=total_quota,
        available_quota=total_quota - total_sent
    )

@router.get("/available")
async def get_available_email(db: AsyncSession = Depends(get_db)):
    """Retourne un email disponible (moins utilisé)"""
    query = (
        select(EmailAccount)
        .where(EmailAccount.is_active == True)
        .where(EmailAccount.sent_today < EmailAccount.quota_daily)
        .order_by(EmailAccount.sent_today.asc())
        .limit(1)
    )
    
    result = await db.execute(query)
    account = result.scalar_one_or_none()
    
    if not account:
        raise HTTPException(status_code=404, detail="No available email account")
    
    return {
        "id": account.id,
        "email": account.email,
        "remaining_quota": account.quota_daily - account.sent_today
    }

@router.post("/", response_model=EmailAccountResponse)
async def create_email(data: EmailAccountCreate, db: AsyncSession = Depends(get_db)):
    """Ajoute un compte email"""
    # Détection auto des serveurs
    domain = data.email.split("@")[-1]
    
    servers = {
        "gmail.com": ("imap.gmail.com", "smtp.gmail.com"),
        "outlook.com": ("outlook.office365.com", "smtp.office365.com"),
        "hotmail.com": ("outlook.office365.com", "smtp.office365.com"),
        "bluewin.ch": ("imaps.bluewin.ch", "smtpauths.bluewin.ch"),
        "yahoo.com": ("imap.mail.yahoo.com", "smtp.mail.yahoo.com"),
    }
    
    imap, smtp = servers.get(domain, (f"imap.{domain}", f"smtp.{domain}"))
    
    account = EmailAccount(
        email=data.email,
        password=data.password,
        imap_server=data.imap_server or imap,
        smtp_server=data.smtp_server or smtp,
        quota_daily=data.quota_daily
    )
    
    db.add(account)
    await db.commit()
    await db.refresh(account)
    
    return account

@router.post("/import")
async def import_emails(file: UploadFile = File(...), db: AsyncSession = Depends(get_db)):
    """Importe des emails depuis un fichier CSV"""
    content = await file.read()
    text = content.decode('utf-8')
    
    reader = csv.reader(io.StringIO(text))
    imported = 0
    errors = []
    
    for row in reader:
        if len(row) < 2:
            continue
        if row[0].startswith('#'):
            continue
            
        email = row[0].strip()
        password = row[1].strip()
        
        try:
            account = EmailAccount(email=email, password=password)
            db.add(account)
            imported += 1
        except Exception as e:
            errors.append(f"{email}: {str(e)}")
    
    await db.commit()
    
    return {
        "imported": imported,
        "errors": errors
    }

@router.put("/{email_id}/toggle")
async def toggle_email(email_id: int, db: AsyncSession = Depends(get_db)):
    """Active/désactive un compte"""
    result = await db.execute(select(EmailAccount).where(EmailAccount.id == email_id))
    account = result.scalar_one_or_none()
    
    if not account:
        raise HTTPException(status_code=404, detail="Email not found")
    
    account.is_active = not account.is_active
    await db.commit()
    
    return {"is_active": account.is_active}

@router.post("/{email_id}/increment")
async def increment_sent(email_id: int, db: AsyncSession = Depends(get_db)):
    """Incrémente le compteur d'envois"""
    await db.execute(
        update(EmailAccount)
        .where(EmailAccount.id == email_id)
        .values(
            sent_today=EmailAccount.sent_today + 1,
            last_used=datetime.utcnow()
        )
    )
    await db.commit()
    return {"success": True}

@router.post("/reset-quotas")
async def reset_quotas(db: AsyncSession = Depends(get_db)):
    """Reset tous les quotas (à appeler à minuit)"""
    await db.execute(update(EmailAccount).values(sent_today=0))
    await db.commit()
    return {"reset": True}

@router.delete("/{email_id}")
async def delete_email(email_id: int, db: AsyncSession = Depends(get_db)):
    """Supprime un compte email"""
    result = await db.execute(select(EmailAccount).where(EmailAccount.id == email_id))
    account = result.scalar_one_or_none()
    
    if not account:
        raise HTTPException(status_code=404, detail="Email not found")
    
    await db.delete(account)
    await db.commit()
    
    return {"deleted": True}

