# =============================================================================
# API PROSPECTS - CRUD et gestion des prospects
# =============================================================================

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update, delete
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime
import uuid
import asyncio

from app.core.database import get_db, Prospect, async_session
from app.core.logger import logger
from app.services.enrichment import run_quality_pipeline_task

router = APIRouter()

# =============================================================================
# SCHEMAS
# =============================================================================

class ProspectCreate(BaseModel):
    nom: str
    prenom: Optional[str] = ""
    telephone: Optional[str] = ""
    email: Optional[str] = ""
    adresse: Optional[str] = ""
    code_postal: Optional[str] = ""
    ville: Optional[str] = ""
    canton: Optional[str] = "GE"
    type_bien: Optional[str] = ""
    surface: Optional[float] = 0
    prix: Optional[float] = 0
    source: Optional[str] = ""
    notes: Optional[str] = ""
    tags: Optional[List[str]] = []

class ProspectUpdate(BaseModel):
    nom: Optional[str] = None
    prenom: Optional[str] = None
    telephone: Optional[str] = None
    email: Optional[str] = None
    adresse: Optional[str] = None
    statut: Optional[str] = None
    score: Optional[int] = None
    notes: Optional[str] = None
    tags: Optional[List[str]] = None

class ProspectResponse(BaseModel):
    id: str
    nom: Optional[str]
    prenom: Optional[str]
    telephone: Optional[str]
    email: Optional[str]
    adresse: Optional[str]
    code_postal: Optional[str]
    ville: Optional[str]
    canton: Optional[str]
    lien_rf: Optional[str] = None
    type_bien: Optional[str]
    surface: Optional[float]
    prix: Optional[float]
    score: int
    quality_score: Optional[int] = None
    quality_flags: Optional[dict] = None
    enrichment_status: Optional[str] = None
    is_duplicate: Optional[bool] = None
    merged_into_id: Optional[str] = None
    do_not_contact: Optional[bool] = None
    do_not_contact_reason: Optional[str] = None
    consent_status: Optional[str] = None
    consent_updated_at: Optional[datetime] = None
    statut: str
    source: Optional[str]
    notes: Optional[str]
    tags: Optional[List[str]]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

# =============================================================================
# SAISIE RAPIDE (SPEED ENTRY)
# =============================================================================

class ManualEnrichRequest(BaseModel):
    nom: str
    prenom: Optional[str] = ""
    adresse: Optional[str] = ""
    notes: Optional[str] = ""


class DoNotContactRequest(BaseModel):
    do_not_contact: bool = True
    reason: Optional[str] = None
    consent_status: Optional[str] = None  # consented, optout, legitimate_interest, unknown

async def enrich_prospect_task(prospect_id: str, db_session_factory):
    """Compat: délègue au pipeline qualité (normalisation/enrichissement/dédup/scoring)."""
    await run_quality_pipeline_task(prospect_id, db_session_factory=db_session_factory)

@router.get("/incomplete", response_model=List[ProspectResponse])
async def get_incomplete_prospects(
    limit: int = 50,
    db: AsyncSession = Depends(get_db)
):
    """
    Récupère les prospects incomplets (lien RF présent mais pas de nom)
    """
    query = select(Prospect).where(
        (Prospect.nom == "") | (Prospect.nom == None)
    ).limit(limit)
    
    result = await db.execute(query)
    return result.scalars().all()

@router.post("/{prospect_id}/enrich-manual", response_model=ProspectResponse)
async def manual_enrich_prospect(
    prospect_id: str,
    data: ManualEnrichRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """
    Sauvegarde les infos manuelles et lance l'enrichissement
    """
    result = await db.execute(select(Prospect).where(Prospect.id == prospect_id))
    prospect = result.scalar_one_or_none()
    
    if not prospect:
        raise HTTPException(status_code=404, detail="Prospect not found")
    
    # Mise à jour
    prospect.nom = data.nom
    prospect.prenom = data.prenom
    if data.adresse:
        prospect.adresse = data.adresse
    
    prospect.updated_at = datetime.utcnow()
    await db.commit()
    await db.refresh(prospect)
    
    # Lancer le pipeline qualité
    background_tasks.add_task(run_quality_pipeline_task, prospect_id, async_session)
    
    return prospect

# =============================================================================
# ROUTES CRUD STANDARD
# =============================================================================

@router.get("/", response_model=List[ProspectResponse])
async def list_prospects(
    skip: int = 0,
    limit: int = 100,
    statut: Optional[str] = None,
    ville: Optional[str] = None,
    search: Optional[str] = None,
    include_merged: bool = False,
    sort_by: str = "created_at",
    sort_order: str = "desc",
    db: AsyncSession = Depends(get_db)
):
    """Liste les prospects avec filtres et pagination"""
    query = select(Prospect)

    # Par défaut, on masque les doublons déjà fusionnés
    if not include_merged:
        query = query.where(Prospect.merged_into_id.is_(None))
    
    if statut:
        query = query.where(Prospect.statut == statut)
    if ville:
        query = query.where(Prospect.ville.ilike(f"%{ville}%"))
    if search:
        query = query.where(
            (Prospect.nom.ilike(f"%{search}%")) |
            (Prospect.prenom.ilike(f"%{search}%")) |
            (Prospect.adresse.ilike(f"%{search}%"))
        )
    
    # Tri
    order_col = getattr(Prospect, sort_by, Prospect.created_at)
    if sort_order == "desc":
        query = query.order_by(order_col.desc())
    else:
        query = query.order_by(order_col.asc())
    
    query = query.offset(skip).limit(limit)
    
    result = await db.execute(query)
    return result.scalars().all()

@router.get("/count")
async def count_prospects(
    statut: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Compte les prospects"""
    query = select(func.count(Prospect.id))
    if statut:
        query = query.where(Prospect.statut == statut)
    
    result = await db.execute(query)
    return {"count": result.scalar()}

@router.get("/pipeline")
async def get_pipeline(db: AsyncSession = Depends(get_db)):
    """Retourne le pipeline (compte par statut)"""
    query = (
        select(Prospect.statut, func.count(Prospect.id))
        .where(Prospect.merged_into_id.is_(None))
        .group_by(Prospect.statut)
    )
    result = await db.execute(query)
    
    pipeline = {}
    for row in result.all():
        pipeline[row[0] or "nouveau"] = row[1]
    
    return pipeline

@router.get("/{prospect_id}", response_model=ProspectResponse)
async def get_prospect(prospect_id: str, db: AsyncSession = Depends(get_db)):
    """Récupère un prospect par ID"""
    result = await db.execute(select(Prospect).where(Prospect.id == prospect_id))
    prospect = result.scalar_one_or_none()
    
    if not prospect:
        raise HTTPException(status_code=404, detail="Prospect not found")
    
    return prospect


@router.post("/{prospect_id}/do-not-contact", response_model=ProspectResponse)
async def set_do_not_contact(
    prospect_id: str,
    payload: DoNotContactRequest,
    db: AsyncSession = Depends(get_db),
):
    """Marque un prospect en opt-out / ne pas contacter."""
    result = await db.execute(select(Prospect).where(Prospect.id == prospect_id))
    prospect = result.scalar_one_or_none()

    if not prospect:
        raise HTTPException(status_code=404, detail="Prospect not found")

    prospect.do_not_contact = payload.do_not_contact
    prospect.do_not_contact_reason = payload.reason
    # Si on met en DNC, on force optout
    if payload.do_not_contact:
        prospect.consent_status = "optout"
    elif payload.consent_status:
        prospect.consent_status = payload.consent_status
    else:
        # revenir à unknown si on retire le DNC sans préciser
        prospect.consent_status = prospect.consent_status or "unknown"

    prospect.consent_updated_at = datetime.utcnow()
    prospect.updated_at = datetime.utcnow()
    await db.commit()
    await db.refresh(prospect)
    return prospect

@router.post("/", response_model=ProspectResponse)
async def create_prospect(
    data: ProspectCreate,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    """Crée un nouveau prospect"""
    prospect = Prospect(
        id=str(uuid.uuid4())[:12],
        **data.model_dump()
    )
    
    db.add(prospect)
    await db.commit()
    await db.refresh(prospect)

    # Pipeline qualité post-création
    background_tasks.add_task(run_quality_pipeline_task, prospect.id, async_session)
    
    return prospect

@router.put("/{prospect_id}", response_model=ProspectResponse)
async def update_prospect(
    prospect_id: str,
    data: ProspectUpdate,
    db: AsyncSession = Depends(get_db)
):
    """Met à jour un prospect"""
    result = await db.execute(select(Prospect).where(Prospect.id == prospect_id))
    prospect = result.scalar_one_or_none()
    
    if not prospect:
        raise HTTPException(status_code=404, detail="Prospect not found")
    
    update_data = data.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(prospect, key, value)
    
    prospect.updated_at = datetime.utcnow()
    await db.commit()
    await db.refresh(prospect)
    
    return prospect

@router.delete("/{prospect_id}")
async def delete_prospect(prospect_id: str, db: AsyncSession = Depends(get_db)):
    """Supprime un prospect"""
    result = await db.execute(delete(Prospect).where(Prospect.id == prospect_id))
    await db.commit()
    
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Prospect not found")
    
    return {"deleted": True}

@router.post("/{prospect_id}/status")
async def change_status(
    prospect_id: str,
    new_status: str = Query(...),
    db: AsyncSession = Depends(get_db)
):
    """Change le statut d'un prospect"""
    result = await db.execute(
        update(Prospect)
        .where(Prospect.id == prospect_id)
        .values(statut=new_status, updated_at=datetime.utcnow())
    )
    await db.commit()
    
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Prospect not found")
    
    return {"success": True, "new_status": new_status}

# =============================================================================
# INTERACTIONS (HISTORIQUE DES CONTACTS)
# =============================================================================

from app.core.database import InteractionLog

class InteractionCreate(BaseModel):
    type: str  # appel, email, rdv, note
    notes: str

class InteractionResponse(BaseModel):
    id: int
    prospect_id: str
    type: str
    notes: Optional[str]
    created_at: datetime
    
    class Config:
        from_attributes = True

@router.get("/{prospect_id}/interactions", response_model=List[InteractionResponse])
async def get_interactions(
    prospect_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Recupere l'historique des interactions avec un prospect"""
    result = await db.execute(
        select(InteractionLog)
        .where(InteractionLog.prospect_id == prospect_id)
        .order_by(InteractionLog.created_at.desc())
    )
    return result.scalars().all()

@router.post("/{prospect_id}/interactions", response_model=InteractionResponse)
async def add_interaction(
    prospect_id: str,
    data: InteractionCreate,
    db: AsyncSession = Depends(get_db)
):
    """Ajoute une interaction (appel, email, rdv, note) a un prospect"""
    # Verifier que le prospect existe
    result = await db.execute(select(Prospect).where(Prospect.id == prospect_id))
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Prospect not found")
    
    interaction = InteractionLog(
        prospect_id=prospect_id,
        type=data.type,
        notes=data.notes
    )
    db.add(interaction)
    await db.commit()
    await db.refresh(interaction)
    
    return interaction

# =============================================================================
# RAPPELS
# =============================================================================

@router.get("/rappels/today", response_model=List[ProspectResponse])
async def get_today_rappels(db: AsyncSession = Depends(get_db)):
    """Recupere les prospects avec un rappel prevu aujourd'hui"""
    from datetime import date
    today = date.today()
    
    query = select(Prospect).where(
        func.date(Prospect.rappel_date) == today
    ).order_by(Prospect.rappel_date.asc())
    
    result = await db.execute(query)
    return result.scalars().all()

@router.post("/{prospect_id}/rappel")
async def set_rappel(
    prospect_id: str,
    rappel_date: str = Query(..., description="Date au format YYYY-MM-DD ou YYYY-MM-DDTHH:MM"),
    db: AsyncSession = Depends(get_db)
):
    """Definit une date de rappel pour un prospect"""
    from datetime import datetime as dt
    
    # Parser la date
    try:
        if "T" in rappel_date:
            parsed_date = dt.fromisoformat(rappel_date)
        else:
            parsed_date = dt.strptime(rappel_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Format de date invalide. Utilisez YYYY-MM-DD")
    
    result = await db.execute(
        update(Prospect)
        .where(Prospect.id == prospect_id)
        .values(rappel_date=parsed_date, updated_at=datetime.utcnow())
    )
    await db.commit()
    
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Prospect not found")
    
    return {"success": True, "rappel_date": parsed_date.isoformat()}

# =============================================================================
# IMPORT CSV
# =============================================================================

from fastapi import File, UploadFile
import csv
import io

@router.post("/import-csv")
async def import_csv(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db)
):
    """
    Importe des prospects depuis un fichier CSV.
    Colonnes attendues : nom, prenom, telephone, email, adresse, code_postal, ville
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Le fichier doit etre au format CSV")
    
    content = await file.read()
    
    # Decoder le contenu
    try:
        decoded = content.decode('utf-8')
    except UnicodeDecodeError:
        decoded = content.decode('latin-1')
    
    reader = csv.DictReader(io.StringIO(decoded), delimiter=';')
    
    # Normaliser les noms de colonnes (enlever espaces, minuscules)
    if reader.fieldnames:
        reader.fieldnames = [f.strip().lower().replace(' ', '_') for f in reader.fieldnames]
    
    added = 0
    errors = []
    
    for i, row in enumerate(reader):
        try:
            # Mapper les colonnes
            nom = row.get('nom', '') or row.get('name', '') or ''
            prenom = row.get('prenom', '') or row.get('firstname', '') or ''
            telephone = row.get('telephone', '') or row.get('phone', '') or row.get('tel', '') or ''
            email = row.get('email', '') or row.get('mail', '') or ''
            adresse = row.get('adresse', '') or row.get('address', '') or row.get('rue', '') or ''
            code_postal = row.get('code_postal', '') or row.get('cp', '') or row.get('zip', '') or ''
            ville = row.get('ville', '') or row.get('city', '') or ''
            
            if not nom:
                errors.append(f"Ligne {i+2}: Nom manquant")
                continue
            
            prospect = Prospect(
                id=str(uuid.uuid4())[:12],
                nom=nom.strip(),
                prenom=prenom.strip(),
                telephone=telephone.strip(),
                email=email.strip(),
                adresse=adresse.strip(),
                code_postal=str(code_postal).strip(),
                ville=ville.strip(),
                source="Import CSV",
                statut="nouveau"
            )
            db.add(prospect)
            added += 1
            
        except Exception as e:
            errors.append(f"Ligne {i+2}: {str(e)}")
    
    await db.commit()
    
    return {
        "success": True,
        "added": added,
        "errors": errors[:10] if errors else [],
        "total_errors": len(errors)
    }