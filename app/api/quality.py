# =============================================================================
# API QUALITY - Qualité des prospects (post-import)
# =============================================================================

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import and_, case, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from app.core.database import (
    Prospect,
    ProspectDuplicateCandidate,
    ProspectMergeLog,
    async_session,
    get_db,
)
from app.services.enrichment import compute_quality, merge_into, run_quality_pipeline_task

router = APIRouter()


# =============================================================================
# SCHEMAS
# =============================================================================


class QualitySummaryResponse(BaseModel):
    total: int
    avg_quality_score: float
    with_phone: int
    with_email: int
    contactable: int
    do_not_contact: int
    consent_status: Dict[str, int]
    duplicates_flagged: int
    duplicates_merged: int
    duplicate_candidates_pending: int
    enrichment_status: Dict[str, int]
    by_source: List[Dict[str, Any]] = []


class ProspectMini(BaseModel):
    id: str
    nom: Optional[str] = None
    prenom: Optional[str] = None
    ville: Optional[str] = None
    telephone: Optional[str] = None
    email: Optional[str] = None
    quality_score: Optional[int] = None
    enrichment_status: Optional[str] = None
    merged_into_id: Optional[str] = None

    class Config:
        from_attributes = True


class DuplicateCandidateItem(BaseModel):
    id: int
    prospect_id: str
    candidate_id: str
    reason: Optional[str] = None
    confidence: float
    status: str
    created_at: datetime
    prospect: ProspectMini
    candidate: ProspectMini


class MergeRequest(BaseModel):
    source_id: str
    target_id: str
    reason: Optional[str] = "manual_merge"


class MergeResponse(BaseModel):
    success: bool
    source_id: str
    target_id: str
    merged_fields: Dict[str, Any]


# =============================================================================
# ROUTES
# =============================================================================


@router.get("/summary", response_model=QualitySummaryResponse)
async def quality_summary(db: AsyncSession = Depends(get_db)):
    """KPIs qualité globaux."""

    total = (await db.execute(select(func.count(Prospect.id)))).scalar() or 0

    avg_q = (await db.execute(select(func.avg(Prospect.quality_score)))).scalar()
    avg_q = float(avg_q or 0.0)

    with_phone = (
        (await db.execute(select(func.count(Prospect.id)).where(Prospect.telephone_norm != None).where(Prospect.telephone_norm != "")))
    ).scalar() or 0
    with_email = (
        (await db.execute(select(func.count(Prospect.id)).where(Prospect.email_norm != None).where(Prospect.email_norm != "")))
    ).scalar() or 0

    # Opt-out / DNC
    do_not_contact = (
        (await db.execute(select(func.count(Prospect.id)).where(Prospect.do_not_contact == True)))
    ).scalar() or 0

    # Répartition consentement
    consent_rows = (await db.execute(select(Prospect.consent_status, func.count(Prospect.id)).group_by(Prospect.consent_status))).all()
    consent_status: Dict[str, int] = {}
    for cs, cnt in consent_rows:
        consent_status[str(cs or "unknown")] = int(cnt)

    # Contactables = joignables (tel/email) ET pas DNC
    contactable = (
        (
            await db.execute(
                select(func.count(Prospect.id)).where(
                    or_(Prospect.do_not_contact.is_(None), Prospect.do_not_contact == False)
                ).where(
                    or_(
                        and_(Prospect.telephone_norm != None, Prospect.telephone_norm != ""),
                        and_(Prospect.email_norm != None, Prospect.email_norm != ""),
                    )
                )
            )
        ).scalar()
        or 0
    )

    duplicates_flagged = (
        (await db.execute(select(func.count(Prospect.id)).where(Prospect.is_duplicate == True)))
    ).scalar() or 0
    duplicates_merged = (
        (await db.execute(select(func.count(Prospect.id)).where(Prospect.merged_into_id != None).where(Prospect.merged_into_id != "")))
    ).scalar() or 0

    duplicate_candidates_pending = (
        (await db.execute(select(func.count(ProspectDuplicateCandidate.id)).where(ProspectDuplicateCandidate.status == "pending")))
    ).scalar() or 0

    # Enrichment status distribution
    rows = (await db.execute(select(Prospect.enrichment_status, func.count(Prospect.id)).group_by(Prospect.enrichment_status))).all()
    enrichment_status: Dict[str, int] = {}
    for status, cnt in rows:
        enrichment_status[str(status or "unknown")] = int(cnt)

    # Top sources (qualité / joignabilité)
    by_source_rows = await db.execute(
        select(
            Prospect.source,
            func.count(Prospect.id).label("total"),
            func.avg(Prospect.quality_score).label("avg_quality"),
            func.sum(
                case((and_(Prospect.telephone_norm != None, Prospect.telephone_norm != ""), 1), else_=0)
            ).label("with_phone"),
            func.sum(case((and_(Prospect.email_norm != None, Prospect.email_norm != ""), 1), else_=0)).label("with_email"),
            func.sum(case((Prospect.enrichment_status == "ok", 1), else_=0)).label("enrich_ok"),
        )
        .group_by(Prospect.source)
        .order_by(func.count(Prospect.id).desc())
        .limit(10)
    )
    by_source: List[Dict[str, Any]] = []
    for row in by_source_rows.all():
        source = row[0] or "unknown"
        by_source.append(
            {
                "source": source,
                "total": int(row[1] or 0),
                "avg_quality_score": round(float(row[2] or 0), 2),
                "with_phone": int(row[3] or 0),
                "with_email": int(row[4] or 0),
                "enrich_ok": int(row[5] or 0),
            }
        )

    return QualitySummaryResponse(
        total=int(total),
        avg_quality_score=round(avg_q, 2),
        with_phone=int(with_phone),
        with_email=int(with_email),
        contactable=int(contactable),
        do_not_contact=int(do_not_contact),
        consent_status=consent_status,
        duplicates_flagged=int(duplicates_flagged),
        duplicates_merged=int(duplicates_merged),
        duplicate_candidates_pending=int(duplicate_candidates_pending),
        enrichment_status=enrichment_status,
        by_source=by_source,
    )


@router.get("/duplicates", response_model=List[DuplicateCandidateItem])
async def list_duplicate_candidates(
    status: str = Query("pending"),
    min_confidence: float = Query(0.7, ge=0.0, le=1.0),
    limit: int = Query(100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    """Retourne les suggestions de doublons (match partiel)."""

    P1 = aliased(Prospect)
    P2 = aliased(Prospect)

    q = (
        select(ProspectDuplicateCandidate, P1, P2)
        .join(P1, P1.id == ProspectDuplicateCandidate.prospect_id)
        .join(P2, P2.id == ProspectDuplicateCandidate.candidate_id)
        .where(ProspectDuplicateCandidate.status == status)
        .where(ProspectDuplicateCandidate.confidence >= min_confidence)
        .order_by(ProspectDuplicateCandidate.confidence.desc())
        .limit(limit)
    )

    rows = (await db.execute(q)).all()

    out: List[DuplicateCandidateItem] = []
    for cand, p, c in rows:
        out.append(
            DuplicateCandidateItem(
                id=cand.id,
                prospect_id=cand.prospect_id,
                candidate_id=cand.candidate_id,
                reason=cand.reason,
                confidence=float(cand.confidence or 0.0),
                status=cand.status,
                created_at=cand.created_at,
                prospect=ProspectMini.model_validate(p),
                candidate=ProspectMini.model_validate(c),
            )
        )

    return out


@router.post("/merge", response_model=MergeResponse)
async def merge_prospects(req: MergeRequest, db: AsyncSession = Depends(get_db)):
    """Fusion manuelle (hybride): fusionner source -> target."""

    if req.source_id == req.target_id:
        raise HTTPException(status_code=400, detail="source_id et target_id doivent être différents")

    source = (
        (await db.execute(select(Prospect).where(Prospect.id == req.source_id))).scalars().first()
    )
    target = (
        (await db.execute(select(Prospect).where(Prospect.id == req.target_id))).scalars().first()
    )

    if not source or not target:
        raise HTTPException(status_code=404, detail="Prospect introuvable")

    if source.merged_into_id:
        raise HTTPException(status_code=409, detail=f"Le prospect source est déjà fusionné vers {source.merged_into_id}")

    merged_fields = merge_into(source=source, target=target)

    # Recompute quality on target
    q_score, q_flags = compute_quality(target)
    target.quality_score = q_score
    target.quality_flags = q_flags
    target.updated_at = datetime.utcnow()

    db.add(
        ProspectMergeLog(
            source_id=source.id,
            target_id=target.id,
            reason=req.reason or "manual_merge",
            merged_fields=merged_fields,
        )
    )

    # Marquer les suggestions source<->target comme merged
    await db.execute(
        update(ProspectDuplicateCandidate)
        .where(
            or_(
                and_(
                    ProspectDuplicateCandidate.prospect_id == source.id,
                    ProspectDuplicateCandidate.candidate_id == target.id,
                ),
                and_(
                    ProspectDuplicateCandidate.prospect_id == target.id,
                    ProspectDuplicateCandidate.candidate_id == source.id,
                ),
            )
        )
        .values(status="merged")
    )

    await db.commit()

    return MergeResponse(success=True, source_id=source.id, target_id=target.id, merged_fields=merged_fields)


@router.post("/recheck/{prospect_id}")
async def recheck_prospect(
    prospect_id: str,
    background_tasks: BackgroundTasks,
):
    """Relance le pipeline qualité pour un prospect."""

    background_tasks.add_task(run_quality_pipeline_task, prospect_id, async_session)
    return {"queued": True, "prospect_id": prospect_id}

