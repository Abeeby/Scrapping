# =============================================================================
# API STATS - Statistiques et KPIs
# =============================================================================

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from datetime import datetime, timedelta
import random
import uuid

from app.core.database import get_db, Prospect, EmailAccount, Bot, Campaign, Activity, Proxy, ProspectDuplicateCandidate

router = APIRouter()

# =============================================================================
# ROUTES
# =============================================================================

@router.get("/dashboard")
async def get_dashboard_stats(db: AsyncSession = Depends(get_db)):
    """Statistiques pour le dashboard"""
    
    # Prospects
    prospects_total = await db.execute(select(func.count(Prospect.id)))
    
    week_ago = datetime.utcnow() - timedelta(days=7)
    prospects_week = await db.execute(
        select(func.count(Prospect.id))
        .where(Prospect.created_at >= week_ago)
    )
    
    # Emails
    emails_result = await db.execute(select(EmailAccount))
    emails = emails_result.scalars().all()
    
    total_sent = sum(e.sent_today for e in emails)
    total_quota = sum(e.quota_daily for e in emails if e.is_active)
    
    # Bots
    bots_result = await db.execute(select(Bot))
    bots = bots_result.scalars().all()
    
    bots_running = len([b for b in bots if b.status == "running"])
    
    # Campagnes
    campaigns_result = await db.execute(
        select(Campaign).where(Campaign.status == "running")
    )
    campaigns_active = len(campaigns_result.scalars().all())
    
    # Score moyen
    avg_score = await db.execute(select(func.avg(Prospect.score)))

    # Qualité (post-import)
    avg_quality = await db.execute(select(func.avg(Prospect.quality_score)))
    with_phone = await db.execute(
        select(func.count(Prospect.id)).where(Prospect.telephone_norm != None).where(Prospect.telephone_norm != "")
    )
    with_email = await db.execute(
        select(func.count(Prospect.id)).where(Prospect.email_norm != None).where(Prospect.email_norm != "")
    )
    duplicates_merged = await db.execute(
        select(func.count(Prospect.id)).where(Prospect.merged_into_id != None).where(Prospect.merged_into_id != "")
    )
    duplicate_candidates_pending = await db.execute(
        select(func.count(ProspectDuplicateCandidate.id)).where(ProspectDuplicateCandidate.status == "pending")
    )
    enrichment_rows = await db.execute(
        select(Prospect.enrichment_status, func.count(Prospect.id)).group_by(Prospect.enrichment_status)
    )
    enrichment_status = {str(row[0] or "unknown"): row[1] for row in enrichment_rows.all()}
    
    return {
        "prospects": {
            "total": prospects_total.scalar(),
            "this_week": prospects_week.scalar(),
            "trend": "+12%"  # TODO: calculer vraiment
        },
        "emails": {
            "accounts": len(emails),
            "active": len([e for e in emails if e.is_active]),
            "sent_today": total_sent,
            "quota_remaining": total_quota - total_sent
        },
        "bots": {
            "total": len(bots),
            "running": bots_running,
            "success_rate": round(
                sum(b.success_count for b in bots) /
                max(sum(b.success_count + b.error_count for b in bots), 1) * 100, 1
            )
        },
        "campaigns": {
            "active": campaigns_active
        },
        "score_moyen": round(avg_score.scalar() or 0, 1)
        ,
        "quality": {
            "avg_quality_score": round(float(avg_quality.scalar() or 0), 2),
            "with_phone": with_phone.scalar() or 0,
            "with_email": with_email.scalar() or 0,
            "duplicates_merged": duplicates_merged.scalar() or 0,
            "duplicate_candidates_pending": duplicate_candidates_pending.scalar() or 0,
            "enrichment_status": enrichment_status,
        }
    }

@router.get("/prospects/by-day")
async def get_prospects_by_day(
    days: int = 30,
    db: AsyncSession = Depends(get_db)
):
    """Prospects par jour sur les N derniers jours"""
    start_date = datetime.utcnow() - timedelta(days=days)
    
    result = await db.execute(
        select(
            func.date(Prospect.created_at).label('date'),
            func.count(Prospect.id).label('count')
        )
        .where(Prospect.created_at >= start_date)
        .group_by(func.date(Prospect.created_at))
        .order_by(func.date(Prospect.created_at))
    )
    
    data = {}
    for row in result.all():
        data[str(row.date)] = row.count
    
    # Remplir les jours manquants
    labels = []
    values = []
    for i in range(days, -1, -1):
        date = (datetime.utcnow() - timedelta(days=i)).strftime("%Y-%m-%d")
        labels.append(date)
        values.append(data.get(date, 0))
    
    return {"labels": labels, "values": values}

@router.get("/prospects/by-city")
async def get_prospects_by_city(
    limit: int = 10,
    db: AsyncSession = Depends(get_db)
):
    """Répartition des prospects par ville"""
    result = await db.execute(
        select(
            Prospect.ville,
            func.count(Prospect.id).label('count')
        )
        .where(Prospect.ville != None)
        .where(Prospect.ville != '')
        .group_by(Prospect.ville)
        .order_by(func.count(Prospect.id).desc())
        .limit(limit)
    )
    
    labels = []
    values = []
    for row in result.all():
        labels.append(row.ville)
        values.append(row.count)
    
    return {"labels": labels, "values": values}

@router.get("/prospects/by-status")
async def get_prospects_by_status(db: AsyncSession = Depends(get_db)):
    """Répartition des prospects par statut (pipeline)"""
    result = await db.execute(
        select(
            Prospect.statut,
            func.count(Prospect.id).label('count')
        )
        .group_by(Prospect.statut)
    )
    
    return dict(result.all())

@router.get("/prospects/by-source")
async def get_prospects_by_source(db: AsyncSession = Depends(get_db)):
    """Répartition des prospects par source"""
    result = await db.execute(
        select(
            Prospect.source,
            func.count(Prospect.id).label('count')
        )
        .where(Prospect.source != None)
        .where(Prospect.source != '')
        .group_by(Prospect.source)
        .order_by(func.count(Prospect.id).desc())
    )
    
    return dict(result.all())

@router.get("/conversion")
async def get_conversion_stats(db: AsyncSession = Depends(get_db)):
    """
    Statistiques de conversion du pipeline.
    Calcule les taux de passage entre chaque etape.
    """
    # Ordre du pipeline
    pipeline_order = [
        "nouveau",
        "a_contacter", 
        "contacte",
        "interesse",
        "rdv",
        "mandat_signe"
    ]
    
    # Compter les prospects par statut
    result = await db.execute(
        select(
            Prospect.statut,
            func.count(Prospect.id).label('count')
        )
        .group_by(Prospect.statut)
    )
    
    counts = {row[0] or "nouveau": row[1] for row in result.all()}
    
    # Calculer les totaux cumules (tous les prospects qui sont passes par chaque etape)
    # Un prospect "interesse" est aussi passe par "contacte", "a_contacter", etc.
    cumulative = {}
    running_total = 0
    
    for status in reversed(pipeline_order):
        running_total += counts.get(status, 0)
        cumulative[status] = running_total
    
    # Calculer les taux de conversion
    conversions = []
    total_prospects = cumulative.get("nouveau", 0)
    
    for i, status in enumerate(pipeline_order):
        count = counts.get(status, 0)
        cumul = cumulative.get(status, 0)
        
        # Taux par rapport au total
        rate_from_total = round((cumul / max(total_prospects, 1)) * 100, 1)
        
        # Taux de conversion vers l'etape suivante
        if i < len(pipeline_order) - 1:
            next_status = pipeline_order[i + 1]
            next_cumul = cumulative.get(next_status, 0)
            conversion_rate = round((next_cumul / max(cumul, 1)) * 100, 1)
        else:
            conversion_rate = 100.0  # Derniere etape
        
        conversions.append({
            "status": status,
            "count": count,
            "cumulative": cumul,
            "rate_from_total": rate_from_total,
            "conversion_to_next": conversion_rate
        })
    
    # Calculer le taux global de conversion (nouveau -> mandat_signe)
    mandats = counts.get("mandat_signe", 0)
    global_conversion = round((mandats / max(total_prospects, 1)) * 100, 2)
    
    return {
        "total_prospects": total_prospects,
        "mandats_signes": mandats,
        "taux_conversion_global": global_conversion,
        "pipeline": conversions
    }

@router.get("/activity")
async def get_recent_activity(
    limit: int = 20,
    db: AsyncSession = Depends(get_db)
):
    """Activité récente"""
    result = await db.execute(
        select(Activity)
        .order_by(Activity.created_at.desc())
        .limit(limit)
    )
    
    activities = result.scalars().all()
    
    return [
        {
            "id": a.id,
            "type": a.type,
            "message": a.message,
            "details": a.details,
            "timestamp": a.created_at.isoformat()
        }
        for a in activities
    ]

# =============================================================================
# DONNÉES DE DÉMONSTRATION
# =============================================================================

# Données pour la génération
PRENOMS = ["Jean", "Pierre", "Marie", "Anne", "Marc", "Sophie", "Claude", "Isabelle",
           "François", "Catherine", "Nicolas", "Christine", "Thomas", "Martine"]
NOMS = ["Müller", "Meier", "Schmid", "Keller", "Weber", "Huber", "Schneider",
        "Meyer", "Steiner", "Fischer", "Gerber", "Brunner", "Favre", "Dupont"]
RUES = ["Rue du Rhône", "Rue de Lausanne", "Boulevard Carl-Vogt", "Avenue de Champel",
        "Rue de Carouge", "Chemin de Miremont", "Rue des Eaux-Vives"]
VILLES = ["Genève", "Carouge", "Vernier", "Lancy", "Meyrin", "Thônex", "Cologny"]
TYPES_BIENS = ["Villa", "Appartement", "Terrain", "Immeuble", "Maison"]

def generate_phone():
    prefixes = ["022", "079", "078", "077"]
    return f"+41 {random.choice(prefixes)} {random.randint(100, 999)} {random.randint(10, 99)} {random.randint(10, 99)}"

def generate_email(prenom: str, nom: str):
    domains = ["gmail.com", "outlook.com", "bluewin.ch"]
    return f"{prenom.lower()}.{nom.lower()}@{random.choice(domains)}".replace("é", "e").replace("è", "e")

@router.post("/demo-data")
async def load_demo_data(db: AsyncSession = Depends(get_db)):
    """Charge des données de démonstration pour la présentation"""
    
    try:
        counts = {"prospects": 0, "emails": 0, "proxies": 0, "bots": 0, "campaigns": 0}
        
        # =====================================================================
        # PROSPECTS (30)
        # =====================================================================
        for _ in range(30):
            prenom = random.choice(PRENOMS)
            nom = random.choice(NOMS)
            ville = random.choice(VILLES)
            
            prospect = Prospect(
                id=str(uuid.uuid4()),
                nom=nom,
                prenom=prenom,
                telephone=generate_phone(),
                email=generate_email(prenom, nom),
                adresse=f"{random.randint(1, 100)} {random.choice(RUES)}",
                code_postal=str(1200 + random.randint(0, 50)),
                ville=ville,
                canton="GE",
                type_bien=random.choice(TYPES_BIENS),
                surface=random.randint(50, 400),
                prix=random.randint(300000, 3000000),
                score=random.randint(30, 95),
                statut=random.choice(["nouveau", "contacte", "interesse", "negociation"]),
                source=random.choice(["SITG Genève", "Search.ch", "Local.ch", "Comparis.ch"]),
                created_at=datetime.now() - timedelta(days=random.randint(0, 30))
            )
            db.add(prospect)
            counts["prospects"] += 1
        
        # =====================================================================
        # EMAILS (8)
        # =====================================================================
        demo_emails = [
            "prospection.immo1@gmail.com", "prospection.immo2@gmail.com",
            "contact.vente@outlook.com", "agent.immo@bluewin.ch",
            "prospection.ge@gmail.com", "recherche.biens@outlook.com",
            "immo.suisse1@gmail.com", "vendeurs.contact@hotmail.com"
        ]
        
        for email_addr in demo_emails:
            email = EmailAccount(
                id=str(uuid.uuid4()),
                email=email_addr,
                password="demo123456",
                imap_server="imap.gmail.com" if "gmail" in email_addr else "outlook.office365.com",
                smtp_server="smtp.gmail.com" if "gmail" in email_addr else "smtp.office365.com",
                quota_daily=50,
                sent_today=random.randint(0, 25),
                is_active=random.random() > 0.2,
                last_used=datetime.now() - timedelta(hours=random.randint(0, 48))
            )
            db.add(email)
            counts["emails"] += 1
        
        # =====================================================================
        # PROXIES (6)
        # =====================================================================
        demo_proxies = [
            ("ch-proxy1.vpn.com", 8080, "CH"), ("ch-proxy2.vpn.com", 8080, "CH"),
            ("swiss-1.proxy.net", 1080, "CH"), ("geneva.proxy.io", 443, "CH"),
            ("eu-west.proxy.com", 8080, "FR"), ("de-berlin.proxy.net", 8080, "DE")
        ]
        
        for host, port, country in demo_proxies:
            proxy = Proxy(
                id=str(uuid.uuid4()),
                host=host,
                port=port,
                protocol="http",
                country=country,
                is_active=True,
                is_valid=random.random() > 0.3,
                latency_ms=random.randint(50, 300),
                success_rate=random.randint(75, 99)
            )
            db.add(proxy)
            counts["proxies"] += 1
        
        # =====================================================================
        # BOTS (4)
        # =====================================================================
        demo_bots = [
            ("Bot Comparis GE", "comparis"), ("Bot ImmoScout", "immoscout"),
            ("Bot Homegate", "homegate"), ("Bot Newhome", "newhome")
        ]
        
        for name, bot_type in demo_bots:
            bot = Bot(
                id=str(uuid.uuid4()),
                name=name,
                type=bot_type,
                status=random.choice(["idle", "running", "paused"]),
                requests_count=random.randint(100, 500),
                success_count=random.randint(80, 450),
                error_count=random.randint(10, 50),
                last_run=datetime.now() - timedelta(hours=random.randint(0, 24)),
                config={"target_city": "Genève", "target_radius": 20}
            )
            db.add(bot)
            counts["bots"] += 1
        
        # =====================================================================
        # CAMPAIGNS (3)
        # =====================================================================
        demo_campaigns = [
            ("Campagne Genève Q4", "comparis", "Genève"),
            ("Campagne Lausanne", "immoscout", "Lausanne"),
            ("Campagne Premium Cologny", "homegate", "Cologny")
        ]
        
        for name, portal, city in demo_campaigns:
            campaign = Campaign(
                id=str(uuid.uuid4()),
                name=name,
                type="brochure",
                target_portal=portal,
                target_city=city,
                target_radius=15,
                status=random.choice(["pending", "active", "completed"]),
                total_targets=random.randint(50, 150),
                sent_count=random.randint(20, 100),
                response_count=random.randint(5, 30),
                created_at=datetime.now() - timedelta(days=random.randint(1, 14))
            )
            db.add(campaign)
            counts["campaigns"] += 1
        
        # Commit
        await db.commit()
        
        return {
            "status": "success",
            "message": "Données de démonstration chargées !",
            "counts": counts
        }
        
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur: {str(e)}")

