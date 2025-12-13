# =============================================================================
# API EXPORT - Export Excel des données
# =============================================================================

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime
import pandas as pd
import io

from app.core.database import get_db, Prospect, EmailAccount, Bot, Campaign, Proxy

router = APIRouter()

# =============================================================================
# HELPERS
# =============================================================================

def style_excel(writer, df, sheet_name):
    """Applique un style professionnel au fichier Excel"""
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]
    
    # Format header
    header_format = workbook.add_format({
        'bold': True,
        'bg_color': '#1e3a8a',
        'font_color': 'white',
        'border': 1,
        'align': 'center',
        'valign': 'vcenter'
    })
    
    # Format data
    data_format = workbook.add_format({
        'border': 1,
        'align': 'left',
        'valign': 'vcenter'
    })
    
    # Appliquer formats
    for col_num, value in enumerate(df.columns.values):
        worksheet.write(0, col_num, value, header_format)
        worksheet.set_column(col_num, col_num, 15)
    
    # Ajuster largeurs colonnes
    for i, col in enumerate(df.columns):
        max_len = max(df[col].astype(str).apply(len).max(), len(col)) + 2
        worksheet.set_column(i, i, min(max_len, 50))

# =============================================================================
# ROUTES - PROSPECTS
# =============================================================================

@router.get("/prospects")
async def export_prospects(
    format: str = Query("xlsx", enum=["xlsx", "csv"]),
    status: Optional[str] = None,
    ville: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Exporte les prospects en Excel ou CSV"""
    
    # Récupérer les données
    query = select(Prospect)
    if status:
        query = query.where(Prospect.statut == status)
    if ville:
        query = query.where(Prospect.ville == ville)
    query = query.order_by(Prospect.created_at.desc())
    
    result = await db.execute(query)
    prospects = result.scalars().all()
    
    # Convertir en DataFrame
    data = []
    for p in prospects:
        data.append({
            "Nom": p.nom,
            "Prénom": p.prenom or "",
            "Téléphone": p.telephone or "",
            "Email": p.email or "",
            "Adresse": p.adresse or "",
            "Code Postal": p.code_postal or "",
            "Ville": p.ville or "",
            "Canton": p.canton or "",
            "Type de Bien": p.type_bien or "",
            "Surface (m²)": p.surface or 0,
            "Prix (CHF)": p.prix or 0,
            "Score": p.score,
            "Statut": p.statut,
            "Source": p.source or "",
            "Notes": p.notes or "",
            "Date Ajout": p.created_at.strftime("%d/%m/%Y %H:%M") if p.created_at else ""
        })
    
    df = pd.DataFrame(data)
    
    # Générer le fichier
    if format == "csv":
        output = io.StringIO()
        df.to_csv(output, index=False, encoding='utf-8-sig')
        output.seek(0)
        
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=prospects_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
            }
        )
    else:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Prospects', index=False)
            style_excel(writer, df, 'Prospects')
        output.seek(0)
        
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename=prospects_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
            }
        )

# =============================================================================
# ROUTES - EMAILS
# =============================================================================

@router.get("/emails")
async def export_emails(
    format: str = Query("xlsx", enum=["xlsx", "csv"]),
    db: AsyncSession = Depends(get_db)
):
    """Exporte les comptes email"""
    
    result = await db.execute(select(EmailAccount))
    emails = result.scalars().all()
    
    data = []
    for e in emails:
        data.append({
            "Email": e.email,
            "Serveur IMAP": e.imap_server or "",
            "Serveur SMTP": e.smtp_server or "",
            "Quota Journalier": e.quota_daily,
            "Envoyés Aujourd'hui": e.sent_today,
            "Actif": "Oui" if e.is_active else "Non",
            "Dernière Utilisation": e.last_used.strftime("%d/%m/%Y %H:%M") if e.last_used else "",
            "Erreurs": e.error_count
        })
    
    df = pd.DataFrame(data)
    
    if format == "csv":
        output = io.StringIO()
        df.to_csv(output, index=False, encoding='utf-8-sig')
        output.seek(0)
        
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=emails_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
            }
        )
    else:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Emails', index=False)
            style_excel(writer, df, 'Emails')
        output.seek(0)
        
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename=emails_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
            }
        )

# =============================================================================
# ROUTES - PROXIES
# =============================================================================

@router.get("/proxies")
async def export_proxies(
    format: str = Query("xlsx", enum=["xlsx", "csv"]),
    db: AsyncSession = Depends(get_db)
):
    """Exporte les proxies"""
    
    result = await db.execute(select(Proxy))
    proxies = result.scalars().all()
    
    data = []
    for p in proxies:
        data.append({
            "Host": p.host,
            "Port": p.port,
            "Protocole": p.protocol,
            "Pays": p.country,
            "Actif": "Oui" if p.is_active else "Non",
            "Valide": "Oui" if p.is_valid else "Non",
            "Latence (ms)": p.latency_ms or "-",
            "Taux Succès": f"{p.success_rate}%"
        })
    
    df = pd.DataFrame(data)
    
    if format == "csv":
        output = io.StringIO()
        df.to_csv(output, index=False, encoding='utf-8-sig')
        output.seek(0)
        
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=proxies_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
            }
        )
    else:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Proxies', index=False)
            style_excel(writer, df, 'Proxies')
        output.seek(0)
        
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename=proxies_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
            }
        )

# =============================================================================
# ROUTES - BOTS
# =============================================================================

@router.get("/bots")
async def export_bots(
    format: str = Query("xlsx", enum=["xlsx", "csv"]),
    db: AsyncSession = Depends(get_db)
):
    """Exporte les statistiques des bots"""
    
    result = await db.execute(select(Bot))
    bots = result.scalars().all()
    
    data = []
    for b in bots:
        data.append({
            "Nom": b.name,
            "Type": b.type,
            "Statut": b.status,
            "Requêtes": b.requests_count,
            "Succès": b.success_count,
            "Erreurs": b.error_count,
            "Taux Succès": f"{round(b.success_count / max(b.success_count + b.error_count, 1) * 100, 1)}%",
            "Dernière Exécution": b.last_run.strftime("%d/%m/%Y %H:%M") if b.last_run else ""
        })
    
    df = pd.DataFrame(data)
    
    if format == "csv":
        output = io.StringIO()
        df.to_csv(output, index=False, encoding='utf-8-sig')
        output.seek(0)
        
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=bots_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
            }
        )
    else:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Bots', index=False)
            style_excel(writer, df, 'Bots')
        output.seek(0)
        
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename=bots_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
            }
        )

# =============================================================================
# ROUTES - CAMPAGNES
# =============================================================================

@router.get("/campaigns")
async def export_campaigns(
    format: str = Query("xlsx", enum=["xlsx", "csv"]),
    db: AsyncSession = Depends(get_db)
):
    """Exporte les campagnes"""
    
    result = await db.execute(select(Campaign))
    campaigns = result.scalars().all()
    
    data = []
    for c in campaigns:
        data.append({
            "Nom": c.name,
            "Type": c.type,
            "Portail": c.target_portal,
            "Ville": c.target_city,
            "Rayon (km)": c.target_radius,
            "Statut": c.status,
            "Cibles": c.total_targets,
            "Envoyés": c.sent_count,
            "Réponses": c.response_count,
            "Taux Réponse": f"{round(c.response_count / max(c.sent_count, 1) * 100, 1)}%",
            "Date Création": c.created_at.strftime("%d/%m/%Y %H:%M") if c.created_at else ""
        })
    
    df = pd.DataFrame(data)
    
    if format == "csv":
        output = io.StringIO()
        df.to_csv(output, index=False, encoding='utf-8-sig')
        output.seek(0)
        
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=campaigns_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
            }
        )
    else:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Campagnes', index=False)
            style_excel(writer, df, 'Campagnes')
        output.seek(0)
        
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename=campaigns_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
            }
        )

# =============================================================================
# ROUTES - SCRAPING RESULTS
# =============================================================================

class ScrapingExportRequest(BaseModel):
    results: List[dict]
    filename: str = "scraping_results"

@router.post("/scraping-results")
async def export_scraping_results(
    request: ScrapingExportRequest,
    format: str = Query("xlsx", enum=["xlsx", "csv"])
):
    """Exporte les résultats de scraping"""
    
    df = pd.DataFrame(request.results)
    
    # Renommer colonnes pour export
    column_mapping = {
        "id": "ID",
        "nom": "Nom",
        "prenom": "Prénom",
        "adresse": "Adresse",
        "code_postal": "Code Postal",
        "ville": "Ville",
        "telephone": "Téléphone",
        "email": "Email",
        "parcelle": "N° Parcelle",
        "surface": "Surface (m²)",
        "zone": "Zone",
        "lien_rf": "Lien Registre Foncier",
        "source": "Source"
    }
    df = df.rename(columns=column_mapping)
    
    if format == "csv":
        output = io.StringIO()
        df.to_csv(output, index=False, encoding='utf-8-sig')
        output.seek(0)
        
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={request.filename}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
            }
        )
    else:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Résultats', index=False)
            style_excel(writer, df, 'Résultats')
        output.seek(0)
        
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename={request.filename}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
            }
        )

# =============================================================================
# ROUTES - EXPORT COMPLET
# =============================================================================

@router.get("/all")
async def export_all(
    format: str = Query("xlsx", enum=["xlsx"]),
    db: AsyncSession = Depends(get_db)
):
    """Exporte toutes les données dans un seul fichier Excel (multi-onglets)"""
    
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        # Prospects
        result = await db.execute(select(Prospect))
        prospects = result.scalars().all()
        df_prospects = pd.DataFrame([{
            "Nom": p.nom, "Prénom": p.prenom or "", "Téléphone": p.telephone or "",
            "Email": p.email or "", "Adresse": p.adresse or "", "Ville": p.ville or "",
            "Score": p.score, "Statut": p.statut, "Source": p.source or ""
        } for p in prospects])
        if not df_prospects.empty:
            df_prospects.to_excel(writer, sheet_name='Prospects', index=False)
            style_excel(writer, df_prospects, 'Prospects')
        
        # Emails
        result = await db.execute(select(EmailAccount))
        emails = result.scalars().all()
        df_emails = pd.DataFrame([{
            "Email": e.email, "Quota": e.quota_daily, "Envoyés": e.sent_today,
            "Actif": "Oui" if e.is_active else "Non"
        } for e in emails])
        if not df_emails.empty:
            df_emails.to_excel(writer, sheet_name='Emails', index=False)
            style_excel(writer, df_emails, 'Emails')
        
        # Bots
        result = await db.execute(select(Bot))
        bots = result.scalars().all()
        df_bots = pd.DataFrame([{
            "Nom": b.name, "Type": b.type, "Statut": b.status,
            "Succès": b.success_count, "Erreurs": b.error_count
        } for b in bots])
        if not df_bots.empty:
            df_bots.to_excel(writer, sheet_name='Bots', index=False)
            style_excel(writer, df_bots, 'Bots')
        
        # Proxies
        result = await db.execute(select(Proxy))
        proxies = result.scalars().all()
        df_proxies = pd.DataFrame([{
            "Host": p.host, "Port": p.port, "Pays": p.country,
            "Valide": "Oui" if p.is_valid else "Non"
        } for p in proxies])
        if not df_proxies.empty:
            df_proxies.to_excel(writer, sheet_name='Proxies', index=False)
            style_excel(writer, df_proxies, 'Proxies')
    
    output.seek(0)
    
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename=prospection_pro_export_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        }
    )






