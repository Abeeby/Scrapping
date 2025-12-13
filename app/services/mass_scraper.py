# =============================================================================
# SERVICE MASS SCRAPER - Scraping massif de rues
# =============================================================================
# Permet de scraper toutes les rues d'un canton/commune pour constituer
# une base de prospects privés complète.
# =============================================================================

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import (
    AsyncSessionLocal,
    Prospect,
    MassScrapingJob,
)
from app.core.logger import logger
from app.core.websocket import emit_activity
from app.data.streets_ge_vd import get_streets, get_communes, get_stats


# =============================================================================
# CLASSES
# =============================================================================

class MassScraperError(Exception):
    """Erreur du service de scraping massif."""
    pass


class MassScraperService:
    """
    Service de scraping massif pour les rues GE/VD.
    
    Usage:
        service = MassScraperService()
        
        # Créer un job
        job_id = await service.create_job(canton="GE", commune="Genève")
        
        # Exécuter le job
        await service.run_job(job_id, progress_callback=my_callback)
    """

    def __init__(self):
        self._stop_flag = False
        self._pause_flag = False

    def stop(self):
        """Arrête le scraping en cours."""
        self._stop_flag = True

    def pause(self):
        """Met en pause le scraping."""
        self._pause_flag = True

    def resume(self):
        """Reprend le scraping."""
        self._pause_flag = False

    async def create_job(
        self,
        canton: str,
        commune: Optional[str] = None,
        source: str = "searchch",
        name: Optional[str] = None,
    ) -> int:
        """
        Crée un nouveau job de scraping massif.
        
        Args:
            canton: Code canton (GE, VD)
            commune: Commune spécifique ou None pour toutes
            source: Source de scraping (searchch, localch, rf)
            name: Nom personnalisé du job
            
        Returns:
            ID du job créé
        """
        streets = get_streets(canton, commune)
        
        if not streets:
            raise MassScraperError(f"Aucune rue trouvée pour {canton}/{commune}")

        async with AsyncSessionLocal() as db:
            job = MassScrapingJob(
                name=name or f"Scraping {canton}" + (f" - {commune}" if commune else ""),
                canton=canton.upper(),
                commune=commune,
                source=source,
                status="pending",
                total_streets=len(streets),
            )
            db.add(job)
            await db.commit()
            await db.refresh(job)
            
            await emit_activity("mass_scraper", f"Job créé: {job.name} ({len(streets)} rues)")
            
            return job.id

    async def run_job(
        self,
        job_id: int,
        delay_seconds: int = 2,
        save_to_prospects: bool = True,
        progress_callback: Optional[Callable[[Dict], None]] = None,
    ) -> Dict[str, Any]:
        """
        Exécute un job de scraping massif.
        
        Args:
            job_id: ID du job à exécuter
            delay_seconds: Délai entre chaque rue (rate limiting)
            save_to_prospects: Sauvegarder les résultats en prospects
            progress_callback: Callback appelé à chaque progression
            
        Returns:
            Statistiques d'exécution
        """
        self._stop_flag = False
        self._pause_flag = False
        
        stats = {
            "processed": 0,
            "found": 0,
            "saved": 0,
            "duplicates": 0,
            "errors": 0,
        }

        async with AsyncSessionLocal() as db:
            # Récupérer le job
            result = await db.execute(
                select(MassScrapingJob).where(MassScrapingJob.id == job_id)
            )
            job = result.scalar_one_or_none()
            
            if not job:
                raise MassScraperError(f"Job {job_id} non trouvé")

            if job.status == "completed":
                raise MassScraperError(f"Job {job_id} déjà terminé")

            # Mettre à jour le statut
            job.status = "running"
            job.started_at = datetime.utcnow()
            await db.commit()

            await emit_activity("mass_scraper", f"Démarrage: {job.name}")

            # Récupérer les rues
            streets = get_streets(job.canton, job.commune)
            
            # Importer le scraper
            try:
                from app.scrapers.searchch import SearchChScraper
            except ImportError:
                job.status = "error"
                job.error_message = "SearchChScraper non disponible"
                await db.commit()
                raise MassScraperError("SearchChScraper non disponible")

            try:
                async with SearchChScraper() as scraper:
                    for i, street in enumerate(streets):
                        # Vérifier l'arrêt
                        if self._stop_flag:
                            job.status = "paused"
                            await db.commit()
                            await emit_activity("mass_scraper", f"Job arrêté: {job.name}")
                            break

                        # Gérer la pause
                        while self._pause_flag:
                            await asyncio.sleep(1)

                        try:
                            job.current_street = street
                            
                            # Déterminer la ville
                            ville = job.commune or ("Genève" if job.canton == "GE" else "Lausanne")
                            
                            # Scraper la rue
                            logger.info(f"[MassScraper] Scraping: {street}, {ville}")
                            results = await scraper.search(
                                query=street,
                                ville=ville,
                                limit=100,
                                type_recherche="person"
                            )
                            
                            stats["found"] += len(results)
                            
                            # Sauvegarder si demandé
                            if save_to_prospects and results:
                                saved = await self._save_results(db, results, street, ville, job.canton)
                                stats["saved"] += saved["added"]
                                stats["duplicates"] += saved["duplicates"]
                            
                            stats["processed"] += 1
                            job.processed_streets = stats["processed"]
                            job.total_found = stats["found"]
                            await db.commit()

                            # Callback de progression
                            if progress_callback:
                                progress_callback({
                                    "job_id": job_id,
                                    "street": street,
                                    "processed": stats["processed"],
                                    "total": len(streets),
                                    "found": stats["found"],
                                    "saved": stats["saved"],
                                })

                        except Exception as e:
                            logger.warning(f"[MassScraper] Erreur sur {street}: {e}")
                            stats["errors"] += 1

                        # Délai anti rate-limit
                        if i < len(streets) - 1:
                            await asyncio.sleep(delay_seconds)

                # Marquer comme terminé
                if not self._stop_flag:
                    job.status = "completed"
                    job.completed_at = datetime.utcnow()
                    await db.commit()
                    
                    await emit_activity("success", f"Scraping terminé: {stats['saved']} prospects sur {stats['found']} trouvés")

            except Exception as e:
                job.status = "error"
                job.error_message = str(e)
                await db.commit()
                raise

        return stats

    async def _save_results(
        self,
        db: AsyncSession,
        results: List[Dict],
        street: str,
        ville: str,
        canton: str,
    ) -> Dict[str, int]:
        """Sauvegarde les résultats en prospects avec déduplication."""
        stats = {"added": 0, "duplicates": 0}
        
        for r in results:
            nom = r.get("nom", "").strip()
            adresse = r.get("adresse", street).strip()
            
            if not nom:
                continue

            # Vérifier doublon
            existing = await db.execute(
                select(Prospect).where(
                    and_(
                        Prospect.nom == nom,
                        Prospect.ville == ville
                    )
                ).limit(1)
            )
            
            if existing.scalar_one_or_none():
                stats["duplicates"] += 1
                continue

            # Créer le prospect
            prospect = Prospect(
                id=f"mass-{uuid.uuid4().hex[:10]}",
                nom=nom,
                prenom=r.get("prenom", ""),
                telephone=r.get("telephone", ""),
                email=r.get("email", ""),
                adresse=adresse,
                code_postal=r.get("code_postal", ""),
                ville=r.get("ville", ville),
                canton=canton,
                source="MassScraper",
                lien_rf=r.get("lien_rf", ""),
            )
            db.add(prospect)
            stats["added"] += 1

        await db.commit()
        return stats

    async def get_job_status(self, job_id: int) -> Optional[Dict[str, Any]]:
        """Récupère le statut d'un job."""
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(MassScrapingJob).where(MassScrapingJob.id == job_id)
            )
            job = result.scalar_one_or_none()
            
            if not job:
                return None

            return {
                "id": job.id,
                "name": job.name,
                "canton": job.canton,
                "commune": job.commune,
                "source": job.source,
                "status": job.status,
                "total_streets": job.total_streets,
                "processed_streets": job.processed_streets,
                "total_found": job.total_found,
                "current_street": job.current_street,
                "error_message": job.error_message,
                "started_at": job.started_at.isoformat() if job.started_at else None,
                "completed_at": job.completed_at.isoformat() if job.completed_at else None,
                "progress_percent": round((job.processed_streets / job.total_streets) * 100, 1) if job.total_streets > 0 else 0,
            }

    async def list_jobs(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Liste les jobs récents."""
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(MassScrapingJob)
                .order_by(MassScrapingJob.created_at.desc())
                .limit(limit)
            )
            jobs = result.scalars().all()
            
            return [
                {
                    "id": j.id,
                    "name": j.name,
                    "canton": j.canton,
                    "commune": j.commune,
                    "status": j.status,
                    "total_streets": j.total_streets,
                    "processed_streets": j.processed_streets,
                    "total_found": j.total_found,
                    "created_at": j.created_at.isoformat() if j.created_at else None,
                }
                for j in jobs
            ]


# =============================================================================
# API ENDPOINTS SUPPORT
# =============================================================================

async def get_scraping_coverage(canton: str) -> Dict[str, Any]:
    """
    Calcule la couverture de scraping pour un canton.
    
    Returns:
        Dict avec stats de couverture
    """
    from app.core.database import Prospect
    
    async with AsyncSessionLocal() as db:
        # Compter les prospects par source MassScraper
        result = await db.execute(
            select(Prospect.ville, Prospect.id)
            .where(Prospect.canton == canton.upper())
            .where(Prospect.source == "MassScraper")
        )
        prospects = result.fetchall()
        
        # Grouper par ville
        by_city = {}
        for ville, pid in prospects:
            by_city[ville] = by_city.get(ville, 0) + 1
        
        # Stats globales
        total_streets = sum(len(s) for s in (
            get_streets(canton, c) for c in get_communes(canton)
        ))
        
        return {
            "canton": canton.upper(),
            "total_prospects": len(prospects),
            "by_city": by_city,
            "communes_with_data": len(by_city),
            "total_communes": len(get_communes(canton)),
            "estimated_streets": total_streets,
        }


async def quick_scrape_street(
    street: str,
    ville: str,
    canton: str = "GE",
    save: bool = True,
) -> Dict[str, Any]:
    """
    Scrape rapidement une seule rue.
    
    Args:
        street: Nom de la rue
        ville: Nom de la ville
        canton: Code canton
        save: Sauvegarder en prospects
        
    Returns:
        Résultats du scraping
    """
    from app.scrapers.searchch import SearchChScraper
    
    async with SearchChScraper() as scraper:
        results = await scraper.search(
            query=street,
            ville=ville,
            limit=100,
            type_recherche="person"
        )
    
    if save and results:
        service = MassScraperService()
        async with AsyncSessionLocal() as db:
            saved = await service._save_results(db, results, street, ville, canton)
            return {
                "found": len(results),
                "saved": saved["added"],
                "duplicates": saved["duplicates"],
                "results": results[:10],  # Limiter pour l'API
            }
    
    return {
        "found": len(results),
        "saved": 0,
        "duplicates": 0,
        "results": results[:10],
    }

